"""流转链重建的持久化：schema 初始化、互斥锁、有限内存读取与幂等发布。

设计约束（对应工程要求）：

- 不修改 ingestion 的 ``records`` 表，只通过 ``json_extract`` 读取快照，
  ``batch_key`` 语义保持不变。
- 新表全部使用 ``CREATE TABLE IF NOT EXISTS``，对既有数据库零破坏。
- 重建互斥：进程内 ``threading.RLock`` + 数据库行级咨询锁
  （``flow_rebuild_locks``，``BEGIN IMMEDIATE``），同时覆盖线程与多进程。
- 有限内存：按 ``batch_id`` 服务端游标流式扫描，单次重建只保留一个批次的
  事实在内存中；发布按批次独立事务提交。
- 中断重试与幂等：每次重建有 ``flow_runs`` 台账；批次结论以内容哈希做
  ``INSERT ... ON CONFLICT DO UPDATE``，同输入重放结果字节级一致，重复发布
  为 no-op；崩溃后可直接重新发起重建。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .reconstruction import BatchChain, FlowFact, parse_event_time

_SCHEMA = """
CREATE TABLE IF NOT EXISTS flow_runs (
    run_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    target_batch_id TEXT,
    status TEXT NOT NULL,
    lock_owner TEXT,
    fact_count INTEGER NOT NULL DEFAULT 0,
    batch_count INTEGER NOT NULL DEFAULT 0,
    published_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    started_at TEXT NOT NULL,
    heartbeat_at TEXT,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS flow_rebuild_locks (
    lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1),
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS flow_fact_index (
    record_key TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    facility_id TEXT NOT NULL,
    batch_scope TEXT NOT NULL,
    status TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    observed_order REAL,
    collected_at TEXT NOT NULL,
    collected_parsed INTEGER NOT NULL,
    source TEXT NOT NULL,
    destination_facility_id TEXT NOT NULL,
    input_fingerprint TEXT,
    indexed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_flow_fact_batch ON flow_fact_index(batch_id, observed_order, record_key);
CREATE TABLE IF NOT EXISTS batch_flow_chains (
    batch_id TEXT PRIMARY KEY,
    chain_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    stages_json TEXT NOT NULL,
    terminal_status TEXT,
    is_complete INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    published_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

# 与存储层保持一致的锁重试参数，便于测试快速触发与恢复。
DEFAULT_BUSY_TIMEOUT_MS = 5_000
DEFAULT_LOCK_STALE_SECONDS = 30 * 60


class FlowRebuildLocked(RuntimeError):
    """另一个进程/线程正在执行重建。"""


class FlowStoreError(RuntimeError):
    """重建过程中的可重试/可观测存储错误。"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.isoformat()


class BatchFlowStore:
    """管理流转链重建所需的全部表与连接。"""

    def __init__(
        self,
        database: str,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
        lock_stale_seconds: int = DEFAULT_LOCK_STALE_SECONDS,
        thread_lock_timeout: float = 5.0,
    ) -> None:
        # check_same_thread=False：由进程内 RLock 保证线程互斥，连接不跨并发段使用。
        self.conn = sqlite3.connect(database, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.busy_timeout_ms = busy_timeout_ms
        self.lock_stale_seconds = lock_stale_seconds
        self.thread_lock_timeout = thread_lock_timeout
        self._thread_lock = threading.RLock()
        self.initialize_schema()

    # ------------------------------------------------------------------ #
    # schema
    # ------------------------------------------------------------------ #
    def initialize_schema(self) -> None:
        self.conn.executescript(_SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        with self._thread_lock:
            self.conn.close()

    # ------------------------------------------------------------------ #
    # 锁
    # ------------------------------------------------------------------ #
    @contextmanager
    def rebuild_lock(self, owner: str) -> Iterator[None]:
        """获取进程间重建锁；持锁期间其他重建立即失败而非排队写坏数据。"""
        # 进程内互斥：限时获取，失败按锁冲突处理，避免线程间无限阻塞。
        if not self._thread_lock.acquire(timeout=self.thread_lock_timeout):
            raise FlowRebuildLocked("flow rebuild already in progress within this process")
        acquired = False
        try:
            run_id = owner
            now = _now()
            expires = now + timedelta(seconds=self.lock_stale_seconds)
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                row = self.conn.execute(
                    "SELECT owner, expires_at FROM flow_rebuild_locks WHERE lock_id = 1"
                ).fetchone()
                if row is not None:
                    stale = datetime.fromisoformat(row["expires_at"]) <= now
                    if not stale:
                        self.conn.rollback()
                        raise FlowRebuildLocked(
                            f"flow rebuild already in progress, owner={row['owner']}"
                        )
                    # 回收崩溃进程遗留的过期锁。
                    self.conn.execute("DELETE FROM flow_rebuild_locks WHERE lock_id = 1")
                self.conn.execute(
                    "INSERT INTO flow_rebuild_locks(lock_id, owner, acquired_at, expires_at) "
                    "VALUES (1, ?, ?, ?)",
                    (run_id, _iso(now), _iso(expires)),
                )
                self.conn.commit()
                acquired = True
            except sqlite3.OperationalError as exc:
                self._rollback_safely()
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    raise FlowRebuildLocked(f"database lock conflict: {exc}") from exc
                raise
            try:
                yield
            finally:
                if acquired:
                    # 持锁者退出（正常或异常）时无条件释放自己的咨询锁。
                    with self._retry_on_locked():
                        self.conn.execute(
                            "DELETE FROM flow_rebuild_locks WHERE lock_id = 1 AND owner = ?",
                            (run_id,),
                        )
                        self.conn.commit()
        finally:
            self._thread_lock.release()

    @contextmanager
    def _retry_on_locked(self) -> Iterator[None]:
        """SQLite 写锁冲突由 busy_timeout 兜底；仍失败时回滚并包装成领域异常。"""
        try:
            yield
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                # 清理失败语句遗留的隐式事务，保证同一连接可以干净重试。
                self._rollback_safely()
                raise FlowStoreError(f"sqlite lock conflict after busy_timeout: {exc}") from exc
            raise

    def _rollback_safely(self) -> None:
        try:
            self.conn.rollback()
        except sqlite3.Error:
            pass

    # ------------------------------------------------------------------ #
    # run 台账（中断后可重试、可观测）
    # ------------------------------------------------------------------ #
    def start_run(self, mode: str, target_batch_id: str | None, run_id: str | None = None) -> str:
        run_id = run_id or f"run-{uuid.uuid4().hex}"
        with self._retry_on_locked():
            self.conn.execute(
                "INSERT INTO flow_runs(run_id, mode, target_batch_id, status, started_at, heartbeat_at) "
                "VALUES (?, ?, ?, 'running', ?, ?)",
                (run_id, mode, target_batch_id, _iso(_now()), _iso(_now())),
            )
            self.conn.commit()
        return run_id

    def heartbeat(self, run_id: str, **counters: int) -> None:
        allowed = ("fact_count", "batch_count", "published_count", "skipped_count")
        updates = {key: value for key, value in counters.items() if key in allowed}
        clauses = [f"{key} = ?" for key in updates]
        clauses.append("heartbeat_at = ?")
        values: list[Any] = list(updates.values()) + [_iso(_now()), run_id]
        with self._retry_on_locked():
            self.conn.execute(
                f"UPDATE flow_runs SET {', '.join(clauses)} WHERE run_id = ?",
                values,
            )
            self.conn.commit()

    def finish_run(
        self,
        run_id: str,
        status: str,
        *,
        fact_count: int = 0,
        batch_count: int = 0,
        published_count: int = 0,
        skipped_count: int = 0,
        error: str | None = None,
    ) -> None:
        with self._retry_on_locked():
            self.conn.execute(
                "UPDATE flow_runs SET status=?, fact_count=?, batch_count=?, published_count=?, "
                "skipped_count=?, error=?, heartbeat_at=?, finished_at=? WHERE run_id=?",
                (
                    status,
                    fact_count,
                    batch_count,
                    published_count,
                    skipped_count,
                    error,
                    _iso(_now()),
                    _iso(_now()),
                    run_id,
                ),
            )
            self.conn.commit()

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM flow_runs WHERE run_id = ?", (run_id,)).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------ #
    # 从 records 快照构建/刷新事实索引（不改动 records 表本身）
    # ------------------------------------------------------------------ #
    def refresh_fact_index(self, progress: Callable[[int], None] | None = None) -> int:
        """全量流式扫描 records，upsert 到独立索引表。返回索引事实数。

        采用服务端游标与分批提交，内存占用不随库规模增长；重复执行幂等。
        若库中尚不存在 ingestion 的 ``records`` 表（从未运行过 ingestion），
        视为零事实安全返回，不代 ingestion 创建其表结构。
        """
        exists = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='records'"
        ).fetchone()
        if exists is None:
            return 0
        total = 0
        cursor = self.conn.execute(
            """
            SELECT record_key, source, observed_at, payload_json
              FROM records
             ORDER BY record_key
            """
        )
        pending: list[FlowFact] = []
        flush_size = 500
        while True:
            rows = cursor.fetchmany(flush_size)
            if not rows:
                break
            for row in rows:
                fact = _fact_from_record_row(row)
                pending.append(fact)
            self._flush_fact_index(pending)
            total += len(pending)
            pending.clear()
            if progress is not None:
                progress(total)
        return total

    def _flush_fact_index(self, facts: list[FlowFact]) -> None:
        now = _iso(_now())
        with self._retry_on_locked():
            self.conn.executemany(
                """
                INSERT INTO flow_fact_index(
                    record_key, batch_id, facility_id, batch_scope, status, observed_at,
                    observed_order, collected_at, collected_parsed, source,
                    destination_facility_id, indexed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    batch_id=excluded.batch_id,
                    facility_id=excluded.facility_id,
                    batch_scope=excluded.batch_scope,
                    status=excluded.status,
                    observed_at=excluded.observed_at,
                    observed_order=excluded.observed_order,
                    collected_at=excluded.collected_at,
                    collected_parsed=excluded.collected_parsed,
                    source=excluded.source,
                    destination_facility_id=excluded.destination_facility_id,
                    indexed_at=excluded.indexed_at
                """,
                [
                    (
                        fact.record_key,
                        fact.batch_id,
                        fact.facility_id,
                        fact.scope_key,
                        fact.status,
                        fact.observed_at,
                        fact.observed_order,
                        fact.collected_at,
                        int(fact.collected_parsed),
                        fact.source,
                        fact.destination_facility_id,
                        now,
                    )
                    for fact in facts
                ],
            )
            self.conn.commit()

    def stream_batch_ids(self, target_batch_id: str | None = None) -> Iterator[str]:
        """流式产出批次 id，避免一次性加载全库批次清单。"""
        if target_batch_id is not None:
            row = self.conn.execute(
                "SELECT 1 FROM flow_fact_index WHERE batch_id = ? LIMIT 1",
                (target_batch_id,),
            ).fetchone()
            if row is not None:
                yield target_batch_id
            return
        cursor = self.conn.execute(
            "SELECT DISTINCT batch_id FROM flow_fact_index ORDER BY batch_id"
        )
        for row in cursor:
            yield row["batch_id"]

    def load_facts(self, batch_id: str) -> list[FlowFact]:
        rows = self.conn.execute(
            """
            SELECT record_key, source, batch_id, facility_id, destination_facility_id,
                   status, observed_at, observed_order, collected_at, collected_parsed,
                   batch_scope
              FROM flow_fact_index
             WHERE batch_id = ?
             ORDER BY observed_order, record_key
            """,
            (batch_id,),
        ).fetchall()
        return [_fact_from_index_row(row) for row in rows]

    def save_batch_fingerprint(self, batch_id: str, fingerprint: str) -> None:
        with self._retry_on_locked():
            self.conn.execute(
                "UPDATE flow_fact_index SET input_fingerprint = ? WHERE batch_id = ?",
                (fingerprint, batch_id),
            )
            self.conn.commit()

    # ------------------------------------------------------------------ #
    # 幂等发布
    # ------------------------------------------------------------------ #
    def publish_chain(
        self,
        chain: BatchChain,
        input_fingerprint: str,
        run_id: str,
    ) -> bool:
        """按批次事务发布；内容哈希未变时为 no-op。返回是否实际写入。"""
        existing = self.conn.execute(
            "SELECT content_hash FROM batch_flow_chains WHERE batch_id = ?",
            (chain.batch_id,),
        ).fetchone()
        if existing is not None and existing["content_hash"] == chain.content_hash:
            return False
        now = _iso(_now())
        stages_json = json.dumps(
            [stage.to_dict() for stage in chain.stages], ensure_ascii=False, sort_keys=True
        )
        with self._retry_on_locked():
            self.conn.execute(
                """
                INSERT INTO batch_flow_chains(
                    batch_id, chain_json, content_hash, input_fingerprint, stages_json,
                    terminal_status, is_complete, run_id, published_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(batch_id) DO UPDATE SET
                    chain_json=excluded.chain_json,
                    content_hash=excluded.content_hash,
                    input_fingerprint=excluded.input_fingerprint,
                    stages_json=excluded.stages_json,
                    terminal_status=excluded.terminal_status,
                    is_complete=excluded.is_complete,
                    run_id=excluded.run_id,
                    updated_at=excluded.updated_at
                """,
                (
                    chain.batch_id,
                    chain.to_json(),
                    chain.content_hash,
                    input_fingerprint,
                    stages_json,
                    chain.terminal_status,
                    int(chain.is_complete),
                    run_id,
                    now,
                    now,
                ),
            )
            self.conn.commit()
        return True

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_chain(self, batch_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT chain_json FROM batch_flow_chains WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        return json.loads(row["chain_json"]) if row else None

    def list_chains(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT batch_id, chain_json FROM batch_flow_chains ORDER BY batch_id"
        ).fetchall()
        return [json.loads(row["chain_json"]) for row in rows]

    def list_batches(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT batch_id,
                   COUNT(*) AS fact_count,
                   COUNT(DISTINCT batch_scope) AS scope_count,
                   COUNT(DISTINCT facility_id) AS facility_count
              FROM flow_fact_index
             GROUP BY batch_id
             ORDER BY batch_id
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def chain_count(self) -> int:
        return int(
            self.conn.execute("SELECT COUNT(*) FROM batch_flow_chains").fetchone()[0]
        )


# --------------------------------------------------------------------------- #
# 行 -> FlowFact
# --------------------------------------------------------------------------- #
def _fact_from_record_row(row: sqlite3.Row) -> FlowFact:
    payload = json.loads(row["payload_json"])
    observed_at = str(row["observed_at"])
    collected_at = str(payload.get("collected_at", ""))
    return FlowFact(
        record_key=row["record_key"],
        source=row["source"],
        batch_id=str(payload["batch_id"]),
        facility_id=str(payload["facility_id"]),
        destination_facility_id=str(payload["destination_facility_id"]),
        status=str(payload["status"]),
        observed_at=observed_at,
        observed_order=parse_event_time(observed_at),
        collected_at=collected_at,
        collected_parsed=parse_event_time(collected_at) is not None,
    )


def _fact_from_index_row(row: sqlite3.Row) -> FlowFact:
    return FlowFact(
        record_key=row["record_key"],
        source=row["source"],
        batch_id=row["batch_id"],
        facility_id=row["facility_id"],
        destination_facility_id=row["destination_facility_id"],
        status=row["status"],
        observed_at=row["observed_at"],
        observed_order=row["observed_order"],
        collected_at=row["collected_at"],
        collected_parsed=bool(row["collected_parsed"]),
    )


def connect(database: str, **kwargs: Any) -> BatchFlowStore:
    if database != ":memory:":
        Path(database).parent.mkdir(parents=True, exist_ok=True)
    return BatchFlowStore(database, **kwargs)
