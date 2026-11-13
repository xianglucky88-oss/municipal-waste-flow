"""批次重量平衡核算的持久化层。

- 只新增 ``balance_*`` 表，不改动 ingestion 既有表结构，旧数据库和旧 CLI
  不受影响；迁移在单事务内完成，失败即回滚，下次打开可安全重试。
- 每次核算是一个新的 ``run_id``（结果版本），结果按 ``run_id`` 隔离，两个
  并发调用的结果版本不会互相覆盖。
- ``balance_result_cache`` 以 ``(rule_version, params_fingerprint, batch_id,
  input_hash)`` 为键，新观测到达后只重算输入发生变化的批次（增量重算）；
  规则版本或结果相关参数变化会自然使旧缓存失效。
- 运行通过心跳对外暴露活性；中断遗留的 ``running`` 运行可由
  ``recover_interrupted`` 标记为 ``interrupted`` 后恢复。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable

from .balance import BalanceMigrationError, BalanceStorageError

SCHEMA_VERSION = 1

_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS balance_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS balance_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_version TEXT NOT NULL,
        params_json TEXT NOT NULL,
        params_fingerprint TEXT NOT NULL,
        status TEXT NOT NULL,
        records_scanned INTEGER NOT NULL DEFAULT 0,
        batches_evaluated INTEGER NOT NULL DEFAULT 0,
        batches_computed INTEGER NOT NULL DEFAULT 0,
        batches_reused INTEGER NOT NULL DEFAULT 0,
        input_fingerprint TEXT,
        error TEXT,
        created_at TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        finished_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS balance_batch_results (
        run_id INTEGER NOT NULL,
        batch_id TEXT NOT NULL,
        input_hash TEXT NOT NULL,
        result_json TEXT NOT NULL,
        PRIMARY KEY (run_id, batch_id),
        FOREIGN KEY (run_id) REFERENCES balance_runs(run_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS balance_result_cache (
        rule_version TEXT NOT NULL,
        params_fingerprint TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        input_hash TEXT NOT NULL,
        result_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (rule_version, params_fingerprint, batch_id, input_hash)
    )
    """,
)

_TERMINAL_STATUSES = ("completed", "failed", "cancelled", "interrupted")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fingerprint_pairs(pairs: Iterable[tuple[str, str]]) -> str:
    """对 (record_key, raw_hash) 序列做稳定指纹，刻画一次核算的输入快照。"""
    digest = sha256()
    for key, raw in sorted(pairs):
        digest.update(f"{key}|{raw}\n".encode("utf-8"))
    return digest.hexdigest()


def fingerprint_params(params: dict[str, Any]) -> str:
    return sha256(json.dumps(params, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


class BalanceStore:
    """核算运行、批次结果与增量缓存的读写；每个实例持有独立连接。"""

    def __init__(self, database: str, busy_timeout_ms: int = 5000) -> None:
        self.database = database
        try:
            self.conn = sqlite3.connect(database, isolation_level=None)
        except sqlite3.Error as exc:
            raise BalanceStorageError(f"cannot open database {database!r}: {exc}") from exc
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        try:
            self.conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.Error:
            pass  # WAL 只是优化；回退到 rollback journal 仍然正确

    def close(self) -> None:
        self.conn.close()

    def migrate(self) -> None:
        """在单事务内建表并记录结构版本；失败回滚，可安全重试。"""
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                table = self.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='balance_meta'"
                ).fetchone()
                if table:
                    row = self.conn.execute(
                        "SELECT value FROM balance_meta WHERE key = 'schema_version'"
                    ).fetchone()
                    if row and int(row[0]) > SCHEMA_VERSION:
                        raise BalanceMigrationError(
                            f"balance schema version {row[0]} is newer than supported version {SCHEMA_VERSION}"
                        )
                for statement in _SCHEMA_STATEMENTS:
                    self.conn.execute(statement)
                self.conn.execute(
                    "INSERT OR REPLACE INTO balance_meta(key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
                self.conn.execute("COMMIT")
            except Exception:
                try:
                    self.conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        except BalanceMigrationError:
            raise
        except sqlite3.Error as exc:
            raise BalanceMigrationError(f"balance schema migration failed: {exc}") from exc

    def has_records_table(self) -> bool:
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='records'"
        ).fetchone()
        return row is not None

    def read_records_chunk(self, after_rowid: int, limit: int) -> list[tuple[Any, ...]]:
        """按 rowid 键集分页读取一段记录，支持分段读取与断点取消。"""
        try:
            return self.conn.execute(
                "SELECT rowid, record_key, source, observed_at, payload_json, raw_hash"
                " FROM records WHERE rowid > ? ORDER BY rowid LIMIT ?",
                (after_rowid, limit),
            ).fetchall()
        except sqlite3.Error as exc:
            raise BalanceStorageError(f"cannot read records: {exc}") from exc

    def start_run(self, *, rule_version: str, params_json: str, params_fingerprint: str) -> int:
        now = _now()
        try:
            cursor = self.conn.execute(
                "INSERT INTO balance_runs(rule_version, params_json, params_fingerprint, status, created_at, heartbeat_at)"
                " VALUES (?, ?, ?, 'running', ?, ?)",
                (rule_version, params_json, params_fingerprint, now, now),
            )
            return int(cursor.lastrowid)
        except sqlite3.Error as exc:
            raise BalanceStorageError(f"cannot start balance run: {exc}") from exc

    def heartbeat(self, run_id: int, records_scanned: int) -> None:
        try:
            self.conn.execute(
                "UPDATE balance_runs SET heartbeat_at = ?, records_scanned = ? WHERE run_id = ? AND status = 'running'",
                (_now(), records_scanned, run_id),
            )
        except sqlite3.Error as exc:
            raise BalanceStorageError(f"cannot update heartbeat for run {run_id}: {exc}") from exc

    def cached_result(self, rule_version: str, params_fingerprint: str, batch_id: str, input_hash: str) -> str | None:
        row = self.conn.execute(
            "SELECT result_json FROM balance_result_cache"
            " WHERE rule_version = ? AND params_fingerprint = ? AND batch_id = ? AND input_hash = ?",
            (rule_version, params_fingerprint, batch_id, input_hash),
        ).fetchone()
        return str(row[0]) if row else None

    def complete_run(
        self,
        run_id: int,
        *,
        results: list[tuple[str, str, str]],
        cache_entries: list[tuple[str, str, str, str, str]],
        records_scanned: int,
        batches_computed: int,
        batches_reused: int,
        input_fingerprint: str,
    ) -> None:
        """在单个事务内写入全部批次结果、缓存并收尾运行；锁冲突整体回滚。"""
        now = _now()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.executemany(
                    "INSERT INTO balance_batch_results(run_id, batch_id, input_hash, result_json) VALUES (?, ?, ?, ?)",
                    [(run_id, batch_id, input_hash, result_json) for batch_id, input_hash, result_json in results],
                )
                self.conn.executemany(
                    "INSERT OR IGNORE INTO balance_result_cache"
                    "(rule_version, params_fingerprint, batch_id, input_hash, result_json, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    [(*entry, now) for entry in cache_entries],
                )
                self.conn.execute(
                    "UPDATE balance_runs SET status = 'completed', records_scanned = ?, batches_evaluated = ?,"
                    " batches_computed = ?, batches_reused = ?, input_fingerprint = ?, heartbeat_at = ?, finished_at = ?"
                    " WHERE run_id = ?",
                    (records_scanned, len(results), batches_computed, batches_reused, input_fingerprint, now, now, run_id),
                )
                self.conn.execute("COMMIT")
            except Exception:
                try:
                    self.conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        except sqlite3.Error as exc:
            raise BalanceStorageError(f"cannot persist balance run {run_id}: {exc}") from exc

    def mark_run_safe(self, run_id: int, status: str, error: str | None = None) -> None:
        """尽力把运行标记为终态；存储不可用时静默放弃（运行可事后恢复）。"""
        now = _now()
        try:
            self.conn.execute(
                "UPDATE balance_runs SET status = ?, error = ?, heartbeat_at = ?, finished_at = ?"
                " WHERE run_id = ? AND status = 'running'",
                (status, error, now, now, run_id),
            )
        except sqlite3.Error:
            pass

    def recover_interrupted(self, stale_after_seconds: float = 300.0) -> int:
        """把心跳过期的 running 运行标记为 interrupted，返回恢复的运行数。"""
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)).isoformat()
        try:
            cursor = self.conn.execute(
                "UPDATE balance_runs SET status = 'interrupted', error = ?, finished_at = ?"
                " WHERE status = 'running' AND heartbeat_at < ?",
                ("run interrupted; marked by recovery after stale heartbeat", _now(), cutoff),
            )
            return int(cursor.rowcount)
        except sqlite3.Error as exc:
            raise BalanceStorageError(f"cannot recover interrupted runs: {exc}") from exc


def _connect_readonly(database: str) -> sqlite3.Connection:
    if database == ":memory:":
        raise BalanceStorageError("read-only balance queries require a file database")
    uri = Path(database).absolute().as_uri() + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        raise BalanceStorageError(f"cannot open database {database!r} read-only: {exc}") from exc
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='balance_runs'"
        ).fetchone()
    except sqlite3.Error as exc:
        conn.close()
        raise BalanceStorageError(f"cannot inspect database {database!r}: {exc}") from exc
    if not table:
        conn.close()
        raise BalanceStorageError("balance tables not found; run a balance analysis first")
    return conn


def _run_row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "run_id": int(row[0]),
        "rule_version": row[1],
        "params": json.loads(row[2]),
        "params_fingerprint": row[3],
        "status": row[4],
        "records_scanned": int(row[5]),
        "batches_evaluated": int(row[6]),
        "batches_computed": int(row[7]),
        "batches_reused": int(row[8]),
        "input_fingerprint": row[9],
        "error": row[10],
        "created_at": row[11],
        "heartbeat_at": row[12],
        "finished_at": row[13],
    }


_RUN_COLUMNS = (
    "run_id, rule_version, params_json, params_fingerprint, status, records_scanned,"
    " batches_evaluated, batches_computed, batches_reused, input_fingerprint, error,"
    " created_at, heartbeat_at, finished_at"
)


def list_runs(database: str, limit: int = 50) -> list[dict[str, Any]]:
    """只读列出核算运行（最新的在前）。"""
    conn = _connect_readonly(database)
    try:
        rows = conn.execute(
            f"SELECT {_RUN_COLUMNS} FROM balance_runs ORDER BY run_id DESC LIMIT ?", (int(limit),)
        ).fetchall()
        return [_run_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def get_run(database: str, run_id: int) -> dict[str, Any]:
    """只读获取单个运行及其全部批次结果。"""
    conn = _connect_readonly(database)
    try:
        row = conn.execute(f"SELECT {_RUN_COLUMNS} FROM balance_runs WHERE run_id = ?", (int(run_id),)).fetchone()
        if row is None:
            raise LookupError(f"balance run not found: {run_id}")
        batches = conn.execute(
            "SELECT batch_id, input_hash, result_json FROM balance_batch_results WHERE run_id = ? ORDER BY batch_id",
            (int(run_id),),
        ).fetchall()
        return {
            "run": _run_row_to_dict(row),
            "batches": [
                {"batch_id": batch_id, "input_hash": input_hash, "result": json.loads(result_json)}
                for batch_id, input_hash, result_json in batches
            ],
        }
    finally:
        conn.close()


def export_run(database: str, run_id: int) -> dict[str, Any]:
    """只读导出一次核算的完整 JSON 文档（元数据 + 批次结果）。"""
    document = get_run(database, run_id)
    return {"format": "balance-export/v1", **document}
