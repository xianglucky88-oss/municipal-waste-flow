"""环境因子核算的 SQLite 账本存储。

设计约束：
- ``emission_ledger`` 只追加不修改：历史结果冻结，更正一律以冲销行
  （row_kind='reversal'）追加，绝不 UPDATE/DELETE 已有账本行。
- 所有写入幂等：账本行与排除行都有确定性主键，重复执行不会产生重复行。
- 每个写事务都是 ``BEGIN IMMEDIATE`` 并在锁冲突时有限重试；事务中途崩溃
  只会留下已完整提交的块，重跑即可恢复。
- schema 通过 ``schema_migrations`` 表版本化迁移；任一迁移失败整体回滚，
  修复后重跑 ``init_schema`` 即可继续。
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Sequence

SCHEMA_VERSION = 1

# 迁移按版本号顺序应用；每条迁移是一个语句列表，整体在单个事务中执行。
MIGRATIONS: list[tuple[int, str, list[str]]] = [
    (
        1,
        "accounting_core",
        [
            """CREATE TABLE IF NOT EXISTS rules_versions (
                rules_version TEXT PRIMARY KEY,
                rule_hash TEXT NOT NULL,
                rule_json TEXT NOT NULL,
                registered_at TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS accounting_runs (
                run_id TEXT PRIMARY KEY,
                rules_version TEXT NOT NULL,
                rule_hash TEXT NOT NULL,
                facilities_json TEXT NOT NULL,
                window_start TEXT,
                window_end TEXT,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                records_seen INTEGER NOT NULL DEFAULT 0,
                groups_seen INTEGER NOT NULL DEFAULT 0,
                entries_written INTEGER NOT NULL DEFAULT 0,
                reversals_written INTEGER NOT NULL DEFAULT 0,
                exclusions_written INTEGER NOT NULL DEFAULT 0,
                error TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS emission_ledger (
                ledger_id TEXT PRIMARY KEY,
                record_key TEXT NOT NULL,
                rules_version TEXT NOT NULL,
                stage TEXT NOT NULL,
                row_kind TEXT NOT NULL,
                reverses_ledger_id TEXT,
                factor_kind TEXT NOT NULL,
                factor_version TEXT NOT NULL,
                factor_value TEXT NOT NULL,
                factor_unit TEXT NOT NULL,
                net_weight_kg TEXT NOT NULL,
                result_value TEXT NOT NULL,
                result_unit TEXT NOT NULL,
                facility_id TEXT NOT NULL,
                destination_facility_id TEXT NOT NULL,
                waste_type TEXT NOT NULL,
                status TEXT NOT NULL,
                collected_at TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                accounting_date TEXT NOT NULL,
                record_hash TEXT NOT NULL,
                run_id TEXT NOT NULL,
                computed_at TEXT NOT NULL
            )""",
            "CREATE INDEX IF NOT EXISTS idx_ledger_record ON emission_ledger(record_key, rules_version)",
            "CREATE INDEX IF NOT EXISTS idx_ledger_facility_date ON emission_ledger(facility_id, accounting_date)",
            "CREATE INDEX IF NOT EXISTS idx_ledger_version_type ON emission_ledger(rules_version, waste_type)",
            "CREATE INDEX IF NOT EXISTS idx_ledger_reverses ON emission_ledger(reverses_ledger_id)",
            """CREATE TABLE IF NOT EXISTS accounting_exclusions (
                record_key TEXT NOT NULL,
                rules_version TEXT NOT NULL,
                stage TEXT NOT NULL,
                category TEXT NOT NULL,
                reason TEXT NOT NULL,
                facility_id TEXT,
                waste_type TEXT,
                collected_at TEXT,
                accounting_date TEXT,
                record_hash TEXT,
                run_id TEXT NOT NULL,
                decided_at TEXT NOT NULL,
                PRIMARY KEY (record_key, rules_version, stage)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_exclusions_facility ON accounting_exclusions(facility_id, rules_version)",
        ],
    ),
]

LEDGER_COLUMNS = (
    "ledger_id",
    "record_key",
    "rules_version",
    "stage",
    "row_kind",
    "reverses_ledger_id",
    "factor_kind",
    "factor_version",
    "factor_value",
    "factor_unit",
    "net_weight_kg",
    "result_value",
    "result_unit",
    "facility_id",
    "destination_facility_id",
    "waste_type",
    "status",
    "collected_at",
    "observed_at",
    "accounting_date",
    "record_hash",
    "run_id",
    "computed_at",
)

EXCLUSION_COLUMNS = (
    "record_key",
    "rules_version",
    "stage",
    "category",
    "reason",
    "facility_id",
    "waste_type",
    "collected_at",
    "accounting_date",
    "record_hash",
    "run_id",
    "decided_at",
)

RECORD_COLUMNS = ("record_key", "source", "observed_at", "updated_at", "raw_hash", "payload_json")


class LockConflictError(RuntimeError):
    """有限重试后仍无法获得数据库写锁。"""


class FactorVersionConflict(RuntimeError):
    """同一 rules_version 被用于不同的因子/规则集合（因子或单位变更必须换新版本）。"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_lock_error(exc: sqlite3.OperationalError) -> bool:
    text = str(exc).lower()
    return "locked" in text or "busy" in text


class LedgerStore:
    """核算模块自己的连接与写路径；只读 records 表，绝不写入它。"""

    def __init__(self, database: str, *, lock_retries: int = 5, lock_retry_delay: float = 0.05, busy_timeout_ms: int = 5000) -> None:
        self.database = database
        self.lock_retries = lock_retries
        self.lock_retry_delay = lock_retry_delay
        self.conn = sqlite3.connect(database, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------
    # 事务与锁恢复
    # ------------------------------------------------------------------
    def transaction(self, work: Callable[[], Any]) -> Any:
        """在 BEGIN IMMEDIATE 事务中执行 work；锁冲突按有限次数退避重试。"""
        attempt = 0
        while True:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if _is_lock_error(exc) and attempt < self.lock_retries:
                    attempt += 1
                    time.sleep(self.lock_retry_delay * attempt)
                    continue
                if _is_lock_error(exc):
                    raise LockConflictError(str(exc)) from exc
                raise
            try:
                result = work()
                self.conn.execute("COMMIT")
                return result
            except sqlite3.OperationalError as exc:
                self._rollback_quietly()
                if _is_lock_error(exc) and attempt < self.lock_retries:
                    attempt += 1
                    time.sleep(self.lock_retry_delay * attempt)
                    continue
                if _is_lock_error(exc):
                    raise LockConflictError(str(exc)) from exc
                raise
            except Exception:
                self._rollback_quietly()
                raise

    def _rollback_quietly(self) -> None:
        try:
            self.conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    # ------------------------------------------------------------------
    # schema 迁移
    # ------------------------------------------------------------------
    def init_schema(self) -> None:
        """应用未执行的迁移；任一失败整体回滚，可安全重试。"""

        def work() -> None:
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )"""
            )
            applied = {row["version"] for row in self.conn.execute("SELECT version FROM schema_migrations")}
            for version, name, statements in MIGRATIONS:
                if version in applied:
                    continue
                for statement in statements:
                    self.conn.execute(statement)
                self.conn.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                    (version, name, _utc_now()),
                )

        self.transaction(work)

    # ------------------------------------------------------------------
    # 运行管理（开始/结束各自独立事务，崩溃后可识别并恢复）
    # ------------------------------------------------------------------
    def begin_run(
        self,
        run_id: str,
        rules_version: str,
        rule_hash: str,
        rule_json: str,
        facilities: Sequence[str] | None,
        window_start: str | None,
        window_end: str | None,
    ) -> None:
        def work() -> None:
            row = self.conn.execute("SELECT rule_hash FROM rules_versions WHERE rules_version = ?", (rules_version,)).fetchone()
            if row is None:
                self.conn.execute(
                    "INSERT INTO rules_versions(rules_version, rule_hash, rule_json, registered_at) VALUES (?, ?, ?, ?)",
                    (rules_version, rule_hash, rule_json, _utc_now()),
                )
            elif row["rule_hash"] != rule_hash:
                raise FactorVersionConflict(
                    f"rules_version {rules_version!r} is already registered with a different factor/rule set; "
                    "factor or unit changes require a new rules_version"
                )
            # 上次崩溃/中断遗留的 running 运行：单写者恢复语义下标记为 interrupted。
            self.conn.execute(
                "UPDATE accounting_runs SET status = 'interrupted', finished_at = ? WHERE status = 'running'",
                (_utc_now(),),
            )
            self.conn.execute(
                """INSERT INTO accounting_runs(run_id, rules_version, rule_hash, facilities_json,
                       window_start, window_end, status, started_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'running', ?)""",
                (run_id, rules_version, rule_hash, json.dumps(list(facilities) if facilities else [], ensure_ascii=False), window_start, window_end, _utc_now()),
            )

        self.transaction(work)

    def finish_run(self, run_id: str, status: str, counters: dict[str, int], error: str | None = None) -> None:
        def work() -> None:
            self.conn.execute(
                """UPDATE accounting_runs SET status = ?, finished_at = ?, records_seen = ?, groups_seen = ?,
                       entries_written = ?, reversals_written = ?, exclusions_written = ?, error = ?
                   WHERE run_id = ? AND status = 'running'""",
                (
                    status,
                    _utc_now(),
                    counters.get("records", 0),
                    counters.get("groups", 0),
                    counters.get("entries", 0),
                    counters.get("reversals", 0),
                    counters.get("exclusions", 0),
                    error,
                    run_id,
                ),
            )

        self.transaction(work)

    # ------------------------------------------------------------------
    # records 快照读取（只读、分块、有限内存）
    # ------------------------------------------------------------------
    def fetch_records_chunk(self, offset: int, limit: int, facilities: Sequence[str] | None) -> list[sqlite3.Row]:
        # payload_json 可能不是严格 JSON（如 Python json.dumps 放行的 NaN/Infinity），
        # json_extract 遇 malformed JSON 会直接报错，因此用 json_valid 守卫：
        # 无效 JSON 的设施/批次键视为 NULL（排在最前），由 Python 侧归入
        # payload_unreadable 冲突路径。
        facility_expr = "CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.facility_id') END"
        batch_expr = "CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.batch_id') END"
        sql = f"SELECT {', '.join(RECORD_COLUMNS)} FROM records"
        params: list[Any] = []
        if facilities:
            placeholders = ", ".join("?" for _ in facilities)
            sql += f" WHERE {facility_expr} IN ({placeholders})"
            params.extend(facilities)
        # 排序保证同一 (facility_id, batch_id) 事件组的记录相邻且整体顺序确定。
        sql += f" ORDER BY {facility_expr}, {batch_expr}, observed_at, record_key LIMIT ? OFFSET ?"
        params.extend((limit, offset))
        return list(self.conn.execute(sql, params))

    # ------------------------------------------------------------------
    # 组状态读取与写入（必须在 transaction() 内调用）
    # ------------------------------------------------------------------
    def load_group_state(self, record_keys: Sequence[str], rules_version: str) -> tuple[dict[str, list[dict[str, Any]]], set[str], set[tuple[str, str]]]:
        """返回 (按 record_key 分组的 entry 行, 已被冲销的 ledger_id 集合, 已有排除键集合)。"""
        keys = sorted(set(record_keys))
        if not keys:
            return {}, set(), set()
        placeholders = ", ".join("?" for _ in keys)
        entries: dict[str, list[dict[str, Any]]] = {}
        for row in self.conn.execute(
            f"SELECT * FROM emission_ledger WHERE rules_version = ? AND row_kind = 'entry' AND record_key IN ({placeholders})",
            [rules_version, *keys],
        ):
            entries.setdefault(row["record_key"], []).append(dict(row))
        reversed_ids = {
            row["reverses_ledger_id"]
            for row in self.conn.execute(
                f"SELECT reverses_ledger_id FROM emission_ledger WHERE rules_version = ? AND row_kind = 'reversal' AND record_key IN ({placeholders})",
                [rules_version, *keys],
            )
        }
        exclusions = {
            (row["record_key"], row["stage"])
            for row in self.conn.execute(
                f"SELECT record_key, stage FROM accounting_exclusions WHERE rules_version = ? AND record_key IN ({placeholders})",
                [rules_version, *keys],
            )
        }
        return entries, reversed_ids, exclusions

    def insert_ledger_rows(self, rows: Iterable[dict[str, Any]]) -> int:
        columns = ", ".join(LEDGER_COLUMNS)
        placeholders = ", ".join("?" for _ in LEDGER_COLUMNS)
        sql = f"INSERT OR IGNORE INTO emission_ledger({columns}) VALUES ({placeholders})"
        written = 0
        for row in rows:
            cursor = self.conn.execute(sql, [row[column] for column in LEDGER_COLUMNS])
            written += cursor.rowcount
        return written

    def insert_exclusions(self, rows: Iterable[dict[str, Any]]) -> int:
        columns = ", ".join(EXCLUSION_COLUMNS)
        placeholders = ", ".join("?" for _ in EXCLUSION_COLUMNS)
        sql = f"INSERT OR IGNORE INTO accounting_exclusions({columns}) VALUES ({placeholders})"
        written = 0
        for row in rows:
            cursor = self.conn.execute(sql, [row[column] for column in EXCLUSION_COLUMNS])
            written += cursor.rowcount
        return written
