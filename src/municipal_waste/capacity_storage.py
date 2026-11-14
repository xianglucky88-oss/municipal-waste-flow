"""SQLite persistence for capacity assessment.

This schema is additive: the ingestion tables (``batches``, ``records``,
``rejected_records``, ``raw_manifests``) are never altered, so the existing CLI
and ingestion pipeline keep working unchanged.

Durability model
----------------
* ``capacity_report_locks`` is the cross-process advisory lock.  A row is held
  for exactly one ``(facility_key, waste_key)`` scope; a second writer that
  cannot acquire it gets :class:`CapacityLockConflict` instead of overwriting
  the first writer's work.  The check-then-insert happens inside a
  ``BEGIN IMMEDIATE`` transaction, so concurrent writers are serialised by
  SQLite rather than by luck.  Locks carry a heartbeat so a crashed process
  leaves a reclaimable, never-permanently-stuck lock.
* All conclusion rows are written idempotently with a content-based
  ``conclusion_hash`` and unique keys, so retries after a crash never create
  duplicate conclusions.
* A report and all its conclusions are committed in one transaction; a crash
  before commit leaves at most an ``interrupted`` run row and no visible report.
* ``capacity_slice_cache`` keys include the engine version, rule version/hash,
  bucket geometry and ``as_of``.  A rule change therefore produces new cache
  keys and never mutates historical conclusions (history is frozen).
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator, Sequence
from datetime import datetime, timezone
from typing import Any

from .capacity import ENGINE_VERSION, LoadRecord, RejectionEvidence, format_timestamp

SCHEMA_VERSION = 1
_SCHEMA_SENTINEL = "capacity"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS capacity_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS capacity_rules (
    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    facility_id TEXT NOT NULL,
    facility_key TEXT NOT NULL,
    waste_type TEXT NOT NULL,
    waste_key TEXT NOT NULL,
    version INTEGER NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to TEXT,
    timezone_offset_minutes INTEGER NOT NULL DEFAULT 0,
    daily_limit_kg REAL,
    hourly_limit_kg REAL,
    rule_hash TEXT NOT NULL,
    rule_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(facility_key, waste_key, version)
);
CREATE TABLE IF NOT EXISTS capacity_report_locks (
    facility_key TEXT NOT NULL,
    waste_key TEXT NOT NULL,
    run_id INTEGER,
    holder TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (facility_key, waste_key)
);
CREATE TABLE IF NOT EXISTS capacity_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    facility_key TEXT NOT NULL,
    waste_key TEXT NOT NULL,
    status TEXT NOT NULL,
    scope_hash TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    as_of TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    ruleset_hash TEXT,
    report_id TEXT,
    error TEXT,
    max_records INTEGER,
    max_slices INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS capacity_slice_cache (
    cache_key TEXT PRIMARY KEY,
    facility_key TEXT NOT NULL,
    waste_key TEXT NOT NULL,
    period TEXT NOT NULL,
    bucket_start TEXT NOT NULL,
    bucket_end TEXT NOT NULL,
    slice_start TEXT NOT NULL,
    slice_end TEXT NOT NULL,
    capacity_version INTEGER,
    capacity_rule_hash TEXT,
    conclusion_json TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_hit_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS capacity_reports (
    report_id TEXT PRIMARY KEY,
    facility_key TEXT NOT NULL,
    waste_key TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    scope_hash TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    as_of TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    ruleset_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS capacity_report_conclusions (
    report_id TEXT NOT NULL,
    slice_index INTEGER NOT NULL,
    conclusion_hash TEXT NOT NULL,
    cache_key TEXT,
    period TEXT NOT NULL,
    bucket_start TEXT NOT NULL,
    bucket_end TEXT NOT NULL,
    slice_start TEXT NOT NULL,
    slice_end TEXT NOT NULL,
    verdict TEXT NOT NULL,
    load_kg REAL NOT NULL,
    capacity_kg REAL,
    capacity_version INTEGER,
    capacity_rule_hash TEXT,
    conclusion_json TEXT NOT NULL,
    PRIMARY KEY (report_id, slice_index),
    UNIQUE (report_id, conclusion_hash),
    FOREIGN KEY(report_id) REFERENCES capacity_reports(report_id)
);
CREATE INDEX IF NOT EXISTS idx_capacity_runs_scope
    ON capacity_runs(facility_key, waste_key, created_at);
CREATE INDEX IF NOT EXISTS idx_capacity_cache_scope
    ON capacity_slice_cache(facility_key, waste_key, slice_start);
CREATE INDEX IF NOT EXISTS idx_capacity_reports_scope
    ON capacity_reports(facility_key, waste_key, created_at);
"""


class CapacityStoreError(Exception):
    """Base class for capacity storage failures."""


class CapacityLockConflict(CapacityStoreError):
    """Another process already holds the report-generation lock for a scope."""


class CapacityMigrationError(CapacityStoreError):
    """The capacity schema could not be initialized or migrated."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fingerprint(raw: Any) -> str:
    canonical = json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class CapacityStore:
    """Owns capacity tables, advisory locking, caches and read-only reports."""

    def __init__(self, database: str, *, read_only: bool = False, busy_timeout_ms: int = 5000) -> None:
        if read_only:
            self.conn = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        else:
            self.conn = sqlite3.connect(database)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.read_only = read_only
        if not read_only:
            self.initialize_schema()

    def initialize_schema(self) -> None:
        """Create capacity tables idempotently inside a transaction.

        A failure rolls the whole migration back and raises
        :class:`CapacityMigrationError`; the database stays at its previous
        state and the operation can be retried.
        """
        try:
            self.conn.executescript(_SCHEMA)
            self.conn.execute(
                "INSERT OR IGNORE INTO capacity_meta(key, value, updated_at) VALUES (?, ?, ?)",
                ("schema_version", str(SCHEMA_VERSION), _now()),
            )
            self.conn.commit()
        except sqlite3.Error as exc:
            self.conn.rollback()
            raise CapacityMigrationError(f"capacity schema migration failed: {exc}") from exc

    def schema_version(self) -> int | None:
        row = self.conn.execute("SELECT value FROM capacity_meta WHERE key = 'schema_version'").fetchone()
        return None if row is None else int(row["value"])

    def replace_rules(self, rules: Sequence[Any]) -> str:
        """Persist a full rule document and return its content hash.

        Replacing rules never rewrites historical reports; existing conclusions
        keep the rule version and rule hash they were assessed against.
        """
        documents = [rule.as_dict(include_hash=True) for rule in rules]
        ruleset_hash = hashlib.sha256(
            json.dumps(documents, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        try:
            with self.conn:
                for rule in rules:
                    self.conn.execute(
                        """INSERT INTO capacity_rules(
                            facility_id, facility_key, waste_type, waste_key, version,
                            effective_from, effective_to, timezone_offset_minutes,
                            daily_limit_kg, hourly_limit_kg, rule_hash, rule_json, created_at
                           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(facility_key, waste_key, version) DO UPDATE SET
                            effective_from=excluded.effective_from,
                            effective_to=excluded.effective_to,
                            timezone_offset_minutes=excluded.timezone_offset_minutes,
                            daily_limit_kg=excluded.daily_limit_kg,
                            hourly_limit_kg=excluded.hourly_limit_kg,
                            rule_hash=excluded.rule_hash,
                            rule_json=excluded.rule_json,
                            created_at=excluded.created_at""",
                        (
                            rule.facility_id,
                            rule.facility_key,
                            rule.waste_type,
                            rule.waste_key,
                            int(rule.version),
                            format_timestamp(rule.effective_from),
                            None if rule.effective_to is None else format_timestamp(rule.effective_to),
                            int(rule.timezone_offset_minutes),
                            rule.daily_limit_kg,
                            rule.hourly_limit_kg,
                            rule.rule_hash,
                            json.dumps(rule.as_dict(include_hash=True), ensure_ascii=False, sort_keys=True),
                            _now(),
                        ),
                    )
        except sqlite3.Error:
            raise
        return ruleset_hash

    def load_rules(self, facility_key: str, waste_key: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """SELECT rule_json FROM capacity_rules
               WHERE facility_key = ? AND waste_key = ?
               ORDER BY version""",
            (facility_key, waste_key),
        ).fetchall()
        return [json.loads(row["rule_json"]) for row in rows]

    def acquire_lock(
        self,
        facility_key: str,
        waste_key: str,
        run_id: int,
        holder: str,
        ttl_seconds: float,
        *,
        grace_seconds: float = 0.05,
    ) -> None:
        """Take the advisory lock or raise :class:`CapacityLockConflict`.

        The check-then-insert runs inside one ``BEGIN IMMEDIATE`` transaction
        with a zero busy timeout and commits before returning, so exactly one
        of two writers racing for the same scope wins; the loser observes the
        winner's committed row and fails fast instead of overwriting it.

        Depending on the caller, a same-scope writer may still be *reading*
        its inputs when we arrive.  ``grace_seconds`` therefore keeps retrying
        the acquisition for a bounded moment, so a peer that is already inside
        ``generate_report`` is reliably reported as a conflict rather than
        simply running later.  An expired lock (its holder crashed) is
        reclaimed atomically.
        """
        waste_key = waste_key.strip().lower()
        deadline = time.monotonic() + max(0.0, grace_seconds)
        while True:
            try:
                self._acquire_lock_once(facility_key, waste_key, run_id, holder, ttl_seconds)
                return
            except CapacityLockConflict:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.001)

    def _acquire_lock_once(
        self, facility_key: str, waste_key: str, run_id: int, holder: str, ttl_seconds: float
    ) -> None:
        now = datetime.now(timezone.utc)
        expires = now.timestamp() + ttl_seconds
        if self.conn.in_transaction:
            self.conn.commit()
        old_timeout = self.conn.execute("PRAGMA busy_timeout").fetchone()[0]
        self.conn.execute("PRAGMA busy_timeout = 0")
        try:
            self.conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            self.conn.execute(f"PRAGMA busy_timeout = {int(old_timeout)}")
            raise CapacityLockConflict(
                f"capacity report lock held by another writer for ({facility_key}, {waste_key})"
            ) from exc
        self.conn.execute(f"PRAGMA busy_timeout = {int(old_timeout)}")
        try:
            row = self.conn.execute(
                "SELECT expires_at, run_id FROM capacity_report_locks WHERE facility_key = ? AND waste_key = ?",
                (facility_key, waste_key),
            ).fetchone()
            if row is not None:
                expires_at = datetime.fromisoformat(row["expires_at"])
                if expires_at > now and int(row["run_id"]) != int(run_id):
                    self.conn.rollback()
                    raise CapacityLockConflict(
                        f"capacity report lock held by run {row['run_id']} for ({facility_key}, {waste_key})"
                    )
                self.conn.execute(
                    "DELETE FROM capacity_report_locks WHERE facility_key = ? AND waste_key = ?",
                    (facility_key, waste_key),
                )
            self.conn.execute(
                """INSERT INTO capacity_report_locks(
                    facility_key, waste_key, run_id, holder,
                    acquired_at, heartbeat_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    facility_key,
                    waste_key,
                    run_id,
                    holder,
                    now.isoformat(),
                    now.isoformat(),
                    datetime.fromtimestamp(expires, tz=timezone.utc).isoformat(),
                ),
            )
            self.conn.commit()
        except CapacityLockConflict:
            raise
        except sqlite3.Error:
            self.conn.rollback()
            raise

    def heartbeat_lock(self, facility_key: str, waste_key: str, run_id: int, ttl_seconds: float) -> None:
        now = datetime.now(timezone.utc)
        expires = datetime.fromtimestamp(now.timestamp() + ttl_seconds, tz=timezone.utc)
        with self.conn:
            self.conn.execute(
                "UPDATE capacity_report_locks SET heartbeat_at = ?, expires_at = ? WHERE facility_key = ? AND waste_key = ? AND run_id = ?",
                (now.isoformat(), expires.isoformat(), facility_key, waste_key.strip().lower(), run_id),
            )

    def release_lock(self, facility_key: str, waste_key: str, run_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM capacity_report_locks WHERE facility_key = ? AND waste_key = ? AND run_id = ?",
                (facility_key, waste_key.strip().lower(), run_id),
            )

    def rollback(self) -> None:
        self.conn.rollback()

    def start_run(
        self,
        facility_key: str,
        waste_key: str,
        scope_hash: str,
        window_start: str,
        window_end: str,
        as_of: str,
        max_records: int | None,
        max_slices: int | None,
    ) -> int:
        now = _now()
        cursor = self.conn.execute(
            """INSERT INTO capacity_runs(
                facility_key, waste_key, status, scope_hash, window_start,
                window_end, as_of, engine_version, max_records, max_slices,
                created_at, updated_at)
               VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                facility_key,
                waste_key,
                scope_hash,
                window_start,
                window_end,
                as_of,
                ENGINE_VERSION,
                max_records,
                max_slices,
                now,
                now,
            ),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def start_run_locked(
        self,
        facility_key: str,
        waste_key: str,
        run_id: int,
        holder: str,
        ttl_seconds: float,
        scope_hash: str,
        window_start: str,
        window_end: str,
        as_of: str,
        max_records: int | None,
        max_slices: int | None,
        *,
        grace_seconds: float = 0.05,
    ) -> int:
        """Acquire the scope lock for an already-created run, or fail.

        The lock row is committed before any read/assess work, so it is the
        first thing a concurrent writer observes.  ``run_id`` comes from
        :meth:`start_run`; a conflicting writer's run row is marked
        ``lock_conflicted`` so failures stay auditable.
        """
        try:
            self.acquire_lock(
                facility_key,
                waste_key,
                run_id,
                holder,
                ttl_seconds,
                grace_seconds=grace_seconds,
            )
        except CapacityLockConflict:
            self.conn.execute(
                """UPDATE capacity_runs
                   SET status = 'lock_conflicted', error = ?, updated_at = ?
                   WHERE run_id = ?""",
                ("report lock held by another process", _now(), run_id),
            )
            self.conn.commit()
            raise
        now = _now()
        self.conn.execute(
            """UPDATE capacity_runs SET scope_hash = ?, updated_at = ? WHERE run_id = ?""",
            (scope_hash, now, run_id),
        )
        self.conn.commit()
        return run_id

    def mark_run(
        self,
        run_id: int,
        status: str,
        error: str | None = None,
        report_id: str | None = None,
        ruleset_hash: str | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE capacity_runs SET status = ?, error = ?, report_id = ?, ruleset_hash = COALESCE(?, ruleset_hash), updated_at = ? WHERE run_id = ?",
                (status, error, report_id, ruleset_hash, _now(), run_id),
            )

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM capacity_runs WHERE run_id = ?", (run_id,)).fetchone()
        return None if row is None else dict(row)

    def interrupted_runs(self, facility_key: str, waste_key: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM capacity_runs WHERE facility_key = ? AND waste_key = ? AND status = 'running' ORDER BY run_id",
            (facility_key, waste_key),
        ).fetchall()
        return [dict(row) for row in rows]

    def iter_load_records(
        self,
        facility_key: str,
        waste_key: str,
        window_start: datetime,
        window_end: datetime,
        as_of: datetime,
        max_records: int | None = None,
    ) -> Iterator[LoadRecord]:
        """Stream accepted records in bounded batches (constant memory).

        Every batch that touches the facility (as source *or* destination) is
        pulled in full so batch-level facility loops stay visible, but the
        engine attributes weight to ``destination_facility_id`` only, so a
        weighing is never counted at both ends.
        """
        batch_size = 500
        lower = format_timestamp(window_start)
        upper = format_timestamp(window_end)
        knowledge = format_timestamp(as_of)
        batch_rows = self.conn.execute(
            """SELECT DISTINCT json_extract(payload_json, '$.batch_id') AS batch_id
               FROM records
               WHERE lower(json_extract(payload_json, '$.waste_type')) = ?
                 AND json_extract(payload_json, '$.collected_at') >= ?
                 AND json_extract(payload_json, '$.collected_at') < ?
                 AND observed_at <= ?
                 AND (
                   upper(json_extract(payload_json, '$.destination_facility_id')) = ?
                   OR upper(json_extract(payload_json, '$.facility_id')) = ?
                 )""",
            (waste_key, lower, upper, knowledge, facility_key, facility_key),
        ).fetchall()
        batch_ids = [row["batch_id"] for row in batch_rows if row["batch_id"]]
        emitted = 0
        for offset in range(0, len(batch_ids), batch_size):
            chunk = batch_ids[offset : offset + batch_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT record_key, source, observed_at, payload_json, raw_hash
                    FROM records
                    WHERE lower(json_extract(payload_json, '$.waste_type')) = ?
                      AND observed_at <= ?
                      AND json_extract(payload_json, '$.batch_id') IN ({placeholders})
                    ORDER BY record_key""",
                [waste_key, knowledge, *chunk],
            ).fetchall()
            for row in rows:
                payload = json.loads(row["payload_json"])
                yield LoadRecord.from_payload(
                    record_key=row["record_key"],
                    source=row["source"],
                    observed_at=row["observed_at"],
                    payload=payload,
                    raw_hash=row["raw_hash"],
                )
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return

    def iter_rejection_evidence(
        self,
        facility_key: str,
        waste_key: str,
        window_start: datetime,
        window_end: datetime,
        max_records: int | None = None,
    ) -> Iterator[RejectionEvidence]:
        """Stream rejected ingestion rows that may hide load for this scope."""
        batch_size = 500
        last_id = 0
        emitted = 0
        lower = format_timestamp(window_start)
        upper = format_timestamp(window_end)
        while True:
            rows = self.conn.execute(
                """SELECT rejection_id, source, error, raw_json, created_at
                   FROM rejected_records
                   WHERE rejection_id > ?
                   ORDER BY rejection_id LIMIT ?""",
                (last_id, batch_size),
            ).fetchall()
            if not rows:
                return
            for row in rows:
                last_id = row["rejection_id"]
                try:
                    raw = json.loads(row["raw_json"])
                except (TypeError, ValueError):
                    raw = {}
                payload = raw.get("payload") if isinstance(raw, dict) else None
                if not isinstance(payload, dict):
                    continue
                destination = str(payload.get("destination_facility_id") or "").strip().upper()
                facility = str(payload.get("facility_id") or "").strip().upper()
                waste = str(payload.get("waste_type") or "").strip().lower()
                collected_at = str(payload.get("collected_at") or "")
                if waste != waste_key:
                    continue
                if facility_key not in (destination, facility):
                    continue
                if not (lower <= collected_at < upper):
                    continue
                yield RejectionEvidence(
                    rejection_id=int(row["rejection_id"]),
                    error=str(row["error"]),
                    collected_at=collected_at,
                    destination_facility_id=destination,
                    waste_type=waste,
                    raw_fingerprint=_fingerprint(raw),
                    batch_id=str(payload.get("batch_id") or ""),
                )
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return
            if len(rows) < batch_size:
                return

    def get_cached_conclusion(self, cache_key: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT conclusion_json FROM capacity_slice_cache WHERE cache_key = ?", (cache_key,)
        ).fetchone()
        if row is None:
            return None
        with self.conn:
            self.conn.execute(
                "UPDATE capacity_slice_cache SET last_hit_at = ? WHERE cache_key = ?", (_now(), cache_key)
            )
        return json.loads(row["conclusion_json"])

    def put_cached_conclusion(
        self, cache_key: str, facility_key: str, waste_key: str, conclusion: dict[str, Any]
    ) -> None:
        with self.conn:
            self.conn.execute(
                """INSERT INTO capacity_slice_cache(
                    cache_key, facility_key, waste_key, period, bucket_start,
                    bucket_end, slice_start, slice_end, capacity_version,
                    capacity_rule_hash, conclusion_json, engine_version,
                    created_at, last_hit_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(cache_key) DO UPDATE SET
                    conclusion_json=excluded.conclusion_json,
                    last_hit_at=excluded.last_hit_at""",
                (
                    cache_key,
                    facility_key,
                    waste_key,
                    conclusion["period"],
                    conclusion["bucket_start"],
                    conclusion["bucket_end"],
                    conclusion["slice_start"],
                    conclusion["slice_end"],
                    conclusion.get("capacity_version"),
                    conclusion.get("capacity_rule_hash"),
                    json.dumps(conclusion, ensure_ascii=False, sort_keys=True),
                    ENGINE_VERSION,
                    _now(),
                    _now(),
                ),
            )

    def save_report(
        self,
        report_id: str,
        run_id: int,
        facility_key: str,
        waste_key: str,
        scope_hash: str,
        window_start: str,
        window_end: str,
        as_of: str,
        ruleset_hash: str,
        conclusions: Sequence[dict[str, Any]],
        intervals: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Persist a report and its conclusions idempotently in one transaction."""
        payload = {
            "report_id": report_id,
            "facility_key": facility_key,
            "waste_key": waste_key,
            "run_id": run_id,
            "scope_hash": scope_hash,
            "window_start": window_start,
            "window_end": window_end,
            "as_of": as_of,
            "engine_version": ENGINE_VERSION,
            "ruleset_hash": ruleset_hash,
            "created_at": _now(),
            "conclusions": list(conclusions),
            "intervals": intervals,
        }
        with self.conn:
            self.conn.execute(
                """INSERT OR IGNORE INTO capacity_reports(
                    report_id, facility_key, waste_key, run_id, scope_hash,
                    window_start, window_end, as_of, engine_version,
                    ruleset_hash, created_at, payload_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    report_id,
                    facility_key,
                    waste_key,
                    run_id,
                    scope_hash,
                    window_start,
                    window_end,
                    as_of,
                    ENGINE_VERSION,
                    ruleset_hash,
                    payload["created_at"],
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                ),
            )
            for index, conclusion in enumerate(conclusions):
                self.conn.execute(
                    """INSERT OR IGNORE INTO capacity_report_conclusions(
                        report_id, slice_index, conclusion_hash, cache_key, period,
                        bucket_start, bucket_end, slice_start, slice_end, verdict,
                        load_kg, capacity_kg, capacity_version, capacity_rule_hash,
                        conclusion_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        report_id,
                        index,
                        conclusion["conclusion_hash"],
                        conclusion.get("cache_key"),
                        conclusion["period"],
                        conclusion["bucket_start"],
                        conclusion["bucket_end"],
                        conclusion["slice_start"],
                        conclusion["slice_end"],
                        conclusion["verdict"],
                        conclusion["load_kg"],
                        conclusion.get("capacity_kg"),
                        conclusion.get("capacity_version"),
                        conclusion.get("capacity_rule_hash"),
                        json.dumps(conclusion, ensure_ascii=False, sort_keys=True),
                    ),
                )

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT payload_json FROM capacity_reports WHERE report_id = ?", (report_id,)
        ).fetchone()
        return None if row is None else json.loads(row["payload_json"])

    def list_reports(
        self, facility_key: str | None = None, waste_key: str | None = None
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT report_id, facility_key, waste_key, run_id, window_start, window_end, "
            "as_of, engine_version, ruleset_hash, created_at FROM capacity_reports WHERE 1 = 1"
        )
        params: list[Any] = []
        if facility_key:
            sql += " AND facility_key = ?"
            params.append(facility_key)
        if waste_key:
            sql += " AND waste_key = ?"
            params.append(waste_key)
        sql += " ORDER BY created_at, report_id"
        return [dict(row) for row in self.conn.execute(sql, params).fetchall()]

    def get_conclusions(self, report_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT conclusion_json FROM capacity_report_conclusions WHERE report_id = ? ORDER BY slice_index",
            (report_id,),
        ).fetchall()
        return [json.loads(row["conclusion_json"]) for row in rows]

    def close(self) -> None:
        self.conn.close()
