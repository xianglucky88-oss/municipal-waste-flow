"""废物流转环境因子核算引擎。

职责边界：本模块只对 records 快照中的每条事实做"逐事实的因子应用"与
可追溯汇总 —— 读取 records 表（只读），把排放/环境影响账本行写入独立的
核算表。它不把记录同时当作输入和输出，不做重量守恒裁决、不做批次状态
排序、不做容量时间片评估。

时间口径：
- ``collected_at`` 是业务事件时间，决定核算日期（accounting_date，UTC）
  与时间窗口归属；``observed_at`` 是系统观测时间，只用于同一事件多次
  观测之间的新旧判定（最新观测为准）。迟到事实按其 collected_at 落入
  对应分区，重跑该分区即被增量纳入。
- 时间戳必须是带时区的 ISO 8601；无时区的时间戳无法判定口径，记为
  ``input_conflict`` 而不是猜测。

四类结果：``computable``（可计算，写入 emission_ledger）、
``factor_missing``（因子缺失）、``input_conflict``（输入冲突）、
``not_included``（不纳入）。后三类写入 accounting_exclusions 并附原因。

冻结与更正：已写入的账本行永不修改或删除。同一 record_key 的内容在核算
后发生变化（同 observed_at 不同 payload）记为 input_conflict 并保留冻结
结果；更新的修订（新 observed_at）取代旧事实时，旧事实的账本行以冲销行
（reversal，结果为负）抵销，保证任意时刻汇总正确且历史可回放。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Sequence

from .factors import (
    FACTOR_KIND_EDGE,
    FACTOR_KIND_SELF_LOOP,
    FACTOR_KIND_STAGE,
    FACTOR_KIND_WASTE_TYPE,
    FactorEntry,
    FactorSet,
    dec_str,
)
from .ledger_store import FactorVersionConflict, LedgerStore, LockConflictError

__all__ = [
    "AccountingEngine",
    "ComputeResult",
    "FactorVersionConflict",
    "LockConflictError",
    "STAGE_WASTE_TYPE",
    "STAGE_TRANSPORT",
    "STAGE_PROCESSING",
    "STAGES",
    "CATEGORY_COMPUTABLE",
    "CATEGORY_FACTOR_MISSING",
    "CATEGORY_INPUT_CONFLICT",
    "CATEGORY_NOT_INCLUDED",
    "RECORD_LEVEL",
    "DEFAULT_INCLUDED_STATUSES",
    "SELF_LOOP_ZERO",
    "SELF_LOOP_REQUIRE_FACTOR",
    "parse_ts",
    "decide_group",
    "rule_digest",
]

# 计算口径（阶段）：每条纳入事实独立产生最多三行账本。
STAGE_WASTE_TYPE = "waste_type"
STAGE_TRANSPORT = "transport"
STAGE_PROCESSING = "processing"
STAGES = (STAGE_WASTE_TYPE, STAGE_TRANSPORT, STAGE_PROCESSING)

CATEGORY_COMPUTABLE = "computable"
CATEGORY_FACTOR_MISSING = "factor_missing"
CATEGORY_INPUT_CONFLICT = "input_conflict"
CATEGORY_NOT_INCLUDED = "not_included"
CATEGORIES = (CATEGORY_COMPUTABLE, CATEGORY_FACTOR_MISSING, CATEGORY_INPUT_CONFLICT, CATEGORY_NOT_INCLUDED)

# 排除行中 record 级决策使用的 stage 占位（stage 列非空以便主键去重）。
RECORD_LEVEL = "*"

DEFAULT_INCLUDED_STATUSES = ("collected", "in_transit", "received", "processed")

SELF_LOOP_ZERO = "zero"
SELF_LOOP_REQUIRE_FACTOR = "require_factor"


def parse_ts(value: Any) -> datetime | None:
    """解析带时区的 ISO 8601 时间戳并归一到 UTC；无法解析或缺时区返回 None。"""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_weight(value: Any) -> Decimal | None:
    """净重必须是正有限数；零/负/非数/非有限一律视为输入冲突。"""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, (int, float, str)):
        try:
            result = Decimal(str(value))
        except Exception:
            return None
    else:
        return None
    if not result.is_finite() or result <= 0:
        return None
    return result


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _lower(value: Any) -> str:
    return value.strip().lower() if isinstance(value, str) else ""


def rule_payload(factors: FactorSet, included_statuses: Sequence[str], self_loop_policy: str) -> dict[str, Any]:
    """规则全集：因子表 + 状态筛选 + 自环策略。任一变化都必须更换 rules_version。"""
    return {
        "factors": factors.canonical(),
        "included_statuses": sorted(included_statuses),
        "self_loop_policy": self_loop_policy,
    }


def rule_digest(factors: FactorSet, included_statuses: Sequence[str], self_loop_policy: str) -> str:
    blob = json.dumps(rule_payload(factors, included_statuses, self_loop_policy), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RecordRow:
    """records 表中一行事实的解析结果。"""

    record_key: str
    source: str
    observed_at: str
    updated_at: str
    raw_hash: str
    payload: Any
    payload_error: str | None


@dataclass(frozen=True)
class DecisionContext:
    factors: FactorSet
    rules_version: str
    included_statuses: tuple[str, ...]
    self_loop_policy: str
    window_start: datetime | None
    window_end: datetime | None
    run_id: str
    now: str


@dataclass(frozen=True)
class ComputeResult:
    run_id: str
    rules_version: str
    status: str
    records_seen: int
    groups_seen: int
    entries_written: int
    reversals_written: int
    exclusions_written: int
    started_at: str
    finished_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_record_row(row: Any) -> RecordRow:
    try:
        payload = json.loads(row["payload_json"], parse_float=Decimal)
        error = None
    except (ValueError, TypeError) as exc:
        payload, error = None, str(exc)
    return RecordRow(
        record_key=row["record_key"],
        source=row["source"],
        observed_at=row["observed_at"],
        updated_at=row["updated_at"],
        raw_hash=row["raw_hash"],
        payload=payload,
        payload_error=error,
    )


def _group_key(record: RecordRow) -> tuple[str, str]:
    """事件分组键 (facility_id, batch_id)；无法识别身份的记录各自成组。"""
    if isinstance(record.payload, dict):
        facility = record.payload.get("facility_id")
        batch = record.payload.get("batch_id")
        if isinstance(facility, str) and facility.strip() and isinstance(batch, str) and batch.strip():
            return (facility.strip(), batch.strip())
    return ("", record.record_key)


def _material_signature(record: RecordRow) -> tuple[str, str, str, str, str]:
    """跨来源一致性比较用的物量签名：类型、重量、去向、事件时间、状态。"""
    payload = record.payload
    waste_type = _lower(payload.get("waste_type"))
    status = _lower(payload.get("status"))
    destination = _text(payload.get("destination_facility_id"))
    weight = _parse_weight(payload.get("net_weight_kg"))
    weight_text = dec_str(weight) if weight is not None else repr(payload.get("net_weight_kg"))
    collected = parse_ts(payload.get("collected_at"))
    collected_text = collected.isoformat() if collected is not None else repr(payload.get("collected_at"))
    return (waste_type, weight_text, destination, collected_text, status)


def _order_key(record: RecordRow, observed: datetime) -> tuple[datetime, str, str]:
    return (observed, record.updated_at or "", record.record_key)


def _entry_row(base: dict[str, Any], stage: str, factor_kind: str, entry: FactorEntry, weight: Decimal) -> dict[str, Any]:
    ledger_id = hashlib.sha256(f"entry|{base['rules_version']}|{base['record_key']}|{stage}".encode("utf-8")).hexdigest()
    row = dict(base)
    row.update(
        {
            "ledger_id": ledger_id,
            "stage": stage,
            "row_kind": "entry",
            "reverses_ledger_id": None,
            "factor_kind": factor_kind,
            "factor_version": entry.factor_version,
            "factor_value": dec_str(entry.value),
            "factor_unit": entry.unit,
            "result_value": dec_str(weight * entry.value),
            "result_unit": entry.result_unit,
        }
    )
    return row


def _self_loop_entry_row(base: dict[str, Any]) -> dict[str, Any]:
    """设施自环：无运输段，显式记零（调用方提供显式自环边因子时优先使用）。"""
    ledger_id = hashlib.sha256(f"entry|{base['rules_version']}|{base['record_key']}|{STAGE_TRANSPORT}".encode("utf-8")).hexdigest()
    row = dict(base)
    row.update(
        {
            "ledger_id": ledger_id,
            "stage": STAGE_TRANSPORT,
            "row_kind": "entry",
            "reverses_ledger_id": None,
            "factor_kind": FACTOR_KIND_SELF_LOOP,
            "factor_version": "self-loop",
            "factor_value": "0",
            "factor_unit": "",
            "result_value": "0",
            "result_unit": "",
        }
    )
    return row


def _reversal_row(entry: dict[str, Any], cause: str, ctx: DecisionContext) -> dict[str, Any]:
    ledger_id = hashlib.sha256(f"reversal|{entry['ledger_id']}|{cause}".encode("utf-8")).hexdigest()
    row = dict(entry)
    row.update(
        {
            "ledger_id": ledger_id,
            "row_kind": "reversal",
            "reverses_ledger_id": entry["ledger_id"],
            "result_value": dec_str(-Decimal(entry["result_value"])),
            "run_id": ctx.run_id,
            "computed_at": ctx.now,
        }
    )
    return row


def _exclusion_row(record: RecordRow, stage: str, category: str, reason: str, ctx: DecisionContext) -> dict[str, Any]:
    payload = record.payload if isinstance(record.payload, dict) else {}
    facility = _text(payload.get("facility_id")) or None
    waste_type = _lower(payload.get("waste_type")) or None
    collected_raw = payload.get("collected_at") if isinstance(payload.get("collected_at"), str) else None
    collected = parse_ts(collected_raw)
    return {
        "record_key": record.record_key,
        "rules_version": ctx.rules_version,
        "stage": stage,
        "category": category,
        "reason": reason,
        "facility_id": facility,
        "waste_type": waste_type,
        "collected_at": collected_raw,
        "accounting_date": collected.date().isoformat() if collected is not None else None,
        "record_hash": record.raw_hash,
        "run_id": ctx.run_id,
        "decided_at": ctx.now,
    }


def decide_group(
    records: list[RecordRow],
    entries: dict[str, list[dict[str, Any]]],
    reversed_ids: set[str],
    exclusions: set[tuple[str, str]],
    ctx: DecisionContext,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """对同一事件组（同 facility+batch 的全部观测）做确定性决策。

    返回 (账本行, 排除行)。纯函数：相同输入与已有状态必然产生相同输出，
    这是幂等重跑与故障恢复的基础。
    """
    ledger_rows: list[dict[str, Any]] = []
    exclusion_rows: list[dict[str, Any]] = []
    emitted: set[tuple[str, str]] = set()

    def exclude(record: RecordRow, stage: str, category: str, reason: str) -> None:
        key = (record.record_key, stage)
        if key in exclusions or key in emitted:
            return
        emitted.add(key)
        exclusion_rows.append(_exclusion_row(record, stage, category, reason, ctx))

    def reverse(record: RecordRow, cause: str) -> None:
        for entry in entries.get(record.record_key, ()):
            if entry["ledger_id"] in reversed_ids:
                continue
            reversed_ids.add(entry["ledger_id"])
            ledger_rows.append(_reversal_row(entry, cause, ctx))

    # 1) 数据质量门：payload 不可读 / 缺身份字段 / 观测时间不可判 → 输入冲突。
    valid: list[tuple[RecordRow, datetime]] = []
    for record in records:
        if record.payload_error is not None or not isinstance(record.payload, dict):
            exclude(record, RECORD_LEVEL, CATEGORY_INPUT_CONFLICT, "payload_unreadable")
            continue
        facility = record.payload.get("facility_id")
        batch = record.payload.get("batch_id")
        if not _text(facility) or not _text(batch):
            exclude(record, RECORD_LEVEL, CATEGORY_INPUT_CONFLICT, "payload_unreadable")
            continue
        observed = parse_ts(record.observed_at)
        if observed is None:
            exclude(record, RECORD_LEVEL, CATEGORY_INPUT_CONFLICT, "invalid_observed_at")
            continue
        valid.append((record, observed))
    if not valid:
        return ledger_rows, exclusion_rows

    # 2) 同一事件多来源：每个来源取其最新观测；来源间物量签名不一致 → 全组冲突。
    latest_by_source: dict[str, tuple[tuple[datetime, str, str], RecordRow, datetime]] = {}
    for record, observed in valid:
        key = _order_key(record, observed)
        source = record.source or ""
        current = latest_by_source.get(source)
        if current is None or key > current[0]:
            latest_by_source[source] = (key, record, observed)
    latest = [(record, observed) for _, record, observed in latest_by_source.values()]
    signatures = {_material_signature(record) for record, _ in latest}
    if len(signatures) > 1:
        sources = sorted({record.source or "" for record, _ in latest})
        reason = "conflicting_sources:" + "|".join(sources)
        for record, _ in valid:
            exclude(record, RECORD_LEVEL, CATEGORY_INPUT_CONFLICT, reason)
            reverse(record, "conflict")
        return ledger_rows, exclusion_rows

    # 3) 最新观测为准（同 batch 修订 / 多来源重复），其余不纳入；已被核算过的
    #    旧事实用冲销行抵销，历史行本身冻结不动。
    canonical, canonical_observed = max(valid, key=lambda item: _order_key(item[0], item[1]))

    def supersede_others() -> None:
        for record, _ in valid:
            if record is not canonical:
                exclude(record, RECORD_LEVEL, CATEGORY_NOT_INCLUDED, f"superseded_by:{canonical.record_key}")
                reverse(record, canonical.record_key)

    # 4) 同一 record_key 核算后内容被改写：保留冻结结果，记输入冲突。
    canonical_entries = entries.get(canonical.record_key, ())
    if canonical_entries and canonical_entries[0]["record_hash"] != canonical.raw_hash:
        exclude(canonical, RECORD_LEVEL, CATEGORY_INPUT_CONFLICT, "content_changed_after_accounting")
        supersede_others()
        return ledger_rows, exclusion_rows

    payload = canonical.payload
    collected = parse_ts(payload.get("collected_at"))
    if collected is None:
        exclude(canonical, RECORD_LEVEL, CATEGORY_INPUT_CONFLICT, "invalid_collected_at")
        supersede_others()
        return ledger_rows, exclusion_rows

    # 5) 时间窗口按权威事实的 collected_at 判定归属；窗口外的事件整体不在本次
    #    计算范围内（不写任何行），由覆盖其核算日期的分区运行处理。
    if ctx.window_start is not None and collected < ctx.window_start:
        return ledger_rows, exclusion_rows
    if ctx.window_end is not None and collected >= ctx.window_end:
        return ledger_rows, exclusion_rows

    supersede_others()

    # 6) 权威事实的状态筛选与输入校验。
    status = _lower(payload.get("status"))
    if status not in ctx.included_statuses:
        exclude(canonical, RECORD_LEVEL, CATEGORY_NOT_INCLUDED, f"status_excluded:{status or 'missing'}")
        return ledger_rows, exclusion_rows
    weight = _parse_weight(payload.get("net_weight_kg"))
    if weight is None:
        exclude(canonical, RECORD_LEVEL, CATEGORY_INPUT_CONFLICT, "invalid_net_weight_kg")
        return ledger_rows, exclusion_rows
    waste_type = _lower(payload.get("waste_type"))
    if not waste_type:
        exclude(canonical, RECORD_LEVEL, CATEGORY_INPUT_CONFLICT, "missing_field:waste_type")
        return ledger_rows, exclusion_rows
    destination = _text(payload.get("destination_facility_id"))
    if not destination:
        exclude(canonical, RECORD_LEVEL, CATEGORY_INPUT_CONFLICT, "missing_field:destination_facility_id")
        return ledger_rows, exclusion_rows
    facility = _text(payload.get("facility_id"))

    # 7) 幂等：同一 record_key 已按相同内容核算过 → 跳过（重复执行不产生重复行）。
    if canonical_entries:
        return ledger_rows, exclusion_rows

    base = {
        "record_key": canonical.record_key,
        "rules_version": ctx.rules_version,
        "net_weight_kg": dec_str(weight),
        "facility_id": facility,
        "destination_facility_id": destination,
        "waste_type": waste_type,
        "status": status,
        "collected_at": payload.get("collected_at"),
        "observed_at": canonical.observed_at,
        "accounting_date": collected.date().isoformat(),
        "record_hash": canonical.raw_hash,
        "run_id": ctx.run_id,
        "computed_at": ctx.now,
    }

    # 8) 逐阶段独立应用因子；任一因子缺失只影响对应阶段，绝不补默认值。
    waste_entry = ctx.factors.waste_type_factor(waste_type)
    if waste_entry is None:
        exclude(canonical, STAGE_WASTE_TYPE, CATEGORY_FACTOR_MISSING, f"no_waste_type_factor:{waste_type}")
    else:
        ledger_rows.append(_entry_row(base, STAGE_WASTE_TYPE, FACTOR_KIND_WASTE_TYPE, waste_entry, weight))

    if facility == destination:
        explicit = ctx.factors.edge_factor(facility, destination)
        if explicit is not None:
            ledger_rows.append(_entry_row(base, STAGE_TRANSPORT, FACTOR_KIND_EDGE, explicit, weight))
        elif ctx.self_loop_policy == SELF_LOOP_ZERO:
            ledger_rows.append(_self_loop_entry_row(base))
        else:
            exclude(canonical, STAGE_TRANSPORT, CATEGORY_FACTOR_MISSING, f"no_edge_factor:{facility}>{destination}")
    else:
        edge_entry = ctx.factors.edge_factor(facility, destination)
        if edge_entry is None:
            exclude(canonical, STAGE_TRANSPORT, CATEGORY_FACTOR_MISSING, f"no_edge_factor:{facility}>{destination}")
        else:
            ledger_rows.append(_entry_row(base, STAGE_TRANSPORT, FACTOR_KIND_EDGE, edge_entry, weight))

    stage_entry = ctx.factors.stage_factor(status)
    if stage_entry is None:
        exclude(canonical, STAGE_PROCESSING, CATEGORY_FACTOR_MISSING, f"no_stage_factor:{status}")
    else:
        ledger_rows.append(_entry_row(base, STAGE_PROCESSING, FACTOR_KIND_STAGE, stage_entry, weight))

    return ledger_rows, exclusion_rows


class AccountingEngine:
    """按设施与时间分区增量计算的核算引擎。

    - 读取 records 快照（只读），按 (facility_id, batch_id) 事件分组决策。
    - 分块流式读取（chunk_size 控制内存），每块一个事务提交。
    - 崩溃恢复：已提交的块幂等跳过，未提交的块重算；遗留 running 运行
      在下次运行开始时被标记为 interrupted。
    """

    def __init__(
        self,
        database: str,
        *,
        chunk_size: int = 500,
        included_statuses: Sequence[str] = DEFAULT_INCLUDED_STATUSES,
        self_loop_policy: str = SELF_LOOP_ZERO,
        lock_retries: int = 5,
        lock_retry_delay: float = 0.05,
        busy_timeout_ms: int = 5000,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self_loop_policy not in (SELF_LOOP_ZERO, SELF_LOOP_REQUIRE_FACTOR):
            raise ValueError("self_loop_policy must be 'zero' or 'require_factor'")
        statuses = tuple(dict.fromkeys(_lower(status) for status in included_statuses))
        if not all(statuses):
            raise ValueError("included_statuses must be non-empty status names")
        self.chunk_size = chunk_size
        self.included_statuses = statuses
        self.self_loop_policy = self_loop_policy
        self.store = LedgerStore(database, lock_retries=lock_retries, lock_retry_delay=lock_retry_delay, busy_timeout_ms=busy_timeout_ms)

    def close(self) -> None:
        self.store.close()

    def init_schema(self) -> None:
        self.store.init_schema()

    def compute(
        self,
        factors: FactorSet,
        rules_version: str,
        *,
        facilities: Sequence[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        on_chunk: Callable[[], None] | None = None,
    ) -> ComputeResult:
        """对 records 快照执行一次增量核算运行。

        ``facilities`` 限定设施分区；``date_from``/``date_to`` 为带时区的
        ISO 8601 边界，按 collected_at 以 [start, end) 判定归属。
        ``on_chunk`` 在每块提交后回调（测试可借此模拟崩溃）。
        """
        if not isinstance(factors, FactorSet):
            raise TypeError("factors must be a FactorSet")
        version = rules_version.strip() if isinstance(rules_version, str) else ""
        if not version:
            raise ValueError("rules_version must be a non-empty string")
        window_start = parse_ts(date_from) if date_from is not None else None
        window_end = parse_ts(date_to) if date_to is not None else None
        if date_from is not None and window_start is None:
            raise ValueError("date_from must be ISO 8601 with timezone")
        if date_to is not None and window_end is None:
            raise ValueError("date_to must be ISO 8601 with timezone")
        if window_start is not None and window_end is not None and window_start >= window_end:
            raise ValueError("date_from must be before date_to")

        self.store.init_schema()
        rule_hash = rule_digest(factors, self.included_statuses, self.self_loop_policy)
        rule_json = json.dumps(rule_payload(factors, self.included_statuses, self.self_loop_policy), ensure_ascii=False, sort_keys=True)
        run_id = uuid.uuid4().hex
        started_at = _utc_now()
        self.store.begin_run(run_id, version, rule_hash, rule_json, facilities, date_from, date_to)

        ctx = DecisionContext(
            factors=factors,
            rules_version=version,
            included_statuses=self.included_statuses,
            self_loop_policy=self.self_loop_policy,
            window_start=window_start,
            window_end=window_end,
            run_id=run_id,
            now=started_at,
        )
        counters = {"records": 0, "groups": 0, "entries": 0, "reversals": 0, "exclusions": 0}
        try:
            self._scan(factors, facilities, ctx, counters, on_chunk)
            finished_at = _utc_now()
            self.store.finish_run(run_id, "committed", counters)
            status = "committed"
        except Exception as exc:
            finished_at = _utc_now()
            try:
                self.store.finish_run(run_id, "failed", counters, str(exc))
            except Exception:
                pass
            raise
        return ComputeResult(
            run_id=run_id,
            rules_version=version,
            status=status,
            records_seen=counters["records"],
            groups_seen=counters["groups"],
            entries_written=counters["entries"],
            reversals_written=counters["reversals"],
            exclusions_written=counters["exclusions"],
            started_at=started_at,
            finished_at=finished_at,
        )

    def _scan(
        self,
        factors: FactorSet,
        facilities: Sequence[str] | None,
        ctx: DecisionContext,
        counters: dict[str, int],
        on_chunk: Callable[[], None] | None,
    ) -> None:
        offset = 0
        pending: list[list[RecordRow]] = []
        current: list[RecordRow] = []
        current_key: tuple[str, str] | None = None
        while True:
            rows = self.store.fetch_records_chunk(offset, self.chunk_size, facilities)
            if not rows:
                break
            offset += len(rows)
            for raw in rows:
                record = _parse_record_row(raw)
                counters["records"] += 1
                key = _group_key(record)
                if current and key != current_key:
                    pending.append(current)
                    current = []
                current_key = key
                current.append(record)
            if pending:
                self._flush(pending, ctx, counters)
                if on_chunk is not None:
                    on_chunk()
                pending = []
        if current:
            self._flush([current], ctx, counters)
            if on_chunk is not None:
                on_chunk()

    def _flush(self, groups: list[list[RecordRow]], ctx: DecisionContext, counters: dict[str, int]) -> None:
        """把一批已完成事件组的决策作为一个事务写入（组内写入原子）。"""

        def work() -> None:
            for group in groups:
                keys = sorted({record.record_key for record in group})
                entries, reversed_ids, exclusions = self.store.load_group_state(keys, ctx.rules_version)
                ledger_rows, exclusion_rows = decide_group(group, entries, reversed_ids, exclusions, ctx)
                counters["groups"] += 1
                entries_rows = [row for row in ledger_rows if row["row_kind"] == "entry"]
                reversal_rows = [row for row in ledger_rows if row["row_kind"] == "reversal"]
                counters["entries"] += self.store.insert_ledger_rows(entries_rows)
                counters["reversals"] += self.store.insert_ledger_rows(reversal_rows)
                counters["exclusions"] += self.store.insert_exclusions(exclusion_rows)

        self.store.transaction(work)
