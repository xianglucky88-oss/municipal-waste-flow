"""废物流转环境因子核算的 SQLite 端到端测试。

覆盖：多阶段因子、边因子缺失、自环、拒绝/回退状态、重复修订、迟到事实、
跨日边界、规则版本回放、浮点精度、并发与故障恢复，以及与既有 ingestion
管线的共存。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

import pytest

from municipal_waste import accounting_cli, ledger_store
from municipal_waste.accounting import (
    CATEGORY_FACTOR_MISSING,
    CATEGORY_INPUT_CONFLICT,
    CATEGORY_NOT_INCLUDED,
    STAGE_PROCESSING,
    STAGE_TRANSPORT,
    STAGE_WASTE_TYPE,
    AccountingEngine,
    FactorVersionConflict,
    LockConflictError,
)
from municipal_waste.accounting_query import AccountingQuery
from municipal_waste.application import run_fixture
from municipal_waste.factors import FactorSet
from municipal_waste.identity import record_key
from municipal_waste.models import WasteRecord
from municipal_waste.storage import DataStore

# ----------------------------------------------------------------------
# 测试辅助
# ----------------------------------------------------------------------

STATUSES = ("collected", "in_transit", "received", "processed")


def make_payload(
    *,
    facility: str = "T-01",
    batch: str = "B-001",
    collected: str = "2026-11-01T10:00:00Z",
    waste_type: str = "mixed",
    weight: object = 1000.0,
    destination: str = "P-01",
    status: str = "received",
) -> dict:
    return {
        "facility_id": facility,
        "batch_id": batch,
        "collected_at": collected,
        "waste_type": waste_type,
        "net_weight_kg": weight,
        "origin_zone": "Z-1",
        "destination_facility_id": destination,
        "status": status,
    }


def ingest(store: DataStore, payload: dict, *, source: str = "src-a", observed: str = "2026-11-01T11:00:00Z") -> str:
    """模拟 ingestion 写入 records 表（可表达 normalize 阶段会拒绝的脏数据）。"""
    key = record_key(str(payload.get("batch_id") or ""), str(payload.get("facility_id") or ""), observed)
    canonical = json.dumps({"source": source, "observed_at": observed, "payload": payload}, ensure_ascii=False, sort_keys=True)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    try:
        weight = float(payload.get("net_weight_kg"))
    except (TypeError, ValueError):
        weight = 0.0
    record = WasteRecord(
        source=source,
        record_key=key,
        observed_at=observed,
        facility_id=str(payload.get("facility_id") or ""),
        batch_id=str(payload.get("batch_id") or ""),
        collected_at=str(payload.get("collected_at") or ""),
        waste_type=str(payload.get("waste_type") or ""),
        net_weight_kg=weight,
        origin_zone=str(payload.get("origin_zone") or ""),
        destination_facility_id=str(payload.get("destination_facility_id") or ""),
        status=str(payload.get("status") or ""),
        payload=payload,
        raw_hash=digest,
    )
    store.upsert(record)
    return key


def make_factors(*, waste: str = "0.5", edge: str = "0.0001", stage: str = "0.02", edges=("T-01>P-01", "T-02>P-01")) -> FactorSet:
    return FactorSet.from_mapping(
        {
            "waste_type_factors": {
                "mixed": {"factor": waste, "unit": "kgCO2e/kg", "result_unit": "kgCO2e", "factor_version": "wt-2026a"},
                "food": {"factor": waste, "unit": "kgCO2e/kg", "result_unit": "kgCO2e", "factor_version": "wt-2026a"},
            },
            "edge_factors": {
                key: {"factor": edge, "unit": "kgCO2e/kg", "result_unit": "kgCO2e", "factor_version": "edge-2026a"}
                for key in edges
            },
            "stage_factors": {
                status: {"factor": stage, "unit": "kgCO2e/kg", "result_unit": "kgCO2e", "factor_version": "st-2026a"}
                for status in STATUSES
            },
        }
    )


@pytest.fixture
def db(tmp_path):
    database = tmp_path / "accounting.sqlite3"
    store = DataStore(str(database))
    yield database, store
    store.close()


def engine_for(database, **kwargs) -> AccountingEngine:
    kwargs.setdefault("chunk_size", 500)
    return AccountingEngine(str(database), **kwargs)


def ledger_rows(database, **filters):
    with AccountingQuery(str(database)) as query:
        return list(query.ledger(**filters))


def exclusion_rows(database, **filters):
    with AccountingQuery(str(database)) as query:
        return list(query.exclusions(**filters))


# ----------------------------------------------------------------------
# 多阶段因子与输出字段
# ----------------------------------------------------------------------

def test_multi_stage_factors_generate_independent_traceable_rows(db):
    database, store = db
    key = ingest(store, make_payload(weight=1000.0))
    engine = engine_for(database)
    result = engine.compute(make_factors(), "v1")
    assert result.status == "committed"
    assert result.entries_written == 3

    rows = ledger_rows(database, rules_version="v1")
    assert len(rows) == 3
    by_stage = {row["stage"]: row for row in rows}
    assert set(by_stage) == {STAGE_WASTE_TYPE, STAGE_TRANSPORT, STAGE_PROCESSING}
    # 计算口径 / 因子版本 / record_key / 原始重量 / 因子值 / 结果值全部落账
    assert by_stage[STAGE_WASTE_TYPE]["factor_kind"] == "waste_type"
    assert by_stage[STAGE_WASTE_TYPE]["factor_version"] == "wt-2026a"
    assert by_stage[STAGE_TRANSPORT]["factor_kind"] == "edge"
    assert by_stage[STAGE_TRANSPORT]["factor_version"] == "edge-2026a"
    assert by_stage[STAGE_PROCESSING]["factor_kind"] == "stage"
    for row in rows:
        assert row["record_key"] == key
        assert row["net_weight_kg"] == "1000"
        assert row["rules_version"] == "v1"
        assert row["row_kind"] == "entry"
        assert row["accounting_date"] == "2026-11-01"
        assert row["result_unit"] == "kgCO2e"
    assert by_stage[STAGE_WASTE_TYPE]["factor_value"] == "0.5"
    assert by_stage[STAGE_WASTE_TYPE]["result_value"] == "500"
    assert by_stage[STAGE_TRANSPORT]["result_value"] == "0.1"
    assert by_stage[STAGE_PROCESSING]["result_value"] == "20"


def test_missing_edge_factor_is_factor_missing_never_fabricated(db):
    database, store = db
    key = ingest(store, make_payload(facility="T-09", destination="P-09"))
    factors = make_factors()  # 只含 T-01>P-01 / T-02>P-01，没有 T-09>P-09
    engine_for(database).compute(factors, "v1")

    rows = ledger_rows(database, rules_version="v1")
    assert {row["stage"] for row in rows} == {STAGE_WASTE_TYPE, STAGE_PROCESSING}
    exclusions = exclusion_rows(database, rules_version="v1")
    assert len(exclusions) == 1
    assert exclusions[0]["record_key"] == key
    assert exclusions[0]["stage"] == STAGE_TRANSPORT
    assert exclusions[0]["category"] == CATEGORY_FACTOR_MISSING
    assert exclusions[0]["reason"] == "no_edge_factor:T-09>P-09"


def test_missing_waste_type_and_stage_factors_are_reported_per_stage(db):
    database, store = db
    ingest(store, make_payload(waste_type="construction", status="received"))
    factors = FactorSet.from_mapping({"edge_factors": {"T-01>P-01": {"factor": "0.1"}}})
    engine_for(database).compute(factors, "v1")
    rows = ledger_rows(database, rules_version="v1")
    assert {row["stage"] for row in rows} == {STAGE_TRANSPORT}
    reasons = {(row["stage"], row["category"], row["reason"]) for row in exclusion_rows(database, rules_version="v1")}
    assert (STAGE_WASTE_TYPE, CATEGORY_FACTOR_MISSING, "no_waste_type_factor:construction") in reasons
    assert (STAGE_PROCESSING, CATEGORY_FACTOR_MISSING, "no_stage_factor:received") in reasons


# ----------------------------------------------------------------------
# 设施自环
# ----------------------------------------------------------------------

def test_self_loop_records_explicit_zero_transport(db):
    database, store = db
    key = ingest(store, make_payload(facility="P-03", destination="P-03", status="processed"))
    engine_for(database).compute(make_factors(), "v1")
    rows = {row["stage"]: row for row in ledger_rows(database, rules_version="v1")}
    transport = rows[STAGE_TRANSPORT]
    assert transport["record_key"] == key
    assert transport["factor_kind"] == "self_loop"
    assert transport["factor_value"] == "0"
    assert transport["result_value"] == "0"
    assert rows[STAGE_WASTE_TYPE]["result_value"] == "500"


def test_self_loop_explicit_edge_factor_wins_over_zero_default(db):
    database, store = db
    ingest(store, make_payload(facility="P-03", destination="P-03", status="processed", weight=1000.0))
    factors = FactorSet.from_mapping(
        {
            "waste_type_factors": {"mixed": {"factor": "0.5"}},
            "edge_factors": {"P-03>P-03": {"factor": "0.05", "factor_version": "loop-1"}},
            "stage_factors": {"processed": {"factor": "0.02"}},
        }
    )
    engine_for(database).compute(factors, "v1")
    transport = [row for row in ledger_rows(database, rules_version="v1") if row["stage"] == STAGE_TRANSPORT][0]
    assert transport["factor_kind"] == "edge"
    assert transport["factor_version"] == "loop-1"
    assert transport["result_value"] == "50"


def test_self_loop_require_factor_policy_reports_missing(db):
    database, store = db
    key = ingest(store, make_payload(facility="P-03", destination="P-03", status="processed"))
    engine = engine_for(database, self_loop_policy="require_factor")
    engine.compute(make_factors(), "v1")
    exclusions = exclusion_rows(database, rules_version="v1")
    transport = [row for row in exclusions if row["record_key"] == key and row["stage"] == STAGE_TRANSPORT]
    assert transport and transport[0]["category"] == CATEGORY_FACTOR_MISSING
    assert transport[0]["reason"] == "no_edge_factor:P-03>P-03"


# ----------------------------------------------------------------------
# 状态筛选、拒绝与回退
# ----------------------------------------------------------------------

def test_rejected_status_is_not_included(db):
    database, store = db
    key = ingest(store, make_payload(status="rejected"))
    engine_for(database).compute(make_factors(), "v1")
    assert ledger_rows(database, rules_version="v1") == []
    exclusions = exclusion_rows(database, rules_version="v1")
    assert len(exclusions) == 1
    assert exclusions[0]["record_key"] == key
    assert exclusions[0]["category"] == CATEGORY_NOT_INCLUDED
    assert exclusions[0]["reason"] == "status_excluded:rejected"


def test_rollback_to_rejected_reverses_frozen_entries(db):
    database, store = db
    first = ingest(store, make_payload(status="received", weight=1000.0), observed="2026-11-01T11:00:00Z")
    engine_for(database).compute(make_factors(), "v1")
    assert len(ledger_rows(database, rules_version="v1")) == 3

    # 回退修订：同一 batch 更新观测为 rejected
    second = ingest(store, make_payload(status="rejected", weight=1000.0), observed="2026-11-01T12:00:00Z")
    engine_for(database).compute(make_factors(), "v1")

    rows = ledger_rows(database, rules_version="v1")
    entries = [row for row in rows if row["row_kind"] == "entry"]
    reversals = [row for row in rows if row["row_kind"] == "reversal"]
    assert len(entries) == 3 and len(reversals) == 3
    assert {row["reverses_ledger_id"] for row in reversals} == {row["ledger_id"] for row in entries}
    assert all(Decimal(row["result_value"]) < 0 for row in reversals)

    with AccountingQuery(str(database)) as query:
        totals = query.summary(rules_version="v1", group_by=("stage",))
    assert all(row["result_total"] == "0" for row in totals)

    reasons = {(row["record_key"], row["category"], row["reason"]) for row in exclusion_rows(database, rules_version="v1")}
    assert (first, CATEGORY_NOT_INCLUDED, f"superseded_by:{second}") in reasons
    assert (second, CATEGORY_NOT_INCLUDED, "status_excluded:rejected") in reasons


# ----------------------------------------------------------------------
# 修订、多来源与幂等
# ----------------------------------------------------------------------

def test_duplicate_revisions_are_idempotent_and_latest_wins(db):
    database, store = db
    first = ingest(store, make_payload(weight=1000.0), observed="2026-11-01T11:00:00Z")
    ingest(store, make_payload(weight=1000.0), observed="2026-11-01T11:00:00Z")  # 重复摄取，无变化
    engine = engine_for(database)
    engine.compute(make_factors(), "v1")
    assert len(ledger_rows(database, rules_version="v1")) == 3

    # 修订：新 observed_at、重量改为 1200
    second = ingest(store, make_payload(weight=1200.0), observed="2026-11-01T12:00:00Z")
    engine.compute(make_factors(), "v1")
    rows = ledger_rows(database, rules_version="v1")
    assert len([row for row in rows if row["row_kind"] == "entry"]) == 6
    assert len([row for row in rows if row["row_kind"] == "reversal"]) == 3

    with AccountingQuery(str(database)) as query:
        totals = {row["stage"]: row["result_total"] for row in query.summary(rules_version="v1", group_by=("stage",))}
    assert totals[STAGE_WASTE_TYPE] == "600"  # 1200 * 0.5，旧 1000 已冲销

    # 再次重跑：完全幂等，不产生任何新行
    before = ledger_rows(database, rules_version="v1")
    result = engine.compute(make_factors(), "v1")
    assert result.entries_written == 0 and result.reversals_written == 0 and result.exclusions_written == 0
    assert ledger_rows(database, rules_version="v1") == before
    reasons = {(row["record_key"], row["reason"]) for row in exclusion_rows(database, rules_version="v1")}
    assert (first, f"superseded_by:{second}") in reasons


def test_same_event_multiple_sources_deduplicates(db):
    database, store = db
    payload = make_payload(weight=1000.0)
    first = ingest(store, payload, source="src-a", observed="2026-11-01T11:00:00Z")
    second = ingest(store, dict(payload), source="src-b", observed="2026-11-01T11:05:00Z")
    engine_for(database).compute(make_factors(), "v1")

    rows = ledger_rows(database, rules_version="v1")
    assert len(rows) == 3
    assert {row["record_key"] for row in rows} == {second}
    exclusions = exclusion_rows(database, rules_version="v1")
    assert [(row["category"], row["reason"]) for row in exclusions] == [
        (CATEGORY_NOT_INCLUDED, f"superseded_by:{second}")
    ]
    assert exclusions[0]["record_key"] == first


def test_conflicting_sources_mark_input_conflict_and_reverse(db):
    database, store = db
    first = ingest(store, make_payload(weight=100.0), source="src-a", observed="2026-11-01T11:00:00Z")
    engine_for(database).compute(make_factors(), "v1")
    assert len(ledger_rows(database, rules_version="v1")) == 3

    # 另一来源对同一事件给出冲突重量 → 全组输入冲突，已核算行冲销
    second = ingest(store, make_payload(weight=200.0), source="src-b", observed="2026-11-01T11:05:00Z")
    engine_for(database).compute(make_factors(), "v1")

    rows = ledger_rows(database, rules_version="v1")
    assert len([row for row in rows if row["row_kind"] == "entry"]) == 3
    assert len([row for row in rows if row["row_kind"] == "reversal"]) == 3
    with AccountingQuery(str(database)) as query:
        totals = query.summary(rules_version="v1", group_by=("stage",))
    assert all(row["result_total"] == "0" for row in totals)

    exclusions = exclusion_rows(database, rules_version="v1")
    assert {row["record_key"] for row in exclusions} == {first, second}
    assert all(row["category"] == CATEGORY_INPUT_CONFLICT for row in exclusions)
    assert all(row["reason"] == "conflicting_sources:src-a|src-b" for row in exclusions)


def test_content_change_after_accounting_is_conflict_and_frozen(db):
    database, store = db
    key = ingest(store, make_payload(weight=100.0), observed="2026-11-01T11:00:00Z")
    engine = engine_for(database)
    engine.compute(make_factors(), "v1")

    # 同一 observed_at（同 record_key）改写内容：records 表被 upsert 覆盖
    ingest(store, make_payload(weight=999.0), observed="2026-11-01T11:00:00Z")
    engine.compute(make_factors(), "v1")

    rows = ledger_rows(database, rules_version="v1")
    assert len(rows) == 3  # 冻结结果保留，未按新内容重算
    assert all(row["net_weight_kg"] == "100" for row in rows)
    exclusions = exclusion_rows(database, rules_version="v1")
    assert [(row["record_key"], row["category"], row["reason"]) for row in exclusions] == [
        (key, CATEGORY_INPUT_CONFLICT, "content_changed_after_accounting")
    ]


# ----------------------------------------------------------------------
# 零/负/非有限重量
# ----------------------------------------------------------------------

@pytest.mark.parametrize("weight", [0, -5, "abc", float("nan"), float("inf")])
def test_zero_negative_and_nonfinite_weight_is_input_conflict(db, weight):
    database, store = db
    key = ingest(store, make_payload(weight=weight))
    engine_for(database).compute(make_factors(), "v1")
    assert ledger_rows(database, rules_version="v1") == []
    exclusions = exclusion_rows(database, rules_version="v1")
    assert [(row["record_key"], row["category"], row["reason"]) for row in exclusions] == [
        (key, CATEGORY_INPUT_CONFLICT, "invalid_net_weight_kg")
    ]


# ----------------------------------------------------------------------
# 时间口径、跨日边界与迟到事实
# ----------------------------------------------------------------------

def test_cross_day_boundary_window_and_timezone(db):
    database, store = db
    at_start = ingest(store, make_payload(batch="B-01", collected="2026-11-01T00:00:00Z"))
    before_start = ingest(store, make_payload(batch="B-02", collected="2026-10-31T23:59:59Z"))
    at_end = ingest(store, make_payload(batch="B-03", collected="2026-11-02T00:00:00Z"))
    shifted = ingest(store, make_payload(batch="B-04", collected="2026-11-01T01:30:00+02:00"))  # = 10-31T23:30Z
    naive = ingest(store, make_payload(batch="B-05", collected="2026-11-01T10:00:00"))  # 无时区
    late_same_day = ingest(store, make_payload(batch="B-06", collected="2026-11-01T23:59:59Z"))

    engine_for(database).compute(
        make_factors(), "v1", date_from="2026-11-01T00:00:00Z", date_to="2026-11-02T00:00:00Z"
    )
    computed = {row["record_key"] for row in ledger_rows(database, rules_version="v1")}
    assert computed == {at_start, late_same_day}
    # 窗口外事实不产生任何行（不属于本次分区范围）
    excluded_keys = {row["record_key"] for row in exclusion_rows(database, rules_version="v1")}
    assert before_start not in excluded_keys
    assert at_end not in excluded_keys
    assert shifted not in excluded_keys
    # 无时区时间戳无法判定口径 → 输入冲突
    naive_rows = [row for row in exclusion_rows(database, rules_version="v1") if row["record_key"] == naive]
    assert [(row["category"], row["reason"]) for row in naive_rows] == [(CATEGORY_INPUT_CONFLICT, "invalid_collected_at")]
    # 核算日期按 collected_at 的 UTC 日期
    rows = ledger_rows(database, rules_version="v1")
    assert {row["accounting_date"] for row in rows} == {"2026-11-01"}


def test_timezone_offset_converts_to_utc_accounting_date(db):
    database, store = db
    key = ingest(store, make_payload(collected="2026-11-01T01:30:00+02:00"))  # UTC 10-31 23:30
    engine_for(database).compute(make_factors(), "v1")
    rows = ledger_rows(database, rules_version="v1")
    assert len(rows) == 3
    assert {row["accounting_date"] for row in rows} == {"2026-10-31"}
    assert {row["record_key"] for row in rows} == {key}


def test_late_fact_joins_frozen_partition_without_rewriting(db):
    database, store = db
    early = ingest(store, make_payload(batch="B-01", collected="2026-11-01T10:00:00Z"), observed="2026-11-01T11:00:00Z")
    window = {"date_from": "2026-11-01T00:00:00Z", "date_to": "2026-11-02T00:00:00Z"}
    engine = engine_for(database)
    engine.compute(make_factors(), "v1", **window)
    before = ledger_rows(database, rules_version="v1")
    assert {row["record_key"] for row in before} == {early}

    # 迟到事实：观测时间晚，但 collected_at 仍落在已计算的分区
    late = ingest(store, make_payload(batch="B-02", collected="2026-11-01T09:00:00Z"), observed="2026-11-03T08:00:00Z")
    engine.compute(make_factors(), "v1", **window)

    after = ledger_rows(database, rules_version="v1")
    assert {row["record_key"] for row in after} == {early, late}
    early_before = [row for row in before if row["record_key"] == early]
    early_after = [row for row in after if row["record_key"] == early]
    assert early_before == early_after  # 历史行完全未变（冻结）
    assert all(row["accounting_date"] == "2026-11-01" for row in after)


# ----------------------------------------------------------------------
# 规则版本回放与因子/单位变更守卫
# ----------------------------------------------------------------------

def test_rules_version_replay_keeps_both_versions_and_freezes_old(db):
    database, store = db
    ingest(store, make_payload(weight=1000.0))
    engine = engine_for(database)
    engine.compute(make_factors(waste="0.5"), "v1")
    engine.compute(make_factors(waste="0.7"), "v2")  # 因子变更 → 新版本回放

    v1_rows = ledger_rows(database, rules_version="v1")
    v2_rows = ledger_rows(database, rules_version="v2")
    assert len(v1_rows) == len(v2_rows) == 3
    v1_waste = [row for row in v1_rows if row["stage"] == STAGE_WASTE_TYPE][0]
    v2_waste = [row for row in v2_rows if row["stage"] == STAGE_WASTE_TYPE][0]
    assert v1_waste["result_value"] == "500"
    assert v2_waste["result_value"] == "700"

    with AccountingQuery(str(database)) as query:
        totals = {row["rules_version"]: row["result_total"] for row in query.summary(group_by=("rules_version",))}
    assert totals["v1"] != totals["v2"]

    # 同版本同因子重跑幂等；同版本不同因子被拒绝
    assert engine.compute(make_factors(waste="0.5"), "v1").entries_written == 0
    with pytest.raises(FactorVersionConflict):
        engine.compute(make_factors(waste="0.9"), "v1")
    assert ledger_rows(database, rules_version="v1") == v1_rows  # 旧版本结果冻结


def test_rule_change_in_status_filter_requires_new_version(db):
    database, store = db
    ingest(store, make_payload(status="received"))
    engine_for(database).compute(make_factors(), "v1")
    narrowed = engine_for(database, included_statuses=("received",))
    with pytest.raises(FactorVersionConflict):
        narrowed.compute(make_factors(), "v1")
    narrowed.compute(make_factors(), "v2")  # 新版本可回放
    assert len(ledger_rows(database, rules_version="v2")) == 3


# ----------------------------------------------------------------------
# 浮点精度
# ----------------------------------------------------------------------

def test_decimal_precision_has_no_float_noise(db):
    database, store = db
    for index in range(3):
        ingest(store, make_payload(batch=f"B-{index}", weight=0.1), observed=f"2026-11-01T11:0{index}:00Z")
    ingest(store, make_payload(batch="B-big", weight=100000000.1), observed="2026-11-01T12:00:00Z")
    factors = FactorSet.from_mapping(
        {
            "waste_type_factors": {"mixed": {"factor": "0.1"}},
            "edge_factors": {"T-01>P-01": {"factor": "0.000001"}},
            "stage_factors": {"received": {"factor": "0.1"}},
        }
    )
    engine_for(database).compute(factors, "v1")

    waste_rows = [row for row in ledger_rows(database, rules_version="v1") if row["stage"] == STAGE_WASTE_TYPE]
    small = [row for row in waste_rows if row["net_weight_kg"] == "0.1"]
    assert len(small) == 3
    assert {row["result_value"] for row in small} == {"0.01"}
    big = [row for row in ledger_rows(database, rules_version="v1") if row["stage"] == STAGE_TRANSPORT and row["net_weight_kg"] == "100000000.1"]
    assert big[0]["result_value"] == "100.0000001"

    with AccountingQuery(str(database)) as query:
        totals = {row["stage"]: row["result_total"] for row in query.summary(rules_version="v1", group_by=("stage",))}
    assert totals[STAGE_WASTE_TYPE] == "10000000.04"  # 3*0.01 + 10000000.01，无浮点误差
    assert totals[STAGE_TRANSPORT] == "100.0000004"


# ----------------------------------------------------------------------
# 并发、锁冲突与故障恢复
# ----------------------------------------------------------------------

def test_concurrent_compute_serialized_without_duplicates(db):
    database, store = db
    for index in range(5):
        ingest(store, make_payload(batch=f"B-{index}"), observed=f"2026-11-01T11:0{index}:00Z")

    def run(version: str):
        engine = engine_for(database)
        try:
            return engine.compute(make_factors(), version)
        finally:
            engine.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(run, ["v1", "v2", "v3", "v4"]))
    assert all(result.status == "committed" for result in results)
    for version in ("v1", "v2", "v3", "v4"):
        rows = ledger_rows(database, rules_version=version)
        assert len(rows) == 15
        assert len({row["ledger_id"] for row in rows}) == 15


def test_lock_conflict_raises_then_recovers(db):
    database, store = db
    ingest(store, make_payload())
    blocker = sqlite3.connect(str(database))
    blocker.execute("BEGIN IMMEDIATE")
    blocker.execute("CREATE TABLE IF NOT EXISTS lock_probe (id INTEGER)")
    try:
        engine = engine_for(database, lock_retries=1, lock_retry_delay=0.01, busy_timeout_ms=50)
        with pytest.raises(LockConflictError):
            engine.compute(make_factors(), "v1")
        engine.close()
    finally:
        blocker.rollback()
        blocker.close()
    result = engine_for(database).compute(make_factors(), "v1")
    assert result.status == "committed"
    assert len(ledger_rows(database, rules_version="v1")) == 3


def test_crash_between_chunks_recovers_without_duplicates(db):
    database, store = db
    for index in range(4):
        ingest(store, make_payload(batch=f"B-{index}"), observed=f"2026-11-01T11:0{index}:00Z")

    engine = engine_for(database, chunk_size=1)
    commits = {"count": 0}

    def crash_on_second_chunk():
        commits["count"] += 1
        if commits["count"] == 2:
            raise RuntimeError("simulated crash after chunk commit")

    with pytest.raises(RuntimeError, match="simulated crash"):
        engine.compute(make_factors(), "v1", on_chunk=crash_on_second_chunk)
    engine.close()

    with AccountingQuery(str(database)) as query:
        runs = query.runs()
    assert [run["status"] for run in runs] == ["failed"]
    partial = len(ledger_rows(database, rules_version="v1"))
    assert 0 < partial < 12  # 已提交的块保留

    result = engine_for(database, chunk_size=1).compute(make_factors(), "v1")
    assert result.status == "committed"
    rows = ledger_rows(database, rules_version="v1")
    assert len(rows) == 12
    assert len({row["ledger_id"] for row in rows}) == 12  # 无重复账本行
    with AccountingQuery(str(database)) as query:
        statuses = sorted(run["status"] for run in query.runs())
    assert statuses == ["committed", "failed"]


def test_crash_during_chunk_rolls_back_and_recovers(db, monkeypatch):
    database, store = db
    for index in range(3):
        ingest(store, make_payload(batch=f"B-{index}"), observed=f"2026-11-01T11:0{index}:00Z")

    def failing_insert(self, rows):
        raise RuntimeError("simulated crash during chunk transaction")

    monkeypatch.setattr(ledger_store.LedgerStore, "insert_ledger_rows", failing_insert)
    with pytest.raises(RuntimeError, match="simulated crash"):
        engine_for(database).compute(make_factors(), "v1")
    assert ledger_rows(database, rules_version="v1") == []  # 未提交任何块

    monkeypatch.undo()
    result = engine_for(database).compute(make_factors(), "v1")
    assert result.status == "committed"
    assert len(ledger_rows(database, rules_version="v1")) == 9


def test_interrupted_run_is_marked_on_recovery(db):
    database, store = db
    ingest(store, make_payload())
    # 手工留下一个 running 运行模拟崩溃现场
    engine = engine_for(database)
    engine.init_schema()
    engine.store.begin_run("stale-run", "v0", "hash", "{}", None, None, None)
    engine.close()

    result = engine_for(database).compute(make_factors(), "v1")
    assert result.status == "committed"
    with AccountingQuery(str(database)) as query:
        runs = {run["run_id"]: run["status"] for run in query.runs()}
    assert runs["stale-run"] == "interrupted"
    assert runs[result.run_id] == "committed"


def test_migration_failure_rolls_back_and_recovers(db, monkeypatch):
    database, _store = db
    broken = list(ledger_store.MIGRATIONS) + [(2, "broken", ["CREATE TABLE broken_schema ("])]
    monkeypatch.setattr(ledger_store, "MIGRATIONS", broken)
    store = ledger_store.LedgerStore(str(database))
    with pytest.raises(sqlite3.OperationalError):
        store.init_schema()
    tables = {row[0] for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "emission_ledger" not in tables
    assert "schema_migrations" not in tables  # 失败整体回滚

    monkeypatch.setattr(ledger_store, "MIGRATIONS", broken[:1])
    store.init_schema()  # 修复后重跑成功
    tables = {row[0] for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"emission_ledger", "accounting_exclusions", "accounting_runs", "rules_versions"} <= tables
    store.close()


# ----------------------------------------------------------------------
# 查询、汇总与导出
# ----------------------------------------------------------------------

def test_query_filters_summary_and_export(db, tmp_path):
    database, store = db
    ingest(store, make_payload(batch="B-01", facility="T-01", waste_type="mixed", weight=1000.0, collected="2026-11-01T10:00:00Z"))
    ingest(store, make_payload(batch="B-02", facility="T-02", waste_type="food", weight=500.0, collected="2026-11-02T10:00:00Z"))
    engine_for(database).compute(make_factors(), "v1")

    with AccountingQuery(str(database)) as query:
        assert {row["facility_id"] for row in query.ledger(rules_version="v1", facility_id="T-01")} == {"T-01"}
        assert {row["waste_type"] for row in query.ledger(rules_version="v1", waste_type="food")} == {"food"}
        assert {row["accounting_date"] for row in query.ledger(rules_version="v1", date_from="2026-11-02", date_to="2026-11-03")} == {"2026-11-02"}
        assert len(list(query.ledger(rules_version="v1", date_from="2026-11-01", date_to="2026-11-02"))) == 3

        by_facility = {row["facility_id"]: row["result_total"] for row in query.summary(rules_version="v1", group_by=("facility_id",))}
        assert by_facility == {"T-01": "520.1", "T-02": "260.05"}  # 1000*(0.5+0.0001+0.02), 500*(...)

        outcome = query.record_outcome(record_key("B-01", "T-01", "2026-11-01T11:00:00Z"), rules_version="v1")
        assert len(outcome["ledger"]) == 3
        assert outcome["exclusions"] == []

        export_path = tmp_path / "ledger.json"
        count = query.export_json("ledger", export_path, rules_version="v1", facility_id="T-01")
        exported = json.loads(export_path.read_text(encoding="utf-8"))
        assert count == 3 and len(exported) == 3
        assert {row["stage"] for row in exported} == {STAGE_WASTE_TYPE, STAGE_TRANSPORT, STAGE_PROCESSING}

        excl_path = tmp_path / "exclusions.json"
        assert query.export_json("exclusions", excl_path, rules_version="v1") == 0
        assert json.loads(excl_path.read_text(encoding="utf-8")) == []

        assert [run["status"] for run in query.runs()] == ["committed"]
        assert [row["rules_version"] for row in query.rules_versions()] == ["v1"]


# ----------------------------------------------------------------------
# CLI 端到端
# ----------------------------------------------------------------------

def test_cli_compute_export_and_summary(db, tmp_path, capsys):
    database, store = db
    ingest(store, make_payload())
    factors_path = tmp_path / "factors.json"
    factors_path.write_text(
        json.dumps(
            {
                "waste_type_factors": {"mixed": {"factor": "0.5", "unit": "kgCO2e/kg", "result_unit": "kgCO2e", "factor_version": "wt-1"}},
                "edge_factors": {"T-01>P-01": {"factor": "0.0001", "factor_version": "edge-1"}},
                "stage_factors": {"received": {"factor": "0.02", "factor_version": "st-1"}},
            }
        ),
        encoding="utf-8",
    )
    accounting_cli.main(["init-schema", "--database", str(database)])
    accounting_cli.main(["compute", "--database", str(database), "--factors", str(factors_path), "--rules-version", "v1"])
    output = capsys.readouterr().out.strip().splitlines()
    compute_result = json.loads(output[-1])
    assert compute_result["status"] == "committed"
    assert compute_result["entries_written"] == 3

    export_path = tmp_path / "out.json"
    accounting_cli.main(["export", "--database", str(database), "--what", "ledger", "--rules-version", "v1", "--output", str(export_path)])
    assert len(json.loads(export_path.read_text(encoding="utf-8"))) == 3

    accounting_cli.main(["summary", "--database", str(database), "--group-by", "facility_id,stage"])
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert {row["stage"] for row in summary} == {STAGE_WASTE_TYPE, STAGE_TRANSPORT, STAGE_PROCESSING}

    accounting_cli.main(["runs", "--database", str(database)])
    runs = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert [run["status"] for run in runs] == ["committed"]


# ----------------------------------------------------------------------
# 与既有 ingestion 管线共存
# ----------------------------------------------------------------------

def test_ingestion_pipeline_and_accounting_coexist(db, tmp_path):
    database, _store = db
    fixture = Path(__file__).parents[1] / "fixtures" / "sample.json"
    first = run_fixture(str(fixture), str(database), str(tmp_path / "raw"))
    assert first["accepted"] == 3

    factors = FactorSet.from_mapping(
        {
            "waste_type_factors": {"mixed": {"factor": "0.5"}, "food": {"factor": "0.4"}, "recyclable": {"factor": "0.3"}},
            "edge_factors": {"T-01>P-01": {"factor": "0.0001"}, "T-02>P-02": {"factor": "0.0002"}},
            "stage_factors": {status: {"factor": "0.02"} for status in STATUSES},
        }
    )
    result = engine_for(database).compute(factors, "v1")
    assert result.status == "committed"
    rows = ledger_rows(database, rules_version="v1")
    # 3 条有效记录：T-01→P-01、T-02→P-02 各 3 行；P-03 自环 3 行（运输段记零）
    assert len(rows) == 9
    self_loop = [row for row in rows if row["facility_id"] == "P-03" and row["stage"] == STAGE_TRANSPORT]
    assert self_loop[0]["factor_kind"] == "self_loop"

    # 既有 records 表与管线不受影响，仍可继续幂等摄取
    second = run_fixture(str(fixture), str(database), str(tmp_path / "raw"))
    assert second["changed"] == 0
    assert second["records"] == 3
