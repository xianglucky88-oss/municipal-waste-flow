"""批次重量平衡核算的合成链路 + SQLite 端到端测试。"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

import pytest

from municipal_waste.application import run_fixture
from municipal_waste.balance import (
    BalanceCancelled,
    BalanceMigrationError,
    BalanceRequest,
    BalanceResourceLimit,
    BalanceStorageError,
)
from municipal_waste.balance_service import BalanceEngine, CancellationToken
from municipal_waste.balance_store import get_run, export_run, list_runs

WINDOW = {"window_start": "2026-11-01T00:00:00Z", "window_end": "2026-11-30T00:00:00Z"}


def _rec(batch, facility, status, weight, collected_at, *, observed_at=None, destination="P-01", source="test"):
    return {
        "source": source,
        "observed_at": observed_at or collected_at,
        "payload": {
            "facility_id": facility,
            "batch_id": batch,
            "collected_at": collected_at,
            "waste_type": "mixed",
            "net_weight_kg": weight,
            "origin_zone": "Z-1",
            "destination_facility_id": destination,
            "status": status,
        },
    }


def _ingest(tmp_path, records, name="balance.sqlite3"):
    database = tmp_path / name
    fixture = tmp_path / f"fixture-{len(list(tmp_path.glob('fixture-*')))}.json"
    fixture.write_text(json.dumps(records), encoding="utf-8")
    run_fixture(str(fixture), str(database))
    return str(database)


def _insert_raw(database, record_key, payload, observed_at="2026-11-02T10:00:00Z"):
    """绕过 normalize 直接写 records，模拟旧库中的脏数据。"""
    conn = sqlite3.connect(database)
    conn.execute(
        "INSERT INTO records(record_key, source, observed_at, payload_json, raw_hash, first_seen_at, updated_at)"
        " VALUES (?, 'legacy', ?, ?, ?, '2026-11-01T00:00:00+00:00', '2026-11-01T00:00:00+00:00')",
        (record_key, observed_at, json.dumps(payload), f"rawhash-{record_key}"),
    )
    conn.commit()
    conn.close()


def _run(database, **overrides):
    params = dict(WINDOW)
    params.update(overrides)
    return BalanceEngine(database).run(BalanceRequest(**params))


def _batch(database, run_id, batch_id):
    document = get_run(database, run_id)
    for entry in document["batches"]:
        if entry["batch_id"] == batch_id:
            return entry["result"]
    raise AssertionError(f"batch {batch_id} not in run {run_id}")


def test_balanced_chain_within_tolerance(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-1", "T-01", "collected", 1000, "2026-11-02T10:00:00Z", destination="P-01"),
        _rec("B-1", "P-01", "received", 995, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-02"),
        _rec("B-1", "P-01", "processed", 995, "2026-11-02T10:00:00Z", observed_at="2026-11-02T14:00:00Z", destination="P-01"),
    ])
    summary = _run(db, tolerance_ratio=0.01)
    assert summary["status"] == "completed"
    assert summary["verdicts"]["balanced"] == 1
    result = _batch(db, summary["run_id"], "B-1")
    assert result["verdict"] == "balanced"
    assert result["input_kg"] == 1000.0
    assert result["received_kg"] == 995.0
    assert result["processed_kg"] == 995.0
    assert result["balance_diff_kg"] == 5.0
    assert result["tolerance_kg"] == 10.0
    assert result["transfer_loss_kg"] == 5.0
    assert result["evidence"]["input"] and result["evidence"]["processed"]


def test_gap_and_surplus_detected_beyond_tolerance(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-GAP", "T-01", "collected", 1000, "2026-11-02T10:00:00Z"),
        _rec("B-GAP", "P-01", "processed", 700, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
        _rec("B-SUR", "T-02", "collected", 1000, "2026-11-03T10:00:00Z"),
        _rec("B-SUR", "P-02", "processed", 1200, "2026-11-03T10:00:00Z", observed_at="2026-11-03T12:00:00Z", destination="P-02"),
    ])
    summary = _run(db, tolerance_ratio=0.01)
    assert summary["verdicts"] == {"balanced": 0, "gap": 1, "surplus": 1, "insufficient": 0}
    assert _batch(db, summary["run_id"], "B-GAP")["balance_diff_kg"] == 300.0
    assert _batch(db, summary["run_id"], "B-SUR")["balance_diff_kg"] == -200.0


def test_tolerance_boundary_is_inclusive(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-EQ", "T-01", "collected", 1000, "2026-11-02T10:00:00Z"),
        _rec("B-EQ", "P-01", "processed", 990, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
        _rec("B-OVER", "T-01", "collected", 1000, "2026-11-02T10:00:00Z"),
        _rec("B-OVER", "P-01", "processed", 989.999, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
    ])
    summary = _run(db, tolerance_ratio=0.01)
    assert _batch(db, summary["run_id"], "B-EQ")["verdict"] == "balanced"
    assert _batch(db, summary["run_id"], "B-OVER")["verdict"] == "gap"


def test_rejected_weight_never_counts_as_processed(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-REJ", "T-01", "collected", 1000, "2026-11-02T10:00:00Z"),
        _rec("B-REJ", "P-01", "rejected", 300, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="L-01"),
        _rec("B-REJ", "P-01", "processed", 700, "2026-11-02T10:00:00Z", observed_at="2026-11-02T14:00:00Z", destination="P-01"),
    ])
    summary = _run(db)
    result = _batch(db, summary["run_id"], "B-REJ")
    assert result["verdict"] == "balanced"
    assert result["processed_kg"] == 700.0
    assert result["rejected_kg"] == 300.0
    assert result["output_kg"] == 1000.0


def test_same_record_never_counts_as_input_and_output(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-ONE", "T-01", "collected", 1000, "2026-11-02T10:00:00Z"),
    ])
    summary = _run(db)
    result = _batch(db, summary["run_id"], "B-ONE")
    assert result["verdict"] == "insufficient"
    assert result["input_kg"] == 1000.0
    assert result["processed_kg"] == 0.0
    assert result["output_kg"] == 0.0
    assert result["balance_diff_kg"] is None
    assert any(link["stage"] == "terminal" for link in result["missing_links"])


def test_duplicate_weighing_latest_observation_wins(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-DUP", "T-01", "collected", 1000, "2026-11-02T10:00:00Z", observed_at="2026-11-02T10:00:00Z"),
    ])
    # 第二个来源对同一批次同一设施重复报量（更晚的观测）
    _ingest(tmp_path, [
        _rec("B-DUP", "T-01", "collected", 1200, "2026-11-02T10:00:00Z", observed_at="2026-11-02T11:00:00Z"),
        _rec("B-DUP", "P-01", "processed", 1200, "2026-11-02T10:00:00Z", observed_at="2026-11-02T15:00:00Z", destination="P-01"),
    ], name="balance.sqlite3")
    summary = _run(db)
    result = _batch(db, summary["run_id"], "B-DUP")
    assert result["verdict"] == "balanced"
    assert result["input_kg"] == 1200.0
    anomalies = [a for a in result["anomalies"] if a["type"] == "duplicate_weighing"]
    assert len(anomalies) == 1
    assert anomalies[0]["count"] == 2
    assert len(anomalies[0]["evidence"]) == 2
    assert anomalies[0]["sources"] == ["fixture://fixture-0.json", "fixture://fixture-1.json"]


def test_late_revision_recomputes_and_preserves_history(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-8", "T-01", "collected", 1000, "2026-11-02T10:00:00Z", observed_at="2026-11-02T10:00:00Z"),
        _rec("B-8", "P-01", "processed", 1000, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
        _rec("B-9", "T-02", "collected", 500, "2026-11-03T10:00:00Z"),
        _rec("B-9", "P-02", "processed", 500, "2026-11-03T10:00:00Z", observed_at="2026-11-03T12:00:00Z", destination="P-02"),
    ])
    first = _run(db, tolerance_ratio=0.01)
    assert first["verdicts"]["balanced"] == 2
    # 迟到修订：同一 record_key（batch|facility|observed_at）重量被改写
    _ingest(tmp_path, [
        _rec("B-8", "T-01", "collected", 900, "2026-11-02T10:00:00Z", observed_at="2026-11-02T10:00:00Z"),
    ], name="balance.sqlite3")
    second = _run(db, tolerance_ratio=0.01)
    assert second["run_id"] != first["run_id"]
    assert second["batches_computed"] == 1  # 只有输入变化的批次被重算
    assert second["batches_reused"] == 1
    revised = _batch(db, second["run_id"], "B-8")
    assert revised["verdict"] == "surplus"
    assert revised["input_kg"] == 900.0
    assert revised["balance_diff_kg"] == -100.0
    # 历史运行结果不被覆盖
    original = _batch(db, first["run_id"], "B-8")
    assert original["input_kg"] == 1000.0
    assert original["verdict"] == "balanced"


def test_facility_cycle_flagged_without_double_counting(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-CYC", "F-A", "collected", 100, "2026-11-02T10:00:00Z", destination="F-B"),
        _rec("B-CYC", "F-A", "in_transit", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T11:00:00Z", destination="F-B"),
        _rec("B-CYC", "F-B", "in_transit", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="F-A"),
        _rec("B-CYC", "F-B", "processed", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T13:00:00Z", destination="F-B"),
    ])
    summary = _run(db)
    result = _batch(db, summary["run_id"], "B-CYC")
    assert result["verdict"] == "balanced"
    cycles = [a for a in result["anomalies"] if a["type"] == "facility_cycle"]
    assert len(cycles) == 1
    assert set(cycles[0]["cycle"]) == {"F-A", "F-B"}
    # 每个 (设施, 状态) 节点只记一次：两个转运节点各 100，环不会放大重量
    assert result["transfer_kg"] == 200.0
    assert len(result["evidence"]["transfer"]) == 2


def test_float_rounding_is_exact_decimal(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-FLT", "F-1", "collected", 0.1, "2026-11-02T10:00:00Z"),
        _rec("B-FLT", "F-2", "collected", 0.2, "2026-11-02T10:00:00Z"),
        _rec("B-FLT", "P-01", "processed", 0.3, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
    ])
    summary = _run(db)  # 容忍度为 0：浮点误差会把 0.1+0.2-0.3 误判为 gap
    result = _batch(db, summary["run_id"], "B-FLT")
    assert result["verdict"] == "balanced"
    assert result["balance_diff_kg"] == 0.0


def test_concurrent_runs_do_not_overwrite_result_versions(tmp_path):
    db = _ingest(tmp_path, [
        _rec(f"B-C{i}", "T-01", "collected", 100 + i, f"2026-11-0{2 + i}T10:00:00Z")
        for i in range(3)
    ] + [
        _rec(f"B-C{i}", "P-01", "processed", 100 + i, f"2026-11-0{2 + i}T10:00:00Z",
             observed_at=f"2026-11-0{2 + i}T12:00:00Z", destination="P-01")
        for i in range(3)
    ])
    outcomes: list[dict] = [None, None]

    def worker(index):
        outcomes[index] = BalanceEngine(db).run(BalanceRequest(**WINDOW))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert all(outcome["status"] == "completed" for outcome in outcomes)
    assert outcomes[0]["run_id"] != outcomes[1]["run_id"]
    runs = list_runs(db)
    assert {run["run_id"] for run in runs} >= {outcomes[0]["run_id"], outcomes[1]["run_id"]}
    first_batches = get_run(db, outcomes[0]["run_id"])["batches"]
    second_batches = get_run(db, outcomes[1]["run_id"])["batches"]
    assert [b["result"] for b in first_batches] == [b["result"] for b in second_batches]


def test_interrupted_run_recovery_via_stale_heartbeat(tmp_path):
    db = _ingest(tmp_path, [_rec("B-1", "T-01", "collected", 10, "2026-11-02T10:00:00Z")])
    completed = _run(db)
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO balance_runs(rule_version, params_json, params_fingerprint, status, created_at, heartbeat_at)"
        " VALUES ('v1', '{}', 'fp', 'running', '2026-11-01T00:00:00+00:00', '2026-11-01T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()
    recovered = BalanceEngine(db).recover_interrupted(stale_after_seconds=60)
    assert recovered == 1
    runs = {run["run_id"]: run["status"] for run in list_runs(db)}
    assert runs[completed["run_id"]] == "completed"
    assert "interrupted" in runs.values()


def test_cancellation_marks_run_cancelled(tmp_path):
    records = []
    for i in range(10):
        records.append(_rec(f"B-X{i}", "T-01", "collected", 10, f"2026-11-0{2 + i % 5}T10:00:00Z"))
    db = _ingest(tmp_path, records)

    class TripToken(CancellationToken):
        def __init__(self, trips):
            super().__init__()
            self._trips = trips

        def throw_if_cancelled(self):
            self._trips -= 1
            if self._trips < 0:
                self.cancel()
            super().throw_if_cancelled()

    with pytest.raises(BalanceCancelled):
        BalanceEngine(db).run(BalanceRequest(chunk_size=1, **WINDOW), TripToken(2))
    assert list_runs(db)[0]["status"] == "cancelled"


def test_lock_conflict_is_recoverable(tmp_path):
    db = _ingest(tmp_path, [_rec("B-1", "T-01", "collected", 10, "2026-11-02T10:00:00Z")])
    blocker = sqlite3.connect(db, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(BalanceStorageError):
            BalanceEngine(db, busy_timeout_ms=100).run(BalanceRequest(**WINDOW))
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()
    summary = BalanceEngine(db, busy_timeout_ms=100).run(BalanceRequest(**WINDOW))
    assert summary["status"] == "completed"


def test_resource_limit_fails_run(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-1", "T-01", "collected", 10, "2026-11-02T10:00:00Z"),
        _rec("B-2", "T-01", "collected", 10, "2026-11-03T10:00:00Z"),
        _rec("B-3", "T-01", "collected", 10, "2026-11-04T10:00:00Z"),
    ])
    with pytest.raises(BalanceResourceLimit):
        _run(db, max_records=2)
    run = list_runs(db)[0]
    assert run["status"] == "failed"
    assert "max_records" in run["error"]


def test_window_endpoints_are_inclusive(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-START", "T-01", "collected", 100, "2026-11-01T00:00:00Z"),
        _rec("B-START", "P-01", "processed", 100, "2026-11-01T00:00:00Z", observed_at="2026-11-01T01:00:00Z", destination="P-01"),
        _rec("B-END", "T-01", "collected", 100, "2026-11-05T00:00:00Z"),
        _rec("B-END", "P-01", "processed", 100, "2026-11-05T00:00:00Z", observed_at="2026-11-05T01:00:00Z", destination="P-01"),
        _rec("B-OUT", "T-01", "collected", 100, "2026-11-05T00:00:01Z"),
        _rec("B-OUT", "P-01", "processed", 100, "2026-11-05T00:00:01Z", observed_at="2026-11-05T02:00:00Z", destination="P-01"),
    ])
    summary = _run(db, window_start="2026-11-01T00:00:00Z", window_end="2026-11-05T00:00:00Z")
    document = get_run(db, summary["run_id"])
    batch_ids = {entry["batch_id"] for entry in document["batches"]}
    assert batch_ids == {"B-START", "B-END"}
    assert summary["verdicts"]["balanced"] == 2


def test_zero_and_negative_weights_excluded_with_reason(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-Z", "T-01", "collected", 100, "2026-11-02T10:00:00Z"),
        _rec("B-Z", "P-01", "processed", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
    ])
    base_payload = {
        "facility_id": "T-01", "batch_id": "B-Z", "collected_at": "2026-11-02T10:00:00Z",
        "waste_type": "mixed", "origin_zone": "Z-1", "destination_facility_id": "P-01", "status": "in_transit",
    }
    _insert_raw(db, "B-Z|T-01|ZERO", {**base_payload, "net_weight_kg": 0})
    _insert_raw(db, "B-Z|T-01|NEG", {**base_payload, "net_weight_kg": -5})
    summary = _run(db)
    result = _batch(db, summary["run_id"], "B-Z")
    assert result["verdict"] == "balanced"
    assert result["transfer_kg"] == 0.0
    reasons = {entry["record_key"]: entry["reason"] for entry in result["excluded_records"]}
    assert reasons == {"B-Z|T-01|ZERO": "non_positive_weight", "B-Z|T-01|NEG": "non_positive_weight"}


def test_missing_destination_flagged_as_missing_link(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-MD", "T-01", "collected", 100, "2026-11-02T10:00:00Z"),
        _rec("B-MD", "P-01", "processed", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
    ])
    _insert_raw(db, "B-MD|P-01|NODST", {
        "facility_id": "P-01", "batch_id": "B-MD", "collected_at": "2026-11-02T10:00:00Z",
        "waste_type": "mixed", "net_weight_kg": 100, "origin_zone": "Z-1",
        "destination_facility_id": "", "status": "received",
    }, observed_at="2026-11-02T11:00:00Z")
    summary = _run(db)
    result = _batch(db, summary["run_id"], "B-MD")
    assert result["verdict"] == "balanced"
    destination_links = [link for link in result["missing_links"] if link["stage"] == "destination"]
    assert destination_links and destination_links[0]["evidence"] == ["B-MD|P-01|NODST"]


def test_status_regression_and_time_interleaving_flagged(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-REG", "T-01", "collected", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T10:00:00Z"),
        _rec("B-REG", "P-01", "received", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T09:00:00Z", destination="P-02"),
        _rec("B-REG", "P-01", "processed", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T11:00:00Z", destination="P-01"),
    ])
    summary = _run(db)
    result = _batch(db, summary["run_id"], "B-REG")
    assert result["verdict"] == "balanced"  # 时间交错不阻断核算
    regressions = [a for a in result["anomalies"] if a["type"] == "status_regression"]
    assert regressions
    assert regressions[0]["from_status"] == "received"
    assert regressions[0]["to_status"] == "collected"


def test_partial_observation_from_facility_filter_is_insufficient(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-PART", "T-01", "collected", 100, "2026-11-02T10:00:00Z"),
        _rec("B-PART", "P-01", "processed", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
    ])
    summary = _run(db, facilities=("T-01",))
    result = _batch(db, summary["run_id"], "B-PART")
    assert result["verdict"] == "insufficient"
    assert any("terminal" in reason for reason in result["reasons"])
    full = _run(db)
    assert _batch(db, full["run_id"], "B-PART")["verdict"] == "balanced"


def test_missing_links_and_insufficient_reasons_listed(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-ML", "P-01", "received", 100, "2026-11-02T10:00:00Z", destination="P-02"),
    ])
    summary = _run(db)
    result = _batch(db, summary["run_id"], "B-ML")
    assert result["verdict"] == "insufficient"
    stages = {link["stage"] for link in result["missing_links"]}
    assert {"collected", "terminal"} <= stages
    assert result["reasons"]


def test_rule_version_validated_and_recorded(tmp_path):
    db = _ingest(tmp_path, [_rec("B-1", "T-01", "collected", 10, "2026-11-02T10:00:00Z")])
    with pytest.raises(ValueError, match="rule_version"):
        _run(db, rule_version="v99")
    summary = _run(db, rule_version="v1")
    runs = list_runs(db)
    assert runs[0]["rule_version"] == "v1"
    assert runs[0]["run_id"] == summary["run_id"]


def test_incremental_recompute_after_new_observations(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-1", "T-01", "collected", 100, "2026-11-02T10:00:00Z"),
        _rec("B-1", "P-01", "processed", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
        _rec("B-2", "T-02", "collected", 200, "2026-11-03T10:00:00Z"),
        _rec("B-2", "P-02", "processed", 200, "2026-11-03T10:00:00Z", observed_at="2026-11-03T12:00:00Z", destination="P-02"),
    ])
    first = _run(db)
    _ingest(tmp_path, [
        _rec("B-3", "T-03", "collected", 300, "2026-11-04T10:00:00Z"),
        _rec("B-3", "P-03", "processed", 300, "2026-11-04T10:00:00Z", observed_at="2026-11-04T12:00:00Z", destination="P-03"),
    ], name="balance.sqlite3")
    second = _run(db)
    assert second["batches_evaluated"] == 3
    assert second["batches_computed"] == 1
    assert second["batches_reused"] == 2
    assert second["input_fingerprint"] != first["input_fingerprint"]
    assert len(get_run(db, first["run_id"])["batches"]) == 2
    for batch_id in ("B-1", "B-2"):
        assert _batch(db, first["run_id"], batch_id) == _batch(db, second["run_id"], batch_id)


def test_export_is_read_only_and_repeatable(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-1", "T-01", "collected", 100, "2026-11-02T10:00:00Z"),
        _rec("B-1", "P-01", "processed", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
    ])
    summary = _run(db)
    first = export_run(db, summary["run_id"])
    second = export_run(db, summary["run_id"])
    assert first == second
    assert first["format"] == "balance-export/v1"
    assert first["run"]["status"] == "completed"
    assert first["batches"][0]["result"]["evidence"]["input"]
    json.dumps(first)  # 可序列化
    assert len(list_runs(db)) == 1  # 导出不产生新运行


def test_sequential_runs_are_repeatable(tmp_path):
    db = _ingest(tmp_path, [
        _rec("B-1", "T-01", "collected", 100, "2026-11-02T10:00:00Z"),
        _rec("B-1", "P-01", "processed", 100, "2026-11-02T10:00:00Z", observed_at="2026-11-02T12:00:00Z", destination="P-01"),
    ])
    first = _run(db)
    second = _run(db)
    assert first["run_id"] != second["run_id"]
    assert first["input_fingerprint"] == second["input_fingerprint"]
    first_batches = get_run(db, first["run_id"])["batches"]
    second_batches = get_run(db, second["run_id"])["batches"]
    assert [b["result"] for b in first_batches] == [b["result"] for b in second_batches]


def test_chunked_reading_with_small_chunks(tmp_path):
    records = []
    for i in range(5):
        records.append(_rec(f"B-K{i}", "T-01", "collected", 50, f"2026-11-0{2 + i}T10:00:00Z"))
        records.append(_rec(f"B-K{i}", "P-01", "processed", 50, f"2026-11-0{2 + i}T10:00:00Z",
                            observed_at=f"2026-11-0{2 + i}T12:00:00Z", destination="P-01"))
    db = _ingest(tmp_path, records)
    summary = _run(db, chunk_size=1)
    assert summary["records_scanned"] == 10
    assert summary["verdicts"]["balanced"] == 5


def test_old_cli_and_old_database_still_work_after_balance(tmp_path):
    fixture = Path(__file__).parents[1] / "fixtures" / "sample.json"
    database = str(tmp_path / "shared.sqlite3")
    first = run_fixture(str(fixture), database)
    assert first["accepted"] == 3
    summary = _run(database, window_start="2026-11-01T00:00:00Z", window_end="2026-11-02T00:00:00Z")
    assert summary["status"] == "completed"
    second = run_fixture(str(fixture), database)  # 旧 CLI 幂等重放不受影响
    assert second["changed"] == 0
    assert second["records"] == 3
    runs = list_runs(database)
    assert len(runs) == 1


def test_migration_rejects_newer_schema_version_without_corruption(tmp_path):
    db = _ingest(tmp_path, [_rec("B-1", "T-01", "collected", 10, "2026-11-02T10:00:00Z")])
    _run(db)
    conn = sqlite3.connect(db)
    conn.execute("UPDATE balance_meta SET value = '999' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    with pytest.raises(BalanceMigrationError):
        _run(db)
    fixture = Path(__file__).parents[1] / "fixtures" / "sample.json"
    result = run_fixture(str(fixture), db)  # 旧链路在迁移失败后仍可用
    assert result["records"] == 4


def test_empty_database_completes_with_zero_batches(tmp_path):
    summary = _run(str(tmp_path / "empty.sqlite3"))
    assert summary["status"] == "completed"
    assert summary["batches_evaluated"] == 0
    assert summary["records_scanned"] == 0
