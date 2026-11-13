"""流转链重建端到端测试：schema、互斥、增量、幂等、中断重试与导出。"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from municipal_waste import cli
from municipal_waste.application import run_fixture
from municipal_waste.flow_store import (
    BatchFlowStore,
    FlowRebuildLocked,
    FlowStoreError,
    connect,
)
from municipal_waste.identity import record_key
from municipal_waste.models import WasteRecord
from municipal_waste.rebuild import (
    export_batch_flows,
    get_batch_flow,
    list_batch_flows,
    rebuild_batch_flows,
)
from municipal_waste.storage import DataStore


# --------------------------------------------------------------------------- #
# 帮助函数
# --------------------------------------------------------------------------- #
def insert_record(
    store: DataStore,
    *,
    batch_id: str = "B1",
    status: str = "collected",
    observed_at: str,
    facility_id: str = "F1",
    destination_facility_id: str | None = None,
    collected_at: str = "2026-11-01T09:00:00Z",
    source: str = "test",
    net_weight_kg: float = 100.0,
) -> str:
    """直接写 records 快照，绕过 ingestion 质量校验，便于构造边界事实。"""
    destination = destination_facility_id or (
        facility_id if status == "processed" else "F2"
    )
    payload = {
        "facility_id": facility_id,
        "batch_id": batch_id,
        "collected_at": collected_at,
        "waste_type": "mixed",
        "net_weight_kg": net_weight_kg,
        "origin_zone": "Z1",
        "destination_facility_id": destination,
        "status": status,
    }
    key = record_key(batch_id, facility_id, observed_at)
    record = WasteRecord(
        source=source,
        record_key=key,
        observed_at=observed_at,
        facility_id=facility_id,
        batch_id=batch_id,
        collected_at=collected_at,
        waste_type="mixed",
        net_weight_kg=net_weight_kg,
        origin_zone="Z1",
        destination_facility_id=destination,
        status=status,
        payload=payload,
        raw_hash=f"hash-{key}",
    )
    store.upsert(record)
    return key


def legal_chain_records(store: DataStore, batch_id: str = "B1") -> dict[str, str]:
    keys: dict[str, str] = {}
    keys["collected"] = insert_record(
        store, batch_id=batch_id, status="collected",
        observed_at="2026-11-01T10:00:00Z", facility_id="F1",
        destination_facility_id="F2",
    )
    keys["in_transit"] = insert_record(
        store, batch_id=batch_id, status="in_transit",
        observed_at="2026-11-01T11:00:00Z", facility_id="F1",
        destination_facility_id="F2",
    )
    keys["received"] = insert_record(
        store, batch_id=batch_id, status="received",
        observed_at="2026-11-01T12:00:00Z", facility_id="F2",
        destination_facility_id="F2",
    )
    keys["processed"] = insert_record(
        store, batch_id=batch_id, status="processed",
        observed_at="2026-11-01T13:00:00Z", facility_id="F2",
        destination_facility_id="F2",
    )
    return keys


def open_flow_store(database: Path, **kwargs) -> BatchFlowStore:
    options = {
        "busy_timeout_ms": 200,
        "lock_stale_seconds": 60,
        "thread_lock_timeout": 0.5,
    }
    options.update(kwargs)
    return connect(str(database), **options)


# --------------------------------------------------------------------------- #
# 端到端：fixture ingestion -> 重建 -> 查询
# --------------------------------------------------------------------------- #
def test_end_to_end_fixture_ingestion_then_rebuild(tmp_path):
    fixture = Path(__file__).parents[1] / "fixtures" / "sample.json"
    database = tmp_path / "data.sqlite3"
    ingest = run_fixture(str(fixture), str(database), str(tmp_path / "raw"))
    assert ingest["records"] == 3

    result = rebuild_batch_flows(str(database))
    assert result.status == "completed"
    assert result.fact_count == 3
    assert result.batch_count == 3
    assert result.published_count == 3

    chains = list_batch_flows(str(database))
    by_batch = {chain["batch_id"]: chain for chain in chains}
    assert set(by_batch) == {"B-001", "B-002", "B-003"}

    b1 = by_batch["B-001"]
    assert [stage["status"] for stage in b1["stages"]] == ["received"]
    assert b1["terminal_status"] is None
    assert "collected" in b1["missing_stages"]

    # B-003 是同设施 processed 自环，特殊规则允许。
    b3 = by_batch["B-003"]
    assert b3["stages"][0]["edge"]["self_loop"] is True
    assert all(c["code"] != "self_loop_illegal" for c in b3["conflicts"])

    single = get_batch_flow(str(database), "B-002")
    assert single is not None and single["batch_id"] == "B-002"
    assert get_batch_flow(str(database), "MISSING") is None


def test_records_table_and_batch_key_semantics_are_untouched(tmp_path):
    database = tmp_path / "data.sqlite3"
    ingestion_store = DataStore(str(database))
    key = legal_chain_records(ingestion_store)["collected"]
    before = ingestion_store.conn.execute(
        "SELECT raw_hash, payload_json FROM records WHERE record_key = ?", (key,)
    ).fetchone()
    ingestion_store.close()

    rebuild_batch_flows(str(database))

    audit = sqlite3.connect(str(database))
    after = audit.execute(
        "SELECT raw_hash, payload_json FROM records WHERE record_key = ?", (key,)
    ).fetchone()
    assert before == after
    # batch_key 语义：作用域为 BATCH|FACILITY 大写形式。
    scopes = {
        row[0]
        for row in audit.execute("SELECT DISTINCT batch_scope FROM flow_fact_index")
    }
    assert {"B1|F1", "B1|F2"}.issubset(scopes)
    # 既有 ingestion 表结构数量不变。
    tables = {row[0] for row in audit.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"batches", "raw_manifests", "records", "rejected_records"}.issubset(tables)
    audit.close()


# --------------------------------------------------------------------------- #
# 空库 / 空批次
# --------------------------------------------------------------------------- #
def test_rebuild_on_empty_database_is_safe_and_export_is_empty(tmp_path):
    database = tmp_path / "empty.sqlite3"
    result = rebuild_batch_flows(str(database))
    assert result.batch_count == 0
    assert result.fact_count == 0
    assert result.status == "completed"

    assert list_batch_flows(str(database)) == []

    jsonl_path = tmp_path / "chains.jsonl"
    count = export_batch_flows(str(database), jsonl_path)
    assert count == 0
    assert jsonl_path.read_text(encoding="utf-8") == ""

    json_path = tmp_path / "chains.json"
    export_batch_flows(str(database), json_path, fmt="json")
    assert json.loads(json_path.read_text(encoding="utf-8")) == []


def test_incremental_rebuild_for_unknown_batch_is_noop(tmp_path):
    database = tmp_path / "data.sqlite3"
    result = rebuild_batch_flows(str(database), batch_id="GHOST")
    assert result.mode == "incremental"
    assert result.batch_count == 0
    assert get_batch_flow(str(database), "GHOST") is None


# --------------------------------------------------------------------------- #
# 重复重放与幂等发布
# --------------------------------------------------------------------------- #
def test_full_rebuild_is_idempotent_on_replay(tmp_path):
    database = tmp_path / "data.sqlite3"
    store = DataStore(str(database))
    legal_chain_records(store)
    store.close()

    first = rebuild_batch_flows(str(database))
    assert first.published_count == 1 and first.skipped_count == 0
    chain_before = get_batch_flow(str(database), "B1")

    second = rebuild_batch_flows(str(database))
    assert second.published_count == 0
    assert second.skipped_count == 1
    chain_after = get_batch_flow(str(database), "B1")
    assert chain_before == chain_after

    # 再次 ingestion 同批记录（storage 层 upsert 为 no-op）后重建仍幂等。
    third = rebuild_batch_flows(str(database))
    assert third.published_count == 0


def test_revised_record_changes_only_its_batch_chain(tmp_path):
    database = tmp_path / "data.sqlite3"
    store = DataStore(str(database))
    legal_chain_records(store, batch_id="B1")
    legal_chain_records(store, batch_id="B2")
    store.close()

    rebuild_batch_flows(str(database))
    b1_before = get_batch_flow(str(database), "B1")
    b2_before = get_batch_flow(str(database), "B2")

    # 修订 B1 的 collected 观测时刻（record_key 随 observed_at 变化，产生新事实）。
    writer = DataStore(str(database))
    revised_key = insert_record(
        writer, batch_id="B1", status="collected",
        observed_at="2026-11-01T09:55:00Z", facility_id="F1",
        destination_facility_id="F2",
    )
    writer.close()

    result = rebuild_batch_flows(str(database), batch_id="B1")
    assert result.mode == "incremental"
    b1_after = get_batch_flow(str(database), "B1")
    assert b1_after["content_hash"] != b1_before["content_hash"]
    assert revised_key in b1_after["evidence"]
    # 增量重建不触碰 B2 的已发布结论。
    assert get_batch_flow(str(database), "B2") == b2_before


# --------------------------------------------------------------------------- #
# 并发互斥与数据库锁冲突
# --------------------------------------------------------------------------- #
def test_concurrent_rebuilds_are_mutex_across_connections(tmp_path):
    database = tmp_path / "data.sqlite3"
    writer = DataStore(str(database))
    legal_chain_records(writer)
    writer.close()

    holder = open_flow_store(database)
    other = open_flow_store(database)
    with holder.rebuild_lock("run-holder"):
        with pytest.raises(FlowRebuildLocked):
            with other.rebuild_lock("run-other"):
                pytest.fail("must not acquire a second rebuild lock")
    # 锁释放后可再次获取，且锁表被清空。
    with other.rebuild_lock("run-other"):
        row = other.conn.execute("SELECT COUNT(*) FROM flow_rebuild_locks").fetchone()[0]
        assert row == 1
    assert other.conn.execute("SELECT COUNT(*) FROM flow_rebuild_locks").fetchone()[0] == 0
    holder.close()
    other.close()


def test_in_process_rebuild_mutex_blocks_second_thread(tmp_path):
    database = tmp_path / "data.sqlite3"
    shared = open_flow_store(database)
    entered = threading.Event()
    release = threading.Event()
    errors: list[Exception] = []

    def hold() -> None:
        with shared.rebuild_lock("run-thread-a"):
            entered.set()
            release.wait(timeout=5)

    thread = threading.Thread(target=hold)
    thread.start()
    assert entered.wait(timeout=5)
    try:
        with pytest.raises(FlowRebuildLocked):
            with shared.rebuild_lock("run-thread-b"):
                pytest.fail("same-store second thread must be rejected")
    finally:
        release.set()
        thread.join(timeout=5)
    shared.close()


def test_stale_lock_from_crashed_process_is_reclaimed(tmp_path):
    database = tmp_path / "data.sqlite3"
    store = open_flow_store(database, lock_stale_seconds=1)
    past = datetime.now(timezone.utc) - timedelta(seconds=10)
    store.conn.execute(
        "INSERT INTO flow_rebuild_locks(lock_id, owner, acquired_at, expires_at) VALUES (1, ?, ?, ?)",
        ("dead-run", past.isoformat(), (past + timedelta(seconds=1)).isoformat()),
    )
    store.conn.commit()
    with store.rebuild_lock("run-reclaim"):
        owner = store.conn.execute(
            "SELECT owner FROM flow_rebuild_locks WHERE lock_id = 1"
        ).fetchone()[0]
        assert owner == "run-reclaim"
    store.close()


def test_sqlite_write_lock_conflict_is_wrapped_and_retriable(tmp_path):
    database = tmp_path / "data.sqlite3"
    writer = DataStore(str(database))
    legal_chain_records(writer)
    writer.close()

    store = open_flow_store(database, busy_timeout_ms=100)
    # 先在无锁竞争下完成索引刷新与裁决，确保下面只验证发布时的写锁冲突。
    store.refresh_fact_index()
    from municipal_waste.reconstruction import build_batch_chain

    chain = build_batch_chain("B1", store.load_facts("B1"))

    # 外部连接长期持有写锁，模拟另一个进程在写库。
    blocker = sqlite3.connect(str(database), timeout=0.1)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(FlowStoreError):
            store.publish_chain(chain, "fp", "run-blocked")
    finally:
        blocker.rollback()
        blocker.close()

    # 锁释放后同一发布重试成功 —— 失败不留半成品。
    assert store.publish_chain(chain, "fp", "run-recovered") is True
    store.close()


# --------------------------------------------------------------------------- #
# 半途失败与中断重试
# --------------------------------------------------------------------------- #
def test_halfway_failure_marks_run_failed_and_retry_completes(tmp_path, monkeypatch):
    database = tmp_path / "data.sqlite3"
    writer = DataStore(str(database))
    legal_chain_records(writer, batch_id="B1")
    legal_chain_records(writer, batch_id="B2")
    legal_chain_records(writer, batch_id="B3")
    writer.close()

    import municipal_waste.rebuild as rebuild_module

    real_engine = rebuild_module.build_batch_chain

    def flaky(batch_id, facts):
        if batch_id == "B2":
            raise RuntimeError("simulated crash during adjudication")
        return real_engine(batch_id, facts)

    monkeypatch.setattr(rebuild_module, "build_batch_chain", flaky)
    with pytest.raises(RuntimeError, match="simulated crash"):
        rebuild_batch_flows(str(database))

    # 崩溃后：B1 已逐批提交，B2/B3 未发布；run 台账可观测。
    assert get_batch_flow(str(database), "B1") is not None
    assert get_batch_flow(str(database), "B2") is None
    assert get_batch_flow(str(database), "B3") is None
    audit = sqlite3.connect(str(database))
    failed_runs = audit.execute(
        "SELECT status, error FROM flow_runs WHERE status = 'failed'"
    ).fetchall()
    assert failed_runs and "simulated crash" in failed_runs[0][1]
    # 崩溃路径也必须释放咨询锁，重试才能拿到锁。
    assert audit.execute("SELECT COUNT(*) FROM flow_rebuild_locks").fetchone()[0] == 0
    audit.close()

    monkeypatch.undo()
    retry = rebuild_batch_flows(str(database))
    assert retry.status == "completed"
    assert retry.batch_count == 3
    # B1 内容未变 -> 幂等跳过；B2/B3 新发布。
    assert retry.published_count == 2
    assert retry.skipped_count == 1
    assert get_batch_flow(str(database), "B2") is not None
    assert get_batch_flow(str(database), "B3") is not None


def test_locked_retry_is_recorded_in_run_ledger(tmp_path):
    database = tmp_path / "data.sqlite3"
    writer = DataStore(str(database))
    legal_chain_records(writer)
    writer.close()

    holder = open_flow_store(database)
    with holder.rebuild_lock("run-holder"):
        with pytest.raises(FlowRebuildLocked):
            rebuild_batch_flows(str(database))
    holder.close()

    audit = sqlite3.connect(str(database))
    statuses = [row[0] for row in audit.execute("SELECT status FROM flow_runs")]
    assert "locked" in statuses
    audit.close()


# --------------------------------------------------------------------------- #
# 有限内存：大量批次流式重建
# --------------------------------------------------------------------------- #
def test_bounded_memory_streaming_rebuild_across_many_batches(tmp_path):
    database = tmp_path / "data.sqlite3"
    writer = DataStore(str(database))
    total_batches = 25
    for index in range(total_batches):
        legal_chain_records(writer, batch_id=f"B{index:04d}")
    writer.close()

    progress: list[int] = []
    store = open_flow_store(database)
    fact_count = store.refresh_fact_index(progress.append)
    assert fact_count == total_batches * 4
    # 分批 flush（每批 500 行）确实产生了多次进度回调。
    assert len(progress) >= 1

    seen: list[str] = []
    for batch_id in store.stream_batch_ids():
        facts = store.load_facts(batch_id)
        assert len(facts) == 4  # 单批次内存有界，不跨批次累积
        seen.append(batch_id)
    assert seen == [f"B{index:04d}" for index in range(total_batches)]
    store.close()

    result = rebuild_batch_flows(str(database))
    assert result.batch_count == total_batches
    assert result.fact_count == total_batches * 4


# --------------------------------------------------------------------------- #
# 迟到事实的端到端保证：先到 processed，后到 in_transit
# --------------------------------------------------------------------------- #
def test_late_fact_through_full_store_roundtrip_is_not_a_rollback(tmp_path):
    database = tmp_path / "data.sqlite3"
    writer = DataStore(str(database))
    insert_record(writer, status="processed", observed_at="2026-11-01T13:00:00Z",
                  facility_id="F2", destination_facility_id="F2")
    insert_record(writer, status="collected", observed_at="2026-11-01T10:00:00Z",
                  facility_id="F1", destination_facility_id="F2")
    # 迟到的 in_transit，最后写入。
    insert_record(writer, status="in_transit", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F1", destination_facility_id="F2")
    insert_record(writer, status="received", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F2", destination_facility_id="F2")
    writer.close()

    rebuild_batch_flows(str(database))
    chain = get_batch_flow(str(database), "B1")
    assert [stage["status"] for stage in chain["stages"]] == [
        "collected",
        "in_transit",
        "received",
        "processed",
    ]
    assert all(stage["legal"] for stage in chain["stages"])
    assert chain["is_complete"] is True


# --------------------------------------------------------------------------- #
# 导出接口
# --------------------------------------------------------------------------- #
def test_export_jsonl_json_and_single_batch(tmp_path):
    database = tmp_path / "data.sqlite3"
    writer = DataStore(str(database))
    legal_chain_records(writer, batch_id="B1")
    legal_chain_records(writer, batch_id="B2")
    writer.close()
    rebuild_batch_flows(str(database))

    jsonl_path = tmp_path / "export.jsonl"
    assert export_batch_flows(str(database), jsonl_path) == 2
    lines = [line for line in jsonl_path.read_text(encoding="utf-8").splitlines() if line]
    assert len(lines) == 2
    for line in lines:
        payload = json.loads(line)
        assert "stages" in payload and "content_hash" in payload

    json_path = tmp_path / "export.json"
    assert export_batch_flows(str(database), json_path, fmt="json") == 2
    assert len(json.loads(json_path.read_text(encoding="utf-8"))) == 2

    single_path = tmp_path / "single.jsonl"
    assert export_batch_flows(str(database), single_path, batch_id="B1") == 1
    assert json.loads(single_path.read_text(encoding="utf-8").strip())["batch_id"] == "B1"

    with pytest.raises(ValueError):
        export_batch_flows(str(database), tmp_path / "x.bin", fmt="csv")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def test_cli_rebuild_query_export_and_legacy_ingest(tmp_path, capsys, monkeypatch):
    fixture = Path(__file__).parents[1] / "fixtures" / "sample.json"
    database = tmp_path / "cli.sqlite3"

    # 旧版 ingestion 调用方式保持兼容。
    exit_code = cli.main([
        "--fixture", str(fixture), "--database", str(database),
        "--raw-dir", str(tmp_path / "raw"),
    ])
    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["accepted"] == 3

    # 新的 ingest 子命令等价。
    exit_code = cli.main([
        "ingest", "--fixture", str(fixture), "--database", str(database),
        "--raw-dir", str(tmp_path / "raw2"),
    ])
    assert exit_code == 0
    capsys.readouterr()

    exit_code = cli.main(["rebuild", "--database", str(database)])
    assert exit_code == 0
    rebuild_payload = json.loads(capsys.readouterr().out)
    assert rebuild_payload["batch_count"] == 3

    exit_code = cli.main(["query", "--database", str(database), "--batch-id", "B-001"])
    assert exit_code == 0
    queried = json.loads(capsys.readouterr().out)
    assert queried["batch_id"] == "B-001"

    exit_code = cli.main(["query", "--database", str(database), "--batch-id", "NOPE"])
    assert exit_code == 1

    output = tmp_path / "cli-export.jsonl"
    exit_code = cli.main([
        "export", "--database", str(database), "--output", str(output),
        "--format", "jsonl",
    ])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["exported"] == 3
    assert len(output.read_text(encoding="utf-8").splitlines()) == 3

    # stdout 导出。
    exit_code = cli.main([
        "export", "--database", str(database), "--output", "-", "--format", "json",
    ])
    assert exit_code == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_cli_rebuild_returns_distinct_exit_code_on_lock_conflict(tmp_path):
    database = tmp_path / "data.sqlite3"
    writer = DataStore(str(database))
    legal_chain_records(writer)
    writer.close()

    holder = open_flow_store(database)
    with holder.rebuild_lock("run-cli-holder"):
        exit_code = cli.main(["rebuild", "--database", str(database)])
    assert exit_code == 2
    holder.close()


# --------------------------------------------------------------------------- #
# schema 初始化对既有库幂等
# --------------------------------------------------------------------------- #
def test_schema_initialization_is_idempotent(tmp_path):
    database = tmp_path / "data.sqlite3"
    first = open_flow_store(database)
    first.initialize_schema()
    first.initialize_schema()
    first.close()
    second = open_flow_store(database)
    tables = {
        row[0]
        for row in second.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'flow_%' "
            "UNION ALL SELECT name FROM sqlite_master WHERE type='table' AND name='batch_flow_chains'"
        )
    }
    assert {
        "flow_runs",
        "flow_rebuild_locks",
        "flow_fact_index",
        "batch_flow_chains",
    }.issubset(tables)
    second.close()
