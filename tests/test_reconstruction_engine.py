"""流转链重建纯引擎测试：裁决策略、冲突解释与事实回指。"""

from __future__ import annotations

import pytest

from municipal_waste.reconstruction import (
    FlowFact,
    build_batch_chain,
    input_fingerprint,
    parse_event_time,
)


def make_fact(
    record_key: str,
    *,
    batch_id: str = "B1",
    status: str = "collected",
    observed_at: str = "2026-11-01T10:00:00Z",
    facility_id: str = "F1",
    destination_facility_id: str | None = None,
    collected_at: str = "2026-11-01T09:00:00Z",
    source: str = "unit",
    observed_order: float | None = "AUTO",
    collected_parsed: bool | None = None,
) -> FlowFact:
    if destination_facility_id is None:
        destination_facility_id = facility_id if status == "processed" else "F2"
    if observed_order == "AUTO":
        observed_order = parse_event_time(observed_at)
    if collected_parsed is None:
        collected_parsed = parse_event_time(collected_at) is not None
    return FlowFact(
        record_key=record_key,
        source=source,
        batch_id=batch_id,
        facility_id=facility_id,
        destination_facility_id=destination_facility_id,
        status=status,
        observed_at=observed_at,
        observed_order=observed_order,
        collected_at=collected_at,
        collected_parsed=collected_parsed,
    )


def conflict_codes(chain) -> list[str]:
    return [conflict.code for conflict in chain.conflicts]


# --------------------------------------------------------------------------- #
# 单阶段
# --------------------------------------------------------------------------- #
def test_single_stage_chain_reports_missing_but_keeps_fact_reference():
    chain = build_batch_chain("B1", [make_fact("k1", status="received", facility_id="F2")])
    assert [stage.status for stage in chain.stages] == ["received"]
    assert chain.stages[0].record_key == "k1"
    # 单阶段不等于完整流向：collected/in_transit 缺失必须显式输出。
    assert "collected" in chain.missing_stages
    assert "in_transit" in chain.missing_stages
    assert chain.is_complete is False
    assert chain.terminal_status is None
    assert chain.evidence == ("k1",)
    assert "missing_stage" in conflict_codes(chain)


def test_single_collected_stage_is_open_chain():
    chain = build_batch_chain("B1", [make_fact("k1", status="collected")])
    assert chain.missing_stages == ()
    assert chain.terminal_status is None
    assert chain.is_complete is False


# --------------------------------------------------------------------------- #
# 完整链路
# --------------------------------------------------------------------------- #
def test_full_legal_chain_across_facilities_is_complete():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
        make_fact("k2", status="in_transit", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
        make_fact("k3", status="received", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F2", destination_facility_id="F3"),
        make_fact("k4", status="processed", observed_at="2026-11-01T13:00:00Z",
                  facility_id="F3", destination_facility_id="F3"),
    ]
    chain = build_batch_chain("B1", facts)
    assert [stage.status for stage in chain.stages] == [
        "collected",
        "in_transit",
        "received",
        "processed",
    ]
    assert all(stage.legal for stage in chain.stages)
    assert chain.missing_stages == ()
    assert chain.illegal_transitions == ()
    assert chain.is_complete is True
    assert chain.terminal_status == "processed"
    # 每条结论都能回指事实。
    assert [stage.record_key for stage in chain.stages] == ["k1", "k2", "k3", "k4"]
    assert [edge.record_key for edge in chain.edges] == ["k1", "k2", "k3", "k4"]
    # 仅终态 processed 允许源设施等于目的设施的自环特殊规则。
    assert chain.edges[-1].self_loop is True
    assert "self_loop_illegal" not in conflict_codes(chain)
    assert "facility_handoff_gap" not in conflict_codes(chain)
    # scope 沿用 identity.batch_key（BATCH|FACILITY 大写语义）。
    assert set(chain.scopes) == {"B1|F1", "B1|F2", "B1|F3"}


def test_edges_capture_facility_adjacency():
    facts = [
        make_fact("k1", status="collected", facility_id="F1", destination_facility_id="F2",
                  observed_at="2026-11-01T10:00:00Z"),
        make_fact("k2", status="in_transit", facility_id="F1", destination_facility_id="F2",
                  observed_at="2026-11-01T11:00:00Z"),
    ]
    chain = build_batch_chain("B1", facts)
    edge = chain.edges[0]
    assert (edge.from_facility, edge.to_facility, edge.status) == ("F1", "F2", "collected")


# --------------------------------------------------------------------------- #
# 非法跳转
# --------------------------------------------------------------------------- #
def test_illegal_jump_is_flagged_and_does_not_advance_baseline():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z"),
        make_fact("k2", status="processed", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
        make_fact("k3", status="in_transit", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    assert [stage.status for stage in chain.stages] == ["collected", "processed", "in_transit"]
    assert chain.stages[0].legal is True
    assert chain.stages[1].legal is False
    # 非法事实不推进裁决基线：collected -> in_transit 本身合法。
    assert chain.stages[2].legal is True
    codes = [item.code for item in chain.illegal_transitions]
    assert codes == ["illegal_transition"]
    # 非法事实的 record_key 被完整回指。
    assert chain.illegal_transitions[0].record_keys == ("k2",)
    assert "received" in chain.missing_stages


# --------------------------------------------------------------------------- #
# 分叉与同刻冲突
# --------------------------------------------------------------------------- #
def test_fork_same_status_different_facilities_at_same_timestamp():
    facts = [
        make_fact("k1", status="received", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
        make_fact("k2", status="received", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F3", destination_facility_id="F3"),
    ]
    chain = build_batch_chain("B1", facts)
    assert len(chain.stages) == 1
    # 全序裁决：facility_id 较小者胜出。
    assert chain.stages[0].record_key == "k1"
    fork = [c for c in chain.conflicts if c.code == "fork_observation"]
    assert len(fork) == 1
    assert set(fork[0].record_keys) == {"k1", "k2"}


def test_same_timestamp_conflicting_statuses_deterministic_winner():
    facts = [
        make_fact("k1", status="processed", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
        make_fact("k2", status="received", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    assert len(chain.stages) == 1
    # 生命周期排名：received(2) 早于 processed(3)，因此 received 胜出。
    assert chain.stages[0].status == "received"
    assert any(c.code == "same_timestamp_conflict" for c in chain.conflicts)


# --------------------------------------------------------------------------- #
# 迟到记录：事件时间裁决，不按到达顺序误判回退
# --------------------------------------------------------------------------- #
def test_late_arriving_fact_is_placed_by_event_time_not_arrival_order():
    # 模拟 processed 先落库、in_transit 迟到：传入顺序故意打乱。
    facts = [
        make_fact("k4", status="processed", observed_at="2026-11-01T13:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
        make_fact("k3", status="received", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
        make_fact("k2", status="in_transit", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    assert [stage.status for stage in chain.stages] == [
        "collected",
        "in_transit",
        "received",
        "processed",
    ]
    assert all(stage.legal for stage in chain.stages)
    assert chain.illegal_transitions == ()


def test_rebuild_is_order_insensitive_and_deterministic():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z"),
        make_fact("k2", status="rejected", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
        make_fact("k3", status="in_transit", observed_at="2026-11-01T10:30:00Z"),
    ]
    first = build_batch_chain("B1", facts)
    second = build_batch_chain("B1", list(reversed(facts)))
    assert first.content_hash == second.content_hash
    assert first.to_json() == second.to_json()


# --------------------------------------------------------------------------- #
# 拒绝与后续恢复
# --------------------------------------------------------------------------- #
def test_rejected_chain_missing_excludes_processed():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z"),
        make_fact("k2", status="rejected", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    assert chain.terminal_status == "rejected"
    # 拒绝路径的期望前缀到 rejected 为止（不含 processed）；未观测的中间阶段都算缺失。
    assert chain.missing_stages == ("in_transit", "received")
    assert "processed" not in chain.missing_stages
    assert chain.is_complete is False


def test_activity_after_rejection_is_conflict_not_silent_recovery():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z"),
        make_fact("k2", status="rejected", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
        make_fact("k3", status="in_transit", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    codes = [c.code for c in chain.illegal_transitions]
    assert codes == ["recovery_after_rejection"]
    assert chain.illegal_transitions[0].record_keys == ("k3",)
    # 所有事实仍被保留为证据。
    assert set(chain.evidence) == {"k1", "k2", "k3"}


def test_transit_after_processed_is_activity_after_terminal():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
        make_fact("k2", status="in_transit", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
        make_fact("k3", status="received", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
        make_fact("k4", status="processed", observed_at="2026-11-01T13:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
        make_fact("k5", status="in_transit", observed_at="2026-11-01T14:00:00Z",
                  facility_id="F2", destination_facility_id="F3"),
    ]
    chain = build_batch_chain("B1", facts)
    assert [c.code for c in chain.illegal_transitions] == ["activity_after_terminal"]
    assert chain.illegal_transitions[0].record_keys == ("k5",)


# --------------------------------------------------------------------------- #
# 跨设施 / 自环特殊规则 / 交接断裂
# --------------------------------------------------------------------------- #
def test_self_loop_non_processed_is_conflict():
    facts = [
        make_fact("k1", status="received", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    loops = [c for c in chain.conflicts if c.code == "self_loop_illegal"]
    assert len(loops) == 1
    assert loops[0].record_keys == ("k1",)


def test_facility_handoff_gap_only_for_real_cross_facility_jump():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
        make_fact("k2", status="in_transit", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F1", destination_facility_id="F9"),
        make_fact("k3", status="received", observed_at="2026-11-01T12:00:00Z",
                  facility_id="F2", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    gaps = [c for c in chain.conflicts if c.code == "facility_handoff_gap"]
    assert len(gaps) == 1
    assert gaps[0].record_keys == ("k2", "k3")
    # k1->k2 同设施留场不报断裂。
    assert all(g.record_keys != ("k1", "k2") for g in gaps)


# --------------------------------------------------------------------------- #
# 跨日 / 无法解析的时间
# --------------------------------------------------------------------------- #
def test_cross_day_chain_is_marked_but_still_legal():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T23:30:00Z",
                  facility_id="F1", destination_facility_id="F2"),
        make_fact("k2", status="in_transit", observed_at="2026-11-02T00:30:00Z",
                  facility_id="F1", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    assert chain.cross_day is True
    assert chain.illegal_transitions == ()


def test_unparseable_observed_at_is_kept_as_evidence_and_unordered():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z"),
        make_fact("k-bad", status="received", observed_at="not-a-timestamp",
                  facility_id="F2", observed_order=None),
    ]
    chain = build_batch_chain("B1", facts)
    assert chain.unordered_record_keys == ("k-bad",)
    codes = conflict_codes(chain)
    assert "unparseable_observed_at" in codes
    # 无法排序的事实不进入阶段序列，但仍是证据。
    assert "k-bad" in chain.evidence
    assert all(stage.record_key != "k-bad" for stage in chain.stages)


def test_unparseable_collected_at_still_participates_in_ordering():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z",
                  collected_at="garbage", collected_parsed=False),
        make_fact("k2", status="in_transit", observed_at="2026-11-01T11:00:00Z"),
    ]
    chain = build_batch_chain("B1", facts)
    assert [stage.record_key for stage in chain.stages] == ["k1", "k2"]
    assert "unparseable_collected_at" in conflict_codes(chain)


def test_parse_event_time_handles_z_and_offsets_and_naive():
    assert parse_event_time("2026-11-01T10:00:00Z") == parse_event_time("2026-11-01T10:00:00+00:00")
    assert parse_event_time("2026-11-01T12:00:00+02:00") == parse_event_time("2026-11-01T10:00:00Z")
    assert parse_event_time("2026-11-01T10:00:00") == parse_event_time("2026-11-01T10:00:00Z")
    assert parse_event_time("nonsense") is None
    assert parse_event_time("") is None


# --------------------------------------------------------------------------- #
# 重复观测 / 修订
# --------------------------------------------------------------------------- #
def test_repeated_same_status_observation_is_explained():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z"),
        make_fact("k2", status="collected", observed_at="2026-11-01T10:30:00Z",
                  facility_id="F1", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    assert "repeated_stage_observation" in conflict_codes(chain)
    assert all(stage.legal for stage in chain.stages)


def test_input_fingerprint_changes_when_facts_are_revised():
    facts = [make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z")]
    revised = [make_fact("k1", status="collected", observed_at="2026-11-01T10:05:00Z")]
    assert input_fingerprint(facts) != input_fingerprint(revised)
    assert input_fingerprint(facts) == input_fingerprint(list(reversed(facts)))


def test_empty_fact_set_builds_empty_chain():
    chain = build_batch_chain("B-EMPTY", [])
    assert chain.stages == ()
    assert chain.edges == ()
    assert chain.missing_stages == ()
    assert chain.terminal_status is None
    assert chain.is_complete is False
    assert chain.evidence == ()
    assert chain.content_hash


def test_every_conclusion_points_back_to_facts():
    facts = [
        make_fact("k1", status="collected", observed_at="2026-11-01T10:00:00Z"),
        make_fact("k2", status="rejected", observed_at="2026-11-01T11:00:00Z",
                  facility_id="F1", destination_facility_id="F2"),
    ]
    chain = build_batch_chain("B1", facts)
    payload = chain.to_dict()
    for stage in payload["stages"]:
        assert stage["record_key"] in chain.evidence
        assert stage["edge"]["record_key"] in chain.evidence
    for conflict in payload["conflicts"]:
        assert conflict["record_keys"]
        for key in conflict["record_keys"]:
            assert key in chain.evidence


def test_batch_id_and_scope_preserved_through_serialization():
    chain = build_batch_chain(
        "b-x",
        [make_fact("k1", batch_id="b-x", status="collected", facility_id="f-a")],
    )
    payload = chain.to_dict()
    assert payload["batch_id"] == "b-x"
    assert "B-X|F-A" in payload["scopes"]
    # JSON 可往返。
    import json

    assert json.loads(chain.to_json())["batch_id"] == "b-x"


def test_two_batches_are_reconstructed_independently():
    chain_a = build_batch_chain(
        "A", [make_fact("a1", batch_id="A", status="collected")]
    )
    chain_b = build_batch_chain(
        "B", [make_fact("b1", batch_id="B", status="processed",
                        facility_id="F2", destination_facility_id="F2",
                        observed_at="2026-11-01T12:00:00Z")]
    )
    assert chain_a.batch_id == "A"
    assert chain_b.terminal_status == "processed"
    assert chain_a.content_hash != chain_b.content_hash


@pytest.mark.parametrize(
    "statuses,expected_terminal,expected_complete",
    [
        ((["collected", "in_transit", "received", "processed"]), "processed", True),
        ((["collected", "rejected"]), "rejected", False),
        ((["collected", "in_transit"]), None, False),
    ],
)
def test_terminal_and_completeness_matrix(statuses, expected_terminal, expected_complete):
    timestamps = [
        "2026-11-01T10:00:00Z",
        "2026-11-01T11:00:00Z",
        "2026-11-01T12:00:00Z",
        "2026-11-01T13:00:00Z",
    ]
    facts = []
    for index, status in enumerate(statuses):
        facility = "F2" if status in {"received", "processed"} else "F1"
        facts.append(
            make_fact(
                f"k{index}",
                status=status,
                observed_at=timestamps[index],
                facility_id=facility,
                destination_facility_id=facility if status == "processed" else "F2",
            )
        )
    chain = build_batch_chain("B1", facts)
    assert chain.terminal_status == expected_terminal
    assert chain.is_complete is expected_complete
