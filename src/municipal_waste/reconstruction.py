"""垃圾批次流转链重建：纯领域裁决引擎。

本模块不接触数据库，只接受 :class:`FlowFact` 事实集合，输出确定性的
:class:`BatchChain` 结论。裁决与发布策略：

1. **事件时间裁决，而非到达顺序。** 事实一律按 ``observed_at``（来源声称事件
   发生的时刻）排序，绝不按 ingestion 的 ``first_seen_at`` / 插入顺序排序。
   因此合法的迟到事实（例如 ``processed`` 已落库后才到达的 ``in_transit``）会
   被放回它自己的事件时间槽位，而不会被误判成状态回退。
2. **同刻决策点。** 相同 ``observed_at`` 的事实归为一个决策点，按
   ``(生命周期排名, facility_id, record_key)`` 的全序确定性选取采纳事实，
   其余事实作为同刻冲突/分叉保留，并回指各自的 ``record_key``。重放任何
   到达顺序都得到同一条链。
3. **状态机裁决。** 对事件时间序列逐个应用 :func:`municipal_waste.flow.can_transition`，
   非法跳转、终态后活动、拒绝后恢复分别标注，但不静默丢弃任何事实。
4. **每条结论可回指事实。** 阶段、边、冲突都携带 ``record_key``，``evidence``
   汇总批次下全部事实键（含无法解析时间的事实）。

“最新状态”只是序列中的一个观测，绝不等于完整流向；缺失阶段与非法跳转都会
显式输出。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .flow import FLOW_STATES, can_transition
from .identity import batch_key as make_scope_key

_STATE_RANK = {status: index for index, status in enumerate(FLOW_STATES)}
_TERMINAL_STATES = frozenset({"processed", "rejected"})


# --------------------------------------------------------------------------- #
# 时间解析
# --------------------------------------------------------------------------- #
def parse_event_time(value: str | None) -> float | None:
    """把 ISO-8601 文本解析为 UTC epoch 秒；无法解析返回 ``None``。

    支持 ``Z`` 后缀与朴素时间（朴素时间按 UTC 处理），跨日时间因此可以在
    同一时间轴上比较。
    """
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def event_date(value: str | None) -> str | None:
    """返回事件对应的 UTC 日期（YYYY-MM-DD），无法解析时返回 ``None``。"""
    epoch = parse_event_time(value)
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).date().isoformat()


# --------------------------------------------------------------------------- #
# 输出模型
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FlowFact:
    """``records`` 快照中一条记录在流转重建视角下的不可变投影。"""

    record_key: str
    source: str
    batch_id: str
    facility_id: str
    destination_facility_id: str
    status: str
    observed_at: str
    observed_order: float | None
    collected_at: str
    collected_parsed: bool

    @property
    def scope_key(self) -> str:
        # 复用 identity.batch_key，保持“批次|设施”作用域语义不变。
        return make_scope_key(self.batch_id, self.facility_id)


@dataclass(frozen=True)
class Conflict:
    code: str
    message: str
    record_keys: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "message": self.message,
            "record_keys": list(self.record_keys),
        }


@dataclass(frozen=True)
class FlowEdge:
    from_facility: str
    to_facility: str
    status: str
    record_key: str
    self_loop: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "from_facility": self.from_facility,
            "to_facility": self.to_facility,
            "status": self.status,
            "record_key": self.record_key,
            "self_loop": self.self_loop,
        }


@dataclass(frozen=True)
class StageConclusion:
    stage: str
    status: str
    record_key: str
    facility_id: str
    observed_at: str
    legal: bool
    edge: FlowEdge

    def to_dict(self) -> dict[str, object]:
        payload = {
            "stage": self.stage,
            "status": self.status,
            "record_key": self.record_key,
            "facility_id": self.facility_id,
            "observed_at": self.observed_at,
            "legal": self.legal,
            "edge": self.edge.to_dict(),
        }
        return payload


@dataclass(frozen=True)
class BatchChain:
    batch_id: str
    scopes: tuple[str, ...]
    stages: tuple[StageConclusion, ...]
    edges: tuple[FlowEdge, ...]
    missing_stages: tuple[str, ...]
    illegal_transitions: tuple[Conflict, ...]
    conflicts: tuple[Conflict, ...]
    unordered_record_keys: tuple[str, ...]
    terminal_status: str | None
    is_complete: bool
    cross_day: bool
    evidence: tuple[str, ...]
    content_hash: str

    def to_dict(self) -> dict[str, object]:
        return {
            "batch_id": self.batch_id,
            "scopes": list(self.scopes),
            "terminal_status": self.terminal_status,
            "is_complete": self.is_complete,
            "cross_day": self.cross_day,
            "stages": [stage.to_dict() for stage in self.stages],
            "edges": [edge.to_dict() for edge in self.edges],
            "missing_stages": list(self.missing_stages),
            "illegal_transitions": [item.to_dict() for item in self.illegal_transitions],
            "conflicts": [item.to_dict() for item in self.conflicts],
            "unordered_record_keys": list(self.unordered_record_keys),
            "evidence": list(self.evidence),
            "content_hash": self.content_hash,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)


# --------------------------------------------------------------------------- #
# 纯裁决引擎
# --------------------------------------------------------------------------- #
def _decision_key(fact: FlowFact) -> tuple[int, str, str]:
    return (_STATE_RANK[fact.status], fact.facility_id, fact.record_key)


def _adjudicate_timestamp_group(group: list[FlowFact]) -> tuple[FlowFact, Conflict | None]:
    """同刻决策点：确定性选出唯一采纳事实，其余记为冲突。"""
    winner = min(group, key=_decision_key)
    if len(group) == 1:
        return winner, None

    statuses = {fact.status for fact in group}
    facilities = {fact.facility_id for fact in group}
    alternatives = sorted(
        (
            {
                "record_key": fact.record_key,
                "status": fact.status,
                "facility_id": fact.facility_id,
            }
            for fact in group
            if fact.record_key != winner.record_key
        ),
        key=lambda item: item["record_key"],
    )
    if len(statuses) == 1 and len(facilities) > 1:
        code = "fork_observation"
        message = (
            f"同一时刻 {winner.observed_at} 在 {len(facilities)} 个设施观测到相同状态 "
            f"{winner.status}，按设施/记录键确定性采纳 {winner.record_key}，其余作为分叉保留"
        )
    else:
        code = "same_timestamp_conflict"
        message = (
            f"同一时刻 {winner.observed_at} 存在 {len(group)} 条互斥观测，"
            f"按生命周期排名采纳 {winner.record_key}（{winner.status}），"
            f"备选 {json.dumps(alternatives, ensure_ascii=False, sort_keys=True)}"
        )
    conflict = Conflict(code, message, tuple(sorted(fact.record_key for fact in group)))
    return winner, conflict


def _missing_stages(adopted: list[FlowFact]) -> list[str]:
    """根据已采纳事实推导期望生命周期前缀中的缺口。"""
    present = {fact.status for fact in adopted}
    if not present:
        return []
    if "rejected" in present:
        # 拒绝是终态：期望路径只到“第一次拒绝所处阶段 + rejected”，
        # processed 永远不属于拒绝路径。
        first_rejected_rank = min(
            _STATE_RANK[fact.status] for fact in adopted if fact.status == "rejected"
        )
        expected = [
            status
            for status in FLOW_STATES[: first_rejected_rank + 1]
            if status != "processed"
        ]
    elif "processed" in present:
        expected = FLOW_STATES[: _STATE_RANK["processed"] + 1]
    else:
        furthest = max(_STATE_RANK[status] for status in present)
        expected = FLOW_STATES[: furthest + 1]
    return [status for status in expected if status not in present]


def build_batch_chain(batch_id: str, facts: list[FlowFact]) -> BatchChain:
    """对单个批次的事实集合执行确定性流转链重建。"""
    conflicts: list[Conflict] = []
    unordered: list[FlowFact] = []
    ordered: list[FlowFact] = []

    for fact in facts:
        if fact.observed_order is None:
            unordered.append(fact)
        else:
            ordered.append(fact)

    for fact in unordered:
        conflicts.append(
            Conflict(
                "unparseable_observed_at",
                f"记录 {fact.record_key} 的 observed_at={fact.observed_at!r} 无法解析，"
                "不参与事件时间排序，仅作为证据保留",
                (fact.record_key,),
            )
        )

    for fact in facts:
        if not fact.collected_parsed:
            conflicts.append(
                Conflict(
                    "unparseable_collected_at",
                    f"记录 {fact.record_key} 的 collected_at={fact.collected_at!r} 无法解析，"
                    "该事实仍按 observed_at 参与排序",
                    (fact.record_key,),
                )
            )

    # 事件时间排序：同刻内部按全序排列，保证任何到达顺序结果一致。
    ordered.sort(key=lambda fact: (fact.observed_order, *_decision_key(fact)))

    adopted: list[FlowFact] = []
    groups: list[list[FlowFact]] = []
    index = 0
    while index < len(ordered):
        current = ordered[index]
        group = [current]
        index += 1
        while index < len(ordered) and ordered[index].observed_order == current.observed_order:
            group.append(ordered[index])
            index += 1
        groups.append(group)
        winner, conflict = _adjudicate_timestamp_group(group)
        adopted.append(winner)
        if conflict is not None:
            conflicts.append(conflict)

    stages: list[StageConclusion] = []
    edges: list[FlowEdge] = []
    illegal: list[Conflict] = []
    last_status: str | None = None
    seen_status_count: dict[str, int] = {}

    for fact in adopted:
        seen_status_count[fact.status] = seen_status_count.get(fact.status, 0) + 1
        if seen_status_count[fact.status] > 1:
            conflicts.append(
                Conflict(
                    "repeated_stage_observation",
                    f"状态 {fact.status} 在不同事件时间被重复观测（{fact.record_key}），"
                    "允许同状态自循环，但保留重复说明",
                    (fact.record_key,),
                )
            )

        legal = True
        if last_status is not None and not can_transition(last_status, fact.status):
            legal = False
            if last_status == "rejected":
                code = "recovery_after_rejection"
                message = (
                    f"拒绝终态之后出现 {fact.status} 观测（{fact.record_key}），"
                    "状态机不允许拒绝后恢复，需人工解释"
                )
            elif last_status == "processed":
                code = "activity_after_terminal"
                message = (
                    f"处理终态之后出现 {fact.status} 观测（{fact.record_key}），"
                    "状态机不允许处理后继续流转"
                )
            else:
                code = "illegal_transition"
                message = (
                    f"非法跳转 {last_status} -> {fact.status}（{fact.record_key}），"
                    "不满足生命周期允许的邻接关系"
                )
            violation = Conflict(code, message, (fact.record_key,))
            illegal.append(violation)
            conflicts.append(violation)
        else:
            # 只有合法推进才更新已确立状态；非法事实不改变裁决基线。
            last_status = fact.status

        self_loop = fact.facility_id == fact.destination_facility_id
        if self_loop and fact.status != "processed":
            # 与 quality.validate 的特殊规则保持一致：仅 processed 允许同设施。
            conflicts.append(
                Conflict(
                    "self_loop_illegal",
                    f"记录 {fact.record_key} 的源设施与目的设施相同（{fact.facility_id}），"
                    "非 processed 状态不允许自环",
                    (fact.record_key,),
                )
            )

        edge = FlowEdge(
            from_facility=fact.facility_id,
            to_facility=fact.destination_facility_id,
            status=fact.status,
            record_key=fact.record_key,
            self_loop=self_loop,
        )
        edges.append(edge)
        stages.append(
            StageConclusion(
                stage=fact.status,
                status=fact.status,
                record_key=fact.record_key,
                facility_id=fact.facility_id,
                observed_at=fact.observed_at,
                legal=legal,
                edge=edge,
            )
        )

    # 跨设施工单连续性：仅当观测设施真正发生变化时才校验交接，
    # 同设施内的连续观测属于正常留场，不构成断裂。
    for previous, current in zip(adopted, adopted[1:]):
        if (
            previous.facility_id != current.facility_id
            and previous.destination_facility_id != current.facility_id
        ):
            conflicts.append(
                Conflict(
                    "facility_handoff_gap",
                    f"设施交接断裂：{previous.record_key} 目的设施 "
                    f"{previous.destination_facility_id} 与 {current.record_key} 观测设施 "
                    f"{current.facility_id} 不一致",
                    (previous.record_key, current.record_key),
                )
            )

    missing = tuple(_missing_stages(adopted))
    if missing:
        conflicts.append(
            Conflict(
                "missing_stage",
                f"批次生命周期缺少阶段：{', '.join(missing)}；不能以最新状态代替完整流向",
                tuple(fact.record_key for fact in adopted),
            )
        )

    terminal_adopted = [fact for fact in adopted if fact.status in _TERMINAL_STATES]
    terminal_status = terminal_adopted[-1].status if terminal_adopted else None
    cross_day = len({event_date(fact.observed_at) for fact in adopted}) > 1
    scopes = tuple(sorted({fact.scope_key for fact in facts}))
    evidence = tuple(sorted(fact.record_key for fact in facts))
    unordered_keys = tuple(sorted(fact.record_key for fact in unordered))

    is_complete = (
        terminal_status in _TERMINAL_STATES
        and not missing
        and not illegal
    )

    content_hash = _content_hash(
        batch_id=batch_id,
        scopes=scopes,
        stages=stages,
        edges=edges,
        missing_stages=missing,
        illegal=illegal,
        conflicts=conflicts,
        unordered_keys=unordered_keys,
        terminal_status=terminal_status,
        is_complete=is_complete,
        cross_day=cross_day,
        evidence=evidence,
    )

    return BatchChain(
        batch_id=batch_id,
        scopes=scopes,
        stages=tuple(stages),
        edges=tuple(edges),
        missing_stages=missing,
        illegal_transitions=tuple(illegal),
        conflicts=tuple(conflicts),
        unordered_record_keys=unordered_keys,
        terminal_status=terminal_status,
        is_complete=is_complete,
        cross_day=cross_day,
        evidence=evidence,
        content_hash=content_hash,
    )


def _content_hash(**parts: object) -> str:
    canonical = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=_hash_default)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _hash_default(value: object) -> object:
    if isinstance(value, (StageConclusion, FlowEdge, Conflict)):
        return value.to_dict()
    if isinstance(value, tuple):
        return list(value)
    raise TypeError(f"unhashable type in chain content: {type(value)!r}")


def input_fingerprint(facts: list[FlowFact]) -> str:
    """批次输入事实集合的指纹：修订 payload 后指纹必然变化。"""
    canonical = [
        (
            fact.record_key,
            fact.status,
            fact.facility_id,
            fact.destination_facility_id,
            fact.observed_at,
            fact.collected_at,
        )
        for fact in facts
    ]
    payload = json.dumps(sorted(canonical), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
