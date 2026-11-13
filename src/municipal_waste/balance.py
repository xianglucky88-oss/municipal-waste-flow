"""批次重量平衡核算的领域逻辑（纯函数，不直接访问存储）。

归属与记账策略（明确约定，核算与测试共同遵守）：

- 每条记录只归属一个 ``(batch_id, facility_id, status)`` 组和一个重量类别
  （input/transfer/received/processed/rejected），同一条记录绝不会同时
  当作输入和输出。
- 同一组内的多次称重 / 多来源重复报量，按 ``observed_at`` 取最新一条为有效
  重量，其余 ``record_key`` 保留在 ``duplicate_weighing`` 异常中作为证据；
  同一 ``record_key`` 的重复修订由存储层 upsert 保证只保留最新内容。
- 重量按节点（设施 × 状态）记账，不沿设施边流动；设施边图存在环时只标记
  ``facility_cycle`` 异常，不会因此重复计量。
- 上下游时间交错或状态回退不阻断核算，按 ``flow.can_transition`` 检测并标记
  ``status_regression`` 异常。
- 分析窗口按 ``collected_at`` 闭区间 ``[window_start, window_end]`` 过滤；
  设施集合是记录级过滤，过滤造成的部分观测会得到 ``insufficient`` 判定和
  缺失链路说明。
- 零 / 负 / 非有限重量被排除并记录在 ``excluded_records``；``rejected`` 状态
  的重量单独归类，计入输出（最终处置），但永远不会计入 processed。
- 重量用 ``Decimal`` 精确求和，仅在输出时按 0.001 kg 四舍五入，避免浮点误差。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Iterable

from .flow import can_transition
from .identity import facility_key

RULE_VERSION = "v1"
SUPPORTED_RULE_VERSIONS = (RULE_VERSION,)

KG_QUANTUM = Decimal("0.001")

CATEGORIES = ("input", "transfer", "received", "processed", "rejected")
TERMINAL_CATEGORIES = ("processed", "rejected")

_STATUS_TO_CATEGORY = {
    "collected": "input",
    "in_transit": "transfer",
    "received": "received",
    "processed": "processed",
    "rejected": "rejected",
}


class BalanceError(Exception):
    """批次重量平衡核算失败的基类。"""


class BalanceCancelled(BalanceError):
    """调用方取消核算运行时抛出。"""


class BalanceResourceLimit(BalanceError):
    """核算运行超出配置的资源上限时抛出。"""


class BalanceStorageError(BalanceError):
    """核算结果的存储操作无法完成时抛出（含锁冲突）。"""


class BalanceMigrationError(BalanceStorageError):
    """核算存储结构无法安全迁移时抛出。"""


def parse_instant(value: str) -> datetime:
    """解析 ISO-8601 时间戳；缺少时区时按 UTC 处理，统一归一到 UTC。"""
    text = str(value).strip()
    if not text:
        raise ValueError("timestamp must not be empty")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"timestamp is not ISO-8601: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def to_kg(value: Any) -> Decimal:
    """把 JSON 重量值转换为精确的 Decimal 公斤数，拒绝非数值与非有限值。"""
    if value is None or isinstance(value, bool):
        raise ValueError("weight is not numeric")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("weight is not finite")
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("weight is not numeric") from exc
    if not dec.is_finite():
        raise ValueError("weight is not finite")
    return dec


def kg_out(value: Decimal) -> float:
    """输出重量：按 0.001 kg 四舍五入，避免二进制浮点噪声进入结果。"""
    return float(value.quantize(KG_QUANTUM, rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class Observation:
    """从 records 表读取的一条已接收（已入库）观测记录。"""

    record_key: str
    source: str
    observed_at: str
    facility_id: str
    batch_id: str
    collected_at: str
    status: str
    net_weight_kg: Any
    destination_facility_id: str
    raw_hash: str


@dataclass(frozen=True)
class BalanceRequest:
    """一次平衡核算的输入：分析窗口、设施集合、损耗容忍度和规则版本。"""

    window_start: str
    window_end: str
    facilities: tuple[str, ...] = ()
    tolerance_ratio: float = 0.0
    tolerance_abs_kg: float = 0.0
    rule_version: str = RULE_VERSION
    chunk_size: int = 500
    max_records: int = 1_000_000

    def validate(self) -> "BalanceRequest":
        """校验并返回归一化后的请求，保证相同语义的输入产生相同指纹。"""
        start = parse_instant(self.window_start)
        end = parse_instant(self.window_end)
        if start > end:
            raise ValueError("window_start must not be after window_end")
        if self.rule_version not in SUPPORTED_RULE_VERSIONS:
            supported = ", ".join(SUPPORTED_RULE_VERSIONS)
            raise ValueError(f"unsupported rule_version {self.rule_version!r}; supported: {supported}")
        for name, raw in (("tolerance_ratio", self.tolerance_ratio), ("tolerance_abs_kg", self.tolerance_abs_kg)):
            if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(raw) or raw < 0:
                raise ValueError(f"{name} must be a non-negative finite number")
        if not isinstance(self.chunk_size, int) or self.chunk_size < 1:
            raise ValueError("chunk_size must be a positive integer")
        if not isinstance(self.max_records, int) or self.max_records < 1:
            raise ValueError("max_records must be a positive integer")
        facilities = tuple(dict.fromkeys(f.strip() for f in self.facilities if f and f.strip()))
        return BalanceRequest(
            window_start=start.isoformat(),
            window_end=end.isoformat(),
            facilities=facilities,
            tolerance_ratio=float(self.tolerance_ratio),
            tolerance_abs_kg=float(self.tolerance_abs_kg),
            rule_version=self.rule_version,
            chunk_size=self.chunk_size,
            max_records=self.max_records,
        )

    @property
    def window_start_dt(self) -> datetime:
        return parse_instant(self.window_start)

    @property
    def window_end_dt(self) -> datetime:
        return parse_instant(self.window_end)

    @property
    def facility_keys(self) -> frozenset[str]:
        return frozenset(facility_key(f) for f in self.facilities)

    def params_dict(self) -> dict[str, Any]:
        """完整运行参数（含读取控制），随运行记录保存，便于审计与复现。"""
        return {
            "window_start": self.window_start,
            "window_end": self.window_end,
            "facilities": sorted(self.facilities),
            "tolerance_ratio": self.tolerance_ratio,
            "tolerance_abs_kg": self.tolerance_abs_kg,
            "chunk_size": self.chunk_size,
            "max_records": self.max_records,
        }

    def result_params_dict(self) -> dict[str, Any]:
        """只包含影响核算结果的参数（窗口、设施集合、容忍度），用于增量缓存键。"""
        return {
            "window_start": self.window_start,
            "window_end": self.window_end,
            "facilities": sorted(self.facility_keys),
            "tolerance_ratio": self.tolerance_ratio,
            "tolerance_abs_kg": self.tolerance_abs_kg,
        }


def _observation_sort_key(obs: Observation) -> tuple[datetime, str]:
    try:
        return (parse_instant(obs.observed_at), obs.record_key)
    except ValueError:
        return (datetime.min.replace(tzinfo=timezone.utc), obs.record_key)


def _find_cycle(edges: Iterable[tuple[str, str]]) -> list[str]:
    """在设施边图中找一个有向环；无环返回空列表。结果确定（按字典序）。"""
    adjacency: dict[str, list[str]] = {}
    for src, dst in sorted(set(edges)):
        adjacency.setdefault(src, []).append(dst)
    color: dict[str, int] = {node: 0 for node in adjacency}
    stack: list[str] = []
    cycle: list[str] = []

    def visit(node: str) -> None:
        if cycle:
            return
        color[node] = 1
        stack.append(node)
        for nxt in adjacency.get(node, []):
            if cycle:
                return
            if color.get(nxt, 0) == 1:
                cycle.extend(stack[stack.index(nxt):] + [nxt])
                return
            if color.get(nxt, 0) == 0:
                visit(nxt)
        stack.pop()
        color[node] = 2

    for node in sorted(adjacency):
        if color[node] == 0:
            visit(node)
        if cycle:
            break
    return cycle


def summarize_batch(
    batch_id: str,
    observations: list[Observation],
    *,
    tolerance_ratio: Decimal,
    tolerance_abs_kg: Decimal,
) -> dict[str, Any]:
    """对单个批次做重量平衡核算，返回可 JSON 序列化的可解释汇总。"""
    excluded: list[dict[str, Any]] = []
    valid: list[tuple[Observation, Decimal]] = []
    for obs in observations:
        try:
            weight = to_kg(obs.net_weight_kg)
        except ValueError:
            excluded.append({"record_key": obs.record_key, "reason": "invalid_weight"})
            continue
        if weight <= 0:
            excluded.append({"record_key": obs.record_key, "reason": "non_positive_weight"})
            continue
        status = obs.status.strip().lower()
        if status not in _STATUS_TO_CATEGORY:
            excluded.append({"record_key": obs.record_key, "reason": f"unknown_status:{obs.status}"})
            continue
        valid.append((obs, weight))

    groups: dict[tuple[str, str], list[tuple[Observation, Decimal]]] = {}
    for item in valid:
        obs = item[0]
        key = (facility_key(obs.facility_id), obs.status.strip().lower())
        groups.setdefault(key, []).append(item)

    anomalies: list[dict[str, Any]] = []
    effective: list[tuple[Observation, Decimal]] = []
    for (fac, status), members in sorted(groups.items()):
        ordered = sorted(members, key=lambda m: _observation_sort_key(m[0]))
        if len(ordered) > 1:
            anomalies.append({
                "type": "duplicate_weighing",
                "facility_id": fac,
                "status": status,
                "count": len(ordered),
                "sources": sorted({m[0].source for m in ordered}),
                "evidence": [m[0].record_key for m in ordered],
                "resolution": "latest_observation_wins",
            })
        effective.append(ordered[-1])

    sums = {category: Decimal("0") for category in CATEGORIES}
    evidence: dict[str, list[str]] = {category: [] for category in CATEGORIES}
    for obs, weight in effective:
        category = _STATUS_TO_CATEGORY[obs.status.strip().lower()]
        sums[category] += weight
        evidence[category].append(obs.record_key)

    missing_links: list[dict[str, Any]] = []
    if not evidence["input"]:
        missing_links.append({"stage": "collected", "reason": "no origin weighing observed in window"})
    if not evidence["received"]:
        missing_links.append({"stage": "received", "reason": "no destination receipt observed in window"})
    if not any(evidence[c] for c in TERMINAL_CATEGORIES):
        missing_links.append({"stage": "terminal", "reason": "no processed or rejected observation in window"})
    dest_missing = sorted(
        obs.record_key
        for obs, _ in effective
        if obs.status.strip().lower() in ("in_transit", "received") and not obs.destination_facility_id.strip()
    )
    if dest_missing:
        missing_links.append({
            "stage": "destination",
            "reason": "destination facility missing on transfer or receipt records",
            "evidence": dest_missing,
        })

    timed = sorted(effective, key=lambda item: _observation_sort_key(item[0]))
    for (prev_obs, _), (curr_obs, _) in zip(timed, timed[1:]):
        prev_status = prev_obs.status.strip().lower()
        curr_status = curr_obs.status.strip().lower()
        if prev_status != curr_status and not can_transition(prev_status, curr_status):
            anomalies.append({
                "type": "status_regression",
                "from_status": prev_status,
                "to_status": curr_status,
                "reason": "observed lifecycle order contradicts allowed transitions (status rollback or interleaved upstream/downstream timestamps)",
                "evidence": [prev_obs.record_key, curr_obs.record_key],
            })

    edges: set[tuple[str, str]] = set()
    for obs, _ in effective:
        src = facility_key(obs.facility_id)
        dst = obs.destination_facility_id.strip()
        if dst:
            dst_key = facility_key(dst)
            if dst_key != src:
                edges.add((src, dst_key))
    cycle = _find_cycle(edges)
    if cycle:
        anomalies.append({
            "type": "facility_cycle",
            "reason": "facility transfer edges form a cycle; weight is attributed per facility/status node and never flowed along edges",
            "cycle": cycle,
        })

    output = sums["processed"] + sums["rejected"]
    reasons: list[str] = []
    balance_diff: Decimal | None = None
    tolerance: Decimal | None = None
    if not valid:
        verdict = "insufficient"
        reasons.append("no valid observations: every record was excluded")
    elif not evidence["input"]:
        verdict = "insufficient"
        reasons.append("cannot determine balance without an input (collected) observation")
    elif not any(evidence[c] for c in TERMINAL_CATEGORIES):
        verdict = "insufficient"
        reasons.append("cannot determine balance without a terminal (processed or rejected) observation")
    else:
        balance_diff = sums["input"] - output
        ratio_tolerance = sums["input"] * tolerance_ratio
        tolerance = tolerance_abs_kg if tolerance_abs_kg >= ratio_tolerance else ratio_tolerance
        if balance_diff > tolerance:
            verdict = "gap"
        elif balance_diff < -tolerance:
            verdict = "surplus"
        else:
            verdict = "balanced"

    transfer_loss: Decimal | None = None
    if evidence["input"] and evidence["received"]:
        transfer_loss = sums["input"] - sums["received"]

    return {
        "batch_id": batch_id,
        "verdict": verdict,
        "reasons": reasons,
        "input_kg": kg_out(sums["input"]),
        "transfer_kg": kg_out(sums["transfer"]),
        "received_kg": kg_out(sums["received"]),
        "processed_kg": kg_out(sums["processed"]),
        "rejected_kg": kg_out(sums["rejected"]),
        "output_kg": kg_out(output),
        "balance_diff_kg": kg_out(balance_diff) if balance_diff is not None else None,
        "tolerance_kg": kg_out(tolerance) if tolerance is not None else None,
        "transfer_loss_kg": kg_out(transfer_loss) if transfer_loss is not None else None,
        "missing_links": missing_links,
        "anomalies": anomalies,
        "evidence": {category: sorted(keys) for category, keys in evidence.items()},
        "excluded_records": excluded,
        "observation_count": len(observations),
    }
