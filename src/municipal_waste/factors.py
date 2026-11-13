"""版本化因子表：调用方提供的废物类型、设施边和处理阶段因子。

核算引擎不自行发明任何因子：它应用的每一个数量都来自调用方提供的
:class:`FactorSet`，并锚定到某个规则版本（rules_version）。因子表中的
数值一律按十进制 Decimal 处理，避免二进制浮点噪声进入账本。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

FACTOR_KIND_WASTE_TYPE = "waste_type"
FACTOR_KIND_EDGE = "edge"
FACTOR_KIND_STAGE = "stage"
FACTOR_KIND_SELF_LOOP = "self_loop"

_TOP_LEVEL_KEYS = {"waste_type_factors", "edge_factors", "stage_factors", "metadata"}
_ENTRY_KEYS = {"factor", "unit", "result_unit", "factor_version"}


def parse_decimal(value: Any, *, field: str) -> Decimal:
    """把调用方输入严格转换为有限 Decimal，拒绝布尔/非数/非有限值。"""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field} must be a number")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, (int, float, str)):
        try:
            result = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"{field} must be a number") from exc
    else:
        raise ValueError(f"{field} must be a number")
    if not result.is_finite():
        raise ValueError(f"{field} must be finite")
    return result


def dec_str(value: Decimal) -> str:
    """输出规范十进制字符串：无指数、无多余尾零，便于账本精确存储与比较。"""
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


@dataclass(frozen=True)
class FactorEntry:
    """单条因子：数值、因子单位、结果单位与因子自身版本号。"""

    value: Decimal
    unit: str = ""
    result_unit: str = ""
    factor_version: str = "unspecified"

    @staticmethod
    def from_mapping(raw: Any, *, context: str) -> "FactorEntry":
        if not isinstance(raw, Mapping):
            raise ValueError(f"{context} must be an object")
        unknown = set(raw) - _ENTRY_KEYS
        if unknown:
            raise ValueError(f"{context} has unknown fields: {sorted(unknown)}")
        if "factor" not in raw:
            raise ValueError(f"{context} requires a factor value")
        value = parse_decimal(raw["factor"], field=f"{context}.factor")
        unit = _text(raw.get("unit"))
        result_unit = _text(raw.get("result_unit")) or unit
        version = _text(raw.get("factor_version")) or "unspecified"
        return FactorEntry(value=value, unit=unit, result_unit=result_unit, factor_version=version)

    def canonical(self) -> dict[str, str]:
        return {
            "factor": dec_str(self.value),
            "unit": self.unit,
            "result_unit": self.result_unit,
            "factor_version": self.factor_version,
        }


@dataclass(frozen=True)
class FactorSet:
    """三张版本化因子表：废物类型、设施边（origin>destination）、处理阶段。

    - ``waste_type_factors``：waste_type -> 因子（按废物类型）
    - ``edge_factors``：(origin_facility, destination_facility) -> 因子（显式边，
      没有距离字段的路径必须由调用方在此显式给出，引擎绝不猜测路线）
    - ``stage_factors``：处理阶段（记录当前 status）-> 因子
    """

    waste_type_factors: dict[str, FactorEntry]
    edge_factors: dict[tuple[str, str], FactorEntry]
    stage_factors: dict[str, FactorEntry]

    @staticmethod
    def from_mapping(raw: Any) -> "FactorSet":
        if not isinstance(raw, Mapping):
            raise ValueError("factor set must be a JSON object")
        unknown = set(raw) - _TOP_LEVEL_KEYS
        if unknown:
            raise ValueError(f"unknown factor tables: {sorted(unknown)}")
        return FactorSet(
            waste_type_factors=_parse_keyed(raw.get("waste_type_factors"), "waste_type_factors"),
            edge_factors=_parse_edges(raw.get("edge_factors")),
            stage_factors=_parse_keyed(raw.get("stage_factors"), "stage_factors"),
        )

    def canonical(self) -> dict[str, Any]:
        return {
            "edge_factors": {f"{origin}>{dest}": entry.canonical() for (origin, dest), entry in sorted(self.edge_factors.items())},
            "stage_factors": {key: entry.canonical() for key, entry in sorted(self.stage_factors.items())},
            "waste_type_factors": {key: entry.canonical() for key, entry in sorted(self.waste_type_factors.items())},
        }

    def digest(self) -> str:
        blob = json.dumps(self.canonical(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def waste_type_factor(self, waste_type: str) -> FactorEntry | None:
        return self.waste_type_factors.get(waste_type)

    def edge_factor(self, origin: str, destination: str) -> FactorEntry | None:
        return self.edge_factors.get((origin, destination))

    def stage_factor(self, status: str) -> FactorEntry | None:
        return self.stage_factors.get(status)


def _parse_keyed(raw: Any, table: str) -> dict[str, FactorEntry]:
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError(f"{table} must be an object")
    parsed: dict[str, FactorEntry] = {}
    for key, entry in raw.items():
        name = _text(key)
        if not name:
            raise ValueError(f"{table} keys must be non-empty")
        parsed[name] = FactorEntry.from_mapping(entry, context=f"{table}.{name}")
    return parsed


def _parse_edges(raw: Any) -> dict[tuple[str, str], FactorEntry]:
    if raw is None:
        return {}
    parsed: dict[tuple[str, str], FactorEntry] = {}
    if isinstance(raw, Mapping):
        for key, entry in raw.items():
            text = _text(key)
            origin, destination = _split_edge_key(text)
            parsed[(origin, destination)] = FactorEntry.from_mapping(entry, context=f"edge_factors.{text}")
        return parsed
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, Mapping):
                raise ValueError("edge_factors entries must be objects")
            origin, destination = _text(item.get("from")), _text(item.get("to"))
            if not origin or not destination:
                raise ValueError("edge_factors entries require from and to")
            body = {k: v for k, v in item.items() if k not in ("from", "to")}
            parsed[(origin, destination)] = FactorEntry.from_mapping(body, context=f"edge_factors.{origin}>{destination}")
        return parsed
    raise ValueError("edge_factors must be an object or a list")


def _split_edge_key(text: str) -> tuple[str, str]:
    if ">" not in text:
        raise ValueError(f"edge_factors key must be ORIGIN>DESTINATION: {text!r}")
    origin, destination = (part.strip() for part in text.split(">", 1))
    if not origin or not destination:
        raise ValueError(f"edge_factors key must be ORIGIN>DESTINATION: {text!r}")
    return origin, destination
