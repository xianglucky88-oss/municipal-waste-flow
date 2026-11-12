from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias


@dataclass(frozen=True)
class WasteRecord:
    source: str
    record_key: str
    observed_at: str
    facility_id: str
    batch_id: str
    collected_at: str
    waste_type: str
    net_weight_kg: float
    origin_zone: str
    destination_facility_id: str
    status: str
    payload: dict[str, Any]
    raw_hash: str


DOMAIN_RECORD_TYPE: TypeAlias = WasteRecord
