from __future__ import annotations

import hashlib
import json
from typing import Any

from .models import WasteRecord
from .identity import record_key


def _text(value: Any) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("numeric field is invalid") from exc


def normalize(raw: Any, source: str) -> WasteRecord:
    if not isinstance(raw, dict):
        raise ValueError("record must be an object")
    payload = raw.get("payload")
    observed_at = _text(raw.get("observed_at"))
    if not isinstance(payload, dict) or not observed_at:
        raise ValueError("observed_at and object payload are required")

    facility_id = _text(payload.get("facility_id"))
    batch_id = _text(payload.get("batch_id"))
    collected_at = _text(payload.get("collected_at"))
    waste_type = _text(payload.get("waste_type")).lower()
    net_weight_kg = _number(payload.get("net_weight_kg"))
    origin_zone = _text(payload.get("origin_zone"))
    destination_facility_id = _text(payload.get("destination_facility_id"))
    status = _text(payload.get("status")).lower()
    if not facility_id or not batch_id or not collected_at or not waste_type or net_weight_kg is None or not destination_facility_id or not status:
        raise ValueError("facility, batch, collection time, type, weight, destination and status are required")
    if waste_type not in {"mixed", "food", "recyclable", "hazardous", "construction"}:
        raise ValueError("unsupported waste type")
    if status not in {"collected", "in_transit", "received", "processed", "rejected"}:
        raise ValueError("unsupported waste status")
    if net_weight_kg <= 0:
        raise ValueError("net_weight_kg must be positive")

    normalized = {"facility_id": facility_id, "batch_id": batch_id, "collected_at": collected_at, "waste_type": waste_type, "net_weight_kg": net_weight_kg, "origin_zone": origin_zone, "destination_facility_id": destination_facility_id, "status": status}
    key = record_key(batch_id, facility_id, observed_at)
    canonical = json.dumps({"source": source, "observed_at": observed_at, "payload": normalized}, ensure_ascii=False, sort_keys=True)
    raw_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return WasteRecord(
        source=source,
        record_key=key,
        observed_at=observed_at,
        facility_id=normalized["facility_id"],
        batch_id=normalized["batch_id"],
        collected_at=normalized["collected_at"],
        waste_type=normalized["waste_type"],
        net_weight_kg=normalized["net_weight_kg"],
        origin_zone=normalized["origin_zone"],
        destination_facility_id=normalized["destination_facility_id"],
        status=normalized["status"],
        payload=normalized,
        raw_hash=raw_hash,
    )
