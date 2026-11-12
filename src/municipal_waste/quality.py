from __future__ import annotations

from .models import WasteRecord


def validate(record: WasteRecord) -> list[str]:
    issues: list[str] = []

    if record.facility_id == record.destination_facility_id and record.status != "processed":
        issues.append("source and destination facility cannot be identical before processing")

    return issues
