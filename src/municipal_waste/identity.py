def record_key(batch_id: str, facility_id: str, observed_at: str) -> str:
    return "|".join(value.strip().upper() for value in (batch_id, facility_id, observed_at))

def facility_key(facility_id: str) -> str:
    return facility_id.strip().upper()

def batch_key(batch_id: str, facility_id: str) -> str:
    return f"{batch_id.strip().upper()}|{facility_key(facility_id)}"
