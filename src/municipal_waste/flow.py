FLOW_STATES = ("collected", "in_transit", "received", "processed", "rejected")


def can_transition(previous: str, current: str) -> bool:
    if previous == current:
        return True
    allowed = {
        "collected": {"in_transit", "rejected"},
        "in_transit": {"received", "rejected"},
        "received": {"processed", "rejected"},
        "processed": set(),
        "rejected": set(),
    }
    return current in allowed.get(previous, set())
