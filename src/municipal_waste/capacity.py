"""Facility processing-capacity overload assessment.

This module deliberately does **not** perform weight balancing (input/output
conservation).  It answers a different question: for one receiving facility and
one waste type, how much accepted load was placed on it during each time slice,
and was that load above the capacity rule that was in force?

Attribution rule (the no-double-count guarantee)
------------------------------------------------
Every weighing is counted at exactly one facility: its
``destination_facility_id`` (the facility that has to process the load).
``facility_id`` is kept as provenance / reporting-site information only and is
never independently counted.  A record whose source and destination are the
same therefore still contributes exactly once, and a weighing reported by both
the source and the destination contributes at most once because all revisions
of a batch resolve to one canonical record.

Time basis
----------
* ``collected_at`` is the *event time*.  It is what places a load into an
  hourly or daily slice.
* ``observed_at`` is the *knowledge time*.  It orders revisions (latest
  revision wins) and defines the ``as_of`` knowledge snapshot.

Timestamps are parsed as ISO-8601.  A ``Z`` suffix or an explicit offset is
honoured; a timestamp without an offset is interpreted as UTC.  Bucket
boundaries use the capacity rule's fixed ``timezone_offset_minutes``, which is
how cross-midnight and cross-day windows are resolved.  A slice clipped by the
requested window (or by a rule's effective period) is assessed on the covered
part only; its capacity is prorated per second.

Deliberately explicit policies
------------------------------
* ``status`` ``received`` and ``processed`` count as received load.  A
  ``rejected`` canonical status excludes the batch and makes the slice
  undecidable.
* A status regression (for example ``received`` followed by ``in_transit``)
  excludes the batch and makes the slice undecidable.
* Revisions of one batch (same ``batch_id``) resolve to the revision with the
  greatest ``observed_at``; ties break on ``raw_hash``.  Competing rows with the
  same ``observed_at`` but different facts make the slice undecidable.
* Rows of one batch that agree on destination, weight and status are reported
  as ``duplicate_report`` and counted once.
* A batch whose facility chain returns to an already visited facility is
  reported as ``facility_loop``, excluded, and makes the slice undecidable.
* Non-positive or missing weight and unparseable event times are undecidable.
* A window region without an effective capacity rule is undecidable.

The engine is pure: it consumes already-stored load records plus rule objects
and returns immutable plain-data results.  Persistence, locking, incremental
slice caching and read-only access live in ``capacity_service.py``.
"""
from __future__ import annotations

import hashlib
import json
import math
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

from .flow import FLOW_STATES

ENGINE_VERSION = "capacity-v1"
_EPSILON = 1e-9
_UTC = timezone.utc

_STATUS_STAGE = {"collected": 1, "in_transit": 2, "received": 3, "processed": 4}
_PERIOD_ORDER = {"hour": 1, "day": 2, "window": 3}
_PERIOD_SECONDS = {"hour": 3600.0, "day": 86400.0}


class CapacityError(Exception):
    """Base class for capacity assessment errors."""


class CapacityRuleConflict(CapacityError):
    """Capacity rules overlap or contradict each other."""


class CapacityCancelled(CapacityError):
    """The caller cancelled the assessment."""


class CapacityResourceLimit(CapacityError):
    """A bounded-resource limit (records or slices) was exceeded."""


class CancelToken:
    """Thread-safe cooperative cancellation token."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def raise_if_cancelled(self) -> None:
        if self._event.is_set():
            raise CapacityCancelled("capacity assessment cancelled")


def parse_timestamp(value: Any) -> datetime:
    """Parse an ISO-8601 timestamp into an aware UTC datetime."""
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("timestamp is empty")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        moment = datetime.fromisoformat(text)
    else:
        raise ValueError(f"timestamp is not a string: {value!r}")
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_UTC)
    return moment.astimezone(_UTC)


def format_timestamp(moment: datetime) -> str:
    """Render a datetime as a canonical UTC ``Z`` timestamp."""
    return moment.astimezone(_UTC).isoformat().replace("+00:00", "Z")


def parse_timezone_offset(value: Any) -> int:
    """Accept ``"+08:00"``, ``"-05:30"``, ``"UTC"`` or a whole minute count."""
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        raise ValueError("timezone offset is invalid")
    if isinstance(value, (int, float)):
        minutes = int(value)
    else:
        text = str(value).strip()
        if text.upper() in {"Z", "UTC", "GMT"}:
            return 0
        sign = 1
        if text.startswith("-"):
            sign = -1
            text = text[1:]
        elif text.startswith("+"):
            text = text[1:]
        if ":" in text:
            hours_text, _, minutes_text = text.partition(":")
            hours, minutes = int(hours_text), int(minutes_text)
        elif len(text) == 4 and text.isdigit():
            hours, minutes = int(text[:2]), int(text[2:])
        elif text.isdigit():
            hours, minutes = int(text), 0
        else:
            raise ValueError(f"timezone offset is invalid: {value!r}")
        minutes = sign * (hours * 60 + minutes)
    if minutes <= -840 or minutes >= 840:
        raise ValueError("timezone offset must be within +/-14 hours")
    return minutes


@dataclass(frozen=True)
class LoadRecord:
    """A stored weighing, decoupled from the ingestion dataclass."""

    record_key: str
    source: str
    observed_at: str
    facility_id: str
    destination_facility_id: str
    waste_type: str
    collected_at: str
    status: str
    net_weight_kg: float | None
    raw_hash: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(
        cls,
        record_key: str,
        source: str,
        observed_at: str,
        payload: Mapping[str, Any],
        raw_hash: str,
    ) -> "LoadRecord":
        weight = payload.get("net_weight_kg")
        try:
            numeric_weight = None if weight in (None, "") else float(weight)
        except (TypeError, ValueError):
            numeric_weight = None
        return cls(
            record_key=record_key,
            source=source,
            observed_at=observed_at,
            facility_id=str(payload.get("facility_id") or ""),
            destination_facility_id=str(payload.get("destination_facility_id") or ""),
            waste_type=str(payload.get("waste_type") or ""),
            collected_at=str(payload.get("collected_at") or ""),
            status=str(payload.get("status") or "").lower(),
            net_weight_kg=numeric_weight,
            raw_hash=raw_hash,
            payload=payload,
        )


@dataclass(frozen=True)
class RejectionEvidence:
    """A rejected ingestion row that may hide an unaccounted load."""

    rejection_id: int
    error: str
    collected_at: str
    destination_facility_id: str
    waste_type: str
    raw_fingerprint: str
    batch_id: str = ""


@dataclass(frozen=True)
class _Period:
    name: str
    seconds: float
    limit_kg: float


@dataclass(frozen=True)
class CapacityRule:
    """One immutable version of a facility/waste-type capacity rule."""

    facility_id: str
    waste_type: str
    version: int
    effective_from: datetime
    effective_to: datetime | None
    timezone_offset_minutes: int
    daily_limit_kg: float | None = None
    hourly_limit_kg: float | None = None

    def __post_init__(self) -> None:
        if not self.facility_id.strip():
            raise ValueError("facility_id is required")
        if not self.waste_type.strip():
            raise ValueError("waste_type is required")
        if int(self.version) < 1:
            raise ValueError("version must be a positive integer")
        if not isinstance(self.effective_from, datetime) or not isinstance(self.effective_to, (datetime, type(None))):
            raise ValueError("effective period must use datetimes")
        if self.effective_to is not None and self.effective_to <= self.effective_from:
            raise ValueError("effective_to must be after effective_from")
        if self.daily_limit_kg is None and self.hourly_limit_kg is None:
            raise ValueError("a daily or hourly limit is required")
        for name, limit in (("daily_limit_kg", self.daily_limit_kg), ("hourly_limit_kg", self.hourly_limit_kg)):
            if limit is not None and float(limit) <= 0:
                raise ValueError(f"{name} must be positive")

    @property
    def facility_key(self) -> str:
        return self.facility_id.strip().upper()

    @property
    def waste_key(self) -> str:
        return self.waste_type.strip().lower()

    @property
    def periods(self) -> tuple[_Period, ...]:
        periods: list[_Period] = []
        if self.hourly_limit_kg is not None:
            periods.append(_Period("hour", _PERIOD_SECONDS["hour"], float(self.hourly_limit_kg)))
        if self.daily_limit_kg is not None:
            periods.append(_Period("day", _PERIOD_SECONDS["day"], float(self.daily_limit_kg)))
        return tuple(periods)

    def covers(self, start: datetime, end: datetime) -> bool:
        return self.effective_from < end and (self.effective_to is None or self.effective_to > start)

    def overlaps(self, other: "CapacityRule") -> bool:
        left_end = self.effective_to or datetime.max.replace(tzinfo=_UTC)
        right_end = other.effective_to or datetime.max.replace(tzinfo=_UTC)
        return self.effective_from < right_end and other.effective_from < left_end

    @property
    def rule_hash(self) -> str:
        return _hash_json(self.as_dict(include_hash=False))

    def as_dict(self, include_hash: bool = True) -> dict[str, Any]:
        data = {
            "facility_id": self.facility_id,
            "waste_type": self.waste_type,
            "version": int(self.version),
            "effective_from": format_timestamp(self.effective_from),
            "effective_to": None if self.effective_to is None else format_timestamp(self.effective_to),
            "timezone_offset_minutes": int(self.timezone_offset_minutes),
            "daily_limit_kg": None if self.daily_limit_kg is None else float(self.daily_limit_kg),
            "hourly_limit_kg": None if self.hourly_limit_kg is None else float(self.hourly_limit_kg),
        }
        if include_hash:
            data["rule_hash"] = self.rule_hash
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CapacityRule":
        def _limit(name: str) -> float | None:
            value = data.get(name)
            return None if value in (None, "") else float(value)

        effective_to = data.get("effective_to")
        return cls(
            facility_id=str(data.get("facility_id") or "").strip(),
            waste_type=str(data.get("waste_type") or "").strip().lower(),
            version=int(data.get("version")),
            effective_from=parse_timestamp(data.get("effective_from")),
            effective_to=None if effective_to in (None, "") else parse_timestamp(effective_to),
            timezone_offset_minutes=parse_timezone_offset(data.get("timezone", data.get("timezone_offset_minutes", 0))),
            daily_limit_kg=_limit("daily_limit_kg"),
            hourly_limit_kg=_limit("hourly_limit_kg"),
        )


def _validate_scope_rules(scoped: Sequence[CapacityRule]) -> list[CapacityRule]:
    """Reject contradictory versions, allowing an open successor to close a gap.

    A version with an open ``effective_to`` is implicitly superseded by a later
    open-ended version, so the pair is not a conflict.  A *bounded* later
    version, however, must sit wholly outside its predecessors: any real
    overlap is a rule conflict.
    """
    ordered = sorted(scoped, key=lambda rule: (rule.effective_from, rule.version))
    for previous, current in zip(ordered, ordered[1:]):
        if previous.effective_to is None and current.effective_to is None:
            continue
        if previous.overlaps(current):
            raise CapacityRuleConflict(
                f"capacity rule versions {previous.version} and {current.version} "
                f"overlap for ({previous.facility_key}, {previous.waste_key})"
            )
    return ordered


def parse_rules(payload: Any) -> list[CapacityRule]:
    """Parse a rule document and reject overlapping versions per scope."""
    if isinstance(payload, Mapping):
        payload = payload.get("rules", [])
    if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes)):
        raise CapacityRuleConflict("capacity rules must be a list")
    rules = [CapacityRule.from_dict(item) for item in payload]
    by_scope: dict[tuple[str, str], list[CapacityRule]] = {}
    for rule in rules:
        by_scope.setdefault((rule.facility_key, rule.waste_key), []).append(rule)
    for scope, scoped in by_scope.items():
        versions = [rule.version for rule in scoped]
        if len(set(versions)) != len(versions):
            raise CapacityRuleConflict(f"duplicate capacity rule version for {scope}")
        _validate_scope_rules(scoped)
    return rules


def _hash_json(value: Any) -> str:
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


BLOCKING_REASONS = frozenset(
    {
        "facility_loop",
        "invalid_weight",
        "rejected_record",
        "rejected_status",
        "missing_batch_id",
        "invalid_timestamp",
        "status_regression",
        "nonpositive_weight",
        "unknown_event_time",
        "unsupported_status",
        "invalid_observed_at",
        "missing_destination",
        "conflicting_revision",
        "missing_capacity_rule",
    }
)
_INFO_REASONS = frozenset({"duplicate_report", "superseded_revision", "pending_not_received", "rejected_then_revised"})


def is_blocking(reason: str) -> bool:
    return reason in BLOCKING_REASONS


@dataclass(frozen=True)
class Evidence:
    """One exclusion / traceability note about a record."""

    record_key: str | None
    reason: str
    detail: str = ""
    batch_id: str | None = None
    observed_at: str | None = None
    collected_at: str | None = None
    weight_kg: float | None = None
    moment: datetime | None = field(default=None, compare=False)

    @property
    def blocks(self) -> bool:
        return is_blocking(self.reason)

    def to_dict(self, capacity_version: int | None = None, capacity_rule_hash: str | None = None) -> dict[str, Any]:
        data: dict[str, Any] = {
            "record_key": self.record_key,
            "reason": self.reason,
            "blocks_verdict": self.blocks,
        }
        if self.detail:
            data["detail"] = self.detail
        if self.batch_id:
            data["batch_id"] = self.batch_id
        if self.observed_at:
            data["observed_at"] = self.observed_at
        if self.collected_at:
            data["collected_at"] = self.collected_at
        if self.weight_kg is not None:
            data["weight_kg"] = round(float(self.weight_kg), 6)
        if capacity_version is not None:
            data["capacity_version"] = int(capacity_version)
        if capacity_rule_hash is not None:
            data["capacity_rule_hash"] = capacity_rule_hash
        return data


@dataclass(frozen=True)
class BatchResolution:
    """The single canonical state of one batch, or its unresolvable blocker."""

    batch_key: str
    counted: bool
    canonical: LoadRecord | None
    canonical_time: datetime | None
    weight_kg: float | None
    status: str | None
    evidence: tuple[Evidence, ...]
    blocking: bool

    @property
    def canonical_record_key(self) -> str | None:
        return self.canonical.record_key if self.canonical else None


def _batch_key(record: LoadRecord) -> str:
    return str(record.payload.get("batch_id") or "").strip().upper()


def _identity(record: LoadRecord) -> tuple[str, str, float | None]:
    return (
        record.destination_facility_id.strip().upper(),
        record.collected_at.strip(),
        None if record.net_weight_kg is None else round(float(record.net_weight_kg), 6),
    )


def _stage(status: str) -> int | None:
    if status == "rejected":
        return -1
    return _STATUS_STAGE.get(status)


def _evidence_for(record: LoadRecord, reason: str, detail: str, moment: datetime | None = None) -> Evidence:
    try:
        anchor = parse_timestamp(record.collected_at)
    except (TypeError, ValueError):
        anchor = moment
    return Evidence(
        record_key=record.record_key,
        reason=reason,
        detail=detail,
        batch_id=_batch_key(record) or None,
        observed_at=record.observed_at,
        collected_at=record.collected_at,
        weight_kg=record.net_weight_kg,
        moment=anchor,
    )


def _detect_loop(rows: Sequence[tuple[datetime, LoadRecord]]) -> bool:
    """Return True when a batch's facility chain forms a directed cycle.

    The distinct directed edges, in knowledge order, describe where the load
    moved.  A loop is a directed cycle: following an edge from source to
    destination eventually returns to the starting facility.
    """
    edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for _moment, record in rows:
        source = record.facility_id.strip().upper()
        destination = record.destination_facility_id.strip().upper()
        if not source or not destination:
            continue
        edge = (source, destination)
        if edge in seen:
            continue
        seen.add(edge)
        edges.append(edge)

    outgoing: dict[str, set[str]] = {}
    for source, destination in edges:
        outgoing.setdefault(source, set()).add(destination)

    def reaches(start: str, target: str) -> bool:
        pending = list(outgoing.get(start, ()))
        visited_nodes: set[str] = set()
        while pending:
            node = pending.pop()
            if node == target:
                return True
            if node in visited_nodes:
                continue
            visited_nodes.add(node)
            pending.extend(outgoing.get(node, ()))
        return False

    for source, destination in edges:
        if reaches(destination, source):
            return True
    return False


def resolve_batches(records: Iterable[LoadRecord], as_of: datetime) -> list[BatchResolution]:
    """Collapse all revisions / duplicate reports into one state per batch.

    Rows observed after ``as_of`` are not knowledge yet and are ignored.
    """
    grouped: dict[str, list[LoadRecord]] = {}
    invalid_observed: list[LoadRecord] = []
    for record in records:
        key = _batch_key(record)
        if not key:
            key = f"~{record.record_key}"
        try:
            observed = parse_timestamp(record.observed_at)
        except (TypeError, ValueError):
            invalid_observed.append(record)
            grouped.setdefault(key, []).append(record)
            continue
        if observed > as_of:
            continue
        grouped.setdefault(key, []).append(record)

    resolutions: list[BatchResolution] = []
    for key, rows in grouped.items():
        if not rows:
            continue
        evidence: list[Evidence] = []
        blocking = False
        parsed: list[tuple[datetime, LoadRecord]] = []
        for record in rows:
            try:
                observed = parse_timestamp(record.observed_at)
            except (TypeError, ValueError):
                blocking = True
                evidence.append(_evidence_for(record, "invalid_observed_at", "observed_at is not parseable"))
                continue
            parsed.append((observed, record))

        if not _batch_key(rows[0]):
            blocking = True
            evidence.append(
                _evidence_for(rows[0], "missing_batch_id", "batch_id is required for revision resolution")
            )

        if _detect_loop(sorted(parsed, key=lambda item: (item[0], item[1].raw_hash))):
            blocking = True
            for moment, record in sorted(parsed, key=lambda item: (item[0], item[1].raw_hash)):
                evidence.append(
                    _evidence_for(
                        record, "facility_loop", "batch returns to a previously visited facility", moment
                    )
                )

        latest_observed = max((moment for moment, _record in parsed), default=None)
        latest_rows = [record for moment, record in parsed if moment == latest_observed]

        canonical: LoadRecord | None = None
        if latest_rows:
            identities = {_identity(record) for record in latest_rows}
            statuses = {record.status for record in latest_rows}
            if len(identities) > 1 or len(statuses) > 1:
                blocking = True
                for record in latest_rows:
                    evidence.append(
                        _evidence_for(
                            record,
                            "conflicting_revision",
                            "latest revisions at one observed_at disagree on destination, weight, event time or status",
                        )
                    )
            else:
                canonical = min(latest_rows, key=lambda record: (record.raw_hash, record.record_key))

        previous_stage: int | None = None
        for moment, record in sorted(parsed, key=lambda item: (item[0], item[1].raw_hash)):
            if record is canonical:
                previous_stage = _stage(record.status)
                continue
            if canonical is None:
                continue
            stage = _stage(record.status)
            if previous_stage is not None and stage is not None and stage < previous_stage:
                blocking = True
                evidence.append(
                    _evidence_for(
                        record,
                        "status_regression",
                        f"status moved backwards from stage {previous_stage} to {stage}",
                        moment,
                    )
                )
            if _identity(record) == _identity(canonical):
                if record.status == canonical.status:
                    evidence.append(_evidence_for(record, "duplicate_report", "same weighing reported more than once", moment))
                else:
                    evidence.append(
                        _evidence_for(
                            record, "superseded_revision", f"superseded by later status {canonical.status}", moment
                        )
                    )
            else:
                evidence.append(
                    _evidence_for(
                        record,
                        "superseded_revision",
                        "superseded by later destination, weight or event time",
                        moment,
                    )
                )
            if stage is not None:
                previous_stage = stage

        if canonical is not None:
            for record in latest_rows:
                if record is canonical:
                    continue
                if _identity(record) != _identity(canonical):
                    continue
                if record.status != canonical.status:
                    continue
                evidence.append(_evidence_for(record, "duplicate_report", "same weighing reported more than once"))

        if canonical is None:
            resolutions.append(
                BatchResolution(
                    batch_key=key,
                    counted=False,
                    canonical=None,
                    canonical_time=None,
                    weight_kg=None,
                    status=None,
                    evidence=tuple(evidence),
                    blocking=True,
                )
            )
            continue

        status = canonical.status
        canonical_time: datetime | None = None
        try:
            canonical_time = parse_timestamp(canonical.collected_at)
        except (TypeError, ValueError):
            blocking = True
            evidence.append(_evidence_for(canonical, "invalid_timestamp", "collected_at is not parseable"))

        if not canonical.destination_facility_id.strip():
            blocking = True
            evidence.append(_evidence_for(canonical, "missing_destination"))
        if not canonical.facility_id.strip():
            blocking = True
            evidence.append(_evidence_for(canonical, "missing_destination", "facility_id is required"))
        if status not in FLOW_STATES:
            blocking = True
            evidence.append(_evidence_for(canonical, "unsupported_status", f"unsupported status {status!r}"))
        elif status == "rejected":
            blocking = True
            evidence.append(_evidence_for(canonical, "rejected_status", "canonical status is rejected"))
        elif status in ("collected", "in_transit"):
            evidence.append(
                _evidence_for(canonical, "pending_not_received", f"status {status} has not been received yet")
            )

        weight = canonical.net_weight_kg
        if weight is None:
            blocking = True
            evidence.append(_evidence_for(canonical, "invalid_weight", "weight is missing or not numeric"))
        elif float(weight) <= 0:
            blocking = True
            evidence.append(_evidence_for(canonical, "nonpositive_weight", "weight is zero or negative"))

        counted = (
            not blocking
            and status in ("received", "processed")
            and weight is not None
            and float(weight) > 0
            and canonical_time is not None
        )

        resolutions.append(
            BatchResolution(
                batch_key=key,
                counted=counted,
                canonical=canonical,
                canonical_time=canonical_time,
                weight_kg=None if weight is None else float(weight),
                status=status,
                evidence=tuple(evidence),
                blocking=blocking,
            )
        )

    for record in invalid_observed:
        if _batch_key(record):
            continue
        resolutions.append(
            BatchResolution(
                batch_key=f"~{record.record_key}",
                counted=False,
                canonical=None,
                canonical_time=None,
                weight_kg=None,
                status=None,
                evidence=(_evidence_for(record, "invalid_observed_at", "observed_at is not parseable"),),
                blocking=True,
            )
        )

    resolutions.sort(key=lambda item: item.batch_key)
    return resolutions


def resolve_rejections(
    events: Iterable[RejectionEvidence], resolutions: Iterable[BatchResolution]
) -> list[Evidence]:
    """Turn ingestion rejections into blocking evidence unless a revision fixed them."""
    known_batches = {resolution.batch_key for resolution in resolutions}
    seen_fingerprints: set[str] = set()
    evidence: list[Evidence] = []
    for event in events:
        if event.raw_fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(event.raw_fingerprint)
        moment: datetime | None = None
        try:
            moment = parse_timestamp(event.collected_at)
        except (TypeError, ValueError):
            moment = None
        batch_key = (event.batch_id or "").strip().upper()
        if batch_key and batch_key in known_batches:
            evidence.append(
                Evidence(
                    record_key=None,
                    reason="rejected_then_revised",
                    detail=f"rejection {event.rejection_id} was superseded by an accepted revision",
                    batch_id=batch_key,
                    observed_at=None,
                    collected_at=event.collected_at or None,
                    weight_kg=None,
                    moment=moment,
                )
            )
            continue
        evidence.append(
            Evidence(
                record_key=None,
                reason="rejected_record",
                detail=f"rejected ingestion row {event.rejection_id}: {event.error}",
                batch_id=batch_key or None,
                observed_at=None,
                collected_at=event.collected_at or None,
                weight_kg=None,
                moment=moment,
            )
        )
    return evidence
HOUR_SECONDS = 3600
DAY_SECONDS = 86400


@dataclass(frozen=True)
class SlicePlan:
    """One assessed segment of a bucket: which rule applies and for how long."""

    period: str
    bucket_start: datetime
    bucket_end: datetime
    start: datetime
    end: datetime
    rule: CapacityRule | None
    capacity_kg: float | None
    limit_kg: float | None

    @property
    def capacity_version(self) -> int | None:
        return None if self.rule is None else int(self.rule.version)

    @property
    def rule_hash(self) -> str | None:
        return None if self.rule is None else self.rule.rule_hash

    def to_dict(self) -> dict[str, Any]:
        return {
            "period": self.period,
            "bucket_start": format_timestamp(self.bucket_start),
            "bucket_end": format_timestamp(self.bucket_end),
            "slice_start": format_timestamp(self.start),
            "slice_end": format_timestamp(self.end),
            "capacity_kg": None if self.capacity_kg is None else round(self.capacity_kg, 6),
            "limit_kg": None if self.limit_kg is None else round(self.limit_kg, 6),
            "covered_seconds": (self.end - self.start).total_seconds(),
            "bucket_seconds": (self.bucket_end - self.bucket_start).total_seconds(),
            "capacity_version": self.capacity_version,
            "capacity_rule_hash": self.rule_hash,
        }


def _bucket_floor(moment: datetime, seconds: int, offset_minutes: int) -> datetime:
    shifted = moment.timestamp() + offset_minutes * 60
    epoch = int(math.floor(shifted / seconds)) * seconds
    return datetime.fromtimestamp(epoch - offset_minutes * 60, tz=_UTC)


def _iter_bucket_cuts(start: datetime, end: datetime, seconds: int, offset_minutes: int) -> Iterable[tuple[datetime, datetime]]:
    cursor = _bucket_floor(start, seconds, offset_minutes)
    while cursor < end:
        upper = cursor + timedelta(seconds=seconds)
        yield cursor, upper
        cursor = upper


def plan_slices(
    rules: Iterable[CapacityRule],
    window_start: datetime,
    window_end: datetime,
    cancel: CancelToken | None = None,
    max_slices: int | None = None,
) -> list[SlicePlan]:
    """Build every assessed segment: rule buckets, prorated clips and rule gaps."""
    if window_start >= window_end:
        raise ValueError("window_start must be before window_end")
    scope_rules = list(rules)
    _validate_scope_rules(scope_rules)

    scope_rules.sort(key=lambda rule: (rule.effective_from, rule.version))

    plans: list[SlicePlan] = []
    covered: list[tuple[datetime, datetime]] = []
    for rule in scope_rules:
        if cancel is not None:
            cancel.raise_if_cancelled()
        region_start = max(rule.effective_from, window_start)
        region_end = window_end if rule.effective_to is None else min(rule.effective_to, window_end)
        if region_start >= region_end:
            continue
        covered.append((region_start, region_end))
        for period in rule.periods:
            seconds = HOUR_SECONDS if period.name == "hour" else DAY_SECONDS
            for bucket_start, bucket_end in _iter_bucket_cuts(
                region_start, region_end, seconds, rule.timezone_offset_minutes
            ):
                if cancel is not None:
                    cancel.raise_if_cancelled()
                slice_start = max(bucket_start, region_start)
                slice_end = min(bucket_end, region_end)
                if slice_start >= slice_end:
                    continue
                ratio = (slice_end - slice_start).total_seconds() / float(seconds)
                plans.append(
                    SlicePlan(
                        period=period.name,
                        bucket_start=bucket_start,
                        bucket_end=bucket_end,
                        start=slice_start,
                        end=slice_end,
                        rule=rule,
                        capacity_kg=period.limit_kg * ratio,
                        limit_kg=period.limit_kg,
                    )
                )
                if max_slices is not None and len(plans) > max_slices:
                    raise CapacityResourceLimit(f"slice plan exceeds max_slices={max_slices}")

    coverage_cursor = window_start
    for start, end in sorted(covered):
        if start > coverage_cursor:
            plans.append(
                SlicePlan(
                    period="coverage_gap",
                    bucket_start=coverage_cursor,
                    bucket_end=start,
                    start=coverage_cursor,
                    end=start,
                    rule=None,
                    capacity_kg=None,
                    limit_kg=None,
                )
            )
            if max_slices is not None and len(plans) > max_slices:
                raise CapacityResourceLimit(f"slice plan exceeds max_slices={max_slices}")
        coverage_cursor = max(coverage_cursor, end)
    if coverage_cursor < window_end:
        plans.append(
            SlicePlan(
                period="coverage_gap",
                bucket_start=coverage_cursor,
                bucket_end=window_end,
                start=coverage_cursor,
                end=window_end,
                rule=None,
                capacity_kg=None,
                limit_kg=None,
            )
        )
        if max_slices is not None and len(plans) > max_slices:
            raise CapacityResourceLimit(f"slice plan exceeds max_slices={max_slices}")

    order = {"coverage_gap": -1, "hour": 0, "day": 1}
    plans.sort(
        key=lambda plan: (order.get(plan.period, 2), plan.start, plan.end, plan.capacity_version or 0)
    )
    return plans


def slice_cache_key(plan: SlicePlan, facility_key: str, waste_key: str, as_of: datetime) -> str:
    material = {
        "engine": ENGINE_VERSION,
        "facility": facility_key,
        "waste": waste_key,
        "period": plan.period,
        "bucket_start": format_timestamp(plan.bucket_start),
        "bucket_end": format_timestamp(plan.bucket_end),
        "slice_start": format_timestamp(plan.start),
        "slice_end": format_timestamp(plan.end),
        "capacity_version": plan.capacity_version,
        "rule_hash": plan.rule_hash,
        "capacity_kg": None if plan.capacity_kg is None else round(plan.capacity_kg, 6),
        "as_of": format_timestamp(as_of),
    }
    return _hash_json(material)


def build_conclusion(
    plan: SlicePlan,
    facility_key: str,
    waste_key: str,
    resolutions: Sequence[BatchResolution],
    rejection_evidence: Sequence[Evidence],
    cancel: CancelToken | None = None,
) -> dict[str, Any]:
    """Assess exactly one time slice with the evidence that lands in it."""
    records_used: list[dict[str, Any]] = []
    load = 0.0
    evidence: list[Evidence] = []
    for resolution in resolutions:
        if cancel is not None:
            cancel.raise_if_cancelled()
        if resolution.canonical_time is not None and plan.start <= resolution.canonical_time < plan.end:
            for item in resolution.evidence:
                if item.moment is not None and plan.start <= item.moment < plan.end:
                    evidence.append(item)
        if not (resolution.counted and resolution.canonical is not None and resolution.canonical_time is not None):
            continue
        if not (plan.start <= resolution.canonical_time < plan.end):
            continue
        record = resolution.canonical
        if record.destination_facility_id.strip().upper() != facility_key:
            continue
        load += float(resolution.weight_kg or 0.0)
        records_used.append(
            {
                "record_key": record.record_key,
                "batch_id": str(record.payload.get("batch_id") or ""),
                "collected_at": record.collected_at,
                "observed_at": record.observed_at,
                "weight_kg": round(float(resolution.weight_kg or 0.0), 6),
                "status": resolution.status,
                "source": record.source,
                "facility_id": record.facility_id,
                "destination_facility_id": record.destination_facility_id,
                "raw_hash": record.raw_hash,
            }
        )

    for item in rejection_evidence:
        if item.moment is not None and plan.start <= item.moment < plan.end:
            evidence.append(item)

    records_used.sort(key=lambda item: item["record_key"])
    evidence_sorted = sorted(
        evidence,
        key=lambda item: (
            item.record_key or "",
            item.reason,
            "" if item.moment is None else format_timestamp(item.moment),
        ),
    )

    exclusions = [
        item.to_dict(plan.capacity_version, plan.rule_hash)
        for item in evidence_sorted
        if item.reason != "duplicate_report"
    ]
    duplicates = [
        item.record_key for item in evidence_sorted if item.reason == "duplicate_report" and item.record_key
    ]

    blocking_reasons = sorted({item.reason for item in evidence_sorted if item.blocks})

    if plan.capacity_kg is None:
        verdict = "undecidable"
        blocking_reasons = sorted(set(blocking_reasons) | {"missing_capacity_rule"})
    elif load > float(plan.capacity_kg) + _EPSILON:
        verdict = "overloaded"
    elif blocking_reasons:
        verdict = "undecidable"
    else:
        verdict = "within_capacity"

    result: dict[str, Any] = {
        "facility_id": None if plan.rule is None else plan.rule.facility_id,
        "waste_type": None if plan.rule is None else plan.rule.waste_type,
        "facility_key": facility_key,
        "waste_key": waste_key,
        "period": plan.period,
    }
    result.update(plan.to_dict())
    result.update(
        {
            "load_kg": round(load, 6),
            "verdict": verdict,
            "blocking_reasons": blocking_reasons,
            "record_keys": [item["record_key"] for item in records_used],
            "records": records_used,
            "duplicate_record_keys": duplicates,
            "exclusions": exclusions,
        }
    )
    return result


def merge_intervals(conclusions: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Merge adjacent same-verdict slices into load/overload/undecidable intervals."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for conclusion in conclusions:
        grouped.setdefault(str(conclusion["period"]), []).append(conclusion)

    merged: dict[str, list[dict[str, Any]]] = {}
    for period, items in grouped.items():
        items = sorted(items, key=lambda item: item["slice_start"])
        current: dict[str, Any] | None = None
        for item in items:
            complete = item["capacity_kg"] is not None

            if current is not None:
                if (
                    current["verdict"] == item["verdict"]
                    and current["slice_end"] == item["slice_start"]
                    and current["capacity_complete"] == complete
                ):
                    item_end = item["slice_end"]
                    current["slice_end"] = item_end
                    current["load_kg"] = round(current["load_kg"] + item["load_kg"], 6)
                    if complete:
                        current["capacity_kg"] = round(current["capacity_kg"] + item["capacity_kg"], 6)
                    current["record_keys"] = sorted(set(current["record_keys"]) | set(item["record_keys"]))
                    for version in item["capacity_versions"]:
                        if version not in current["capacity_versions"]:
                            current["capacity_versions"].append(version)
                    for rule_hash in item["capacity_rule_hashes"]:
                        if rule_hash not in current["capacity_rule_hashes"]:
                            current["capacity_rule_hashes"].append(rule_hash)
                    known = {entry.get("record_key") or "" for entry in current["exclusions"]}
                    for entry in item["exclusions"]:
                        marker = (entry.get("record_key") or "", entry["reason"])
                        if marker not in current["_exclusion_markers"]:
                            current["exclusions"].append(entry)
                            current["_exclusion_markers"].append(marker)
                    continue

            current = {
                "period": period,
                "slice_start": item["slice_start"],
                "slice_end": item["slice_end"],
                "verdict": item["verdict"],
                "load_kg": item["load_kg"],
                "capacity_kg": item["capacity_kg"],
                "capacity_complete": complete,
                "capacity_versions": list(item["capacity_versions"]),
                "capacity_rule_hashes": list(item["capacity_rule_hashes"]),
                "record_keys": list(item["record_keys"]),
                "exclusions": list(item["exclusions"]),
                "_exclusion_markers": [
                    (entry.get("record_key") or "", entry["reason"]) for entry in item["exclusions"]
                ],
            }
            merged.setdefault(period, []).append(current)

        for interval in merged.get(period, []):
            interval.pop("_exclusion_markers", None)

    return merged
