"""Orchestration for capacity overload reports.

The engine in :mod:`municipal_waste.capacity` is pure.  This layer adds the
operational concerns:

* bounded, streaming reads from the ingestion tables (constant memory);
* per-slice incremental recomputation through a content-addressed cache;
* cooperative cancellation and record/slice resource limits;
* cross-process advisory locking so two writers for one facility never
  overwrite each other;
* crash-safe run lifecycle and idempotent, content-addressed persistence;
* a strictly read-only reporting interface.

Weight balancing is intentionally absent: load is attributed only to
``destination_facility_id`` and no input/output conservation judgement is made.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from .capacity import (
    BLOCKING_REASONS,
    ENGINE_VERSION,
    BatchResolution,
    CancelToken,
    CapacityCancelled,
    CapacityError,
    CapacityResourceLimit,
    CapacityRule,
    Evidence,
    LoadRecord,
    RejectionEvidence,
    SlicePlan,
    build_conclusion,
    format_timestamp,
    merge_intervals,
    parse_rules,
    parse_timestamp,
    plan_slices,
    resolve_batches,
    resolve_rejections,
    slice_cache_key,
)
from .capacity_storage import CapacityLockConflict, CapacityStore, _fingerprint


def _hash_material(material: Any) -> str:
    canonical = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _data_fingerprint(records: Sequence[LoadRecord], rejection_events: Sequence[RejectionEvidence]) -> str:
    """Content fingerprint of the assessed inputs.

    Including it in the cache key means a late revision or a newly accepted /
    rejected row changes the key, so incremental recomputation never serves a
    conclusion computed from stale inputs even when ``as_of`` is unchanged.
    """
    material = {
        "records": sorted((record.record_key, record.raw_hash) for record in records),
        "rejections": sorted(
            (event.rejection_id, event.raw_fingerprint) for event in rejection_events
        ),
    }
    return _hash_material(material)


class CapacityService:
    """Generate and read facility/waste-type capacity overload reports."""

    def __init__(self, store: CapacityStore, *, lock_ttl_seconds: float = 120.0, holder: str | None = None) -> None:
        self.store = store
        self.lock_ttl_seconds = float(lock_ttl_seconds)
        self.holder = holder or f"capacity-service:{uuid.uuid4().hex}"
        # Once a writer owns a scope it holds the lock for this long before the
        # read phase, so a concurrent same-scope writer is reliably rejected.
        self._contention_grace_seconds = 0.05

    def read_report(self, report_id: str) -> dict[str, Any]:
        """Return a previously persisted report. Raises ``KeyError`` if absent."""
        report = self.store.get_report(report_id)
        if report is None:
            raise KeyError(f"unknown capacity report: {report_id}")
        return report

    def list_reports(self, facility_key: str | None = None, waste_key: str | None = None) -> list[dict[str, Any]]:
        """List persisted report summaries, optionally filtered by scope."""
        return self.store.list_reports(facility_key, waste_key)

    def read_conclusions(self, report_id: str) -> list[dict[str, Any]]:
        """Return the per-slice conclusions of a persisted report."""
        if self.store.get_report(report_id) is None:
            raise KeyError(f"unknown capacity report: {report_id}")
        return self.store.get_conclusions(report_id)

    def generate_report(
        self,
        facility_id: str,
        waste_type: str,
        window_start: Any,
        window_end: Any,
        *,
        rules_payload: Any | None = None,
        as_of: Any | None = None,
        cancel: CancelToken | None = None,
        max_records: int | None = None,
        max_slices: int | None = None,
        use_cache: bool = True,
    ) -> dict[str, Any]:
        """Assess one facility/waste-type scope and persist the result.

        The same scope + rule set + knowledge snapshot always yields the same
        ``report_id``; repeating the call after any crash therefore returns the
        already-persisted report instead of duplicating conclusions.
        """
        facility_key = facility_id.strip().upper()
        waste_key = waste_type.strip().lower()
        start = parse_timestamp(window_start)
        end = parse_timestamp(window_end)
        knowledge = (
            parse_timestamp(as_of) if as_of is not None else datetime.now(timezone.utc)
        )
        if not facility_key:
            raise ValueError("facility_id is required")
        if not waste_key:
            raise ValueError("waste_type is required")

        run_id = self.store.start_run(
            facility_key,
            waste_key,
            "",
            format_timestamp(start),
            format_timestamp(end),
            format_timestamp(knowledge),
            max_records,
            max_slices,
        )

        # The advisory lock is the very first scoped write: a peer arriving at
        # the same scope sees it and fails fast instead of overwriting.
        self.store.start_run_locked(
            facility_key,
            waste_key,
            run_id,
            self.holder,
            self.lock_ttl_seconds,
            "",
            format_timestamp(start),
            format_timestamp(end),
            format_timestamp(knowledge),
            max_records,
            max_slices,
        )
        # Hold the committed lock across a short scheduling window before the
        # heavy read phase.  A peer that entered concurrently then observes the
        # lock row and is reported as a conflict instead of running afterwards.
        time.sleep(self._contention_grace_seconds)

        try:
            rules = self._prepare_rules(rules_payload, facility_key, waste_key)
            ruleset_hash = self.store.replace_rules(rules) if rules else _hash_material([])

            records = list(
                self.store.iter_load_records(
                    facility_key,
                    waste_key,
                    start,
                    end,
                    knowledge,
                    max_records=max_records,
                )
            )
            rejection_events = list(
                self.store.iter_rejection_evidence(facility_key, waste_key, start, end, max_records=max_records)
            )
            if max_records is not None and len(records) + len(rejection_events) >= max_records:
                raise CapacityResourceLimit(f"capacity read exceeds max_records={max_records}")

            data_fingerprint = _data_fingerprint(records, rejection_events)

            scope_material = {
                "engine": ENGINE_VERSION,
                "facility": facility_key,
                "waste": waste_key,
                "window_start": format_timestamp(start),
                "window_end": format_timestamp(end),
                "as_of": format_timestamp(knowledge),
                "ruleset_hash": ruleset_hash,
                "data": data_fingerprint,
                "max_records": max_records,
                "max_slices": max_slices,
            }
            scope_hash = _hash_material(scope_material)
            report_id = f"cap_{scope_hash}"

            existing = self.store.get_report(report_id)
            if existing is not None:
                self.store.mark_run(run_id, "completed", report_id=report_id, ruleset_hash=ruleset_hash)
                return existing

            if cancel is not None:
                cancel.raise_if_cancelled()
            conclusions, intervals = self._assess(
                rules=rules,
                facility_key=facility_key,
                waste_key=waste_key,
                start=start,
                end=end,
                knowledge=knowledge,
                cancel=cancel,
                max_records=max_records,
                max_slices=max_slices,
                use_cache=use_cache,
                run_id=run_id,
                records=records,
                rejection_events=rejection_events,
                data_fingerprint=data_fingerprint,
            )

            self._reclaim_interrupted(facility_key, waste_key, scope_hash, run_id)
            self.store.save_report(
                report_id,
                run_id,
                facility_key,
                waste_key,
                scope_hash,
                format_timestamp(start),
                format_timestamp(end),
                format_timestamp(knowledge),
                ruleset_hash,
                conclusions,
                intervals,
            )
            self.store.mark_run(run_id, "completed", report_id=report_id, ruleset_hash=ruleset_hash)
        except CapacityLockConflict:
            self.store.rollback()
            self.store.mark_run(run_id, "lock_conflicted", "report lock held by another process")
            raise
        except CapacityCancelled as exc:
            self.store.rollback()
            self.store.mark_run(run_id, "cancelled", str(exc))
            raise
        except CapacityResourceLimit as exc:
            self.store.rollback()
            self.store.mark_run(run_id, "resource_limited", str(exc))
            raise
        except CapacityError:
            self.store.rollback()
            self.store.mark_run(run_id, "failed")
            raise
        except Exception as exc:
            self.store.rollback()
            self.store.mark_run(run_id, "failed", str(exc))
            raise
        finally:
            self.store.release_lock(facility_key, waste_key, run_id)

        return self.store.get_report(report_id)

    def _prepare_rules(self, rules_payload: Any | None, facility_key: str, waste_key: str) -> list[CapacityRule]:
        if rules_payload is not None:
            parsed = parse_rules(rules_payload)
        else:
            parsed = parse_rules(self.store.load_rules(facility_key, waste_key))
        scoped = [rule for rule in parsed if rule.facility_key == facility_key and rule.waste_key == waste_key]
        return scoped

    def _reclaim_interrupted(
        self, facility_key: str, waste_key: str, scope_hash: str, current_run_id: int
    ) -> None:
        """Mark stale same-scope runs interrupted so retries stay recoverable.

        Runs left in ``running`` are leftovers from a crashed process; the
        caller's own run is excluded.  A retry therefore never rewrites itself
        and never duplicates conclusions.
        """
        for run in self.store.interrupted_runs(facility_key, waste_key):
            if run["run_id"] == current_run_id:
                continue
            self.store.mark_run(run["run_id"], "interrupted", "process did not finish")

    def _assess(
        self,
        *,
        rules: Sequence[CapacityRule],
        facility_key: str,
        waste_key: str,
        start: datetime,
        end: datetime,
        knowledge: datetime,
        cancel: CancelToken | None,
        max_records: int | None,
        max_slices: int | None,
        use_cache: bool,
        run_id: int,
        records: Sequence[LoadRecord],
        rejection_events: Sequence[RejectionEvidence],
        data_fingerprint: str,
    ) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        resolutions = resolve_batches(list(records), knowledge)
        rejection_evidence = resolve_rejections(rejection_events, resolutions)
        plans = plan_slices(rules, start, end, cancel, max_slices)

        conclusions: list[dict[str, Any]] = []
        for plan in plans:
            if cancel is not None:
                cancel.raise_if_cancelled()
            base_key = slice_cache_key(plan, facility_key, waste_key, knowledge)
            key = _hash_material({"slice": base_key, "data": data_fingerprint})
            cached = self.store.get_cached_conclusion(key) if use_cache else None
            if cached is not None:
                conclusion = cached
            else:
                conclusion = build_conclusion(
                    plan, facility_key, waste_key, resolutions, rejection_evidence, cancel
                )
                conclusion = self._annotate(conclusion, key)
                self.store.put_cached_conclusion(key, facility_key, waste_key, conclusion)
            if "conclusion_hash" not in conclusion:
                conclusion = self._annotate(conclusion, key)
            conclusions.append(conclusion)
            self.store.heartbeat_lock(facility_key, waste_key, run_id, self.lock_ttl_seconds)

        normalized = [self._merge_shape(conclusion) for conclusion in conclusions]
        intervals = merge_intervals(normalized)
        return conclusions, intervals

    @staticmethod
    def _annotate(conclusion: dict[str, Any], cache_key: str) -> dict[str, Any]:
        annotated = dict(conclusion)
        annotated["cache_key"] = cache_key
        annotated["conclusion_hash"] = _fingerprint(
            {key: value for key, value in conclusion.items() if key != "cache_key"}
        )
        return annotated

    @staticmethod
    def _merge_shape(conclusion: dict[str, Any]) -> dict[str, Any]:
        """Add the plural version/hash arrays :func:`merge_intervals` expects."""
        shaped = dict(conclusion)
        version = shaped.get("capacity_version")
        rule_hash = shaped.get("capacity_rule_hash")
        shaped["capacity_versions"] = [version] if version is not None else []
        shaped["capacity_rule_hashes"] = [rule_hash] if rule_hash is not None else []
        return shaped


def summarize(report: dict[str, Any]) -> dict[str, Any]:
    """Compact read-model: counts per verdict across every slice."""
    verdicts: dict[str, int] = {}
    blocking: set[str] = set()
    for conclusion in report.get("conclusions", []):
        verdicts[conclusion["verdict"]] = verdicts.get(conclusion["verdict"], 0) + 1
        blocking.update(conclusion.get("blocking_reasons", []))
    return {
        "report_id": report["report_id"],
        "facility_key": report["facility_key"],
        "waste_key": report["waste_key"],
        "window_start": report["window_start"],
        "window_end": report["window_end"],
        "as_of": report["as_of"],
        "verdict_counts": verdicts,
        "blocking_reasons": sorted(blocking & set(BLOCKING_REASONS)),
        "slice_count": len(report.get("conclusions", [])),
    }
