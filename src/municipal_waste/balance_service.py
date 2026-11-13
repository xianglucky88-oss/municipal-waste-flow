"""批次重量平衡核算的编排层：分段读取、可取消、增量重算、单事务落库。"""

from __future__ import annotations

import json
import threading
from collections import defaultdict
from decimal import Decimal
from typing import Any

from .balance import (
    BalanceCancelled,
    BalanceError,
    BalanceRequest,
    BalanceResourceLimit,
    Observation,
    parse_instant,
    summarize_batch,
)
from .balance_store import BalanceStore, fingerprint_pairs, fingerprint_params
from .identity import facility_key


class CancellationToken:
    """协作式取消令牌：核算在每个读取分段之间检查，可安全取消。"""

    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def throw_if_cancelled(self) -> None:
        if self.cancelled:
            raise BalanceCancelled("balance run cancelled by caller")


def _to_observation(record_key: Any, source: Any, observed_at: Any, payload_json: Any, raw_hash: Any) -> Observation | None:
    """把 records 表的一行还原为观测；载荷损坏或缺少批次标识时返回 None。"""
    try:
        payload = json.loads(payload_json)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    batch_id = str(payload.get("batch_id") or "").strip()
    if not batch_id:
        return None
    return Observation(
        record_key=str(record_key),
        source=str(source),
        observed_at=str(observed_at),
        facility_id=str(payload.get("facility_id") or ""),
        batch_id=batch_id,
        collected_at=str(payload.get("collected_at") or ""),
        status=str(payload.get("status") or ""),
        net_weight_kg=payload.get("net_weight_kg"),
        destination_facility_id=str(payload.get("destination_facility_id") or ""),
        raw_hash=str(raw_hash),
    )


class BalanceEngine:
    """对 records 表执行批次重量平衡核算。

    每次 ``run`` 使用独立连接、独立 ``run_id``（结果版本），可并发、可重复；
    读取按 ``chunk_size`` 分段进行，分段之间响应取消并维持心跳。
    """

    def __init__(self, database: str, *, busy_timeout_ms: int = 5000) -> None:
        self.database = database
        self.busy_timeout_ms = busy_timeout_ms

    def recover_interrupted(self, stale_after_seconds: float = 300.0) -> int:
        store = BalanceStore(self.database, self.busy_timeout_ms)
        try:
            store.migrate()
            return store.recover_interrupted(stale_after_seconds)
        finally:
            store.close()

    def run(self, request: BalanceRequest, token: CancellationToken | None = None) -> dict[str, Any]:
        request = request.validate()
        token = token or CancellationToken()
        store = BalanceStore(self.database, self.busy_timeout_ms)
        try:
            store.migrate()
            run_id = store.start_run(
                rule_version=request.rule_version,
                params_json=json.dumps(request.params_dict(), ensure_ascii=False, sort_keys=True),
                params_fingerprint=fingerprint_params(request.result_params_dict()),
            )
            try:
                batches, scanned, skipped, included_pairs = self._read_observations(store, request, run_id, token)
                params_fingerprint = fingerprint_params(request.result_params_dict())
                results, computed, reused = self._aggregate(store, request, batches)
                store.complete_run(
                    run_id,
                    results=results,
                    cache_entries=[
                        (request.rule_version, params_fingerprint, batch_id, input_hash, result_json)
                        for batch_id, input_hash, result_json in results
                        if (batch_id, input_hash) in computed
                    ],
                    records_scanned=scanned,
                    batches_computed=len(computed),
                    batches_reused=reused,
                    input_fingerprint=fingerprint_pairs(included_pairs),
                )
            except BalanceCancelled as exc:
                store.mark_run_safe(run_id, "cancelled", str(exc))
                raise
            except KeyboardInterrupt:
                store.mark_run_safe(run_id, "cancelled", "interrupted by user")
                raise
            except BalanceError as exc:
                store.mark_run_safe(run_id, "failed", str(exc))
                raise
            except Exception as exc:
                store.mark_run_safe(run_id, "failed", str(exc))
                raise
            return self._summary(store, run_id, request, results, scanned, skipped)
        finally:
            store.close()

    def _read_observations(
        self,
        store: BalanceStore,
        request: BalanceRequest,
        run_id: int,
        token: CancellationToken,
    ) -> tuple[dict[str, list[Observation]], int, int, list[tuple[str, str]]]:
        batches: dict[str, list[Observation]] = defaultdict(list)
        included_pairs: list[tuple[str, str]] = []
        scanned = 0
        skipped = 0
        after_rowid = 0
        start = request.window_start_dt
        end = request.window_end_dt
        facilities = request.facility_keys
        has_table = store.has_records_table()
        while has_table:
            token.throw_if_cancelled()
            rows = store.read_records_chunk(after_rowid, request.chunk_size)
            if not rows:
                break
            after_rowid = int(rows[-1][0])
            for _rowid, record_key, source, observed_at, payload_json, raw_hash in rows:
                scanned += 1
                if scanned > request.max_records:
                    raise BalanceResourceLimit(
                        f"records scanned exceed max_records={request.max_records}; aborting run"
                    )
                obs = _to_observation(record_key, source, observed_at, payload_json, raw_hash)
                if obs is None:
                    skipped += 1
                    continue
                try:
                    collected = parse_instant(obs.collected_at)
                except ValueError:
                    skipped += 1
                    continue
                if collected < start or collected > end:
                    continue
                if facilities and facility_key(obs.facility_id) not in facilities:
                    continue
                batches[obs.batch_id.strip().upper()].append(obs)
                included_pairs.append((obs.record_key, obs.raw_hash))
            store.heartbeat(run_id, scanned)
        return batches, scanned, skipped, included_pairs

    def _aggregate(
        self,
        store: BalanceStore,
        request: BalanceRequest,
        batches: dict[str, list[Observation]],
    ) -> tuple[list[tuple[str, str, str]], set[tuple[str, str]], int]:
        """逐批次核算；输入未变化的批次直接复用缓存结果（增量重算）。"""
        params_fingerprint = fingerprint_params(request.result_params_dict())
        tolerance_ratio = Decimal(str(request.tolerance_ratio))
        tolerance_abs = Decimal(str(request.tolerance_abs_kg))
        results: list[tuple[str, str, str]] = []
        computed: set[tuple[str, str]] = set()
        reused = 0
        for batch_id in sorted(batches):
            observations = batches[batch_id]
            input_hash = fingerprint_pairs((obs.record_key, obs.raw_hash) for obs in observations)
            cached = store.cached_result(request.rule_version, params_fingerprint, batch_id, input_hash)
            if cached is not None:
                results.append((batch_id, input_hash, cached))
                reused += 1
                continue
            result = summarize_batch(
                batch_id,
                observations,
                tolerance_ratio=tolerance_ratio,
                tolerance_abs_kg=tolerance_abs,
            )
            result_json = json.dumps(result, ensure_ascii=False, sort_keys=True)
            results.append((batch_id, input_hash, result_json))
            computed.add((batch_id, input_hash))
        return results, computed, reused

    @staticmethod
    def _summary(
        store: BalanceStore,
        run_id: int,
        request: BalanceRequest,
        results: list[tuple[str, str, str]],
        scanned: int,
        skipped: int,
    ) -> dict[str, Any]:
        verdicts = {"balanced": 0, "gap": 0, "surplus": 0, "insufficient": 0}
        for _batch_id, _input_hash, result_json in results:
            verdicts[json.loads(result_json)["verdict"]] += 1
        row = store.conn.execute(
            "SELECT status, batches_computed, batches_reused, input_fingerprint FROM balance_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return {
            "run_id": run_id,
            "status": row[0],
            "rule_version": request.rule_version,
            "records_scanned": scanned,
            "records_skipped": skipped,
            "batches_evaluated": len(results),
            "batches_computed": int(row[1]),
            "batches_reused": int(row[2]),
            "verdicts": verdicts,
            "input_fingerprint": row[3],
        }
