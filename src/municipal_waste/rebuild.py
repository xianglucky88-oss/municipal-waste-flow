"""批次流转链重建编排与导出接口。

入口：

- :func:`rebuild_batch_flows`：全量或单批次增量重建，带互斥、run 台账、
  逐批次事务、幂等发布与崩溃重试。
- :func:`get_batch_flow` / :func:`list_batch_flows` / :func:`export_batch_flows`：
  查询与导出。

该模块是 ingestion 之后的独立读侧能力：不改变 ingestion 入口、不改变
``identity.batch_key`` 语义、不写入既有 ``records`` 表。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from .flow_store import BatchFlowStore, FlowRebuildLocked, connect
from .reconstruction import BatchChain, build_batch_chain, input_fingerprint


@dataclass(frozen=True)
class RebuildResult:
    run_id: str
    mode: str
    target_batch_id: str | None
    fact_count: int
    batch_count: int
    published_count: int
    skipped_count: int
    status: str
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "mode": self.mode,
            "target_batch_id": self.target_batch_id,
            "fact_count": self.fact_count,
            "batch_count": self.batch_count,
            "published_count": self.published_count,
            "skipped_count": self.skipped_count,
            "status": self.status,
            "error": self.error,
        }


def rebuild_batch_flows(
    database: str,
    *,
    batch_id: str | None = None,
    store: BatchFlowStore | None = None,
) -> RebuildResult:
    """重建批次流转链。

    ``batch_id`` 为 ``None`` 时全量重建（有限内存流式扫描）；给定具体批次时
    只刷新并重建该批次（单批次增量更新），其他批次结论不受影响。

    裁决与发布策略：

    - 裁决在 :mod:`municipal_waste.reconstruction` 中按事件时间确定性完成，
      到达顺序不影响结论，合法迟到事实不会被当成回退；
    - 发布以 ``content_hash`` 去重：同输入重放为 no-op，修订后只更新受影响批次；
    - 每个批次独立事务提交，进程中断后已发布批次保持有效，重新运行即可续完；
    - 重建全程持咨询锁，并发重建的第二个调用抛出
      :class:`~municipal_waste.flow_store.FlowRebuildLocked`。
    """
    owns_store = store is None
    flow_store = store or connect(database)
    mode = "incremental" if batch_id is not None else "full"
    run_id = flow_store.start_run(mode, batch_id)

    def mark(status: str, **fields: Any) -> None:
        # 台账写入失败不能掩盖真正的重建异常。
        try:
            flow_store.finish_run(run_id, status, **fields)
        except Exception:  # noqa: BLE001
            pass

    try:
        with flow_store.rebuild_lock(run_id):
            # 1. 从不可变 records 快照刷新独立事实索引（不触碰 records 表）。
            fact_count = flow_store.refresh_fact_index()
            flow_store.heartbeat(run_id, fact_count=fact_count)

            published = 0
            skipped = 0
            rebuilt = 0

            # 2. 按批次流式重建，单批次内存占用有界。
            for current_batch_id in flow_store.stream_batch_ids(batch_id):
                facts = flow_store.load_facts(current_batch_id)
                fingerprint = input_fingerprint(facts)
                chain: BatchChain = build_batch_chain(current_batch_id, facts)
                flow_store.save_batch_fingerprint(current_batch_id, fingerprint)

                # 3. 幂等发布：内容哈希相同则跳过（重复重放零写入）。
                changed = flow_store.publish_chain(chain, fingerprint, run_id)
                if changed:
                    published += 1
                else:
                    skipped += 1
                rebuilt += 1
                flow_store.heartbeat(
                    run_id,
                    fact_count=fact_count,
                    batch_count=rebuilt,
                    published_count=published,
                    skipped_count=skipped,
                )

            mark(
                "completed",
                fact_count=fact_count,
                batch_count=rebuilt,
                published_count=published,
                skipped_count=skipped,
            )
            return RebuildResult(
                run_id=run_id,
                mode=mode,
                target_batch_id=batch_id,
                fact_count=fact_count,
                batch_count=rebuilt,
                published_count=published,
                skipped_count=skipped,
                status="completed",
            )
    except FlowRebuildLocked:
        # 锁冲突属于调用方可处理的业务冲突：标记失败并原样抛出，由调用方重试。
        mark("locked", error="rebuild lock held by another process")
        raise
    except Exception as exc:  # noqa: BLE001 - 台账需要记录任何中断原因，保证可重试
        mark("failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        if owns_store:
            flow_store.close()


# --------------------------------------------------------------------------- #
# 查询 / 导出
# --------------------------------------------------------------------------- #
def get_batch_flow(database: str, batch_id: str, *, store: BatchFlowStore | None = None) -> dict[str, Any] | None:
    owns_store = store is None
    flow_store = store or connect(database)
    try:
        return flow_store.get_chain(batch_id)
    finally:
        if owns_store:
            flow_store.close()


def list_batch_flows(database: str, *, store: BatchFlowStore | None = None) -> list[dict[str, Any]]:
    owns_store = store is None
    flow_store = store or connect(database)
    try:
        return flow_store.list_chains()
    finally:
        if owns_store:
            flow_store.close()


def export_batch_flows(
    database: str,
    output: str | Path | TextIO,
    *,
    fmt: str = "jsonl",
    batch_id: str | None = None,
    store: BatchFlowStore | None = None,
) -> int:
    """导出已发布的批次流转结论，返回写出的批次数量。

    ``jsonl`` 逐行写出，内存有界；``json`` 输出单个数组。空库导出空结果。
    """
    if fmt not in {"jsonl", "json"}:
        raise ValueError("fmt must be 'jsonl' or 'json'")
    owns_store = store is None
    flow_store = store or connect(database)
    try:
        chains: list[dict[str, Any]]
        if batch_id is not None:
            single = flow_store.get_chain(batch_id)
            chains = [single] if single is not None else []
        else:
            chains = flow_store.list_chains()

        handle: TextIO
        close_after = False
        if isinstance(output, (str, Path)):
            target = Path(output)
            target.parent.mkdir(parents=True, exist_ok=True)
            handle = target.open("w", encoding="utf-8")
            close_after = True
        else:
            handle = output
        try:
            if fmt == "jsonl":
                for chain in chains:
                    handle.write(json.dumps(chain, ensure_ascii=False, sort_keys=True))
                    handle.write("\n")
            else:
                json.dump(chains, handle, ensure_ascii=False, sort_keys=True, indent=2)
        finally:
            if close_after:
                handle.close()
        return len(chains)
    finally:
        if owns_store:
            flow_store.close()
