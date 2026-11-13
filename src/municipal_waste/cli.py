from __future__ import annotations

import argparse
import json
import sys

from .application import run_fixture, run_url
from .flow_store import FlowRebuildLocked
from .rebuild import export_batch_flows, get_batch_flow, list_batch_flows, rebuild_batch_flows

_SUBCOMMANDS = ("ingest", "rebuild", "query", "export")


def _build_ingest_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run 城市生活垃圾流向与资源化处理数据平台 ingestion")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture")
    source.add_argument("--url")
    parser.add_argument("--database", required=True)
    parser.add_argument("--raw-dir")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--records-key")
    return parser


def _run_ingest(args: argparse.Namespace) -> int:
    if args.fixture:
        result = run_fixture(args.fixture, args.database, args.raw_dir)
    else:
        result = run_url(args.url, args.database, args.raw_dir, args.timeout, args.records_key)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="municipal-waste-flow",
        description="城市生活垃圾流向与资源化处理数据平台（ingestion 与批次流转链重建）",
    )
    subparsers = parser.add_subparsers(dest="command")

    ingest = subparsers.add_parser("ingest", help="运行 ingestion（等价于旧版直接传参）")
    source = ingest.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture")
    source.add_argument("--url")
    ingest.add_argument("--database", required=True)
    ingest.add_argument("--raw-dir")
    ingest.add_argument("--timeout", type=float, default=30.0)
    ingest.add_argument("--records-key")
    ingest.set_defaults(handler=_run_ingest)

    rebuild = subparsers.add_parser("rebuild", help="按批次重建流转链（全量或单批次增量）")
    rebuild.add_argument("--database", required=True)
    rebuild.add_argument("--batch-id", help="只重建指定批次；缺省为全量重建")
    rebuild.set_defaults(handler=_run_rebuild)

    query = subparsers.add_parser("query", help="查询已发布的批次流转结论")
    query.add_argument("--database", required=True)
    query.add_argument("--batch-id", help="只查询指定批次；缺省列出全部批次")
    query.set_defaults(handler=_run_query)

    export = subparsers.add_parser("export", help="导出批次流转结论（jsonl/json）")
    export.add_argument("--database", required=True)
    export.add_argument("--output", default="-", help="输出文件路径，'-' 表示标准输出")
    export.add_argument("--format", choices=("jsonl", "json"), default="jsonl")
    export.add_argument("--batch-id")
    export.set_defaults(handler=_run_export)
    return parser


def _run_rebuild(args: argparse.Namespace) -> int:
    try:
        result = rebuild_batch_flows(args.database, batch_id=args.batch_id)
    except FlowRebuildLocked as exc:
        print(json.dumps({"error": "locked", "detail": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0


def _run_query(args: argparse.Namespace) -> int:
    if args.batch_id:
        chain = get_batch_flow(args.database, args.batch_id)
        if chain is None:
            print(json.dumps({"error": "not_found", "batch_id": args.batch_id}, ensure_ascii=False), file=sys.stderr)
            return 1
        print(json.dumps(chain, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        chains = list_batch_flows(args.database)
        print(json.dumps(chains, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


def _run_export(args: argparse.Namespace) -> int:
    output = sys.stdout if args.output == "-" else args.output
    count = export_batch_flows(
        args.database,
        output,
        fmt=args.format,
        batch_id=args.batch_id,
    )
    if args.output != "-":
        print(json.dumps({"exported": count, "path": args.output}, ensure_ascii=False, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if raw_args and raw_args[0] in _SUBCOMMANDS:
        parser = _build_parser()
        args = parser.parse_args(raw_args)
        return int(args.handler(args))
    # 向后兼容：旧版 ingestion 调用方式（--fixture/--url 直接作为顶层参数）。
    legacy = _build_ingest_parser()
    args = legacy.parse_args(raw_args)
    return _run_ingest(args)


if __name__ == "__main__":
    raise SystemExit(main())
