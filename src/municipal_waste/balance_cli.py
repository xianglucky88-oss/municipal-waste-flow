"""批次重量平衡核算的命令行入口（独立于既有 ingestion CLI）。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .balance import RULE_VERSION, BalanceCancelled, BalanceError, BalanceRequest
from .balance_service import BalanceEngine
from .balance_store import export_run, get_run, list_runs


def _print(document: object) -> None:
    print(json.dumps(document, ensure_ascii=False, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="municipal-waste-balance",
        description="批次重量平衡核算：窗口 + 设施集合 + 损耗容忍度 + 规则版本 -> 平衡差与证据",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run", help="执行一次平衡核算")
    run_parser.add_argument("--database", required=True)
    run_parser.add_argument("--window-start", required=True, help="ISO-8601，按 collected_at 闭区间过滤")
    run_parser.add_argument("--window-end", required=True, help="ISO-8601，闭区间右端点")
    run_parser.add_argument("--facility", action="append", default=[], help="设施过滤，可重复；缺省为全部设施")
    run_parser.add_argument("--tolerance-ratio", type=float, default=0.0, help="相对输入重量的损耗容忍比例")
    run_parser.add_argument("--tolerance-kg", type=float, default=0.0, help="绝对损耗容忍（kg）")
    run_parser.add_argument("--rule-version", default=RULE_VERSION)
    run_parser.add_argument("--chunk-size", type=int, default=500, help="分段读取的记录数")
    run_parser.add_argument("--max-records", type=int, default=1_000_000, help="单次运行的记录数资源上限")

    list_parser = commands.add_parser("list-runs", help="只读列出核算运行")
    list_parser.add_argument("--database", required=True)
    list_parser.add_argument("--limit", type=int, default=50)

    show_parser = commands.add_parser("show-run", help="只读查看一次运行及批次结果")
    show_parser.add_argument("--database", required=True)
    show_parser.add_argument("--run-id", type=int, required=True)

    export_parser = commands.add_parser("export", help="只读导出一次运行的完整 JSON 文档")
    export_parser.add_argument("--database", required=True)
    export_parser.add_argument("--run-id", type=int, required=True)
    export_parser.add_argument("--output", help="输出文件；缺省打印到标准输出")

    recover_parser = commands.add_parser("recover", help="把心跳过期的 running 运行标记为 interrupted")
    recover_parser.add_argument("--database", required=True)
    recover_parser.add_argument("--stale-seconds", type=float, default=300.0)

    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            request = BalanceRequest(
                window_start=args.window_start,
                window_end=args.window_end,
                facilities=tuple(args.facility),
                tolerance_ratio=args.tolerance_ratio,
                tolerance_abs_kg=args.tolerance_kg,
                rule_version=args.rule_version,
                chunk_size=args.chunk_size,
                max_records=args.max_records,
            )
            _print(BalanceEngine(args.database).run(request))
            return 0
        if args.command == "list-runs":
            _print({"runs": list_runs(args.database, args.limit)})
            return 0
        if args.command == "show-run":
            _print(get_run(args.database, args.run_id))
            return 0
        if args.command == "export":
            document = export_run(args.database, args.run_id)
            text = json.dumps(document, ensure_ascii=False, sort_keys=True, indent=2)
            if args.output:
                Path(args.output).write_text(text + "\n", encoding="utf-8")
            else:
                print(text)
            return 0
        if args.command == "recover":
            recovered = BalanceEngine(args.database).recover_interrupted(args.stale_seconds)
            _print({"recovered": recovered})
            return 0
        return 1
    except BalanceCancelled as exc:
        print(json.dumps({"status": "cancelled", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 3
    except (BalanceError, ValueError, LookupError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
