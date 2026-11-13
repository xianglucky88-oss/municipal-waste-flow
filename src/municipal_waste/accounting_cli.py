"""废物流转环境因子核算 CLI（独立于既有 ingestion CLI，互不影响）。

子命令：
- ``init-schema``：初始化/迁移核算 schema（幂等）。
- ``compute``：按规则版本对 records 快照做增量核算。
- ``runs``：列出核算运行（审计）。
- ``export``：只读导出账本行或排除行（JSON）。
- ``summary``：按维度汇总（冲销后净额，Decimal 精确）。
"""
from __future__ import annotations

import argparse
import json
from typing import Any, Sequence

from .accounting import (
    DEFAULT_INCLUDED_STATUSES,
    SELF_LOOP_REQUIRE_FACTOR,
    SELF_LOOP_ZERO,
    AccountingEngine,
)
from .accounting_query import AccountingQuery
from .factors import FactorSet


def _add_read_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--rules-version")
    parser.add_argument("--facility", dest="facility_id")
    parser.add_argument("--waste-type", dest="waste_type")
    parser.add_argument("--stage")
    parser.add_argument("--from", dest="date_from", help="核算日期下界（含），YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", help="核算日期上界（不含），YYYY-MM-DD")


def _read_filters(args: argparse.Namespace) -> dict[str, Any]:
    filters = {
        "rules_version": args.rules_version,
        "facility_id": args.facility_id,
        "waste_type": args.waste_type,
        "stage": args.stage,
        "date_from": args.date_from,
        "date_to": args.date_to,
    }
    return {key: value for key, value in filters.items() if value is not None}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="municipal-waste-accounting", description="废物流转环境因子核算：逐事实因子应用与可追溯汇总")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-schema", help="初始化或迁移核算 schema")
    p_init.add_argument("--database", required=True)

    p_compute = sub.add_parser("compute", help="按规则版本增量计算账本")
    p_compute.add_argument("--database", required=True)
    p_compute.add_argument("--factors", required=True, help="版本化因子表 JSON 文件")
    p_compute.add_argument("--rules-version", required=True)
    p_compute.add_argument("--facility", action="append", dest="facilities", help="限定设施分区，可重复")
    p_compute.add_argument("--from", dest="date_from", help="collected_at 下界（含），带时区 ISO 8601")
    p_compute.add_argument("--to", dest="date_to", help="collected_at 上界（不含），带时区 ISO 8601")
    p_compute.add_argument("--include-status", action="append", dest="included_statuses", help="纳入状态，可重复；默认排除 rejected")
    p_compute.add_argument("--self-loop", choices=("zero", "require"), default="zero", help="设施自环运输段：显式记零或要求显式边因子")
    p_compute.add_argument("--chunk-size", type=int, default=500)

    p_runs = sub.add_parser("runs", help="列出核算运行")
    p_runs.add_argument("--database", required=True)

    p_export = sub.add_parser("export", help="只读导出账本或排除清单")
    p_export.add_argument("--database", required=True)
    p_export.add_argument("--what", choices=("ledger", "exclusions"), default="ledger")
    p_export.add_argument("--output", help="输出文件；缺省写标准输出")
    _add_read_filters(p_export)

    p_summary = sub.add_parser("summary", help="按维度汇总（冲销后净额）")
    p_summary.add_argument("--database", required=True)
    p_summary.add_argument("--group-by", default="rules_version,facility_id,waste_type,stage")
    _add_read_filters(p_summary)

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    if args.command == "init-schema":
        engine = AccountingEngine(args.database)
        try:
            engine.init_schema()
        finally:
            engine.close()
        result: Any = {"status": "ok", "schema": "initialized"}
    elif args.command == "compute":
        with open(args.factors, "r", encoding="utf-8") as handle:
            factors = FactorSet.from_mapping(json.load(handle))
        engine = AccountingEngine(
            args.database,
            chunk_size=args.chunk_size,
            included_statuses=tuple(args.included_statuses) if args.included_statuses else DEFAULT_INCLUDED_STATUSES,
            self_loop_policy=SELF_LOOP_ZERO if args.self_loop == "zero" else SELF_LOOP_REQUIRE_FACTOR,
        )
        try:
            result = engine.compute(
                factors,
                args.rules_version,
                facilities=args.facilities,
                date_from=args.date_from,
                date_to=args.date_to,
            ).to_dict()
        finally:
            engine.close()
    elif args.command == "runs":
        with AccountingQuery(args.database) as query:
            result = query.runs()
    elif args.command == "export":
        with AccountingQuery(args.database) as query:
            filters = _read_filters(args)
            if args.output:
                count = query.export_json(args.what, args.output, **filters)
                result = {"status": "ok", "exported": count, "output": args.output}
            elif args.what == "ledger":
                result = list(query.ledger(**filters))
            else:
                result = list(query.exclusions(**filters))
    else:  # summary
        with AccountingQuery(args.database) as query:
            group_by = tuple(part.strip() for part in args.group_by.split(",") if part.strip())
            result = query.summary(group_by=group_by, **_read_filters(args))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
