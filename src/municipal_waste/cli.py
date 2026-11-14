from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .application import run_fixture, run_url


def _capacity_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "capacity", help="Generate or read facility processing-capacity overload reports"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--generate", action="store_true")
    mode.add_argument("--read", metavar="REPORT_ID")
    mode.add_argument("--list", action="store_true")
    parser.add_argument("--database", required=True)
    parser.add_argument("--facility-id")
    parser.add_argument("--waste-type")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--as-of")
    parser.add_argument("--rules-file")
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--max-slices", type=int)
    parser.add_argument("--no-cache", action="store_true")


def _run_capacity(args: argparse.Namespace) -> None:
    from .capacity_service import CapacityService
    from .capacity_storage import CapacityStore

    store = CapacityStore(args.database)
    try:
        service = CapacityService(store)
        if args.read:
            print(json.dumps(service.read_report(args.read), ensure_ascii=False, sort_keys=True))
            return
        if args.list:
            facility_key = args.facility_id.strip().upper() if args.facility_id else None
            waste_key = args.waste_type.strip().lower() if args.waste_type else None
            listings = service.list_reports(facility_key, waste_key)
            print(json.dumps({"reports": listings}, ensure_ascii=False, sort_keys=True))
            return

        missing = [
            name
            for name, value in (
                ("--facility-id", args.facility_id),
                ("--waste-type", args.waste_type),
                ("--window-start", args.window_start),
                ("--window-end", args.window_end),
            )
            if not value
        ]
        if missing:
            raise SystemExit(f"capacity --generate requires {', '.join(missing)}")
        rules_payload = None
        if args.rules_file:
            rules_payload = json.loads(Path(args.rules_file).read_text(encoding="utf-8"))
        report = service.generate_report(
            args.facility_id,
            args.waste_type,
            args.window_start,
            args.window_end,
            rules_payload=rules_payload,
            as_of=args.as_of,
            max_records=args.max_records,
            max_slices=args.max_slices,
            use_cache=not args.no_cache,
        )
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run 城市生活垃圾流向与资源化处理数据平台 ingestion")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture")
    source.add_argument("--url")
    parser.add_argument("--database")
    parser.add_argument("--raw-dir")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--records-key")

    if any(arg == "capacity" for arg in sys.argv[1:2]):
        capacity_root = argparse.ArgumentParser(
            description="Facility processing-capacity overload assessment"
        )
        subparsers = capacity_root.add_subparsers(dest="command", required=True)
        _capacity_parser(subparsers)
        capacity_args = capacity_root.parse_args(sys.argv[1:])
        _run_capacity(capacity_args)
        return

    args = parser.parse_args()
    if not args.database:
        parser.error("--database is required")
    if args.fixture:
        result = run_fixture(args.fixture, args.database, args.raw_dir)
    else:
        result = run_url(args.url, args.database, args.raw_dir, args.timeout, args.records_key)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
