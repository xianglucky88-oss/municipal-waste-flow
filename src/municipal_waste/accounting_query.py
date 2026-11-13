"""核算结果的只读查询与导出。

以只读模式（mode=ro）打开数据库，绝不写库；支持按设施、废物类型、
时间段（accounting_date）和规则版本过滤账本行与排除行，汇总使用
Decimal 精确求和（冲销行结果为负，直接求和即得净额）。导出按行流式
写出，内存占用有界。
"""
from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Sequence, TextIO

from .factors import dec_str

_LEDGER_EQUAL_FILTERS = {
    "rules_version": "rules_version",
    "facility_id": "facility_id",
    "destination_facility_id": "destination_facility_id",
    "waste_type": "waste_type",
    "stage": "stage",
    "row_kind": "row_kind",
    "factor_kind": "factor_kind",
    "status": "status",
}

_EXCLUSION_EQUAL_FILTERS = {
    "rules_version": "rules_version",
    "facility_id": "facility_id",
    "waste_type": "waste_type",
    "stage": "stage",
    "category": "category",
}

_SUMMARY_GROUP_KEYS = {
    "rules_version",
    "facility_id",
    "destination_facility_id",
    "waste_type",
    "stage",
    "row_kind",
    "factor_kind",
    "status",
    "accounting_date",
    "result_unit",
}


class AccountingQuery:
    """只读查询入口；构造时不修改数据库。"""

    def __init__(self, database: str) -> None:
        if database == ":memory:":
            self.conn = sqlite3.connect(database)
        else:
            self.conn = sqlite3.connect(f"file:{Path(database).absolute()}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "AccountingQuery":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # 行级查询（流式）
    # ------------------------------------------------------------------
    def ledger(
        self,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
        **filters: Any,
    ) -> Iterator[dict[str, Any]]:
        """流式返回账本行；date_from/date_to 按 accounting_date 以 [from, to) 过滤。"""
        where, params = _build_where(filters, _LEDGER_EQUAL_FILTERS, "ledger")
        if date_from is not None:
            where.append("accounting_date >= ?")
            params.append(date_from)
        if date_to is not None:
            where.append("accounting_date < ?")
            params.append(date_to)
        sql = "SELECT * FROM emission_ledger"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY facility_id, accounting_date, record_key, stage, ledger_id"
        cursor = self.conn.execute(sql, params)
        for row in cursor:
            yield dict(row)

    def exclusions(
        self,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
        **filters: Any,
    ) -> Iterator[dict[str, Any]]:
        """流式返回排除/无法判定行及其原因。"""
        where, params = _build_where(filters, _EXCLUSION_EQUAL_FILTERS, "exclusions")
        if date_from is not None:
            where.append("accounting_date >= ?")
            params.append(date_from)
        if date_to is not None:
            where.append("accounting_date < ?")
            params.append(date_to)
        sql = "SELECT * FROM accounting_exclusions"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY record_key, rules_version, stage"
        cursor = self.conn.execute(sql, params)
        for row in cursor:
            yield dict(row)

    def record_outcome(self, record_key: str, *, rules_version: str | None = None) -> dict[str, Any]:
        """单条事实的完整核算轨迹：账本行（含冲销）与排除行。"""
        filters: dict[str, Any] = {}
        if rules_version is not None:
            filters["rules_version"] = rules_version
        ledger = [row for row in self._select("SELECT * FROM emission_ledger WHERE record_key = ?", record_key, filters)]
        exclusions = [row for row in self._select("SELECT * FROM accounting_exclusions WHERE record_key = ?", record_key, filters)]
        return {"record_key": record_key, "ledger": ledger, "exclusions": exclusions}

    def _select(self, sql: str, record_key: str, filters: dict[str, Any]) -> Iterator[dict[str, Any]]:
        params: list[Any] = [record_key]
        for key, value in filters.items():
            sql += f" AND {key} = ?"
            params.append(value)
        sql += " ORDER BY 1"
        cursor = self.conn.execute(sql, params)
        for row in cursor:
            yield dict(row)

    # ------------------------------------------------------------------
    # 汇总（Decimal 精确求和，冲销后为净额）
    # ------------------------------------------------------------------
    def summary(
        self,
        *,
        group_by: Sequence[str] = ("rules_version", "facility_id", "waste_type", "stage"),
        date_from: str | None = None,
        date_to: str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        unknown = set(group_by) - _SUMMARY_GROUP_KEYS
        if unknown:
            raise ValueError(f"unknown group_by keys: {sorted(unknown)}")
        totals: dict[tuple[Any, ...], list[Any]] = {}
        for row in self.ledger(date_from=date_from, date_to=date_to, **filters):
            key = tuple(row[column] for column in group_by)
            bucket = totals.setdefault(key, [Decimal(0), 0])
            bucket[0] += Decimal(row["result_value"])
            bucket[1] += 1
        result = []
        for key in sorted(totals, key=lambda item: tuple(str(part) for part in item)):
            total, count = totals[key]
            row = dict(zip(group_by, key))
            row["result_total"] = dec_str(total)
            row["row_count"] = count
            result.append(row)
        return result

    # ------------------------------------------------------------------
    # 运行审计与导出
    # ------------------------------------------------------------------
    def runs(self) -> list[dict[str, Any]]:
        cursor = self.conn.execute("SELECT * FROM accounting_runs ORDER BY started_at, run_id")
        return [dict(row) for row in cursor]

    def rules_versions(self) -> list[dict[str, Any]]:
        cursor = self.conn.execute("SELECT rules_version, rule_hash, registered_at FROM rules_versions ORDER BY registered_at, rules_version")
        return [dict(row) for row in cursor]

    def export_json(self, what: str, target: str | Path | TextIO, **filters: Any) -> int:
        """把 ledger 或 exclusions 流式导出为 JSON 数组；返回导出行数。"""
        if what == "ledger":
            rows = self.ledger(**filters)
        elif what == "exclusions":
            rows = self.exclusions(**filters)
        else:
            raise ValueError("what must be 'ledger' or 'exclusions'")
        if isinstance(target, (str, Path)):
            with open(target, "w", encoding="utf-8") as handle:
                return _write_json_array(rows, handle)
        return _write_json_array(rows, target)


def _build_where(filters: dict[str, Any], allowed: dict[str, str], what: str) -> tuple[list[str], list[Any]]:
    unknown = set(filters) - set(allowed)
    if unknown:
        raise ValueError(f"unknown {what} filters: {sorted(unknown)}")
    where: list[str] = []
    params: list[Any] = []
    for key, value in filters.items():
        if value is None:
            continue
        where.append(f"{allowed[key]} = ?")
        params.append(value)
    return where, params


def _write_json_array(rows: Iterator[dict[str, Any]], handle: TextIO) -> int:
    count = 0
    handle.write("[")
    for row in rows:
        handle.write("," if count else "")
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
        count += 1
    handle.write("]\n")
    return count
