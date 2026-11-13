# 城市生活垃圾流向与资源化处理数据平台

采集垃圾收集、转运、称重、焚烧、填埋和资源化处理数据，构建从产生到最终处置的流向链路。

## 数据工程链路

`source adapters -> transport -> raw landing -> schema normalization -> identity resolution -> quality control -> deduplication -> batch state -> structured storage -> replay`

## 模块职责

- `sources.py`：fixture 和 HTTP(S) JSON 数据源；认证只从运行环境读取。
- `models.py`：不可变领域记录和来源批次模型。
- `normalize.py`：字段标准化、类型转换、领域枚举校验和稳定哈希。
- `identity.py`：领域主键和去重键。
- `quality.py`：领域质量规则和拒绝原因。
- `storage.py`：SQLite 结构化表、幂等写入、批次、原始清单和拒绝记录。
- `pipeline.py`：只负责编排标准化、质量校验和存储。
- `application.py`：组装依赖、保存原始响应和管理资源生命周期。
- `cli.py`：fixture/HTTP 两种运行入口。
- `identity.py, flow.py, quality.py`：领域事件、生命周期和后续扩展规则。
- `factors.py`：调用方提供的版本化因子表（废物类型 / 设施边 / 处理阶段），Decimal 精确数值。
- `ledger_store.py`：核算 schema 迁移、只追加账本、幂等写入、锁重试与运行恢复。
- `accounting.py`：废物流转环境因子核算引擎——逐事实因子应用与可追溯汇总。
- `accounting_query.py`：核算结果只读查询 / 汇总 / 导出。
- `accounting_cli.py`：核算专用 CLI（独立于 ingestion CLI）。

## 废物流转环境因子核算

在 `records` 快照之上做**逐事实的因子应用**：对每条符合纳入条件的事实，按"废物类型 / 运输边 / 处理阶段"三个口径独立生成排放/环境影响账本行，写入独立核算表（`emission_ledger`、`accounting_exclusions`、`accounting_runs`、`rules_versions`）。该模块只读 `records` 表、绝不回写，不做重量守恒裁决、批次状态排序或容量时间片评估。

### 因子表（调用方提供，版本化）

```json
{
  "waste_type_factors": {"mixed": {"factor": "0.5", "unit": "kgCO2e/kg", "result_unit": "kgCO2e", "factor_version": "wt-2026.11"}},
  "edge_factors": {"T-01>P-01": {"factor": "0.0001", "factor_version": "edge-2026.11"}},
  "stage_factors": {"received": {"factor": "0.02", "factor_version": "st-2026.11"}}
}
```

- 没有距离字段的路径必须由调用方在 `edge_factors` 中显式给出 `ORIGIN>DESTINATION` 的版本化边因子；缺失即记 `factor_missing`，系统不猜测路线、不补默认值。
- 设施自环（facility == destination）默认显式记零运输（`factor_kind=self_loop`），调用方提供显式自环边因子时优先使用；也可用 `--self-loop require` 改为要求显式因子。
- 因子、单位或纳入状态集合的任何变更都必须使用新的 `rules_version`；同版本不同规则会被拒绝（`FactorVersionConflict`），旧版本结果冻结可回放。

### 时间口径与纳入规则

- `collected_at`（业务事件时间）决定核算日期 `accounting_date`（UTC）与窗口归属；`observed_at`（系统观测时间）只用于同一事件多次观测间的新旧判定。时间戳必须带时区，否则记 `input_conflict`。
- 窗口为 `[date_from, date_to)`，按权威事实的 `collected_at` 判定；窗口外事件不属于本次分区，不产生任何行。
- 同一 `(facility_id, batch_id)` 事件的多次观测：最新观测为准（同 batch 修订、多来源重复），其余记 `not_included(superseded_by:...)`；来源间物量冲突记 `input_conflict(conflicting_sources:...)`。
- 默认纳入状态为 `collected/in_transit/received/processed`（即排除 `rejected`，可用 `--include-status` 覆盖）；零/负/非有限重量、缺失字段、不可读 payload 记 `input_conflict`。

### 四类结果

`computable`（写入 `emission_ledger`）、`factor_missing`、`input_conflict`、`not_included`（后三类写入 `accounting_exclusions` 并附原因）。每行输出计算口径（stage/factor_kind）、因子版本、record_key、原始重量、因子值、结果值与单位。

### 冻结、幂等与恢复

- 账本行只追加不修改：历史结果冻结；旧事实被新修订取代时以冲销行（`row_kind=reversal`，结果为负）抵销，汇总即净额。
- 确定性主键 + `INSERT OR IGNORE`：重复执行不产生重复账本行；按设施（`--facility`）和时间窗口（`--from/--to`）分区增量计算，分块流式读取（`--chunk-size`），内存占用有界。
- 每块一个 `BEGIN IMMEDIATE` 事务，锁冲突有限重试；块提交前后崩溃均可重跑恢复，遗留的 `running` 运行在下次运行时被标记为 `interrupted`。

### 核算 CLI

```bash
PYTHONPATH=src python3 -m municipal_waste.accounting_cli init-schema --database /tmp/mw.sqlite3
PYTHONPATH=src python3 -m municipal_waste.accounting_cli compute \
  --database /tmp/mw.sqlite3 --factors factors.json --rules-version v2026.11 \
  [--facility T-01] [--from 2026-11-01T00:00:00Z] [--to 2026-11-02T00:00:00Z]
PYTHONPATH=src python3 -m municipal_waste.accounting_cli runs --database /tmp/mw.sqlite3
PYTHONPATH=src python3 -m municipal_waste.accounting_cli export --database /tmp/mw.sqlite3 \
  --what ledger --rules-version v2026.11 --facility T-01 --output ledger.json
PYTHONPATH=src python3 -m municipal_waste.accounting_cli summary --database /tmp/mw.sqlite3 \
  --group-by rules_version,facility_id,waste_type,stage
```

查询/导出为只读连接，支持按设施、废物类型、时间段（`accounting_date`）和规则版本过滤；汇总用 Decimal 精确求和（含冲销净额）。

## 运行

```bash
PYTHONPATH=src python3 -m municipal_waste.cli \
  --fixture fixtures/sample.json \
  --database /tmp/municipal-waste-flow.sqlite3 \
  --raw-dir /tmp/municipal-waste-flow-raw
```

HTTP 模式：

```bash
PYTHONPATH=src python3 -m municipal_waste.cli \
  --url https://example.invalid/api/records \
  --database /tmp/municipal-waste-flow.sqlite3 \
  --raw-dir /tmp/municipal-waste-flow-raw
```

接口需要返回 JSON 数组，或包含 `records`、`data`、`items`、`results` 的对象；其他嵌套结构使用 `--records-key`。认证变量 `DATA_SOURCE_BEARER_TOKEN` 和 `DATA_SOURCE_API_KEY` 只作为环境变量名称示例，不写入真实值。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）
- SQLite 3.x（本地结构化存储）
- HTTP(S) JSON 数据源（生产接入时按具体来源协议配置）

## 验证

```bash
PYTHONPATH=src python3 -m pytest --import-mode=importlib tests
PYTHONPATH=src python3 -m compileall -q src
```
