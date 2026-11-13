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
- `balance.py`：批次重量平衡核算的领域逻辑（纯函数）：分组归属、Decimal 精确求和、缺失链路与异常检测、判定。
- `balance_store.py`：核算运行、批次结果、增量缓存的持久化；只读查询与导出。
- `balance_service.py`：核算编排——分段读取、协作式取消、资源上限、心跳与中断恢复、单事务落库。
- `balance_cli.py`：核算命令行入口（`run` / `list-runs` / `show-run` / `export` / `recover`）。

## 批次重量平衡核算

`WasteRecord.net_weight_kg` 只保存在单条记录上；平衡核算模块以**分析窗口、设施集合、损耗容忍度、规则版本**为输入，从已接收（已入库）记录按 `batch_id / facility_id / destination_facility_id / status / collected_at` 计算输入、转移、接收、处理、拒绝五类汇总，输出每个批次的**平衡差、证据 record_key、缺失链路和“不足以判定”的原因**。

判定：`balanced`（|平衡差| ≤ 容忍度，容忍度 = max(绝对值, 比例 × 输入)）/ `gap`（缺口或损耗超限）/ `surplus`（输出大于输入，疑似重复计量）/ `insufficient`（缺少输入或终态观测，无法判定）。

归属与记账策略（明确约定）：

- 每条记录只归属一个 `(batch, facility, status)` 组和一个类别，同一条记录不会同时当作输入和输出；`rejected` 重量单独归类、计入输出，但永远不会计入 processed。
- 同组多次称重 / 多来源重复报量：按 `observed_at` 取最新为有效重量，其余 record_key 作为证据保留在 `duplicate_weighing` 异常中；同一 record_key 的重复修订由存储层 upsert 去重。
- 设施边图存在环：重量按节点记账、不沿边流动，环只标记 `facility_cycle`，不会重复计量。
- 上下游时间交错 / 状态回退：不阻断核算，按 `flow.can_transition` 检测并标记 `status_regression`。
- 窗口按 `collected_at` 闭区间过滤；设施集合是记录级过滤，过滤造成的部分观测会得到 `insufficient` 与缺失链路说明；零/负/非有限重量被排除并记录在 `excluded_records`。
- 重量用 `Decimal` 精确求和，仅在输出时按 0.001 kg 四舍五入，避免浮点误差。

工程特性：分段读取（`--chunk-size`）、协作式取消（`CancellationToken`）、结果可重复；每次运行为独立 `run_id`（结果版本），并发调用互不覆盖；新观测到达后按批次输入哈希增量重算，规则版本或结果参数变化使旧缓存自然失效；心跳暴露运行活性，中断的运行可由 `recover` 标记恢复；迁移在单事务内完成、失败可重试，只新增 `balance_*` 表，旧数据库和旧 CLI 不受影响；`list-runs` / `show-run` / `export` 为只读接口。

```bash
PYTHONPATH=src python3 -m municipal_waste.balance_cli run \
  --database /tmp/municipal-waste-flow.sqlite3 \
  --window-start 2026-11-01T00:00:00Z --window-end 2026-11-30T00:00:00Z \
  --facility T-01 --facility P-01 \
  --tolerance-ratio 0.01 --tolerance-kg 50 \
  --rule-version v1 --chunk-size 500 --max-records 1000000

PYTHONPATH=src python3 -m municipal_waste.balance_cli list-runs --database /tmp/municipal-waste-flow.sqlite3
PYTHONPATH=src python3 -m municipal_waste.balance_cli show-run --database /tmp/municipal-waste-flow.sqlite3 --run-id 1
PYTHONPATH=src python3 -m municipal_waste.balance_cli export --database /tmp/municipal-waste-flow.sqlite3 --run-id 1 --output run-1.json
PYTHONPATH=src python3 -m municipal_waste.balance_cli recover --database /tmp/municipal-waste-flow.sqlite3 --stale-seconds 300
```

退出码：`0` 完成，`1` 参数/存储错误，`3` 运行被取消。

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
