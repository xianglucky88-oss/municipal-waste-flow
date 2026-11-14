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
- `cli.py`：fixture/HTTP 两种运行入口，以及 `capacity` 报告子命令。
- `identity.py, flow.py, quality.py`：领域事件、生命周期和后续扩展规则。
- `capacity.py`：纯函数容量评估引擎：规则解析、批次修订归并、时间片规划、结论与区间合并。
- `capacity_storage.py`：增量持久化：规则版本、报告/结论、跨进程锁、时间片缓存、只读访问。
- `capacity_service.py`：编排有界读取、取消/资源上限、锁、崩溃恢复与幂等落盘。

## 设施处理能力超载评估

在 `WasteRecord` 的 `facility_id`、`destination_facility_id`、`net_weight_kg`、
`collected_at` 与 `status` 之上做“某接收设施在某时间片是否超载”的判定。它**不是**
重量平衡/输入输出守恒判定：每条称重只按 `destination_facility_id` 计入一次，
`facility_id` 仅作溯源，因此同一称重不会被来源和目的设施重复计数。

- 时间口径：`collected_at` 是事件时间，决定落入哪个小时/日时间片；`observed_at`
  是知识时间，决定修订新旧顺序与 `as_of` 快照。分桶边界按规则的
  `timezone_offset_minutes` 计算，支持跨午夜/跨日；窗口或规则有效段截断时按秒折算容量。
- 结论：`within_capacity` / `overloaded` / `undecidable`，每条结论带
  `record_keys`、`records`（含批次、重量、状态、来源）、容量版本与规则哈希、`exclusions`。
- 不可判定来源：无容量规则、拒绝记录、零/负重量、时间不可解析、批次内设施环路、
  同 `observed_at` 冲突修订、status 回退等，均以 `blocking_reasons` 与排除原因给出。
- 同批次多来源重复上报归并为一条 `duplicate_report`；规则版本变更后历史报告冻结，
  旧版本可回放。

规则文件按设施、废物类型、版本和有效期描述容量：

```json
[{"facility_id": "P-01", "waste_type": "mixed", "version": 1,
  "effective_from": "2026-11-01T00:00:00Z", "timezone_offset_minutes": 480,
  "hourly_limit_kg": 1000, "daily_limit_kg": 10000}]
```

```bash
# 生成报告
PYTHONPATH=src python3 -m municipal_waste.cli capacity --generate \
  --database /tmp/municipal-waste-flow.sqlite3 --facility-id P-01 --waste-type mixed \
  --window-start 2026-11-01T10:00:00Z --window-end 2026-11-01T12:00:00Z \
  --as-of 2026-11-02T00:00:00Z --rules-file rules.json

# 只读读取/列出
PYTHONPATH=src python3 -m municipal_waste.cli capacity --read cap_<id> --database /tmp/municipal-waste-flow.sqlite3
PYTHONPATH=src python3 -m municipal_waste.cli capacity --list --database /tmp/municipal-waste-flow.sqlite3 --facility-id P-01
```

并发与恢复：同一 `(facility, waste_type)` 的报告生成持有跨进程建议锁，第二个写者会
得到 `CapacityLockConflict` 而不是覆盖；锁带心跳，崩溃后可回收；报告与结论在同一事务
幂等落盘，重试不产生重复结论；`capacity_slice_cache` 以引擎版本、规则版本/哈希、
分桶边界与 `as_of` 及输入指纹为键，实现按设施与时间片的增量重算。读取按批流式进行，
并支持 `max_records`/`max_slices` 上限与协作式取消。

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
