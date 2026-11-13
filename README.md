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
- `reconstruction.py`：批次流转链重建的纯裁决引擎（无 I/O，可单测）。
- `flow_store.py`：重建侧 schema、互斥锁、流式事实索引与幂等发布。
- `rebuild.py`：全量/单批次增量重建编排与查询、导出接口。

## 批次流转链重建

ingestion 只保存“观测事实”，重建是独立的读侧能力：按 `batch_id` 把收集、
运输、接收、处理或拒绝观测重新组合成阶段序列，输出每条批次的阶段结论、
采纳的 `record_key`、设施边、缺失阶段、非法跳转与冲突解释。它不改变
ingestion 入口、`identity.batch_key` 语义或已保存的 `records`，只新增
`flow_runs`、`flow_rebuild_locks`、`flow_fact_index`、`batch_flow_chains`
四张表。

### 裁决与发布策略

- **事件时间裁决，而非到达顺序。** 事实按来源声称的 `observed_at` 排序，
  相同时间戳归入同一个决策点，按 `(生命周期排名, facility_id, record_key)`
  全序确定性选取采纳事实，其余作为 `same_timestamp_conflict` /
  `fork_observation` 冲突保留。因此乱序到达、迟到的合法事实会被放回自己的
  事件时间槽位，不会因为“后到”被误判成状态回退；任意到达顺序重放都得到
  字节级一致的结论。
- **状态机裁决。** 逐点应用 `flow.can_transition`：非法跳转
  `illegal_transition`、处理后活动 `activity_after_terminal`、拒绝后恢复
  `recovery_after_rejection` 分别标注；非法事实不推进裁决基线，但不会被
  静默丢弃。拒绝路径的缺失阶段不含 `processed`。
- **不把最新状态当成完整流向。** 单阶段批次会显式输出 `missing_stage`；
  每条阶段、边、冲突都回指 `record_key`，并在 `evidence` 中汇总批次全部
  事实（含 `observed_at` 无法解析、无法参与排序的事实）。
- **特殊规则。** 源设施等于目的设施的自环仅 `processed` 合法，否则记
  `self_loop_illegal`；跨设施交接断裂记 `facility_handoff_gap`；跨日链标
  `cross_day`；无法解析的 `collected_at` 仅告警，事实仍按 `observed_at`
  参与排序。
- **幂等发布与中断重试。** 结论以 `content_hash` 去重，同输入重放零写入；
  修订只更新受影响批次。每个批次独立事务提交，崩溃后已发布批次保持有效，
  重新运行即可续完，`flow_runs` 台账记录 `running/completed/failed/locked`。
- **互斥与有限内存。** 进程内 `RLock` 加数据库行级咨询锁
  （`BEGIN IMMEDIATE`，过期锁可回收）保证并发重建互斥；按批次服务端游标
  流式扫描，单次只保留一个批次的事实，SQLite 写锁冲突由 `busy_timeout`
  兜底并包装成可重试错误。

### 命令

```bash
# 全量重建
PYTHONPATH=src python3 -m municipal_waste.cli rebuild --database /tmp/mw.sqlite3
# 单批次增量重建（不触碰其他批次结论）
PYTHONPATH=src python3 -m municipal_waste.cli rebuild --database /tmp/mw.sqlite3 --batch-id B-001
# 查询
PYTHONPATH=src python3 -m municipal_waste.cli query --database /tmp/mw.sqlite3 [--batch-id B-001]
# 导出（jsonl 逐行有界，或 json 数组）
PYTHONPATH=src python3 -m municipal_waste.cli export --database /tmp/mw.sqlite3 --output chains.jsonl --format jsonl
```

Python 接口：`rebuild.rebuild_batch_flows(database, batch_id=None)`、
`rebuild.get_batch_flow(...)`、`rebuild.list_batch_flows(...)`、
`rebuild.export_batch_flows(...)`。本能力止于批次流向重建，不包含重量平衡、
原始响应篡改检测或通用调度。

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
