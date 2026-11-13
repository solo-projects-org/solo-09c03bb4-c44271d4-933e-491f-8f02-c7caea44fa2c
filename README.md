# LIMS 检验数仓入仓服务

消费 LIMS 的 Debezium 变更流（**报告审核表**、**检验值表**、**事务元数据主题**，JSON 无 schema
包装），按事务整笔入仓到 SQLite 仓表，供研究取数。核心目标：**新审核状态绝不能配上旧检验值**
—— 同一源事务的审核状态与检验值要么一起可见，要么一起不可见。

## 架构

```
LIMS 源库 (Debezium)
   │  lims.report_audit   (报告审核表, 单分区)
   │  lims.test_values    (检验值表,   单分区)
   │  lims.transaction    (BEGIN/END,  单分区)
   ▼
┌────────────────────────────── FastAPI 服务 ──────────────────────────────┐
│ 消费线程 (confluent-kafka, 手动提交 offset)                                │
│   1. 消息落库：tx_buffer / tx_meta（去重 + 收件进度，同一 SQLite 事务）      │
│   2. 落库成功才 commit offset；失败重试，绝不确认                           │
│   3. apply_pending：按 END 顺序取队头事务，收齐 (received>=event_count)     │
│      才在单个 SQLite 事务里按 total_order 整笔应用并标记 applied            │
│ 查询接口 (WAL 快照读)：/reports/{report_no} 单读事务取审核状态+全部检验值    │
└───────────────────────────────────────────────────────────────────────────┘
   ▼
SQLite (/data/lims.db, WAL, synchronous=FULL)：缓冲、进度、队列、仓表同库持久化
```

## 一致性设计（对应需求）

| 需求 | 实现 |
| --- | --- |
| 消息与 END 跨主题乱序 | 数据消息先落 `tx_buffer`（与进度同事务），END 只负责登记 `event_count` 与入仓顺序，谁先到都可以 |
| 按 event_count 收齐 | `tx_meta.received_count >= event_count` 才视为收齐 |
| 事务内按 total_order 应用 | 应用时 `ORDER BY total_order` |
| 事务间按 END 顺序整笔入仓 | END 到达即入 `end_queue`（seq 递增），只从队头开始应用；整笔应用是**单个 SQLite 事务** |
| 后事务收齐也须等前序 | 队头未收齐即停止（head-of-line blocking） |
| 未齐消息与进度一起持久化 | `tx_buffer` 插入与 `received_count` 递增在**同一 SQLite 事务**提交 |
| 保存失败不确认消费 | 落库成功才 `commit(offset)`；失败无限重试不确认 |
| 重启/落库后未确认退出，重读不重复、不倒退 | 按 `(topic, partition, offset)` 与 `(tx_id, total_order)` 去重；`applied` 标记 + 顺序应用防倒退；重启先从 SQLite 恢复并补应用 |
| 查询只读同一已提交状态 | 仓表只在整笔事务提交时变化；查询用单读事务取 WAL 快照，并返回 `as_of_tx_seq` 水位 |

## 快速开始

```bash
docker compose up -d --build          # 启动 Kafka + 入仓服务
# Swagger UI: http://localhost:8000/docs
```

## 演示剧本（先送 END、延迟检验值、重启续收、整笔入仓）

一键演示：`bash scripts/demo.sh`，或手动：

```bash
# 1) 场景一：tx-1001 完整；tx-1002 先发 END、暂扣 R002/NA 检验值；tx-1003 完整但须等待
docker compose --profile tools run --rm generator demo1

# 2) 看进度：tx-1002 收 2/3 等待；tx-1003 已齐但被队头阻塞
curl -s localhost:8000/ingest/status | python3 -m json.tool

# 3) 一致性读：R002 仍是旧状态（pending、无检验值）——不会新审核配旧值
curl -s localhost:8000/reports/R002 | python3 -m json.tool

# 4) 重启服务：缓冲与进度从 SQLite 恢复，tx-1001 不重复应用
docker compose restart app
curl -s localhost:8000/ingest/status | python3 -m json.tool

# 5) 场景二：补发暂扣的检验值 + 完整 tx-1004
docker compose --profile tools run --rm generator demo2

# 6) 整笔入仓：R002 的 approved 与 GLU/NA 同时可见；tx-1003、tx-1004 随后依次入仓
curl -s localhost:8000/reports/R002 | python3 -m json.tool
```

## 可选：崩溃演练（落库后未确认便退出）

```bash
docker compose stop app
INGEST_CRASH_AFTER_PERSIST=5 docker compose up -d app   # 落库 5 条后、确认前退出进程
docker compose logs -f app        # 观察退出与自动重启
# 服务重启后重投的消息全部判重：不重复应用、不倒退
INGEST_CRASH_AFTER_PERSIST=0 docker compose up -d app   # 练完恢复
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/reports/{report_no}` | 审核状态 + 全部检验值（同一已提交快照，含 `as_of_tx_seq` 水位） |
| GET | `/ingest/status` | 等待中/已应用事务、收件进度、消费与确认位点、仓表行数 |
| GET | `/health` | 消费线程存活与最近错误 |
| GET | `/docs` | Swagger UI |

## 本地逻辑测试（不依赖 Kafka）

```bash
python3 tests/smoke_test.py
```

覆盖：跨主题乱序（END 先行）、event_count 收齐、total_order 事务内顺序、END 顺序整笔入仓、
队头阻塞、重启恢复、整段重读去重（不重复应用、不倒退）、一致性读。

## 目录结构

```
app/
  config.py     环境变量配置
  db.py         SQLite 持久化层（缓冲/进度/队列/整笔应用/一致读，纯标准库）
  consumer.py   Kafka 消费线程（先落库后确认，重启恢复）
  main.py       FastAPI 入口（Swagger /docs）
generator/
  generator.py  本地消息发生器（Debezium JSON 无 schema 包装）
tests/smoke_test.py   核心逻辑冒烟测试
scripts/demo.sh       一键演示
docker-compose.yml    kafka + app + generator
```

## 假设与边界

- 三个主题均为单分区；源端从空表开始，只发这两表的 `c`/`u`（`d` 也已兼容处理）。
- END 的 `event_count` 只计这两张表的事件（源端只捕获这两表）。
- 数据消息缺事务元数据时降级为“单事件事务”立即入队（有告警日志），不影响主流程。
- 无法解析的消息落 `dead_letters` 表后再确认，不阻塞主流。
