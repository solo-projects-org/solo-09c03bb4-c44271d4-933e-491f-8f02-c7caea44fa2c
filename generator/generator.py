#!/usr/bin/env python3
"""本地消息发生器：按 Debezium JSON（无 schema 包装）格式向 Kafka 造数。

用法:
  python generator/generator.py topics   创建主题（单分区）
  python generator/generator.py demo1    场景一：tx-1001 完整；
                                         tx-1002 先发 END、暂扣一条检验值；
                                         tx-1003 完整（须等 tx-1002）
  python generator/generator.py demo2    场景二：补发 tx-1002 暂扣的检验值；
                                         再发完整 tx-1004
  python generator/generator.py all      无延迟全量（tx-1001..tx-1004 完整）
"""
from __future__ import annotations

import json
import os
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_AUDIT = os.environ.get("TOPIC_AUDIT", "lims.report_audit")
TOPIC_VALUES = os.environ.get("TOPIC_VALUES", "lims.test_values")
TOPIC_TX = os.environ.get("TOPIC_TX", "lims.transaction")

AUDIT = "report_audit"
VALUES = "test_values"

# ---------------------------------------------------------------- 数据集

R001_AUDIT = {"report_no": "R001", "audit_status": "approved", "auditor": "王复核",
              "audit_time": "2026-11-13T09:05:00Z", "remark": "首次审核通过"}
R001_GLU = {"report_no": "R001", "item_code": "GLU", "item_name": "空腹血糖",
            "result_value": "5.6", "unit": "mmol/L", "ref_range": "3.9-6.1", "flag": "N"}
R001_K = {"report_no": "R001", "item_code": "K", "item_name": "血清钾",
          "result_value": "4.1", "unit": "mmol/L", "ref_range": "3.5-5.3", "flag": "N"}

R002_AUDIT_V1 = {"report_no": "R002", "audit_status": "pending", "auditor": None,
                 "audit_time": None, "remark": "待审核"}
R002_AUDIT_V2 = {"report_no": "R002", "audit_status": "approved", "auditor": "李审核",
                 "audit_time": "2026-11-13T09:20:00Z", "remark": "复核通过"}
R002_GLU = {"report_no": "R002", "item_code": "GLU", "item_name": "空腹血糖",
            "result_value": "7.8", "unit": "mmol/L", "ref_range": "3.9-6.1", "flag": "H"}
R002_NA = {"report_no": "R002", "item_code": "NA", "item_name": "血清钠",
           "result_value": "139", "unit": "mmol/L", "ref_range": "136-145", "flag": "N"}

R003_AUDIT = {"report_no": "R003", "audit_status": "approved", "auditor": "王复核",
              "audit_time": "2026-11-13T09:40:00Z", "remark": "常规审核"}
R003_WBC = {"report_no": "R003", "item_code": "WBC", "item_name": "白细胞计数",
            "result_value": "6.2", "unit": "10^9/L", "ref_range": "3.5-9.5", "flag": "N"}
R003_HGB = {"report_no": "R003", "item_code": "HGB", "item_name": "血红蛋白",
            "result_value": "135", "unit": "g/L", "ref_range": "130-175", "flag": "N"}

R004_AUDIT = {"report_no": "R004", "audit_status": "approved", "auditor": "李审核",
              "audit_time": "2026-11-13T10:10:00Z", "remark": "加急审核"}
R004_CRP = {"report_no": "R004", "item_code": "CRP", "item_name": "C反应蛋白",
            "result_value": "2.1", "unit": "mg/L", "ref_range": "0-8", "flag": "N"}

# ---------------------------------------------------------------- 信封构造


def now_ms() -> int:
    return int(time.time() * 1000)


def envelope(*, table, op, tx_id, total_order, after=None, before=None):
    """Debezium 数据事件（value.converter.schemas.enable=false 的形态）。"""
    return {
        "before": before,
        "after": after,
        "source": {
            "version": "2.7.3.Final",
            "connector": "mysql",
            "name": "lims",
            "ts_ms": now_ms(),
            "snapshot": "false",
            "db": "lims",
            "table": table,
            "server_id": 1,
            "file": "mysql-bin.000003",
            "pos": 1000 + total_order,
            "row": 0,
        },
        "op": op,
        "ts_ms": now_ms(),
        "transaction": {
            "id": tx_id,
            "total_order": total_order,
            "data_collection_order": 1,
        },
    }


def tx_begin(tx_id):
    return {"status": "BEGIN", "id": tx_id, "ts_ms": now_ms(),
            "event_count": None, "data_collections": None}


def tx_end(tx_id, counts):
    return {
        "status": "END",
        "id": tx_id,
        "ts_ms": now_ms(),
        "event_count": sum(counts.values()),
        "data_collections": [
            {"data_collection": f"lims.{t}", "event_count": n} for t, n in counts.items()
        ],
    }


# ---------------------------------------------------------------- 发送


def _delivered(err, msg):
    if err is not None:
        print(f"  !! 投递失败 {msg.topic()}[{msg.partition()}]: {err}", file=sys.stderr)


class Sender:
    def __init__(self):
        self.producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})

    def send(self, topic, key, value, desc=""):
        self.producer.produce(
            topic,
            key=json.dumps(key, ensure_ascii=False).encode("utf-8"),
            value=json.dumps(value, ensure_ascii=False).encode("utf-8"),
            on_delivery=_delivered,
        )
        self.producer.poll(0)
        print(f"  -> {topic} {desc}")

    def flush(self):
        self.producer.flush(30)


def audit_key(row):
    return {"report_no": row["report_no"]}


def value_key(row):
    return {"report_no": row["report_no"], "item_code": row["item_code"]}


def send_audit(s, tx_id, order, op, after, before=None):
    s.send(TOPIC_AUDIT, audit_key(after or before),
           envelope(table=AUDIT, op=op, tx_id=tx_id, total_order=order,
                    after=after, before=before),
           desc=f"audit {after['report_no'] if after else before['report_no']} "
                f"op={op} tx={tx_id} order={order}")


def send_value(s, tx_id, order, op, after, before=None):
    row = after or before
    s.send(TOPIC_VALUES, value_key(row),
           envelope(table=VALUES, op=op, tx_id=tx_id, total_order=order,
                    after=after, before=before),
           desc=f"value {row['report_no']}/{row['item_code']} op={op} tx={tx_id} order={order}")


def send_tx(s, tx_id, payload, desc):
    s.send(TOPIC_TX, tx_id, payload, desc=desc)


# ---------------------------------------------------------------- 场景


def ensure_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(t, num_partitions=1, replication_factor=1)
           for t in (TOPIC_AUDIT, TOPIC_VALUES, TOPIC_TX) if t not in existing]
    if not new:
        print("主题已存在:", TOPIC_AUDIT, TOPIC_VALUES, TOPIC_TX)
        return
    for t, f in admin.create_topics(new).items():
        try:
            f.result(30)
            print("已创建主题:", t)
        except Exception as exc:  # noqa: BLE001
            print(f"创建主题 {t} 返回: {exc}")


def send_tx1001(s):
    print("[tx-1001] 完整事务：R001 审核+两项检验值，R002 登记(pending)")
    send_tx(s, "tx-1001", tx_begin("tx-1001"), "BEGIN tx-1001")
    send_audit(s, "tx-1001", 0, "c", R001_AUDIT)
    send_value(s, "tx-1001", 1, "c", R001_GLU)
    send_value(s, "tx-1001", 2, "c", R001_K)
    send_audit(s, "tx-1001", 3, "c", R002_AUDIT_V1)
    send_tx(s, "tx-1001", tx_end("tx-1001", {AUDIT: 2, VALUES: 2}),
            "END tx-1001 event_count=4")


def send_tx1002_head(s):
    print("[tx-1002] END 先行 + 暂扣一条检验值（R002 审核通过 + GLU；NA 暂扣）")
    send_tx(s, "tx-1002", tx_begin("tx-1002"), "BEGIN tx-1002")
    # END 先于本事务全部数据到达（跨主题乱序场景）
    send_tx(s, "tx-1002", tx_end("tx-1002", {AUDIT: 1, VALUES: 2}),
            "END tx-1002 event_count=3 （先于数据到达）")
    send_audit(s, "tx-1002", 0, "u", R002_AUDIT_V2, before=R002_AUDIT_V1)
    send_value(s, "tx-1002", 1, "c", R002_GLU)
    print("       （R002/NA 检验值被暂扣，留待 demo2 补发）")


def send_tx1002_delayed(s):
    print("[tx-1002] 补发暂扣的 R002/NA 检验值")
    send_value(s, "tx-1002", 2, "c", R002_NA)


def send_tx1003(s):
    print("[tx-1003] 完整事务（R003），但排在 tx-1002 之后，须等其先入仓")
    send_tx(s, "tx-1003", tx_begin("tx-1003"), "BEGIN tx-1003")
    send_audit(s, "tx-1003", 0, "c", R003_AUDIT)
    send_value(s, "tx-1003", 1, "c", R003_WBC)
    send_value(s, "tx-1003", 2, "c", R003_HGB)
    send_tx(s, "tx-1003", tx_end("tx-1003", {AUDIT: 1, VALUES: 2}),
            "END tx-1003 event_count=3")


def send_tx1004(s):
    print("[tx-1004] 新的完整事务（R004）")
    send_tx(s, "tx-1004", tx_begin("tx-1004"), "BEGIN tx-1004")
    send_audit(s, "tx-1004", 0, "c", R004_AUDIT)
    send_value(s, "tx-1004", 1, "c", R004_CRP)
    send_tx(s, "tx-1004", tx_end("tx-1004", {AUDIT: 1, VALUES: 1}),
            "END tx-1004 event_count=2")


def demo1():
    ensure_topics()
    s = Sender()
    send_tx1001(s)
    send_tx1002_head(s)
    send_tx1003(s)
    s.flush()
    print("\n完成。预期：tx-1001 入仓；tx-1002 收 2/3 等待；tx-1003 已齐但被队头阻塞。")
    print("查看：curl -s localhost:8000/ingest/status | python3 -m json.tool")


def demo2():
    ensure_topics()
    s = Sender()
    send_tx1002_delayed(s)
    send_tx1004(s)
    s.flush()
    print("\n完成。预期：tx-1002 收齐整笔入仓，随后 tx-1003、tx-1004 依次入仓。")
    print("查看：curl -s localhost:8000/reports/R002 | python3 -m json.tool")


def send_all():
    ensure_topics()
    s = Sender()
    send_tx1001(s)
    print("[tx-1002] 完整事务（无暂扣）")
    send_tx(s, "tx-1002", tx_begin("tx-1002"), "BEGIN tx-1002")
    send_audit(s, "tx-1002", 0, "u", R002_AUDIT_V2, before=R002_AUDIT_V1)
    send_value(s, "tx-1002", 1, "c", R002_GLU)
    send_value(s, "tx-1002", 2, "c", R002_NA)
    send_tx(s, "tx-1002", tx_end("tx-1002", {AUDIT: 1, VALUES: 2}),
            "END tx-1002 event_count=3")
    send_tx1003(s)
    send_tx1004(s)
    s.flush()
    print("\n完成。tx-1001..tx-1004 应全部整笔入仓。")


def main(argv):
    if len(argv) < 2 or argv[1] in ("-h", "--help"):
        print(__doc__)
        return 0
    cmd = argv[1]
    actions = {"topics": ensure_topics, "demo1": demo1, "demo2": demo2, "all": send_all}
    if cmd not in actions:
        print(f"未知命令: {cmd}\n{__doc__}")
        return 2
    actions[cmd]()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
