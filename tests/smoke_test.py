#!/usr/bin/env python3
"""核心入仓逻辑冒烟测试（仅标准库，不依赖 Kafka / FastAPI）。

运行: python3 tests/smoke_test.py

覆盖需求要点：
  1. 消息与 END 跨主题乱序到达（END 先于数据）
  2. 按 event_count 收齐后，事务内按 total_order 应用
  3. 事务间按 END 顺序整笔入仓；后事务收齐也须等缺件的前序事务
  4. 重启后续收；落库后未确认导致的重读不重复应用、不倒退结果
  5. 查询只读同一已提交状态（新审核状态不会配上旧检验值）
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from app import db  # noqa: E402

AUDIT_TOPIC = "lims.report_audit"
VALUES_TOPIC = "lims.test_values"
TX_TOPIC = "lims.transaction"

# ------------------------------------------------------------ 测试数据

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


class Feeder:
    """模拟 consumer 的落库调用，维护各主题的 kafka offset。"""

    def __init__(self, conn):
        self.conn = conn
        self.offsets = {AUDIT_TOPIC: 0, VALUES_TOPIC: 0, TX_TOPIC: 0}

    def tx(self, tx_id, status, event_count=None, offset=None):
        off = self.offsets[TX_TOPIC] if offset is None else offset
        self.offsets[TX_TOPIC] = max(self.offsets[TX_TOPIC], off + 1)
        return db.record_tx_event(self.conn, topic=TX_TOPIC, partition=0, offset=off,
                                  tx_id=tx_id, status=status, event_count=event_count)

    def data(self, topic, tx_id, total_order, op, payload, offset=None):
        off = self.offsets[topic] if offset is None else offset
        self.offsets[topic] = max(self.offsets[topic], off + 1)
        table = "report_audit" if topic == AUDIT_TOPIC else "test_values"
        return db.buffer_data_event(self.conn, topic=topic, partition=0, offset=off,
                                    tx_id=tx_id, total_order=total_order, table_name=table,
                                    op=op, payload=json.dumps(payload, ensure_ascii=False))


def check(label, cond):
    if not cond:
        raise AssertionError(f"FAILED: {label}")
    print(f"  ok - {label}")


def received_count(conn, tx_id):
    row = conn.execute("SELECT received_count FROM tx_meta WHERE tx_id=?", (tx_id,)).fetchone()
    return row["received_count"] if row else None


def main() -> int:
    tmpdir = tempfile.mkdtemp(prefix="lims-smoke-")
    path = os.path.join(tmpdir, "lims.db")
    try:
        db.init_db(path)
        conn = db.connect(path)
        f = Feeder(conn)

        print("[1] tx-1001 完整事务（数据乱序到达：order 2 先于 0/1）")
        f.tx("tx-1001", "BEGIN")
        f.data(VALUES_TOPIC, "tx-1001", 2, "c", R001_K)       # 乱序
        f.data(AUDIT_TOPIC, "tx-1001", 0, "c", R001_AUDIT)
        f.data(VALUES_TOPIC, "tx-1001", 1, "c", R001_GLU)
        f.data(AUDIT_TOPIC, "tx-1001", 3, "c", R002_AUDIT_V1)
        f.tx("tx-1001", "END", 4)
        check("tx-1001 收齐后整笔应用", db.apply_pending(conn) == ["tx-1001"])
        r = db.read_report(path, "R001")
        check("R001 审核 approved 且两项检验值齐全",
              r["audit"]["audit_status"] == "approved" and len(r["test_values"]) == 2)
        check("水位 as_of_tx_seq=1", r["as_of_tx_seq"] == 1)

        print("[2] tx-1002：END 先于数据到达，且一条检验值延迟")
        f.tx("tx-1002", "BEGIN")
        f.tx("tx-1002", "END", 3)                              # END 先行
        f.data(AUDIT_TOPIC, "tx-1002", 0, "u", R002_AUDIT_V2)
        f.data(VALUES_TOPIC, "tx-1002", 1, "c", R002_GLU)
        check("tx-1002 收 2/3 不应用", db.apply_pending(conn) == [])
        check("tx-1002 收件进度持久化 received_count=2", received_count(conn, "tx-1002") == 2)

        print("[3] tx-1003 已收齐，但必须等待缺件的前序 tx-1002")
        f.tx("tx-1003", "BEGIN")
        f.data(AUDIT_TOPIC, "tx-1003", 0, "c", R003_AUDIT)
        f.data(VALUES_TOPIC, "tx-1003", 1, "c", R003_WBC)
        f.data(VALUES_TOPIC, "tx-1003", 2, "c", R003_HGB)
        f.tx("tx-1003", "END", 3)
        check("tx-1003 被队头阻塞", db.apply_pending(conn) == [])

        print("[4] 一致性读：R002 仍是旧已提交状态（pending + 无检验值）")
        r = db.read_report(path, "R002")
        check("R002 审核仍 pending", r["audit"]["audit_status"] == "pending")
        check("R002 无检验值", r["test_values"] == [])
        check("水位仍 as_of_tx_seq=1", r["as_of_tx_seq"] == 1)

        print("[5] 模拟重启：重新打开库，恢复时不重复应用、不缺件不应用")
        conn.close()
        db.init_db(path)                                       # 幂等
        conn = db.connect(path)
        f = Feeder(conn)  # 消费线程重建，位点从 Kafka 已提交 offset 恢复
        check("重启后恢复应用为空（tx-1002 仍缺件）", db.apply_pending(conn) == [])
        check("重启后 tx-1002 进度仍在 received_count=2", received_count(conn, "tx-1002") == 2)

        print("[6] 模拟“落库后未确认便退出”导致的整段重读：全部去重")
        g = Feeder(conn)
        replays = [
            g.tx("tx-1001", "BEGIN", offset=0),
            g.data(AUDIT_TOPIC, "tx-1001", 0, "c", R001_AUDIT, offset=0),
            g.data(VALUES_TOPIC, "tx-1001", 1, "c", R001_GLU, offset=1),
            g.data(VALUES_TOPIC, "tx-1001", 2, "c", R001_K, offset=0),
            g.data(AUDIT_TOPIC, "tx-1001", 3, "c", R002_AUDIT_V1, offset=1),
            g.tx("tx-1001", "END", 4, offset=1),
            g.tx("tx-1002", "BEGIN", offset=2),
            g.tx("tx-1002", "END", 3, offset=3),
            g.data(AUDIT_TOPIC, "tx-1002", 0, "u", R002_AUDIT_V2, offset=2),
            g.data(VALUES_TOPIC, "tx-1002", 1, "c", R002_GLU, offset=2),
            g.tx("tx-1003", "BEGIN", offset=4),
            g.data(AUDIT_TOPIC, "tx-1003", 0, "c", R003_AUDIT, offset=3),
            g.data(VALUES_TOPIC, "tx-1003", 1, "c", R003_WBC, offset=3),
            g.data(VALUES_TOPIC, "tx-1003", 2, "c", R003_HGB, offset=4),
            g.tx("tx-1003", "END", 3, offset=5),
        ]
        check("重读消息全部判重", all(x == "duplicate" for x in replays))
        check("重读后无重复应用", db.apply_pending(conn) == [])
        check("tx-1002 进度未被重读污染 received_count=2", received_count(conn, "tx-1002") == 2)
        r = db.read_report(path, "R002")
        check("重读后结果不倒退（仍 pending、无检验值）",
              r["audit"]["audit_status"] == "pending" and r["test_values"] == [])

        print("[7] 补发 tx-1002 延迟的检验值：tx-1002、tx-1003 按 END 顺序整笔入仓")
        f.data(VALUES_TOPIC, "tx-1002", 2, "c", R002_NA, offset=5)  # 续收：VALUES 主题下一个位点
        check("依次应用 tx-1002 -> tx-1003", db.apply_pending(conn) == ["tx-1002", "tx-1003"])
        r = db.read_report(path, "R002")
        check("R002 审核通过与两条检验值同时可见（同一已提交状态）",
              r["audit"]["audit_status"] == "approved"
              and {v["item_code"] for v in r["test_values"]} == {"GLU", "NA"})
        check("R002 审核行来源 tx-1002", r["audit"]["updated_tx"] == "tx-1002")
        check("水位推进 as_of_tx_seq=3", r["as_of_tx_seq"] == 3)
        r3 = db.read_report(path, "R003")
        check("R003 随之入仓", r3["audit"]["audit_status"] == "approved"
              and len(r3["test_values"]) == 2)

        print("[8] 入仓后再重读旧消息：不重复应用、不倒退")
        check("旧消息判重", f.data(AUDIT_TOPIC, "tx-1001", 3, "c", R002_AUDIT_V1, offset=1) == "duplicate")
        check("无新增应用", db.apply_pending(conn) == [])
        r = db.read_report(path, "R002")
        check("R002 仍 approved（不倒退）", r["audit"]["audit_status"] == "approved")

        print("[9] 状态接口数据完整")
        status = db.read_status(path, consumer_state={"committed_offsets": {"t[0]": 1}})
        check("无等待事务", status["pending_transactions"] == [])
        check("已应用 3 笔", len(status["applied_transactions"]) == 3)
        check("仓表行数 audits=3 values=6",
              status["warehouse_rows"]["audits"] == 3
              and status["warehouse_rows"]["test_values"] == 6)

        conn.close()
        print("\n全部通过 ✔")
        return 0
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
