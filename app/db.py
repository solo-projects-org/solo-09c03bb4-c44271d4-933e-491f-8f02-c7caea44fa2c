"""SQLite 持久化层：收件缓冲、事务队列、整笔应用、一致性读。

只依赖标准库，可脱离 Kafka/FastAPI 独立测试。

一致性设计：
  1. 消息落库（按 kafka 坐标去重）与收件进度（tx_meta.received_count）
     在同一个 SQLite 事务里提交 —— “未齐消息和收件进度一起持久保存”。
  2. Kafka offset 只有在落库提交成功后才被确认（见 consumer.py）；
     落库失败不确认，重投后按 (topic, partition, offset) 去重，
     不会重复应用、不会倒退结果。
  3. 源事务按 END 到达顺序进入 end_queue；只有队头事务收齐
     （received_count >= event_count）才整笔应用，后续事务即使收齐
     也必须等待缺件的前序事务。
  4. 整笔应用（写仓表 + 标记 applied）在单个 SQLite 事务里完成，
     读者（WAL 快照）永远只看到完整的前一个已提交状态。
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator

SCHEMA = """
-- 源事务登记表：BEGIN/END 到达情况、应收/已收事件数、是否已入仓
CREATE TABLE IF NOT EXISTS tx_meta (
  tx_id          TEXT PRIMARY KEY,
  begin_seen     INTEGER NOT NULL DEFAULT 0,
  end_seen       INTEGER NOT NULL DEFAULT 0,
  event_count    INTEGER,               -- END 携带的应收入仓事件总数
  received_count INTEGER NOT NULL DEFAULT 0,  -- 已落库的数据事件数（与 tx_buffer 同步维护）
  applied        INTEGER NOT NULL DEFAULT 0,
  created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  applied_at     TEXT
);

-- 事务主题消息去重（按 kafka 坐标）
CREATE TABLE IF NOT EXISTS tx_control (
  kafka_topic     TEXT NOT NULL,
  kafka_partition INTEGER NOT NULL,
  kafka_offset    INTEGER NOT NULL,
  tx_id           TEXT NOT NULL,
  status          TEXT NOT NULL,
  PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

-- 入仓顺序队列：按 END 到达顺序（单分区 => 消费顺序即 offset 顺序）
CREATE TABLE IF NOT EXISTS end_queue (
  seq   INTEGER PRIMARY KEY AUTOINCREMENT,
  tx_id TEXT NOT NULL UNIQUE
);

-- 数据消息缓冲：未收齐的事务消息持久保存在这里
CREATE TABLE IF NOT EXISTS tx_buffer (
  kafka_topic     TEXT NOT NULL,
  kafka_partition INTEGER NOT NULL,
  kafka_offset    INTEGER NOT NULL,
  tx_id           TEXT NOT NULL,
  total_order     INTEGER NOT NULL,     -- 事务内绝对顺序
  table_name      TEXT NOT NULL,        -- report_audit | test_values
  op              TEXT NOT NULL,        -- c | u | d | r
  payload         TEXT NOT NULL,        -- after 的 JSON（d 时为 before）
  received_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset),
  UNIQUE (tx_id, total_order)
);

-- 无法解析的消息（落库到死信表后再确认，不阻塞主流）
CREATE TABLE IF NOT EXISTS dead_letters (
  kafka_topic     TEXT NOT NULL,
  kafka_partition INTEGER NOT NULL,
  kafka_offset    INTEGER NOT NULL,
  raw             TEXT,
  reason          TEXT,
  PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

-- 仓表：报告审核
CREATE TABLE IF NOT EXISTS wh_report_audit (
  report_no    TEXT PRIMARY KEY,
  audit_status TEXT NOT NULL,
  auditor      TEXT,
  audit_time   TEXT,
  remark       TEXT,
  updated_tx   TEXT
);

-- 仓表：检验值
CREATE TABLE IF NOT EXISTS wh_test_values (
  report_no    TEXT NOT NULL,
  item_code    TEXT NOT NULL,
  item_name    TEXT,
  result_value TEXT,
  unit         TEXT,
  ref_range    TEXT,
  flag         TEXT,
  updated_tx   TEXT,
  PRIMARY KEY (report_no, item_code)
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """打开连接：WAL + 全同步（提交即落盘，之后才可确认 offset）。"""
    directory = os.path.dirname(os.path.abspath(db_path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def _tx(conn: sqlite3.Connection) -> Iterator[None]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


# ---------------------------------------------------------------- 写入侧

def record_tx_event(
    conn: sqlite3.Connection,
    *,
    topic: str,
    partition: int,
    offset: int,
    tx_id: str,
    status: str,
    event_count: int | None = None,
) -> str:
    """记录事务主题消息（BEGIN/END）。返回 'recorded' 或 'duplicate'。"""
    with _tx(conn):
        cur = conn.execute(
            "INSERT OR IGNORE INTO tx_control"
            " (kafka_topic, kafka_partition, kafka_offset, tx_id, status)"
            " VALUES (?,?,?,?,?)",
            (topic, partition, offset, tx_id, status),
        )
        if cur.rowcount == 0:
            return "duplicate"  # 重投：已落库过，直接跳过（不重复处理）
        conn.execute("INSERT OR IGNORE INTO tx_meta (tx_id) VALUES (?)", (tx_id,))
        if status == "BEGIN":
            conn.execute("UPDATE tx_meta SET begin_seen=1 WHERE tx_id=?", (tx_id,))
        elif status == "END":
            conn.execute(
                "UPDATE tx_meta SET end_seen=1, event_count=? WHERE tx_id=?",
                (event_count, tx_id),
            )
            # 进入入仓顺序队列（重复 END 已由 tx_control 拦截，这里再兜底）
            conn.execute("INSERT OR IGNORE INTO end_queue (tx_id) VALUES (?)", (tx_id,))
        return "recorded"


def buffer_data_event(
    conn: sqlite3.Connection,
    *,
    topic: str,
    partition: int,
    offset: int,
    tx_id: str,
    total_order: int,
    table_name: str,
    op: str,
    payload: str,
    immediate: bool = False,
) -> str:
    """缓冲数据消息，并同事务更新收件进度。返回 'buffered' 或 'duplicate'。

    immediate=True 用于无事务元数据的降级场景：作为单事件事务直接排入 END 队列。
    """
    with _tx(conn):
        cur = conn.execute(
            "INSERT OR IGNORE INTO tx_buffer"
            " (kafka_topic, kafka_partition, kafka_offset, tx_id,"
            "  total_order, table_name, op, payload)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (topic, partition, offset, tx_id, total_order, table_name, op, payload),
        )
        if cur.rowcount == 0:
            return "duplicate"  # 重投：已落库过，不重复计数、不重复应用
        conn.execute("INSERT OR IGNORE INTO tx_meta (tx_id) VALUES (?)", (tx_id,))
        conn.execute(
            "UPDATE tx_meta SET received_count = received_count + 1 WHERE tx_id=?",
            (tx_id,),
        )
        if immediate:
            conn.execute(
                "UPDATE tx_meta SET begin_seen=1, end_seen=1, event_count=1 WHERE tx_id=?",
                (tx_id,),
            )
            conn.execute("INSERT OR IGNORE INTO end_queue (tx_id) VALUES (?)", (tx_id,))
        return "buffered"


def record_dead_letter(
    conn: sqlite3.Connection,
    *,
    topic: str,
    partition: int,
    offset: int,
    raw: str,
    reason: str,
) -> None:
    with _tx(conn):
        conn.execute(
            "INSERT OR IGNORE INTO dead_letters"
            " (kafka_topic, kafka_partition, kafka_offset, raw, reason)"
            " VALUES (?,?,?,?,?)",
            (topic, partition, offset, raw, reason),
        )


def apply_pending(conn: sqlite3.Connection) -> list[str]:
    """按 END 顺序整笔应用所有已收齐的事务，返回本次应用的 tx_id 列表。

    队头未收齐即停止 —— 后续事务即使收齐也必须等待缺件的前序事务。
    整个循环在一个 SQLite 事务里：要么全部落仓，要么全部不落。
    """
    applied: list[str] = []
    with _tx(conn):
        while True:
            head = conn.execute(
                "SELECT q.seq AS seq, m.tx_id AS tx_id,"
                "       m.event_count AS event_count, m.received_count AS received_count"
                " FROM end_queue q JOIN tx_meta m ON m.tx_id = q.tx_id"
                " WHERE m.applied = 0 ORDER BY q.seq LIMIT 1"
            ).fetchone()
            if head is None:
                break
            if head["event_count"] is None or head["received_count"] < head["event_count"]:
                break  # 队头缺件：阻塞后续事务
            events = conn.execute(
                "SELECT * FROM tx_buffer WHERE tx_id=? ORDER BY total_order",
                (head["tx_id"],),
            ).fetchall()
            for ev in events:
                _apply_event(conn, ev)
            conn.execute(
                "UPDATE tx_meta SET applied=1,"
                " applied_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE tx_id=?",
                (head["tx_id"],),
            )
            applied.append(head["tx_id"])
    return applied


def _apply_event(conn: sqlite3.Connection, ev: sqlite3.Row) -> None:
    table = ev["table_name"]
    op = ev["op"]
    data = json.loads(ev["payload"])
    if table == "report_audit":
        if op == "d":
            conn.execute("DELETE FROM wh_report_audit WHERE report_no=?", (data["report_no"],))
        else:
            conn.execute(
                "INSERT INTO wh_report_audit"
                " (report_no, audit_status, auditor, audit_time, remark, updated_tx)"
                " VALUES (?,?,?,?,?,?)"
                " ON CONFLICT(report_no) DO UPDATE SET"
                "   audit_status=excluded.audit_status, auditor=excluded.auditor,"
                "   audit_time=excluded.audit_time, remark=excluded.remark,"
                "   updated_tx=excluded.updated_tx",
                (
                    data["report_no"],
                    data.get("audit_status", ""),
                    data.get("auditor"),
                    data.get("audit_time"),
                    data.get("remark"),
                    ev["tx_id"],
                ),
            )
    elif table == "test_values":
        if op == "d":
            conn.execute(
                "DELETE FROM wh_test_values WHERE report_no=? AND item_code=?",
                (data["report_no"], data["item_code"]),
            )
        else:
            conn.execute(
                "INSERT INTO wh_test_values"
                " (report_no, item_code, item_name, result_value, unit, ref_range, flag, updated_tx)"
                " VALUES (?,?,?,?,?,?,?,?)"
                " ON CONFLICT(report_no, item_code) DO UPDATE SET"
                "   item_name=excluded.item_name, result_value=excluded.result_value,"
                "   unit=excluded.unit, ref_range=excluded.ref_range, flag=excluded.flag,"
                "   updated_tx=excluded.updated_tx",
                (
                    data["report_no"],
                    data["item_code"],
                    data.get("item_name"),
                    data.get("result_value"),
                    data.get("unit"),
                    data.get("ref_range"),
                    data.get("flag"),
                    ev["tx_id"],
                ),
            )
    else:
        raise ValueError(f"未知表: {table}")


# ---------------------------------------------------------------- 读取侧

def read_report(db_path: str, report_no: str) -> dict[str, Any]:
    """按报告号查询：审核状态与全部检验值来自同一个已提交快照（单读事务）。"""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN")
        audit = conn.execute(
            "SELECT report_no, audit_status, auditor, audit_time, remark, updated_tx"
            " FROM wh_report_audit WHERE report_no=?",
            (report_no,),
        ).fetchone()
        values = conn.execute(
            "SELECT report_no, item_code, item_name, result_value, unit, ref_range, flag, updated_tx"
            " FROM wh_test_values WHERE report_no=? ORDER BY item_code",
            (report_no,),
        ).fetchall()
        as_of = conn.execute(
            "SELECT COALESCE(MAX(q.seq),0) AS s FROM end_queue q"
            " JOIN tx_meta m ON m.tx_id = q.tx_id WHERE m.applied=1"
        ).fetchone()["s"]
        conn.execute("COMMIT")
    finally:
        conn.close()
    return {
        "report_no": report_no,
        "as_of_tx_seq": as_of,
        "audit": dict(audit) if audit else None,
        "test_values": [dict(v) for v in values],
    }


def read_status(db_path: str, consumer_state: dict[str, Any] | None = None) -> dict[str, Any]:
    """入仓进度：等待中的事务（含收件进度）、已应用事务、仓表行数、消费位点。"""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN")
        pending = [
            dict(r)
            for r in conn.execute(
                "SELECT q.seq, m.tx_id, m.event_count, m.received_count,"
                "       m.begin_seen, m.end_seen"
                " FROM end_queue q JOIN tx_meta m ON m.tx_id = q.tx_id"
                " WHERE m.applied = 0 ORDER BY q.seq"
            )
        ]
        applied = [
            dict(r)
            for r in conn.execute(
                "SELECT q.seq, m.tx_id, m.event_count, m.applied_at"
                " FROM end_queue q JOIN tx_meta m ON m.tx_id = q.tx_id"
                " WHERE m.applied = 1 ORDER BY q.seq DESC LIMIT 20"
            )
        ]
        open_txs = [
            dict(r)
            for r in conn.execute(
                "SELECT tx_id, received_count, begin_seen FROM tx_meta"
                " WHERE end_seen = 0 ORDER BY created_at"
            )
        ]
        counts = conn.execute(
            "SELECT (SELECT COUNT(*) FROM wh_report_audit) AS audits,"
            "       (SELECT COUNT(*) FROM wh_test_values)  AS test_values,"
            "       (SELECT COUNT(*) FROM tx_buffer)       AS buffered,"
            "       (SELECT COUNT(*) FROM dead_letters)    AS dead_letters"
        ).fetchone()
        conn.execute("COMMIT")
    finally:
        conn.close()
    return {
        "consumer": consumer_state or {},
        "pending_transactions": pending,
        "applied_transactions": applied,
        "open_transactions_no_end": open_txs,
        "warehouse_rows": dict(counts),
    }
