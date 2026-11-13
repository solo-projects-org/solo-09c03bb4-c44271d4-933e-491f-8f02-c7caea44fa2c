"""Kafka 消费线程：先持久化、后确认；按 END 顺序整笔入仓。

关键保证：
  - 每条消息先写入 SQLite（含去重、收件进度），提交成功后才 commit offset；
    落库失败则无限重试，绝不确认（保存失败不确认消费）。
  - 重启后从 Kafka 已提交位点继续消费；SQLite 里的缓冲与进度不丢，
    启动时先补应用已收齐的事务；重投的消息按坐标去重，不重复应用。
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException

from . import db
from .config import Settings

log = logging.getLogger("lims.ingest")


class Ingestor(threading.Thread):
    def __init__(self, settings: Settings) -> None:
        super().__init__(name="ingestor", daemon=True)
        self.settings = settings
        self._stop_event = threading.Event()
        self._wconn: db.sqlite3.Connection | None = None
        self.state: dict = {
            "started_at": None,
            "last_error": None,
            "consumed_offsets": {},
            "committed_offsets": {},
            "applied_txs": [],
            "persisted_messages": 0,
        }

    def stop(self, timeout: float = 10.0) -> None:
        self._stop_event.set()
        self.join(timeout=timeout)

    # ------------------------------------------------------------ 主循环

    def run(self) -> None:
        self.state["started_at"] = datetime.now(timezone.utc).isoformat()
        self._wconn = db.connect(self.settings.db_path)
        while not self._stop_event.is_set():
            consumer: Consumer | None = None
            try:
                consumer = self._make_consumer()
                self._consume_loop(consumer)
            except KafkaException:
                log.exception("Kafka 异常，3 秒后重连")
                self.state["last_error"] = "kafka reconnect"
                time.sleep(3.0)
            except Exception:  # noqa: BLE001
                log.exception("消费循环异常，3 秒后重连")
                time.sleep(3.0)
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:  # noqa: BLE001
                        pass
        try:
            self._wconn.close()
        except Exception:  # noqa: BLE001
            pass

    def _make_consumer(self) -> Consumer:
        consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap,
                "group.id": self.settings.group_id,
                "enable.auto.commit": False,   # 手动确认：先落库后提交
                "auto.offset.reset": "earliest",
                "session.timeout.ms": 45000,
            }
        )
        consumer.subscribe(self.settings.topics)
        log.info("已订阅主题 %s (group=%s)", self.settings.topics, self.settings.group_id)
        return consumer

    def _consume_loop(self, consumer: Consumer) -> None:
        # 重启恢复：落库时已收齐但崩溃前来不及应用的事务，先补上
        recovered = db.apply_pending(self._wconn)
        if recovered:
            log.info("重启恢复：整笔应用 %s", recovered)
            self.state["applied_txs"].extend(recovered)
        while not self._stop_event.is_set():
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka 错误: %s", msg.error())
                continue
            self._handle(consumer, msg)

    # ------------------------------------------------------------ 单条处理

    def _handle(self, consumer: Consumer, msg) -> None:
        while True:  # 落库失败：不确认 offset，重试直到成功
            try:
                self._persist(msg)
                applied = db.apply_pending(self._wconn)
                if applied:
                    log.info("整笔入仓: %s", applied)
                    self.state["applied_txs"].extend(applied)
                    self.state["applied_txs"] = self.state["applied_txs"][-50:]
                break
            except Exception as exc:  # noqa: BLE001
                self.state["last_error"] = f"{type(exc).__name__}: {exc}"
                log.exception(
                    "落库失败，offset 不确认，1 秒后重试: %s[%d]@%d",
                    msg.topic(), msg.partition(), msg.offset(),
                )
                time.sleep(1.0)

        self.state["persisted_messages"] += 1

        # 崩溃演练：落库后、确认前直接退出进程（验证重投去重）
        n = self.settings.crash_after_persist
        if n and self.state["persisted_messages"] >= n:
            log.error("INGEST_CRASH_AFTER_PERSIST=%d 触发：落库后未确认即退出", n)
            os._exit(1)

        consumer.commit(msg, asynchronous=False)
        key = f"{msg.topic()}[{msg.partition()}]"
        self.state["consumed_offsets"][key] = msg.offset() + 1
        self.state["committed_offsets"][key] = msg.offset() + 1

    def _persist(self, msg) -> None:
        topic = msg.topic()
        raw = msg.value()
        if raw is None:
            log.info("跳过 tombstone: %s[%d]@%d", topic, msg.partition(), msg.offset())
            return
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            db.record_dead_letter(
                self._wconn, topic=topic, partition=msg.partition(), offset=msg.offset(),
                raw=raw.decode("utf-8", "replace"), reason=f"JSON 解析失败: {exc}",
            )
            return
        if topic == self.settings.topic_tx:
            self._persist_tx(msg, value)
        elif topic in self.settings.data_topics:
            self._persist_data(msg, value)
        else:
            log.warning("未知主题 %s，忽略", topic)

    def _persist_tx(self, msg, value: dict) -> None:
        tx_id = value.get("id")
        status = value.get("status")
        if not tx_id or status not in ("BEGIN", "END"):
            db.record_dead_letter(
                self._wconn, topic=msg.topic(), partition=msg.partition(),
                offset=msg.offset(), raw=json.dumps(value, ensure_ascii=False),
                reason=f"事务元数据格式异常: {value!r}",
            )
            return
        result = db.record_tx_event(
            self._wconn,
            topic=msg.topic(), partition=msg.partition(), offset=msg.offset(),
            tx_id=tx_id, status=status, event_count=value.get("event_count"),
        )
        log.info("事务元数据 %s tx=%s -> %s", status, tx_id, result)

    def _persist_data(self, msg, value: dict) -> None:
        table = self.settings.data_topics[msg.topic()]
        op = value.get("op", "c")
        tx_info = value.get("transaction") or {}
        tx_id = tx_info.get("id")
        total_order = tx_info.get("total_order")
        immediate = False
        if tx_id is None:
            # 降级：无事务元数据 -> 作为单事件事务立即入队
            tx_id = f"auto:{msg.topic()}:{msg.partition()}:{msg.offset()}"
            total_order = 0
            immediate = True
            log.warning("消息无事务元数据，按单事件事务处理: %s", tx_id)
        payload = value.get("before") if op == "d" else value.get("after")
        if payload is None:
            db.record_dead_letter(
                self._wconn, topic=msg.topic(), partition=msg.partition(),
                offset=msg.offset(), raw=json.dumps(value, ensure_ascii=False),
                reason=f"空 payload (op={op})",
            )
            return
        result = db.buffer_data_event(
            self._wconn,
            topic=msg.topic(), partition=msg.partition(), offset=msg.offset(),
            tx_id=tx_id, total_order=int(total_order or 0), table_name=table, op=op,
            payload=json.dumps(payload, ensure_ascii=False), immediate=immediate,
        )
        log.info(
            "数据 %s.%s op=%s tx=%s order=%s -> %s",
            table, payload.get("report_no"), op, tx_id, total_order, result,
        )
