"""运行配置（环境变量驱动）。"""
from __future__ import annotations

import os


class Settings:
    def __init__(self) -> None:
        self.kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        self.group_id: str = os.getenv("KAFKA_GROUP_ID", "lims-dwh-ingestor")
        self.db_path: str = os.getenv("DB_PATH", "./data/lims.db")

        self.topic_audit: str = os.getenv("TOPIC_AUDIT", "lims.report_audit")
        self.topic_values: str = os.getenv("TOPIC_VALUES", "lims.test_values")
        self.topic_tx: str = os.getenv("TOPIC_TX", "lims.transaction")

        # 崩溃演练：落库 N 条后、确认 offset 前直接退出进程（0=关闭）
        self.crash_after_persist: int = int(os.getenv("INGEST_CRASH_AFTER_PERSIST", "0") or "0")

        # 数据主题 -> 仓表名
        self.data_topics = {
            self.topic_audit: "report_audit",
            self.topic_values: "test_values",
        }
        self.topics = [self.topic_audit, self.topic_values, self.topic_tx]
