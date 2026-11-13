"""LIMS 检验数仓入仓服务：FastAPI 入口（Swagger 见 /docs）。"""
from __future__ import annotations

import copy
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from . import db
from .config import Settings
from .consumer import Ingestor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

settings = Settings()
ingestor = Ingestor(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db(settings.db_path)
    ingestor.start()
    yield
    ingestor.stop(timeout=10)


app = FastAPI(
    title="LIMS 检验数仓入仓服务",
    version="1.0.0",
    description=(
        "消费 LIMS Debezium 变更流（报告审核表 / 检验值表 / 事务主题），"
        "按 event_count 收齐、事务内按 total_order、事务间按 END 顺序整笔入仓；"
        "查询只读同一已提交状态，不会把新审核状态配上旧检验值。"
    ),
    lifespan=lifespan,
)


class AuditOut(BaseModel):
    report_no: str
    audit_status: str
    auditor: str | None = None
    audit_time: str | None = None
    remark: str | None = None
    updated_tx: str | None = None


class TestValueOut(BaseModel):
    report_no: str
    item_code: str
    item_name: str | None = None
    result_value: str | None = None
    unit: str | None = None
    ref_range: str | None = None
    flag: str | None = None
    updated_tx: str | None = None


class ReportOut(BaseModel):
    report_no: str
    as_of_tx_seq: int
    audit: AuditOut | None
    test_values: list[TestValueOut]


@app.get("/", include_in_schema=False)
def index():
    return RedirectResponse("/docs")


@app.get(
    "/reports/{report_no}",
    response_model=ReportOut,
    tags=["查询"],
    summary="按报告号查询审核状态与全部检验值（同一已提交快照）",
)
def get_report(report_no: str) -> dict:
    report = db.read_report(settings.db_path, report_no)
    if report["audit"] is None and not report["test_values"]:
        raise HTTPException(status_code=404, detail=f"报告 {report_no} 不存在")
    return report


@app.get(
    "/ingest/status",
    tags=["运维"],
    summary="入仓进度：等待中/已应用事务、收件进度、消费位点",
)
def ingest_status() -> dict:
    return db.read_status(settings.db_path, consumer_state=copy.deepcopy(ingestor.state))


@app.get("/health", tags=["运维"], summary="健康检查")
def health() -> dict:
    alive = ingestor.is_alive()
    return {
        "status": "ok" if alive else "degraded",
        "consumer_alive": alive,
        "last_error": ingestor.state.get("last_error"),
    }
