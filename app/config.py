"""运行配置：全部通过环境变量注入，容器内由 docker-compose 提供默认值。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@db:5432/report_warehouse",
    )
    # CREATE INDEX CONCURRENTLY 等待锁/长事务的上限（毫秒），0 表示不限制。
    lock_timeout_ms: int = int(os.getenv("BUILD_LOCK_TIMEOUT_MS", "0"))
    # 后台对账轮询间隔（秒）：确认取消、重启接续。
    reconcile_interval_s: float = float(os.getenv("RECONCILE_INTERVAL_S", "1.5"))
    # 取消核实宽限（秒）：轮次从未登记过后端时，watcher 判定“SQL 已停止”前的等待
    # 窗口，避免取消早于构建线程登记 backend_pid 时凭“无进程号”误报取消。
    cancel_verify_grace_s: float = float(os.getenv("CANCEL_VERIFY_GRACE_S", "10"))
    index_name: str = os.getenv("UNIQUE_INDEX_NAME", "uq_reports_lab_no_report_no")
    table_name: str = "reports"


settings = Settings()
