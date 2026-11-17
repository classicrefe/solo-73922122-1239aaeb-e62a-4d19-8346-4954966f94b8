"""PostgreSQL 访问层：连接池 + 索引/后端/轮次状态查询。"""
from __future__ import annotations

import pathlib
import re
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import settings

_pool: ConnectionPool | None = None

SCHEMA_FILE = pathlib.Path(__file__).resolve().parent.parent / "sql" / "01_schema.sql"


def init_pool() -> None:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            settings.database_url,
            min_size=2,
            max_size=10,
            kwargs={"row_factory": dict_row},
            open=True,
        )


def pool() -> ConnectionPool:
    if _pool is None:
        init_pool()
    assert _pool is not None
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def ensure_schema() -> None:
    """幂等建表（容器首次启动由 initdb 执行，这里保证本地/非容器环境同样可用）。"""
    ddl = SCHEMA_FILE.read_text(encoding="utf-8")
    with pool().connection() as conn:
        conn.execute(ddl)
        conn.commit()


# ----------------------------- 索引状态 ---------------------------------

def get_index_status(conn: psycopg.Connection, index_name: str) -> dict[str, Any] | None:
    """返回索引元数据；不存在返回 None。

    valid=True 且 ready=True 才是真正可用的唯一索引。
    """
    row = conn.execute(
        """
        SELECT i.indexrelid::regclass::text              AS index_name,
               i.indisunique                               AS is_unique,
               i.indisvalid                                AS is_valid,
               i.indisready                                AS is_ready,
               EXISTS (SELECT 1 FROM pg_constraint c
                        WHERE c.conindid = i.indexrelid
                          AND c.contype = 'u')             AS has_constraint
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = %s
        """,
        (index_name,),
    ).fetchone()
    return dict(row) if row else None


def index_is_valid_unique(conn: psycopg.Connection, index_name: str) -> bool:
    meta = get_index_status(conn, index_name)
    return bool(meta and meta["is_unique"] and meta["is_valid"] and meta["is_ready"])


# ----------------------------- 后端进程 ---------------------------------

# pg_stat_activity 中语句文本被权限遮蔽时显示的占位内容
_MASKED_QUERY_MARK = "insufficient privilege"


def get_backend_state(conn: psycopg.Connection, pid: int | None) -> dict[str, Any] | None:
    if not pid:
        return None
    row = conn.execute(
        """
        SELECT pid, state, wait_event_type, wait_event,
               left(query, 400) AS query, query_start, backend_start
        FROM pg_stat_activity
        WHERE pid = %s
        """,
        (pid,),
    ).fetchone()
    return dict(row) if row else None


# 本轮构建语句的固定形态：CREATE UNIQUE INDEX CONCURRENTLY "<index_name>" ON ...
_CREATE_UNIQUE_INDEX_RE = re.compile(
    r"\bCREATE\b\s+\bUNIQUE\b\s+\bINDEX\b\s+\bCONCURRENTLY\b"
)


def query_matches_create_index(query: str | None, index_name: str) -> bool | None:
    """判断语句是否为本索引的 CREATE UNIQUE INDEX CONCURRENTLY。

    返回 True（确认是）/ False（确认不是）/ None（语句被权限遮蔽，无法确认）。
    补录 INSERT、其它索引的 DDL 等均确认不是，可用于取消信号的归属核对。
    """
    if not query:
        return False
    if _MASKED_QUERY_MARK in query.lower():
        return None
    normalized = " ".join(query.upper().split())
    if not _CREATE_UNIQUE_INDEX_RE.search(normalized):
        return False
    return index_name.upper() in normalized


def create_index_backend_state(
    conn: psycopg.Connection,
    index_name: str,
    pid: int | None = None,
    backend_start: Any = None,
) -> str:
    """确认本索引的 CREATE 语句是否仍在执行："running" / "stopped" / "unknown"。

    依据“语句身份 + 连接代际”判定，而非单纯进程号：
    * 记录在案的 (pid, backend_start) 代际相符、活跃且正在执行本语句 -> running；
    * 代际不符（PID 已被新连接复用）说明原构建后端已退出，该 PID 不算在跑；
    * 再全库扫描是否有任何后端活跃执行本语句（覆盖 PID 未登记等时序）；
    * 语句文本被权限遮蔽而无法判定时返回 unknown——不可据此裁决终局。
    """
    unknown = False
    if pid:
        st = get_backend_state(conn, pid)
        if st is not None:
            same_generation = backend_start is None or st["backend_start"] == backend_start
            if same_generation and st.get("state") == "active":
                match = query_matches_create_index(st.get("query"), index_name)
                if match is True:
                    return "running"
                if match is None:
                    unknown = True
            # 同代但已空闲/在跑别的语句：本语句已结束；代际不符：原后端已退出。
            # 两者都继续走全库扫描兜底。
    rows = conn.execute(
        """
        SELECT pid, left(query, 400) AS query
        FROM pg_stat_activity
        WHERE state = 'active' AND pid <> pg_backend_pid()
          AND datname = current_database()  -- 只认本库后端：他库同名索引构建不算
        """
    ).fetchall()
    for row in rows:
        match = query_matches_create_index(row["query"], index_name)
        if match is True:
            return "running"
        if match is None:
            unknown = True
    return "unknown" if unknown else "stopped"


# ----------------------------- 轮次表 -----------------------------------

def get_active_round(conn: psycopg.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT * FROM index_build_rounds
        WHERE status IN ('running', 'cancel_requested')
        ORDER BY id
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row else None


def get_round_by_id(conn: psycopg.Connection, round_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM index_build_rounds WHERE round_id = %s",
        (round_id,),
    ).fetchone()
    return dict(row) if row else None


def latest_round(conn: psycopg.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM index_build_rounds ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None
