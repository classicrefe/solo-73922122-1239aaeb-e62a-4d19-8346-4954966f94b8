"""唯一索引构建编排：单轮约束、后台构建、取消、重启接续、失败残留安全清理。

状态机（index_build_rounds.status）：
    running ──成功且索引有效──▶ complete
    running ──重复键等失败───▶ failed（先安全清理无效索引，原表数据不动）
    running ──受理取消────────▶ cancel_requested ──原 SQL 确认停止且索引无效──▶ cancelled
    complete 之后再取消/发起：直接报完成，不动索引。

取消的后端归属核对（防 PID 复用误停补录）：
    取消信号只发给“确认仍在执行本轮 CREATE UNIQUE INDEX CONCURRENTLY”的连接，
    核对三重身份：backend_pid 一致、连接代际 backend_start 一致、当前语句文本
    确为本索引的 CREATE 语句。无法确认时仅受理并保持 cancel_requested 待核实，
    不向任何连接发信号，也不凭进程号消失/变化报取消。
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime, timezone

import psycopg

from . import db
from .config import settings

log = logging.getLogger("index_builder")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _classify_error(exc: Exception) -> tuple[str, str]:
    """把 PostgreSQL 错误归类，重复键单独识别。"""
    if isinstance(exc, psycopg.errors.DuplicateTable):  # 索引名已存在（异常情况）
        return "other", str(exc).strip()
    msg = str(exc)
    # CONCURRENTLY 构建遇到重复键时 PG 报 could not create unique index ...
    if isinstance(exc, psycopg.errors.UniqueViolation) or "could not create unique index" in msg:
        return "duplicate_key", msg.strip()
    if isinstance(exc, psycopg.errors.LockNotAvailable) or "lock timeout" in msg.lower():
        return "lock_timeout", msg.strip()
    if isinstance(exc, psycopg.errors.QueryCanceled):
        return "cancelled", msg.strip()
    return "other", msg.strip()


class IndexBuildService:
    """进程内单例。线程模型：最多一个构建线程；重启后以 watcher 线程接续。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()          # 保护 _thread/_watching/_signalled
        self._thread: threading.Thread | None = None
        self._watching_round_id: str | None = None
        # 已发取消信号的连接身份：round_id -> (pid, backend_start)，
        # 防止 watcher 与请求线程对同一代后端重复发信号。
        self._signalled: dict[str, tuple[int, str]] = {}

    # --------------------------- 工具方法 -------------------------------

    def _safe_drop_invalid_index(self, index_name: str) -> bool:
        """只清理“无效”的残留索引；有效唯一索引绝不删除。返回是否已不存在。

        DROP INDEX CONCURRENTLY 不能在事务块中执行，需 autocommit 连接。
        若被长事务持有的表锁阻塞，不无限挂起：设置短 lock_timeout 并重试，
        直到删除成功或确认索引已不存在。
        """
        with db.pool().connection() as conn:
            meta = db.get_index_status(conn, index_name)
            if meta is None:
                return True
            if meta["is_valid"] and meta["is_ready"]:
                # 有效索引：拒绝清理。
                log.warning("skip dropping VALID index %s", index_name)
                return False
        conn = psycopg.connect(settings.database_url, autocommit=True)
        try:
            conn.execute("SET lock_timeout = '2000ms'")
            deadline = time.monotonic() + 60.0
            while time.monotonic() < deadline:
                try:
                    log.info("dropping invalid leftover index %s", index_name)
                    conn.execute(f'DROP INDEX CONCURRENTLY IF EXISTS "{index_name}"')
                    return True
                except psycopg.errors.LockNotAvailable:
                    # 表被长事务占用：等待后重试，期间不影响补录。
                    log.info("drop of %s blocked by long tx; retrying", index_name)
                    time.sleep(1.0)
            log.error("could not drop invalid index %s within retry window", index_name)
            return False
        finally:
            conn.close()

    def _mark(self, round_id: str, **fields: object) -> None:
        if not fields:
            return
        fields["updated_at"] = _now()
        assignments = ", ".join(f"{k} = %s" for k in fields)
        params = list(fields.values()) + [round_id]
        with db.pool().connection() as conn:
            conn.execute(
                f"UPDATE index_build_rounds SET {assignments} WHERE round_id = %s",
                params,
            )
            conn.commit()

    def _public_round(self, row: dict | None) -> dict | None:
        if row is None:
            return None
        backend_start = row.get("backend_start")
        return {
            "round_id": row["round_id"],
            "status": row["status"],
            "backend_pid": row["backend_pid"],
            "backend_start": backend_start.isoformat() if backend_start else None,
            "index_name": row["index_name"],
            "started_at": row["started_at"].isoformat(),
            "updated_at": row["updated_at"].isoformat(),
            "failure_type": row.get("failure_type"),
            "error": row.get("error"),
            "restarted": row.get("restarted", False),
        }

    # --------------------------- 构建线程 -------------------------------

    def _run_build(self, round_id: str) -> None:
        """在独立线程、独立 autocommit 连接中执行 CREATE INDEX CONCURRENTLY。"""
        conn = psycopg.connect(settings.database_url, autocommit=True)
        index_name = settings.index_name
        try:
            if settings.lock_timeout_ms and settings.lock_timeout_ms > 0:
                conn.execute(f"SET lock_timeout = '{int(settings.lock_timeout_ms)}ms'")
            # 记录后端 PID 与连接代际（backend_start），供取消与对账精确核对归属：
            # PID 可能被后续连接复用，(pid, backend_start) 才唯一标识这一代构建连接。
            pid, backend_start = conn.execute(
                "SELECT pid, backend_start FROM pg_stat_activity "
                "WHERE pid = pg_backend_pid()"
            ).fetchone()
            # 双保险：无论轮次是否已被标记 cancel_requested 都登记后端身份，
            # 否则 watcher 可能因 backend_pid 为空而无法确认后端状态。
            with db.pool().connection() as meta_conn:
                meta_conn.execute(
                    "UPDATE index_build_rounds "
                    "SET backend_pid = %s, backend_start = %s, updated_at = %s "
                    "WHERE round_id = %s",
                    (pid, backend_start, _now(), round_id),
                )
                meta_conn.commit()
            log.info("round %s: CREATE UNIQUE INDEX CONCURRENTLY on backend pid=%s",
                     round_id, pid)
            conn.execute(
                f'CREATE UNIQUE INDEX CONCURRENTLY "{index_name}" '
                f'ON {settings.table_name} (lab_no, report_no)'
            )
            # 语句返回后以系统目录为准判定，绝不凭“没报错”就报成功。
            with db.pool().connection() as chk:
                if db.index_is_valid_unique(chk, index_name):
                    self._mark(round_id, status="complete", error=None,
                               failure_type=None)
                    log.info("round %s complete", round_id)
                else:
                    raise RuntimeError("CREATE INDEX returned but index is not valid")
        except Exception as exc:  # noqa: BLE001 - 需要全量捕获并落库
            ftype, message = _classify_error(exc)
            log.warning("round %s failed (%s): %s", round_id, ftype, message[:200])
            # 取消信号导致的 QueryCanceled（或受限环境下连接被关闭）：交给 watcher
            # 根据轮次状态裁决。为消除“SQL 先中止、cancel_requested 后落库”的竞态，
            # 在短窗口内等待取消状态登记。
            with db.pool().connection() as chk:
                round_row = db.get_round_by_id(chk, round_id)
            if round_row and round_row["status"] == "running":
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline:
                    time.sleep(0.1)
                    with db.pool().connection() as chk:
                        round_row = db.get_round_by_id(chk, round_id)
                    if round_row and round_row["status"] != "running":
                        break
            with db.pool().connection() as chk:
                round_row = db.get_round_by_id(chk, round_id)
                index_valid = db.index_is_valid_unique(chk, index_name)
            if round_row and round_row["status"] == "cancel_requested":
                return  # watcher 会完成 cancelled/complete 裁决与清理
            if index_valid:  # 极端竞态下索引已有效：报完成
                self._mark(round_id, status="complete", error=None, failure_type=None)
                return
            self._safe_drop_invalid_index(index_name)
            self._mark(round_id, status="failed", failure_type=ftype, error=message)
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            with self._lock:
                if self._thread is threading.current_thread():
                    self._thread = None

    # --------------------------- 取消信号归属核对 ------------------------

    def _confirmed_build_backend(self, conn, round_row: dict) -> dict | None:
        """确认记录在案的后端“仍是本轮 CREATE 语句的执行者”，否则返回 None。

        三重核对：PID 存在、连接代际（backend_start）一致、当前活跃执行本索引的
        CREATE UNIQUE INDEX CONCURRENTLY。任一不符都说明该 PID 已不属于本轮构建
        （旧连接断开、PID 被补录等新连接复用、语句不符），绝不可对其发取消信号。
        """
        pid = round_row["backend_pid"]
        if not pid:
            return None
        st = db.get_backend_state(conn, pid)
        if st is None:
            return None  # 后端已退出
        recorded_start = round_row.get("backend_start")
        if recorded_start is not None and st["backend_start"] != recorded_start:
            return None  # 连接已换代：该 PID 现在属于另一条连接
        if st.get("state") != "active":
            return None
        if db.query_matches_create_index(st.get("query"), settings.index_name) is not True:
            return None  # 语句不符（如补录 INSERT）或不可见：不确认
        return st

    def _signal_cancel_if_confirmed(self, round_row: dict) -> str:
        """归属核对通过才向构建后端发取消信号。返回 sent/already_sent/pending_verification。

        无法确认归属时不发任何信号：轮次保持 cancel_requested 待核实，
        由 watcher 依据“原 SQL 是否确认停止”继续裁决，绝不误停补录连接。
        说明：受限环境若禁止进程信号，pg_cancel_backend 会返回 false；此时可设置
        BUILD_LOCK_TIMEOUT_MS，让被阻塞的构建在锁等待超时后自行中止，watcher 仍
        依据“后端 SQL 是否真正停止”来裁决，不会误报取消。
        """
        rid = str(round_row["round_id"])
        if not round_row["backend_pid"]:
            log.info("round %s: 构建后端尚未登记，取消保持待核实", rid)
            return "pending_verification"
        with db.pool().connection() as conn:
            st = self._confirmed_build_backend(conn, round_row)
        if st is None:
            log.warning(
                "round %s: 记录的 backend_pid=%s 未通过归属核对（连接可能已断开/换代），"
                "不发取消信号，保持待核实", rid, round_row["backend_pid"])
            return "pending_verification"
        identity = (st["pid"], str(st["backend_start"]))
        with self._lock:
            if self._signalled.get(rid) == identity:
                return "already_sent"
            self._signalled[rid] = identity
        accepted = self._pg_cancel_backend(st["pid"])
        log.info("round %s: 已向确认归属的构建后端发取消信号 pid=%s backend_start=%s accepted=%s",
                 rid, st["pid"], st["backend_start"], accepted)
        return "sent"

    # --------------------------- 取消轮询 -------------------------------

    def _watch_cancel(self, round_id: str, pid: int | None) -> None:
        """轮询直到后端 SQL 真正停止，再裁决 cancelled / complete，并清理残留。"""
        try:
            self._watch_cancel_loop(round_id, pid)
        except Exception:  # noqa: BLE001 - 后台守护线程兜底，避免异常在解释器退出时外溢
            log.exception("watcher for round %s exited with error", round_id)
        finally:
            with self._lock:
                if self._watching_round_id == round_id:
                    self._watching_round_id = None
                self._signalled.pop(round_id, None)

    def _watch_cancel_loop(self, round_id: str, pid: int | None) -> None:
        index_name = settings.index_name
        no_identity_since: float | None = None  # 首次发现“从未登记过后端”的时刻
        while True:
            with db.pool().connection() as conn:
                round_row = db.get_round_by_id(conn, round_id)
                if round_row is None:
                    return
                if round_row["status"] not in ("running", "cancel_requested"):
                    return  # 构建线程已自行终局（complete/failed）
                # 以“语句身份 + 连接代际”确认原 CREATE 语句是否仍在执行，
                # 不凭进程号消失/变化下结论。
                state = db.create_index_backend_state(
                    conn, index_name,
                    round_row["backend_pid"], round_row.get("backend_start"),
                )
                index_valid = db.index_is_valid_unique(conn, index_name)
            if index_valid:
                # 取消未生效或生效前索引已建成：索引有效即报完成。
                self._mark(round_id, status="complete", error=None,
                           failure_type=None)
                log.info("round %s: index valid before cancel took effect -> complete",
                         round_id)
                return
            if state == "running":
                no_identity_since = None
                # 已受理取消且确认归属后补发信号（覆盖“取消早于 PID 登记”等时序）；
                # 同一代后端只发一次，换代后身份变化才允许再发。
                if round_row["status"] == "cancel_requested":
                    self._signal_cancel_if_confirmed(round_row)
            elif state == "unknown":
                # 无法确认（如系统视图权限受限）：保持待核实，绝不凭猜测裁决。
                no_identity_since = None
                log.info("round %s: 后端状态无法确认，保持 cancel_requested 待核实", round_id)
            else:  # stopped：原 CREATE 语句确认已不在任何后端执行
                if round_row["backend_pid"] is None:
                    # 从未登记过后端：可能是取消早于构建线程登记。宽限期内保持
                    # 待核实，避免凭“无进程号”误报取消。
                    if no_identity_since is None:
                        no_identity_since = time.monotonic()
                    if time.monotonic() - no_identity_since < settings.cancel_verify_grace_s:
                        threading.Event().wait(settings.reconcile_interval_s)
                        continue
                self._finalize_stopped_round(round_id, round_row["status"])
                return
            threading.Event().wait(settings.reconcile_interval_s)

    def _finalize_stopped_round(self, round_id: str, status: str) -> None:
        """原 SQL 已确认停止且索引无效：按轮次状态裁决并安全清理残留索引。"""
        self._safe_drop_invalid_index(settings.index_name)
        if status == "cancel_requested":
            self._mark(round_id, status="cancelled",
                       failure_type="cancelled",
                       error="cancel confirmed: backend CREATE INDEX stopped; "
                             "invalid index cleaned up")
            log.info("round %s cancelled and cleaned up", round_id)
        else:
            # 重启接续的 running 轮次：构建中断且未建成，如实报失败（去重后可重试）。
            self._mark(round_id, status="failed",
                       failure_type="other",
                       error="backend CREATE INDEX stopped without a valid index; "
                             "invalid index cleaned up")
            log.info("round %s: backend stopped without valid index -> failed", round_id)

    # --------------------------- 重启对账 -------------------------------

    def reconcile_on_startup(self) -> None:
        """API 启动时核对数据库中的活跃轮次与索引真实状态，接续跟踪。

        不重跑任何 CREATE INDEX；无效索引不会被报成成功。
        """
        # 仅在轮次表缺失时才执行 DDL（避免在存在未提交写事务时因 DDL 锁阻塞对账）。
        with db.pool().connection() as conn:
            exists = conn.execute(
                "SELECT to_regclass('public.index_build_rounds') IS NOT NULL AS ok"
            ).fetchone()["ok"]
        if not exists:
            db.ensure_schema()
        with db.pool().connection() as conn:
            active = db.get_active_round(conn)
            index_valid = db.index_is_valid_unique(conn, settings.index_name)

        if active is None:
            # 无活跃轮次。若存在无效残留索引（例如进程在 failed 落库前崩溃），安全清理。
            if not index_valid:
                with db.pool().connection() as conn:
                    meta = db.get_index_status(conn, settings.index_name)
                if meta is not None:
                    log.warning("found invalid leftover index at startup; cleaning")
                    self._safe_drop_invalid_index(settings.index_name)
            return

        round_id = str(active["round_id"])
        pid = active["backend_pid"]
        if index_valid:
            self._mark(round_id, status="complete", error=None, failure_type=None)
            log.info("startup: round %s index is valid -> complete", round_id)
            return

        # 索引尚无效：按“语句身份 + 连接代际”确认构建 SQL 是否还在跑。
        with db.pool().connection() as conn:
            state = db.create_index_backend_state(
                conn, settings.index_name, pid, active.get("backend_start"))
        if state in ("running", "unknown"):
            # running：上一进程的构建仍在 PG 内运行，只接续跟踪，绝不重跑；
            # unknown：无法确认（如视图权限受限），保持原状态待核实，同样只挂
            # watcher 持续核对，不凭进程号变化误判终局。
            self._mark(round_id, restarted=True)
            log.info("startup: round %s backend state=%s (pid=%s), attaching watcher",
                     round_id, state, pid)
            self._start_watcher(round_id, pid)
        else:
            # 后端已不存在且索引无效：构建已中断。先立即落终局状态（不被长事务阻塞），
            # 再安全清理无效索引。清理带 lock_timeout 重试：无长事务时立即完成，
            # 被长事务占用时在重试窗口内等待，且绝不删除有效索引。
            with db.pool().connection() as conn:
                row = db.get_round_by_id(conn, round_id)
            if row and row["status"] == "cancel_requested":
                self._mark(round_id, status="cancelled",
                           failure_type="cancelled",
                           error="confirmed after API restart: backend stopped; "
                                 "invalid index cleanup scheduled")
            else:
                self._mark(round_id, status="failed",
                           failure_type="other",
                           error=(row.get("error") or
                                  "build interrupted by API shutdown; invalid index cleanup scheduled"))
            log.info("startup: round %s was interrupted; finalized as %s",
                     round_id, "cancelled" if row and row["status"] == "cancel_requested"
                     else "failed")
            self._safe_drop_invalid_index(settings.index_name)

    def _start_watcher(self, round_id: str, pid: int | None) -> None:
        with self._lock:
            if self._watching_round_id == round_id:
                return
            t = threading.Thread(
                target=self._watch_cancel, args=(round_id, pid),
                name=f"index-watcher-{round_id[:8]}", daemon=True,
            )
            self._watching_round_id = round_id
            t.start()

    # --------------------------- 对外操作 -------------------------------

    def start_build(self) -> tuple[dict, int]:
        """发起构建。返回 (轮次视图, HTTP 状态码)。重复发起复用同一轮。"""
        with self._lock:
            with db.pool().connection() as conn:
                # 1) 索引已有效：直接报完成。
                if db.index_is_valid_unique(conn, settings.index_name):
                    row = db.latest_round(conn)
                    base = self._public_round(row) or {
                        "round_id": None,
                        "status": "complete",
                        "index_name": settings.index_name,
                    }
                    base["status"] = "complete"
                    base["index_valid"] = True
                    return base, 200

                # 2) 已有活跃轮次：复用，不新开一轮（单轮约束）。
                active = db.get_active_round(conn)
                if active is not None:
                    view = self._public_round(active)
                    view["reused"] = True
                    return view, 200

                # 3) 历史失败遗留的无效索引先安全清理（有效索引上面已拦截，不会删）。
                meta = db.get_index_status(conn, settings.index_name)
            if meta is not None and not (meta["is_valid"] and meta["is_ready"]):
                self._safe_drop_invalid_index(settings.index_name)

            # 4) 新建轮次；部分唯一索引在数据库层兜底单轮约束。
            round_id = uuid.uuid4()
            try:
                with db.pool().connection() as conn:
                    conn.execute(
                        "INSERT INTO index_build_rounds (round_id, status, index_name) "
                        "VALUES (%s, 'running', %s)",
                        (round_id, settings.index_name),
                    )
                    conn.commit()
            except psycopg.errors.UniqueViolation:
                # 并发下别的实例抢先插入活跃轮次：复用它。
                with db.pool().connection() as conn:
                    conn.rollback()
                    active = db.get_active_round(conn)
                view = self._public_round(active)
                view["reused"] = True
                return view, 200

            t = threading.Thread(
                target=self._run_build, args=(str(round_id),),
                name=f"index-build-{str(round_id)[:8]}", daemon=True,
            )
            self._thread = t
            t.start()

        with db.pool().connection() as conn:
            row = db.get_round_by_id(conn, str(round_id))
        return self._public_round(row), 202

    def request_cancel(self, round_id: str | None = None) -> tuple[dict, int]:
        """受理取消：先落 cancel_requested 并返回 202，终局由 watcher 裁决。

        取消信号只发给“确认仍在执行本轮 CREATE UNIQUE INDEX CONCURRENTLY”的连接；
        无法确认（后端未登记、已断开、PID 被补录等连接复用、语句不符）时，
        仅保持 cancel_requested 待核实，不向任何连接发信号。
        """
        with db.pool().connection() as conn:
            if db.index_is_valid_unique(conn, settings.index_name):
                row = db.latest_round(conn)
                view = self._public_round(row) or {"status": "complete"}
                view["status"] = "complete"
                view["index_valid"] = True
                return view, 200

            if round_id:
                row = db.get_round_by_id(conn, round_id)
            else:
                row = db.get_active_round(conn)
            if row is None:
                return {"detail": "no active build round to cancel"}, 404

            if row["status"] in ("complete", "failed", "cancelled"):
                return self._public_round(row), 200

            if row["status"] == "running":
                conn.execute(
                    "UPDATE index_build_rounds SET status = 'cancel_requested', "
                    "updated_at = %s WHERE round_id = %s",
                    (_now(), row["round_id"]),
                )
                conn.commit()
                row = db.get_round_by_id(conn, row["round_id"])

        # 归属核对：仅确认连接仍执行本轮 CREATE 语句时才发取消信号；
        # 否则保持待核实，由 watcher 继续确认，绝不误停补录等无关连接。
        signal = self._signal_cancel_if_confirmed(row)
        self._start_watcher(str(row["round_id"]), row["backend_pid"])
        view = self._public_round(row)
        view["cancel_accepted"] = True
        view["cancel_signal"] = signal
        return view, 202

    def _pg_cancel_backend(self, pid: int) -> bool:
        with db.pool().connection() as conn:
            row = conn.execute(
                "SELECT pg_cancel_backend(%s) AS ok", (pid,)
            ).fetchone()
            conn.commit()
            return bool(row["ok"])

    def get_status(self, round_id: str | None = None) -> dict:
        with db.pool().connection() as conn:
            index_meta = db.get_index_status(conn, settings.index_name)
            if round_id:
                row = db.get_round_by_id(conn, round_id)
            else:
                row = db.get_active_round(conn) or db.latest_round(conn)
            backend = db.get_backend_state(conn, row["backend_pid"]) if row else None
        view = self._public_round(row) or {"status": "idle", "rounds": []}
        view["index"] = {
            "name": settings.index_name,
            "exists": index_meta is not None,
            "is_unique": index_meta["is_unique"] if index_meta else False,
            "is_valid": index_meta["is_valid"] if index_meta else False,
            "is_ready": index_meta["is_ready"] if index_meta else False,
        }
        if backend:
            view["backend"] = {
                "pid": backend["pid"],
                "backend_start": backend.get("backend_start"),
                "state": backend["state"],
                "wait_event_type": backend["wait_event_type"],
                "wait_event": backend["wait_event"],
                "query": backend["query"],
            }
        return view


service = IndexBuildService()
