"""关键功能自测：补录、并发构建、单轮复用、重复键失败与重试、取消、重启接续。"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from app import db as dbmod
from app.config import settings
from app.service import service
from conftest import poll_status


def _open_tx_conn():
    """未提交的写事务：CREATE INDEX CONCURRENTLY 等待其快照（Lock/virtualxid）。"""
    conn = psycopg.connect(settings.database_url)
    conn.execute("BEGIN")
    conn.execute(
        "INSERT INTO reports(lab_no, report_no, measured) "
        "VALUES ('TX-BLOCKER', 'X', 0)"
    )
    return conn


def test_backfill_crud_and_unique_enforcement(client):
    r = client.post("/reports", json={"lab_no": "L1", "report_no": "A", "measured": 1})
    assert r.status_code == 201
    rid = r.json()["id"]
    assert isinstance(rid, int)

    # 唯一索引未建时允许重复
    r = client.post("/reports", json={"lab_no": "L1", "report_no": "A", "measured": 2})
    assert r.status_code == 201

    client.post("/index/build")
    final = poll_status(client, "failed")  # 因有重复键而失败
    assert final["failure_type"] == "duplicate_key"

    # 原数据保留
    listed = client.get("/reports").json()
    assert listed["total"] == 2

    # 无效索引已清理
    with dbmod.pool().connection() as conn:
        assert dbmod.get_index_status(conn, settings.index_name) is None

    # 去重后重试成功
    with dbmod.pool().connection() as conn:
        conn.execute("""
            DELETE FROM reports r USING (
              SELECT id, row_number() OVER (PARTITION BY lab_no, report_no ORDER BY id) rn
              FROM reports) d WHERE r.id=d.id AND d.rn>1""")
        conn.commit()
    client.post("/index/build")
    poll_status(client, "complete")

    # 唯一索引生效：重复键 409，原数据不变
    dup = client.post("/reports", json={"lab_no": "L1", "report_no": "A", "measured": 3})
    assert dup.status_code == 409
    assert dup.json()["detail"]["code"] == "duplicate_report"
    assert client.get("/reports").json()["total"] == 1

    # 批量接口跳过重复、写入新行
    b = client.post("/reports/batch", json={"items": [
        {"lab_no": "L1", "report_no": "A", "measured": 3},
        {"lab_no": "L1", "report_no": "B", "measured": 4},
    ]}).json()
    assert b["inserted_count"] == 1 and b["skipped_count"] == 1


def test_single_active_round_is_reused(client):
    client.post("/reports", json={"lab_no": "L", "report_no": "1", "measured": 1})
    blocker = _open_tx_conn()
    try:
        first = client.post("/index/build")
        assert first.status_code == 202
        round_id = first.json()["round_id"]
        time.sleep(1)
        # 重复发起：复用同一轮
        second = client.post("/index/build")
        assert second.status_code == 200
        assert second.json()["reused"] is True
        assert second.json()["round_id"] == round_id

        with dbmod.pool().connection() as conn:
            n = conn.execute(
                "SELECT count(*) AS n FROM index_build_rounds WHERE status='running'"
            ).fetchone()["n"]
        assert n == 1
    finally:
        blocker.rollback()
        blocker.close()
    poll_status(client, "complete")


def test_backfill_not_blocked_during_build(client):
    for i in range(5):
        client.post("/reports", json={"lab_no": "L", "report_no": f"R{i}", "measured": i})
    blocker = _open_tx_conn()
    try:
        client.post("/index/build")
        time.sleep(2)
        # 构建期间补录立即成功
        r = client.post("/reports", json={"lab_no": "L2", "report_no": "BX", "measured": 99})
        assert r.status_code == 201
    finally:
        blocker.rollback()
        blocker.close()
    poll_status(client, "complete")


def _create_invalid_index_and_stopped_round(round_status: str):
    """构造“后端已停止、索引无效”的现场：直接跑一次必然失败的 CIC（重复键），
    再手工登记一轮。返回 round_id。无存活构建后端、无长事务，清理可立即完成。"""
    with dbmod.pool().connection() as conn:
        conn.execute(
            "INSERT INTO reports(lab_no,report_no,measured) VALUES "
            "('DUP','K',1),('DUP','K',2)"
        )
        conn.commit()
    # 独立 autocommit 连接执行 CIC：重复键 -> 语句失败并留下无效索引，后端随即结束。
    cic = psycopg.connect(settings.database_url, autocommit=True)
    try:
        with pytest.raises(psycopg.errors.UniqueViolation):
            cic.execute(
                f'CREATE UNIQUE INDEX CONCURRENTLY "{settings.index_name}" '
                "ON reports (lab_no, report_no)"
            )
    finally:
        cic.close()
    with dbmod.pool().connection() as conn:
        meta = dbmod.get_index_status(conn, settings.index_name)
        assert meta and meta["is_valid"] is False
        rid = conn.execute(
            "INSERT INTO index_build_rounds(round_id,status,index_name,backend_pid) "
            "VALUES (gen_random_uuid(),%s,%s,999999) RETURNING round_id",
            (round_status, settings.index_name),
        ).fetchone()["round_id"]
        conn.commit()
    return rid


def test_cancel_acceptance_then_backend_stop_finalizes_cancelled(client):
    """取消语义（两阶段）：

    1) 对存活构建发起取消：先受理为 cancel_requested 并返回 202，不提前报 cancelled；
    2) watcher 仅在“后端 CREATE INDEX 真正停止且索引无效”时才裁决 cancelled 并清理；
    3) 若后端最终把索引建成（取消未生效），则报 complete，且有效索引绝不被删除；
    4) 无效残留索引安全清理，不影响补录。

    本沙箱 seccomp 禁止一切跨进程信号，pg_cancel_backend 无法中止后端，
    因此“后端已停止”的现场用一次必然失败的 CIC 合成，裁决/清理路径与生产一致。
    """
    # 场景 A：存活构建的取消受理（信号在此环境不生效，构建随后应正常完成 -> complete）。
    for i in range(5):
        client.post("/reports", json={"lab_no": "L", "report_no": f"R{i}", "measured": i})
    blocker = _open_tx_conn()
    try:
        build = client.post("/index/build").json()
        deadline = time.time() + 10
        pid = None
        while time.time() < deadline:
            pid = client.get("/index/status").json().get("backend_pid")
            if pid:
                break
            time.sleep(0.2)
        assert pid
        ack = client.post("/index/cancel")
        assert ack.status_code == 202
        assert ack.json()["status"] == "cancel_requested"
        assert ack.json()["cancel_accepted"] is True
        assert client.get("/index/status").json()["status"] == "cancel_requested"
    finally:
        blocker.rollback()
        blocker.close()
    # 信号未生效，watcher 看到索引建成 -> complete，且有效索引保留。
    final = poll_status(client, "complete", timeout_s=30)
    assert final["index"]["is_valid"] is True
    with dbmod.pool().connection() as conn:
        assert dbmod.index_is_valid_unique(conn, settings.index_name)

    # 场景 B：合成“cancel_requested + 后端已停止 + 无效索引”，重启对账 -> cancelled + 清理。
    with dbmod.pool().connection() as conn:
        conn.execute(f'DROP INDEX {settings.index_name}')
        conn.execute("DELETE FROM index_build_rounds")
        conn.commit()
    rid = _create_invalid_index_and_stopped_round("cancel_requested")
    with service._lock:
        service._thread = None
        service._watching_round_id = None
    service.reconcile_on_startup()
    st = poll_status(client, "cancelled", timeout_s=30)
    assert st["round_id"] == str(rid)
    assert st["failure_type"] == "cancelled"
    # 无效索引由后台重试任务安全删除（轮询等待其完成）。
    deadline = time.time() + 30
    while time.time() < deadline:
        with dbmod.pool().connection() as conn:
            if dbmod.get_index_status(conn, settings.index_name) is None:
                break
        time.sleep(0.5)
    with dbmod.pool().connection() as conn:
        assert dbmod.get_index_status(conn, settings.index_name) is None

    # 清理后先去重，再重新发起并成功。
    with dbmod.pool().connection() as conn:
        conn.execute("""
            DELETE FROM reports r USING (
              SELECT id, row_number() OVER (PARTITION BY lab_no, report_no ORDER BY id) rn
              FROM reports) d WHERE r.id=d.id AND d.rn>1""")
        conn.commit()
    client.post("/index/build")
    poll_status(client, "complete", timeout_s=30)


def test_watcher_confirms_cancel_when_backend_disappears(client):
    """不经过重启：request_cancel 启动的 watcher 在确认后端消失后裁决 cancelled。"""
    rid = _create_invalid_index_and_stopped_round("running")
    resp = client.post("/index/cancel", params={"round_id": str(rid)})
    assert resp.status_code == 202
    st = poll_status(client, "cancelled", timeout_s=30)
    assert st["round_id"] == str(rid)
    with dbmod.pool().connection() as conn:
        assert dbmod.get_index_status(conn, settings.index_name) is None


def test_cancel_without_active_round_returns_404(client):
    r = client.post("/index/cancel")
    assert r.status_code == 404


def test_restart_reconcile_attaches_and_completes(client):
    for i in range(20):
        client.post("/reports", json={"lab_no": "L", "report_no": f"R{i}", "measured": i})
    blocker = _open_tx_conn()
    try:
        build = client.post("/index/build").json()
        round_id = build["round_id"]
        # 等待构建线程登记后端 PID（构建正被写事务阻塞）。
        deadline = time.time() + 10
        pid = None
        while time.time() < deadline:
            pid = client.get("/index/status").json().get("backend_pid")
            if pid:
                break
            time.sleep(0.3)
        assert pid

        # 模拟 API 重启：丢弃服务线程状态后执行启动对账（PG 内构建仍在跑）
        with service._lock:
            service._thread = None
            service._watching_round_id = None
        service.reconcile_on_startup()

        st = client.get("/index/status").json()
        assert st["round_id"] == round_id
        assert st["backend_pid"] == pid
        assert st["restarted"] is True
        assert st["status"] == "running"
    finally:
        blocker.rollback()
        blocker.close()

    final = poll_status(client, "complete", timeout_s=60)
    assert final["index"]["is_valid"] is True


def test_restart_after_interrupted_build_marks_failed_and_cleans(client):
    client.post("/reports", json={"lab_no": "L", "report_no": "R", "measured": 1})
    build = client.post("/index/build").json()
    poll_status(client, "complete")

    # 制造“无效索引 + 活跃轮次”的崩溃现场（模拟进程在构建中断后未及落库）
    with dbmod.pool().connection() as conn:
        conn.execute(f'DROP INDEX {settings.index_name}')
        conn.execute("""
            UPDATE index_build_rounds SET status='running', backend_pid=999999
            WHERE round_id=%s""", (build["round_id"],))
        conn.commit()
    with service._lock:
        service._thread = None
    service.reconcile_on_startup()

    st = client.get("/index/status").json()
    assert st["status"] == "failed"
    with dbmod.pool().connection() as conn:
        assert dbmod.get_index_status(conn, settings.index_name) is None


def test_start_build_when_index_already_valid_reports_complete(client):
    client.post("/reports", json={"lab_no": "L", "report_no": "R", "measured": 1})
    client.post("/index/build")
    poll_status(client, "complete")
    resp = client.post("/index/build")
    assert resp.status_code == 200
    assert resp.json()["status"] == "complete"
    assert resp.json()["index_valid"] is True


# --------------------- 取消的后端归属核对（连接换代交错） ---------------------

def test_query_matches_create_index():
    """语句身份识别：只有本索引的 CREATE UNIQUE INDEX CONCURRENTLY 才算匹配。"""
    idx = settings.index_name
    f = dbmod.query_matches_create_index
    assert f(f'CREATE UNIQUE INDEX CONCURRENTLY "{idx}" '
             "ON reports (lab_no, report_no)", idx) is True
    assert f(f"create  unique  index concurrently {idx} on reports(lab_no)", idx) is True
    assert f("INSERT INTO reports(lab_no,report_no,measured) VALUES ($1,$2,$3)", idx) is False
    assert f("CREATE INDEX CONCURRENTLY idx_other ON reports(lab_no)", idx) is False
    assert f("CREATE UNIQUE INDEX CONCURRENTLY other_name ON reports(lab_no)", idx) is False
    assert f("<insufficient privilege>", idx) is None
    assert f(None, idx) is False
    assert f("", idx) is False


def _make_invalid_index_leftover():
    """重复键导致 CIC 失败，留下无效索引（执行后端随即结束）。"""
    with dbmod.pool().connection() as conn:
        conn.execute(
            "INSERT INTO reports(lab_no,report_no,measured) VALUES "
            "('DUP','K',1),('DUP','K',2)"
        )
        conn.commit()
    cic = psycopg.connect(settings.database_url, autocommit=True)
    try:
        with pytest.raises(psycopg.errors.UniqueViolation):
            cic.execute(
                f'CREATE UNIQUE INDEX CONCURRENTLY "{settings.index_name}" '
                "ON reports (lab_no, report_no)"
            )
    finally:
        cic.close()
    with dbmod.pool().connection() as conn:
        meta = dbmod.get_index_status(conn, settings.index_name)
        assert meta and meta["is_valid"] is False


def _register_round_with_backend(status: str, pid: int, backend_start) -> str:
    """手工登记一轮活跃构建，其 backend_pid/backend_start 指向给定连接身份。"""
    with dbmod.pool().connection() as conn:
        rid = conn.execute(
            "INSERT INTO index_build_rounds(round_id,status,index_name,"
            "backend_pid,backend_start) "
            "VALUES (gen_random_uuid(),%s,%s,%s,%s) RETURNING round_id",
            (status, settings.index_name, pid, backend_start),
        ).fetchone()["round_id"]
        conn.commit()
    return str(rid)


def _spy_cancel_signal(monkeypatch) -> list[int]:
    """记录 service 实际发出 pg_cancel_backend 的 PID 列表。"""
    sent: list[int] = []
    orig = service._pg_cancel_backend
    monkeypatch.setattr(service, "_pg_cancel_backend",
                        lambda p: (sent.append(p), orig(p))[1])
    return sent


def test_cancel_does_not_signal_reused_backfill_pid(client, monkeypatch):
    """连接换代交错（同 PID 复用）：旧构建连接已断开，记录的 backend_pid 连同
    backend_start 当前属于一条补录连接。取消不得向该连接发信号、不得误停补录；
    原 SQL 确认停止且索引无效后才裁决 cancelled 并清理。"""
    _make_invalid_index_leftover()
    backfill = psycopg.connect(settings.database_url, autocommit=True)
    try:
        pid, bstart = backfill.execute(
            "SELECT pid, backend_start FROM pg_stat_activity "
            "WHERE pid = pg_backend_pid()"
        ).fetchone()
        rid = _register_round_with_backend("running", pid, bstart)

        sent = _spy_cancel_signal(monkeypatch)
        ack = client.post("/index/cancel", params={"round_id": rid})
        assert ack.status_code == 202
        assert ack.json()["status"] == "cancel_requested"
        # 归属核对失败（该连接在跑补录而非本轮 CREATE）：只受理，不发信号
        assert ack.json()["cancel_signal"] == "pending_verification"

        # 补录连接未被误停：取消受理后立即可写
        backfill.execute(
            "INSERT INTO reports(lab_no,report_no,measured) VALUES ('BF','ALIVE',1)")

        # 原 SQL 确认停止（无任何后端执行本索引 CREATE）且索引无效 -> cancelled + 清理
        st = poll_status(client, "cancelled", timeout_s=30)
        assert st["round_id"] == rid
        assert st["failure_type"] == "cancelled"
        with dbmod.pool().connection() as conn:
            assert dbmod.get_index_status(conn, settings.index_name) is None

        # 全过程未向被复用的 PID 发过任何取消信号；补录连接仍然可用
        assert sent == []
        assert backfill.execute("SELECT 1").fetchone()[0] == 1
    finally:
        backfill.close()


def test_cancel_does_not_signal_when_generation_mismatch(client, monkeypatch):
    """连接换代交错（代际不符）：记录的 backend_start 属于上一代连接，
    该 PID 现已被新连接复用。归属核对失败 -> 不发信号，保持待核实直至确认停止。"""
    _make_invalid_index_leftover()
    backfill = psycopg.connect(settings.database_url, autocommit=True)
    try:
        pid = backfill.execute("SELECT pg_backend_pid()").fetchone()[0]
        stale_start = datetime.now(timezone.utc) - timedelta(hours=1)
        rid = _register_round_with_backend("running", pid, stale_start)

        sent = _spy_cancel_signal(monkeypatch)
        ack = client.post("/index/cancel", params={"round_id": rid})
        assert ack.status_code == 202
        assert ack.json()["cancel_signal"] == "pending_verification"
        backfill.execute(
            "INSERT INTO reports(lab_no,report_no,measured) VALUES ('BF','ALIVE2',1)")
        st = poll_status(client, "cancelled", timeout_s=30)
        assert st["round_id"] == rid
        assert sent == []
        assert backfill.execute("SELECT 1").fetchone()[0] == 1
    finally:
        backfill.close()


def test_cancel_waits_grace_when_backend_never_registered(client):
    """取消早于后端登记：宽限期内保持待核实，不凭“无进程号”立即报取消；
    宽限过后确认无任何后端执行本语句，才裁决 cancelled。"""
    _make_invalid_index_leftover()
    rid = _register_round_with_backend("running", None, None)
    old_grace = settings.cancel_verify_grace_s
    object.__setattr__(settings, "cancel_verify_grace_s", 2.0)
    try:
        t0 = time.monotonic()
        ack = client.post("/index/cancel", params={"round_id": rid})
        assert ack.status_code == 202
        assert ack.json()["cancel_signal"] == "pending_verification"
        # 宽限期内：仍为 cancel_requested，未误报取消
        time.sleep(0.8)
        st = client.get("/index/status", params={"round_id": rid}).json()
        assert st["status"] == "cancel_requested"
        # 宽限过后：确认无后端执行本语句且索引无效 -> cancelled 并清理
        st = poll_status(client, "cancelled", timeout_s=30)
        assert st["round_id"] == rid
        assert time.monotonic() - t0 >= 1.5
        with dbmod.pool().connection() as conn:
            assert dbmod.get_index_status(conn, settings.index_name) is None
    finally:
        object.__setattr__(settings, "cancel_verify_grace_s", old_grace)


def test_cancel_signals_only_confirmed_build_backend(client, monkeypatch):
    """归属核对通过才发信号：存活构建被取消时，信号恰好发给确认的构建后端一次；
    待核实期间重复构建复用同一轮、补录与状态查询照常；重复取消不重复发信号。"""
    for i in range(5):
        client.post("/reports", json={"lab_no": "L", "report_no": f"R{i}", "measured": i})
    blocker = _open_tx_conn()
    sent = _spy_cancel_signal(monkeypatch)
    try:
        build = client.post("/index/build").json()
        round_id = build["round_id"]
        # 等待构建线程登记后端身份（pid + 连接代际 backend_start）
        deadline = time.time() + 10
        pid = None
        while time.time() < deadline:
            st = client.get("/index/status").json()
            pid = st.get("backend_pid")
            if pid and st.get("backend_start"):
                break
            time.sleep(0.2)
        assert pid

        ack = client.post("/index/cancel")
        assert ack.status_code == 202
        assert ack.json()["status"] == "cancel_requested"
        assert ack.json()["cancel_signal"] == "sent"
        assert sent == [pid]  # 恰好一次、只发给确认归属的构建后端

        # 重复取消：同一代后端不重复发信号
        ack2 = client.post("/index/cancel")
        assert ack2.status_code == 202
        assert ack2.json()["cancel_signal"] == "already_sent"
        assert sent == [pid]

        # 待核实期间：重复构建复用同一轮，补录照常提交
        again = client.post("/index/build")
        assert again.status_code == 200
        assert again.json()["reused"] is True
        assert again.json()["round_id"] == round_id
        r = client.post("/reports", json={"lab_no": "L9", "report_no": "BX", "measured": 1})
        assert r.status_code == 201
    finally:
        blocker.rollback()
        blocker.close()
    # 沙箱禁止信号 -> 构建跑完、索引有效 -> complete（有效索引保留）
    final = poll_status(client, "complete", timeout_s=30)
    assert final["index"]["is_valid"] is True
    with dbmod.pool().connection() as conn:
        assert dbmod.index_is_valid_unique(conn, settings.index_name)
    assert sent == [pid]
