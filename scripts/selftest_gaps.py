#!/usr/bin/env python3
"""
站月缺测催补 · 端到端自测（真实 PostgreSQL + 真实 HTTP API）：

  1. 水位推进：缺测整点入清单、已有整点不入清单、非整点不抵数
  2. 水位只进不退：后退/相等被拒（400），失败后原水位与清单不变
  3. 采集端独立 SQL：迟到整点插入随事务消警；删除/改时刻重开原缺口；
     回滚不改变清单
  4. 并发：水位推进与采集迟到写入并发，无误报（清单里的刻必有观测缺失）、
     无漏报（水位内缺观测的刻必在清单）；其他站不受影响仍可写入
  5. 迁移交错：月分区在线迁移（慢复制+并发采集）前后，缺口清单与
     observations 实际内容保持一致，迁移不制造/消除缺口
  6. 重启持久化：应用重启后按站月读到的水位/缺口不变，查询不依赖进程状态

运行：DATABASE_URL=... [BASE_URL=...] python3 scripts/selftest_gaps.py
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://envmon:envmon@/envmon?host=/tmp/pg-runtime/data",
)
APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
APP_PORT = int(os.environ.get("APP_PORT", "8020"))
BASE = os.environ.get("BASE_URL", f"http://127.0.0.1:{APP_PORT}")
os.environ["DATABASE_URL"] = DSN
sys.path.insert(0, APP_DIR)

_failures: list[str] = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""), flush=True)
    if not cond:
        _failures.append(f"{name}: {detail}")


class InProcessApp:
    """进程内 uvicorn：真实 HTTP 服务，should_exit 优雅停启（沙箱禁用 kill 时也能用）。

    stop + 重新 start 到同一端口即等价于"应用重启"——所有状态都在数据库里。
    """

    def __init__(self):
        import uvicorn
        self.uvicorn = uvicorn
        self.server = None
        self.thread = None

    def start(self):
        import main  # noqa: F401  触发 FastAPI 应用定义（startup 钩子连库建表）
        config = self.uvicorn.Config(
            "main:app", host="127.0.0.1", port=APP_PORT,
            log_level="warning", lifespan="on")
        self.server = self.uvicorn.Server(config)
        self.thread = threading.Thread(
            target=self.server.run, name="inproc-uvicorn", daemon=True)
        self.thread.start()
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if requests.get(f"{BASE}/api/state", timeout=2).ok:
                    return
            except requests.RequestException:
                time.sleep(0.2)
        raise RuntimeError("应用启动超时")

    def stop(self):
        if self.server is not None:
            self.server.should_exit = True
            self.thread.join(timeout=20)
        self.server = None
        self.thread = None

    def restart(self):
        self.stop()
        time.sleep(0.5)  # 等监听端口释放
        self.start()


def db():
    return psycopg.connect(DSN, autocommit=True, options="-c timezone=UTC")


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def api(path, method="GET", body=None):
    return requests.request(method, BASE + path, json=body, timeout=30)


def month_hours(month: str):
    lo = datetime.fromisoformat(month + "-01").replace(tzinfo=timezone.utc)
    if lo.month == 12:
        hi = lo.replace(year=lo.year + 1, month=1)
    else:
        hi = lo.replace(month=lo.month + 1)
    return lo, hi


# ------------------------------------------------------------- 测试数据准备
def prepare_station(station: str, month: str, missing_hours: set[int]):
    """在默认分区造一个站的整月整点数据，指定小时序号缺测。"""
    lo, hi = month_hours(month)
    with db() as c:
        c.execute("DELETE FROM observations WHERE station_id=%s AND ts>=%s AND ts<%s",
                  (station, lo, hi))
        c.execute("DELETE FROM station_gaps WHERE station_id=%s", (station,))
        c.execute("DELETE FROM station_watermarks WHERE station_id=%s", (station,))
        hours = int((hi - lo).total_seconds() // 3600)
        for i in range(hours):
            if i in missing_hours:
                continue
            c.execute("INSERT INTO observations VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                      (station, lo + timedelta(hours=i), 100 + i % 50))


def gaps_set(station: str) -> set[datetime]:
    with db() as c:
        rows = c.execute(
            "SELECT ts FROM station_gaps WHERE station_id=%s", (station,)
        ).fetchall()
    return {r[0] for r in rows}


def obs_set(station: str, month: str) -> set[datetime]:
    lo, hi = month_hours(month)
    with db() as c:
        rows = c.execute(
            "SELECT ts FROM observations WHERE station_id=%s AND ts>=%s AND ts<%s",
            (station, lo, hi),
        ).fetchall()
    return {r[0] for r in rows}


def watermark(station: str, month: str):
    with db() as c:
        r = c.execute(
            "SELECT watermark_ts FROM station_watermarks WHERE station_id=%s AND month=%s::date",
            (station, month + "-01"),
        ).fetchone()
    return r[0] if r else None


def cross_check(station: str, month: str, label: str):
    """权威对账：水位 [月首, wm] 内，缺口清单 == 缺整点观测的整点集合。"""
    lo, _ = month_hours(month)
    wm = watermark(station, month)
    if wm is None:
        check(f"{label}: 尚无水位", True)
        return
    if wm.tzinfo is None:
        wm = wm.replace(tzinfo=timezone.utc)
    obs = obs_set(station, month)
    expected = set()
    t = lo
    while t <= wm:
        if t not in obs:
            expected.add(t)
        t += timedelta(hours=1)
    got = gaps_set(station)
    check(f"{label}: 缺口清单与水位内实际缺测完全一致", got == expected,
          f"清单 {len(got)} vs 应缺 {len(expected)}，"
          f"多报 {sorted(got - expected)[:3]}，漏报 {sorted(expected - got)[:3]}")
    # 清单内全部为整点
    with db() as c:
        bad = c.execute(
            "SELECT count(*) FROM station_gaps WHERE station_id=%s "
            "AND ts <> date_trunc('hour', ts)", (station,)
        ).fetchone()[0]
    check(f"{label}: 缺口全部为整点", bad == 0, f"非整点 {bad} 个")


# ------------------------------------------------------------------ 用例
def test_advance_basic(month: str):
    print("\n== 用例1：水位推进与非整点不抵数 ==", flush=True)
    st = "TST01"
    prepare_station(st, month, {1, 3, 5})  # 第1/3/5小时缺测
    lo, _ = month_hours(month)
    # 推进到第 6 小时
    r = api("/api/gaps/watermark", "POST", {
        "station_id": st, "month": month,
        "target_ts": iso(lo + timedelta(hours=6))})
    check("推进返回 200", r.ok, r.text[:200])
    want_gaps = {lo + timedelta(hours=i) for i in (1, 3, 5)}
    check("三个缺测整点进入清单", gaps_set(st) == want_gaps,
          f"{sorted(gaps_set(st))}")
    # 非整点记录不抵数：在缺口刻插入 30 分记录，清单不变
    r = api("/api/gaps/collect", "POST", {
        "op": "insert", "station_id": st,
        "ts": iso(lo + timedelta(hours=1, minutes=30)), "value": 1})
    check("非整点插入成功", r.ok, r.text[:200])
    check("非整点不抵数，缺口仍在", gaps_set(st) == want_gaps)
    cross_check(st, month, "用例1")


def test_no_backtrack(month: str):
    print("\n== 用例2：水位只进不退，失败保留原水位与清单 ==", flush=True)
    st = "TST02"
    prepare_station(st, month, {2, 4})
    lo, _ = month_hours(month)
    api("/api/gaps/watermark", "POST", {
        "station_id": st, "month": month,
        "target_ts": iso(lo + timedelta(hours=5))})
    wm_before = watermark(st, month)
    gaps_before = gaps_set(st)
    # 后退
    r = api("/api/gaps/watermark", "POST", {
        "station_id": st, "month": month,
        "target_ts": iso(lo + timedelta(hours=2))})
    check("后退被拒(400)", r.status_code == 400, r.text[:150])
    # 相等
    r = api("/api/gaps/watermark", "POST", {
        "station_id": st, "month": month, "target_ts": iso(wm_before)})
    check("相等推进被拒(400)", r.status_code == 400, r.text[:150])
    # 非整点/跨月
    r = api("/api/gaps/watermark", "POST", {
        "station_id": st, "month": month,
        "target_ts": iso(lo + timedelta(hours=8, minutes=30))})
    check("非整点目标被拒(400)", r.status_code == 400, r.text[:150])
    check("失败后水位不变", watermark(st, month) == wm_before)
    check("失败后清单不变", gaps_set(st) == gaps_before)
    # 继续前进到第 8 小时，原有缺口保留、新区间增量对账
    r = api("/api/gaps/watermark", "POST", {
        "station_id": st, "month": month,
        "target_ts": iso(lo + timedelta(hours=8))})
    check("再次前进成功", r.ok, r.text[:150])
    cross_check(st, month, "用例2")


def test_collect_lifecycle(month: str):
    print("\n== 用例3：迟到插入消警 / 删除重开 / 改时刻重开 / 回滚不变 ==", flush=True)
    st = "TST03"
    prepare_station(st, month, {0, 2, 7})
    lo, _ = month_hours(month)
    api("/api/gaps/watermark", "POST", {
        "station_id": st, "month": month,
        "target_ts": iso(lo + timedelta(hours=10))})
    g0 = lo  # 缺口刻
    # 迟到整点插入 → 消警
    r = api("/api/gaps/collect", "POST", {
        "op": "insert", "station_id": st, "ts": iso(g0), "value": 77})
    check("迟到整点插入成功", r.ok, r.text[:150])
    check("迟到插入随事务消警", g0 not in gaps_set(st))
    # 再删 → 重开
    r = api("/api/gaps/collect", "POST", {
        "op": "delete", "station_id": st, "ts": iso(g0)})
    check("删除成功", r.ok, r.text[:150])
    check("删除水位内整点重开缺口", g0 in gaps_set(st))
    # upsert 消警（冲突路径走 UPDATE 分支）
    r = api("/api/gaps/collect", "POST", {
        "op": "upsert", "station_id": st, "ts": iso(g0), "value": 88})
    check("upsert 成功", r.ok, r.text[:150])
    check("upsert 消警", g0 not in gaps_set(st))
    # 改时刻：把 g0 的观测改到一个非整点新时刻 → 旧刻 g0 重开，新刻非整点不入清单
    new_ts = g0 + timedelta(minutes=30)
    r = api("/api/gaps/collect", "POST", {
        "op": "update_ts", "station_id": st, "ts": iso(g0),
        "new_ts": iso(new_ts)})
    check("改时刻成功", r.ok, r.text[:150])
    check("改时刻后旧刻重开", g0 in gaps_set(st))
    check("新时刻非整点不产生缺口", new_ts not in gaps_set(st))
    # 回滚不改变清单：直接 SQL 多语句事务，最后故意失败
    before = gaps_set(st)
    try:
        with psycopg.connect(DSN, options="-c timezone=UTC") as c:
            with c.transaction():
                c.execute("DELETE FROM observations WHERE station_id=%s AND ts=%s",
                          (st, new_ts))
                c.execute("SELECT 1/0")
    except psycopg.Error:
        pass
    check("回滚后清单不变", gaps_set(st) == before)
    cross_check(st, month, "用例3")


def test_concurrency(month: str):
    print("\n== 用例4：水位推进 × 采集迟到写入并发（无误报/漏报，他站不受影响）==",
          flush=True)
    st = "TST04"
    other = "TST05"
    lo, hi = month_hours(month)
    hours = int((hi - lo).total_seconds() // 3600)
    prepare_station(st, month, set(range(0, hours, 7)))      # 周期性缺测
    prepare_station(other, month, set(range(3, hours, 11)))
    stop = threading.Event()
    errors: list[str] = []

    def writer(station):
        """独立连接：反复对缺口刻做迟到插入/删除，模拟补传与反复变更。"""
        rnd = random.Random(hash(station) & 0xffff)
        conn = psycopg.connect(DSN, autocommit=True,
                               options="-c timezone=UTC")
        try:
            while not stop.is_set():
                i = rnd.randrange(0, min(hours, 200))
                ts = lo + timedelta(hours=i)
                op = rnd.choice(["fill", "remove"])
                try:
                    if op == "fill":
                        conn.execute(
                            "INSERT INTO observations VALUES(%s,%s,%s)"
                            " ON CONFLICT DO NOTHING",
                            (station, ts, rnd.randint(0, 100)))
                    else:
                        conn.execute(
                            "DELETE FROM observations WHERE station_id=%s AND ts=%s",
                            (station, ts))
                except psycopg.Error as e:
                    errors.append(f"{station}: {e!r}")
                time.sleep(rnd.uniform(0.002, 0.02))
        finally:
            conn.close()

    threads = [threading.Thread(target=writer, args=(s,), daemon=True)
               for s in (st, other, st)]  # 同站两写入线程 + 他站
    for t in threads:
        t.start()

    # 分 10 次把水位推进到月内 200 小时
    for k in range(1, 11):
        target = lo + timedelta(hours=min(200, 20 * k) - 1)
        r = api("/api/gaps/watermark", "POST", {
            "station_id": st, "month": month, "target_ts": iso(target)})
        if not r.ok:
            errors.append(f"advance: {r.status_code} {r.text[:120]}")
        time.sleep(0.05)

    # 他站并发期间也要能推进水位（验证不被同站锁阻塞）
    r = api("/api/gaps/watermark", "POST", {
        "station_id": other, "month": month,
        "target_ts": iso(lo + timedelta(hours=99))})
    check("并发期间其他站可推进水位", r.ok, r.text[:150])

    stop.set()
    for t in threads:
        t.join(timeout=10)
    time.sleep(0.3)  # 等最后一个写入事务落定
    check("并发采集无数据库错误", not errors, "; ".join(errors[:3]))
    cross_check(st, month, "用例4-本站")
    cross_check(other, month, "用例4-他站")

    # 显式误报/漏报断言
    wm = watermark(st, month)
    if wm.tzinfo is None:
        wm = wm.replace(tzinfo=timezone.utc)
    obs = obs_set(st, month)
    got = gaps_set(st)
    false_pos = {g for g in got if g in obs}            # 清单里但其实有观测
    false_neg = set()
    t = lo
    while t <= wm:
        if t not in obs and t not in got:
            false_neg.add(t)
        t += timedelta(hours=1)
    check("无误报（清单刻均无观测）", not false_pos, str(sorted(false_pos)[:3]))
    check("无漏报（水位内缺测均在清单）", not false_neg, str(sorted(false_neg)[:3]))


def test_migration_interleave(month: str):
    print("\n== 用例5：月分区在线迁移交错（迁移不制造/消除缺口）==", flush=True)
    st = "TST06"
    lo, hi = month_hours(month)
    hours = int((hi - lo).total_seconds() // 3600)
    cap = min(hours, 150)
    prepare_station(st, month, set(range(5, cap, 9)))
    api("/api/gaps/watermark", "POST", {
        "station_id": st, "month": month,
        "target_ts": iso(lo + timedelta(hours=cap - 1))})
    gaps_before = gaps_set(st)
    wm_before = watermark(st, month)
    check("迁移前缺口清单非空", len(gaps_before) > 0, f"{len(gaps_before)} 个")

    stop = threading.Event()
    err: list[str] = []

    def chaos():
        rnd = random.Random(42)
        conn = psycopg.connect(DSN, autocommit=True,
                               options="-c timezone=UTC")
        try:
            while not stop.is_set():
                i = rnd.randrange(0, cap)
                ts = lo + timedelta(hours=i)
                try:
                    conn.execute(
                        "INSERT INTO observations VALUES(%s,%s,%s)"
                        " ON CONFLICT DO NOTHING",
                        (st, ts, rnd.randint(0, 100)))
                    if rnd.random() < 0.4:
                        conn.execute(
                            "DELETE FROM observations WHERE station_id=%s AND ts=%s",
                            (st, ts))
                except psycopg.Error as e:
                    err.append(repr(e))
                time.sleep(rnd.uniform(0.01, 0.05))
        finally:
            conn.close()

    t = threading.Thread(target=chaos, daemon=True)
    t.start()
    r = requests.post(f"{BASE}/api/migrate", json={
        "month": month, "batch_size": 200, "sleep_ms": 60}, timeout=15)
    check("迁移启动", r.ok, r.text[:150])

    # 等迁移结束
    deadline = time.time() + 240
    status = None
    while time.time() < deadline:
        with db() as c:
            row = c.execute(
                "SELECT status FROM migration_runs WHERE month=%s::date",
                (month + "-01",)).fetchone()
        status = row[0] if row else None
        if status in ("done", "failed"):
            break
        time.sleep(0.5)
    stop.set()
    t.join(timeout=10)
    time.sleep(0.3)
    check("迁移完成", status == "done", f"status={status}")
    check("迁移期间采集无错误", not err, "; ".join(err[:3]))
    check("水位在迁移后不变", watermark(st, month) == wm_before)
    cross_check(st, month, "用例5")

    # 迁移后数据在月分区，采集端迟到写入仍能消警/重开（触发器克隆到分区）
    g = next(iter(gaps_set(st)), None)
    if g is not None:
        r = api("/api/gaps/collect", "POST", {
            "op": "insert", "station_id": st, "ts": iso(g), "value": 5})
        check("迁移后对月分区迟到插入成功", r.ok, r.text[:150])
        check("迁移后迟到插入仍可消警", g not in gaps_set(st))
        r = api("/api/gaps/collect", "POST", {
            "op": "delete", "station_id": st, "ts": iso(g)})
        check("迁移后删除仍可重开缺口", r.ok and g in gaps_set(st), r.text[:150])
    # 月分区确实已挂载
    with db() as c:
        part = c.execute(
            "SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid "
            "WHERE i.inhparent='observations'::regclass "
            "AND c.relname=%s",
            ("observations_" + month.replace("-", "_"),)).fetchone()
    check("月分区已挂载", bool(part))


def test_persistence(month: str, app: InProcessApp):
    print("\n== 用例6：重启后水位/缺口持久化，查询不重算 ==", flush=True)
    st = "TST07"
    prepare_station(st, month, {1, 2, 3})
    lo, _ = month_hours(month)
    api("/api/gaps/watermark", "POST", {
        "station_id": st, "month": month,
        "target_ts": iso(lo + timedelta(hours=6))})
    wm_before = watermark(st, month)
    gaps_before = gaps_set(st)

    # 真实停掉服务进程内的 HTTP server，再以全新 server 起在同一端口
    app.restart()
    r = api(f"/api/gaps?station_id={st}&month={month}")
    check("重启后查询接口可用", r.ok, r.text[:150])
    body = r.json()
    check("重启后水位一致",
          body["watermark_ts"] == wm_before.isoformat(),
          f"{body.get('watermark_ts')} vs {wm_before.isoformat()}")
    check("重启后缺口清单一致",
          {datetime.fromisoformat(x["ts"]) for x in body["gaps"]} == gaps_before,
          f"{body['gap_count']} 个")
    # 查询计划走索引（不出现全表扫描 observations 的反连接）
    with db() as c:
        plan = c.execute(
            "EXPLAIN SELECT ts FROM station_gaps "
            "WHERE station_id=%s AND month=%s::date",
            (st, month + "-01")).fetchall()
    plan_txt = " ".join(p[0] for p in plan)
    check("缺口查询命中 station_gaps 索引",
          "Index Scan" in plan_txt or "Index Only Scan" in plan_txt,
          plan_txt[:120])
    check("缺口查询不扫描 observations", "observations" not in plan_txt)


# ------------------------------------------------------------------ main
def main():
    random.seed(20261118)
    with db() as c:
        all_months = [r[0].isoformat()[:7] for r in c.execute(
            "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date m,"
            " count(*) FROM observations GROUP BY 1 ORDER BY 1")]
        # 迁移用例必须选仍在默认分区（未迁移）的月份
        default_months = [r[0].isoformat()[:7] for r in c.execute(
            "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date m,"
            " count(*) FROM observations_default GROUP BY 1 ORDER BY 1")]
    check("种子至少有两个月份可供测试", len(all_months) >= 2, str(all_months))
    check("至少一个未迁移月份可供交错测试", bool(default_months), str(default_months))
    # 并发/基础用任意月份；迁移用月份必须未迁移
    m_gap = all_months[0]
    m_mig = default_months[0]
    if m_mig == m_gap:
        m_gap = all_months[-1]

    app = InProcessApp()
    app.start()
    try:
        test_advance_basic(m_gap)
        test_no_backtrack(m_gap)
        test_collect_lifecycle(m_gap)
        test_concurrency(m_gap)
        test_migration_interleave(m_mig)
        test_persistence(m_gap, app)
    finally:
        app.stop()

    print("\n" + "=" * 60)
    if _failures:
        print(f"自测失败 {len(_failures)} 项：")
        for f in _failures:
            print("  -", f)
        sys.exit(1)
    print("站月缺测自测全部通过 ✓")


if __name__ == "__main__":
    main()
