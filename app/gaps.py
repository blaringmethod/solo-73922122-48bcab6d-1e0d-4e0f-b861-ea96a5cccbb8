"""
站月缺测催补：水位推进 + 持久缺口清单维护。

核心约定：
  * station_watermarks 每站每月一行，watermark_ts 为已催补到的整点（含），
    覆盖 [月首整点, watermark_ts]；水位只能前进，不能后退。
  * station_gaps 为持久缺口清单，(station_id, ts) 唯一，ts 必为整点。
  * 采集端对 observations 的任意独立 SQL 写事务由数据库触发器
    obs_gap_trg 自动维护清单：整点插入消警、删除/改时刻在水位内重开、
    非整点记录不抵数；事务回滚则触发器效果一并回滚。
  * 本模块只负责"水位推进"：单事务内取行锁 → 校验不后退 → 推进水位 →
    按推进区间增量对账（整点缺观测即入清单，有观测则确保不在清单）。
    推进失败整体回滚，原水位与清单保持不变。
  * 查询直接读 station_gaps / station_watermarks，按 (station_id, month)
    索引返回，不做整月重算。
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone

import psycopg

from db import connect, log

MONTH_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
TS_RE = re.compile(
    r"^(\d{4})-(0[1-9]|1[0-2])-([0-2]\d|3[01])[T ]"
    r"([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)


class WatermarkError(Exception):
    """水位推进的业务错误（后退/非整点/跨月等），映射为 HTTP 400。"""


def _acquire_station_lock(conn, station: str, *, timeout_s: float = 30.0) -> None:
    """获取按站咨询锁（事务级）。

    用 pg_try_advisory_xact_lock + 退避自旋，而不是阻塞式加锁：
    本事务已持有时可重入立即返回，他人持有时等待但不进入死锁等待队列，
    从而杜绝"持锁事务内再写 observations"的队首死锁，同时保持最终互斥。
    """
    import time as _time

    deadline = _time.monotonic() + timeout_s
    while True:
        got = conn.execute(
            "SELECT pg_try_advisory_xact_lock("
            "hashtextextended('obs_gap:' || %s, 0))",
            (station,),
        ).fetchone()[0]
        if got:
            return
        if _time.monotonic() > deadline:
            raise RuntimeError(f"获取站点 {station} 的缺口维护锁超时（{timeout_s}s）")
        conn.execute("SELECT pg_sleep(0.05)")


def parse_month(month: str) -> date:
    m = MONTH_RE.match(month or "")
    if not m:
        raise WatermarkError(f"月份格式应为 YYYY-MM，收到: {month!r}")
    return date(int(m.group(1)), int(m.group(2)), 1)


def next_month(month: date) -> date:
    if month.month == 12:
        return date(month.year + 1, 1, 1)
    return date(month.year, month.month + 1, 1)


def month_bounds(month: date) -> tuple[datetime, datetime]:
    lo = datetime(month.year, month.month, 1, tzinfo=timezone.utc)
    if month.month == 12:
        hi = datetime(month.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        hi = datetime(month.year, month.month + 1, 1, tzinfo=timezone.utc)
    return lo, hi


def parse_hour(ts: str, month: date) -> datetime:
    """解析目标整点并校验：UTC 整点、落在该月内。"""
    if not TS_RE.match(ts or ""):
        raise WatermarkError(f"时刻格式应为 ISO 8601（如 2026-11-03T08:00Z），收到: {ts!r}")
    s = ts.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        raise WatermarkError(f"时刻无法解析: {ts!r} ({exc})")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise WatermarkError(f"水位只能推进到整点，收到: {ts!r}")
    lo, hi = month_bounds(month)
    if not (lo <= dt < hi):
        raise WatermarkError(f"整点 {dt.isoformat()} 不在月份 {month.isoformat()} 内")
    return dt


def list_stations() -> list[str]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT station_id FROM observations ORDER BY 1"
        ).fetchall()
    return [r[0] for r in rows]


def get_watermark(conn, station: str, month: date):
    return conn.execute(
        "SELECT watermark_ts, updated_at FROM station_watermarks"
        " WHERE station_id = %s AND month = %s",
        (station, month),
    ).fetchone()


def advance_watermark(station: str, month_str: str, target_str: str) -> dict:
    """
    把 (station, month) 的催补水位推进到 target 整点（含）。

    单事务完成：
      1. 对 station_watermarks 行加 FOR UPDATE 行锁（无行则先插入再锁），
         串行化同站同月的并发推进；
      2. 拒绝后退（target <= 原水位 → 400，清单不变）；
      3. upsert 新水位；
      4. 仅对新增水位区间 (old_wm, target] 做增量对账：
         整点序列反连接 observations（只认同站同刻的整点观测），
         缺失即 INSERT ... ON CONFLICT DO NOTHING 入持久清单，
         已有观测的整点确保不在清单（防历史残留/外部清理场景）。
    其他站月不持任何锁，采集端对其他站的写入不受影响。
    """
    month = parse_month(month_str)
    target = parse_hour(target_str, month)
    lo, hi = month_bounds(month)

    with connect() as conn:
        try:
            with conn.transaction():
                # 1a) 按站咨询锁：与采集写触发器互斥，串行化同站的
                #     "迟到写入" 与 "水位对账"，消除反连接快照竞争；
                #     只锁本站，其他站点的采集写入照常提交。
                #     用 try+自旋而非阻塞式加锁，避免持锁事务内再次经触发器
                #     请求同锁时排进等待队列形成队首死锁。
                _acquire_station_lock(conn, station)
                # 1b) 确保站月水位行存在并锁住（同站同月推进串行化）
                conn.execute(
                    "INSERT INTO station_watermarks (station_id, month, watermark_ts)"
                    " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (station, month, lo - timedelta(hours=1)),
                )
                row = conn.execute(
                    "SELECT watermark_ts FROM station_watermarks"
                    " WHERE station_id = %s AND month = %s FOR UPDATE",
                    (station, month),
                ).fetchone()
                current = row[0]
                if current.tzinfo is None:
                    current = current.replace(tzinfo=timezone.utc)
                current = current.astimezone(timezone.utc)

                # 2) 水位不得后退（相等也视为无效推进）
                if target <= current:
                    raise WatermarkError(
                        f"水位不得后退：{station} {month_str} 当前水位 "
                        f"{current.isoformat()}，目标 {target.isoformat()}"
                    )

                # 3) 推进水位
                conn.execute(
                    "UPDATE station_watermarks SET watermark_ts = %s, updated_at = now()"
                    " WHERE station_id = %s AND month = %s",
                    (target, station, month),
                )

                # 4) 增量对账新区间 (current, target]。
                #    反连接 observations：同站、同刻、整点的观测才算数；
                #    非整点记录的 ts 不会等于整点序列，天然不抵数。
                conn.execute(
                    """
                    INSERT INTO station_gaps (station_id, ts, month)
                    SELECT %s, h.hour, %s
                    FROM generate_series(%s::timestamptz,
                                         %s::timestamptz,
                                         interval '1 hour') AS h(hour)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM observations o
                        WHERE o.station_id = %s AND o.ts = h.hour
                    )
                    ON CONFLICT (station_id, ts) DO NOTHING
                    """,
                    (station, month, current + timedelta(hours=1), target, station),
                )
                # 新区间里已有整点观测的位置，确保清单干净
                # （正常情况下触发器已消警；此句兜底幂等）。
                conn.execute(
                    """
                    DELETE FROM station_gaps g
                    WHERE g.station_id = %s AND g.month = %s
                      AND g.ts > %s AND g.ts <= %s
                      AND EXISTS (
                          SELECT 1 FROM observations o
                          WHERE o.station_id = g.station_id AND o.ts = g.ts
                      )
                    """,
                    (station, month, current, target),
                )
                gap_count = conn.execute(
                    "SELECT count(*) FROM station_gaps"
                    " WHERE station_id = %s AND month = %s",
                    (station, month),
                ).fetchone()[0]
        except psycopg.Error as exc:
            # 数据库层错误（锁超时/约束等）：事务已回滚，原样上抛
            raise RuntimeError(f"水位推进失败：{exc}") from exc

    log(f"[水位] {station} {month_str} 推进 {current.isoformat()} → "
        f"{target.isoformat()}，当前缺口 {gap_count} 个")
    return {
        "station_id": station,
        "month": month_str,
        "previous_watermark": current.isoformat(),
        "watermark_ts": target.isoformat(),
        "gap_count": gap_count,
    }


def query_station_month(station: str, month_str: str) -> dict:
    """按站月查询水位与持久缺口清单：直接读表，不做整月重算。"""
    month = parse_month(month_str)
    with connect() as conn:
        wm = get_watermark(conn, station, month)
        rows = conn.execute(
            "SELECT ts, opened_at FROM station_gaps"
            " WHERE station_id = %s AND month = %s ORDER BY ts",
            (station, month),
        ).fetchall()
    return {
        "station_id": station,
        "month": month_str,
        "watermark_ts": wm[0].isoformat() if wm else None,
        "updated_at": wm[1].isoformat() if wm else None,
        "gap_count": len(rows),
        "gaps": [
            {"ts": r[0].isoformat(), "opened_at": r[1].isoformat()} for r in rows
        ],
    }


def station_month_summary(month: date) -> dict:
    """月份总览用：该月各站水位与缺口数（纯索引读）。"""
    with connect() as conn:
        wm_rows = conn.execute(
            "SELECT station_id, watermark_ts FROM station_watermarks"
            " WHERE month = %s ORDER BY station_id",
            (month,),
        ).fetchall()
        gap_rows = conn.execute(
            "SELECT station_id, count(*) FROM station_gaps"
            " WHERE month = %s GROUP BY station_id",
            (month,),
        ).fetchall()
    gaps = {r[0]: r[1] for r in gap_rows}
    return {
        r[0]: {"watermark_ts": r[1].isoformat(), "gap_count": gaps.get(r[0], 0)}
        for r in wm_rows
    }
