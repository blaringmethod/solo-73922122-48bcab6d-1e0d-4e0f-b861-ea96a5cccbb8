"""数据库连接与进程内日志缓冲。"""

from __future__ import annotations

import os
import threading
from collections import deque
from datetime import datetime, timezone

import psycopg

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://envmon:envmon@localhost:5432/envmon"
)

_lock = threading.Lock()
_logs: deque[dict] = deque(maxlen=500)
_seq = 0


def connect() -> psycopg.Connection:
    """新建独立连接（采集写入演示也用它，模拟独立采集连接）。"""
    return psycopg.connect(
        DATABASE_URL, autocommit=True, options="-c timezone=UTC"
    )


def log(msg: str):
    global _seq
    with _lock:
        _seq += 1
        _logs.append(
            {
                "seq": _seq,
                "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "msg": msg,
            }
        )
    print(f"[log] {msg}", flush=True)


def logs_since(seq: int) -> list[dict]:
    with _lock:
        return [e for e in _logs if e["seq"] > seq]


def ensure_meta_tables():
    """元数据表兜底创建（init.sql 已建则跳过）。"""
    with connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_runs (
                month           date        PRIMARY KEY,
                status          text        NOT NULL DEFAULT 'preparing',
                total_estimate  bigint      NOT NULL DEFAULT 0,
                copied_rows     bigint      NOT NULL DEFAULT 0,
                applied_delta   bigint      NOT NULL DEFAULT 0,
                last_station    text,
                last_ts         timestamptz,
                error           text,
                updated_at      timestamptz NOT NULL DEFAULT now(),
                created_at      timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_delta (
                seq         bigserial PRIMARY KEY,
                month       date        NOT NULL,
                op          text        NOT NULL,
                station_id  text        NOT NULL,
                ts          timestamptz NOT NULL,
                value       integer
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS migration_delta_month_seq "
            "ON migration_delta (month, seq)"
        )
        # ---- 站月缺测催补：水位 + 持久缺口清单 + 自动维护触发器 ----
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS station_watermarks (
                station_id    text        NOT NULL,
                month         date        NOT NULL,
                watermark_ts  timestamptz NOT NULL,
                updated_at    timestamptz NOT NULL DEFAULT now(),
                created_at    timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (station_id, month),
                CONSTRAINT station_watermarks_hour_chk
                    CHECK (watermark_ts = date_trunc('hour', watermark_ts)),
                CONSTRAINT station_watermarks_month_chk
                    CHECK (watermark_ts >= (month::timestamptz - interval '1 hour')
                           AND watermark_ts < (month + interval '1 month'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS station_gaps (
                station_id  text        NOT NULL,
                ts          timestamptz NOT NULL,
                month       date        NOT NULL,
                opened_at   timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (station_id, ts),
                CONSTRAINT station_gaps_hour_chk
                    CHECK (ts = date_trunc('hour', ts))
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS station_gaps_station_month "
            "ON station_gaps (station_id, month)"
        )
        conn.execute(_GAP_FN_SQL)
        conn.execute("DROP TRIGGER IF EXISTS obs_gap_trg ON observations")
        conn.execute(
            "CREATE TRIGGER obs_gap_trg AFTER INSERT OR UPDATE OR DELETE "
            "ON observations FOR EACH ROW EXECUTE FUNCTION obs_gap_maintain_fn()"
        )


_GAP_FN_SQL = """
CREATE OR REPLACE FUNCTION obs_gap_lock_station(p_station text) RETURNS void AS $fn$
DECLARE
    v_key bigint := hashtextextended('obs_gap:' || p_station, 0);
    v_tries integer := 0;
BEGIN
    -- try + 自旋退避：本事务可重入、不进死锁队列，最终仍互斥
    LOOP
        IF pg_try_advisory_xact_lock(v_key) THEN
            RETURN;
        END IF;
        v_tries := v_tries + 1;
        IF v_tries > 600 THEN
            RAISE EXCEPTION '获取站点 % 的缺口维护锁超时', p_station;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
END
$fn$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION obs_gap_maintain_fn() RETURNS trigger AS $fn$
DECLARE
    v_station text := COALESCE(NEW.station_id, OLD.station_id);
BEGIN
    IF current_setting('app.skip_gap_maintenance', true) = 'on' THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    -- 按站号字典序统一加锁顺序，跨站 UPDATE 不产生交叉死锁
    IF TG_OP = 'UPDATE' AND OLD.station_id IS DISTINCT FROM NEW.station_id THEN
        IF OLD.station_id < NEW.station_id THEN
            PERFORM obs_gap_lock_station(OLD.station_id);
            PERFORM obs_gap_lock_station(NEW.station_id);
        ELSE
            PERFORM obs_gap_lock_station(NEW.station_id);
            PERFORM obs_gap_lock_station(OLD.station_id);
        END IF;
    ELSE
        PERFORM obs_gap_lock_station(v_station);
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.ts = date_trunc('hour', NEW.ts) THEN
            DELETE FROM station_gaps
            WHERE station_id = NEW.station_id AND ts = NEW.ts;
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.ts = date_trunc('hour', NEW.ts) THEN
            DELETE FROM station_gaps
            WHERE station_id = NEW.station_id AND ts = NEW.ts;
        END IF;
        -- 旧刻观测从"旧站/旧时刻"消失（改时刻或改站号），且旧刻为整点：
        -- 只要该刻已纳入旧站催补水位，就重开旧站原缺口
        IF (OLD.ts IS DISTINCT FROM NEW.ts
            OR OLD.station_id IS DISTINCT FROM NEW.station_id)
           AND OLD.ts = date_trunc('hour', OLD.ts) THEN
            INSERT INTO station_gaps (station_id, ts, month)
            SELECT OLD.station_id, OLD.ts,
                   (date_trunc('month', OLD.ts AT TIME ZONE 'UTC'))::date
            WHERE EXISTS (
                SELECT 1 FROM station_watermarks w
                WHERE w.station_id = OLD.station_id
                  AND w.month = (date_trunc('month', OLD.ts AT TIME ZONE 'UTC'))::date
                  AND w.watermark_ts >= OLD.ts
            )
            ON CONFLICT (station_id, ts) DO NOTHING;
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.ts = date_trunc('hour', OLD.ts) THEN
        INSERT INTO station_gaps (station_id, ts, month)
        SELECT OLD.station_id, OLD.ts,
               (date_trunc('month', OLD.ts AT TIME ZONE 'UTC'))::date
        WHERE EXISTS (
            SELECT 1 FROM station_watermarks w
            WHERE w.station_id = OLD.station_id
              AND w.month = (date_trunc('month', OLD.ts AT TIME ZONE 'UTC'))::date
              AND w.watermark_ts >= OLD.ts
        )
        ON CONFLICT (station_id, ts) DO NOTHING;
    END IF;
    RETURN OLD;
END
$fn$ LANGUAGE plpgsql
"""
