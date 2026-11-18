-- ============================================================================
-- 环境监测数仓 · 观测表分区迁移 + 站月缺测催补 — 数据库初始化
-- ----------------------------------------------------------------------------
--  * observations        : 父表，按 UTC 时刻 RANGE 分区，主键 (station_id, ts)
--  * observations_default: 唯一默认分区，历史观测最初全部落在这里
--  * migration_runs      : 每月迁移状态机（幂等/断点续迁依据）
--  * migration_delta     : 迁移期间触发器捕获的增量变更
--  * station_watermarks  : 每站每月的"催补水位"（已催补到的整点，单调不退）
--  * station_gaps        : 持久化缺测清单（整点缺同站同刻观测即入清单）
--
-- 缺口正确性的核心是 observations 上的行级触发器 obs_gap_trg：
--   * 采集端任意独立 SQL（INSERT/UPDATE/DELETE）随写事务自动维护缺口，
--     回滚则触发器效果一并回滚，清单不被改变；
--   * 只有整点（date_trunc('hour', ts)=ts）观测才能消除缺口，非整点不抵数；
--   * 迁移搬数据不是真实采集变更，finalizing 事务内用会话开关
--     app.skip_gap_maintenance 跳过缺口维护，避免搬迁制造/消除缺口。
-- ============================================================================

CREATE TABLE IF NOT EXISTS observations (
    station_id  text          NOT NULL,
    ts          timestamptz   NOT NULL,
    value       integer       NOT NULL,
    PRIMARY KEY (station_id, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE IF NOT EXISTS observations_default
    PARTITION OF observations DEFAULT;

-- 迁移状态机：idle → preparing → copying → applying → finalizing → done
CREATE TABLE IF NOT EXISTS migration_runs (
    month           date        PRIMARY KEY,   -- 月份首日（UTC）
    status          text        NOT NULL DEFAULT 'preparing',
    total_estimate  bigint      NOT NULL DEFAULT 0,
    copied_rows     bigint      NOT NULL DEFAULT 0,
    applied_delta   bigint      NOT NULL DEFAULT 0,
    last_station    text,
    last_ts         timestamptz,
    error           text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- 增量变更日志：迁移期间默认分区上该月数据的 INSERT/UPDATE/DELETE
CREATE TABLE IF NOT EXISTS migration_delta (
    seq         bigserial PRIMARY KEY,
    month       date        NOT NULL,
    op          text        NOT NULL CHECK (op IN ('insert','update','delete')),
    station_id  text        NOT NULL,
    ts          timestamptz NOT NULL,
    value       integer
);
CREATE INDEX IF NOT EXISTS migration_delta_month_seq
    ON migration_delta (month, seq);

-- ============================================================================
-- 站月催补水位：每站每月一行。watermark_ts 为已催补到的整点（含），
-- 覆盖范围 = [月首整点, watermark_ts]。水位只允许前进，不允许后退。
-- ============================================================================
CREATE TABLE IF NOT EXISTS station_watermarks (
    station_id    text        NOT NULL,
    month         date        NOT NULL,          -- UTC 月份首日
    watermark_ts  timestamptz NOT NULL,          -- 必为该月内整点
    updated_at    timestamptz NOT NULL DEFAULT now(),
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, month),
    CONSTRAINT station_watermarks_hour_chk
        CHECK (watermark_ts = date_trunc('hour', watermark_ts)),
    CONSTRAINT station_watermarks_month_chk
        -- 允许"月首前 1 小时"作为该站月尚未推进的哨兵值
        CHECK (watermark_ts >= (month::timestamptz - interval '1 hour')
               AND watermark_ts < (month + interval '1 month'))
);

-- ============================================================================
-- 持久缺口清单：(站号, 整点时刻) 唯一；时刻必为整点。
-- 查询按站月走索引，直接读清单，不做整月重算。
-- ============================================================================
CREATE TABLE IF NOT EXISTS station_gaps (
    station_id  text        NOT NULL,
    ts          timestamptz NOT NULL,
    month       date        NOT NULL,          -- ts 的 UTC 月份首日（冗余便于查询）
    opened_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, ts),
    CONSTRAINT station_gaps_hour_chk
        CHECK (ts = date_trunc('hour', ts))
);
CREATE INDEX IF NOT EXISTS station_gaps_station_month
    ON station_gaps (station_id, month);

-- ============================================================================
-- 缺口维护触发器：挂在分区父表 observations 上，PostgreSQL 会把它克隆到
-- 每个分区（含默认分区与后续 ATTACH 的月分区），因此数据搬到月分区后
-- 采集端对月分区的迟到写入/删除依旧随事务维护同一份缺口清单。
--
-- 语义（触发器内先取按站咨询锁，与水位推进事务串行化，杜绝反连接快照竞争
--       造成的虚假缺口/漏报；其他站点的写入互不阻塞）：
--   INSERT（整点）          → 该 (站,刻) 缺口消警；非整点不抵数
--   UPSERT 冲突走 UPDATE   → UPDATE 分支同样按整点消警
--   UPDATE 改到整点行      → 新时刻缺口消警；旧时刻若已在水位内则重开缺口
--   DELETE 整点行          → 若该刻已在水位内，重开原缺口
-- 迁移 finalizing 事务设置 app.skip_gap_maintenance=on，整段搬迁不碰缺口。
-- ============================================================================
CREATE OR REPLACE FUNCTION obs_gap_lock_station(p_station text) RETURNS void AS $fn$
DECLARE
    v_key bigint := hashtextextended('obs_gap:' || p_station, 0);
    v_tries integer := 0;
BEGIN
    -- 用 try 版本 + 自旋退避，而不是阻塞式 pg_advisory_xact_lock：
    --   * 本事务已持有时 try 直接返回 true（可重入），不会把自己排进等待队列
    --     从而避免"持锁事务内再写 observations"形成的队首死锁；
    --   * 被其他事务持有时返回 false，短暂 pg_sleep 后重试，最终仍互斥。
    LOOP
        IF pg_try_advisory_xact_lock(v_key) THEN
            RETURN;
        END IF;
        v_tries := v_tries + 1;
        IF v_tries > 600 THEN   -- 约 30s（600 × 50ms）
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
    -- 迁移搬迁路径：只搬数据，不改变缺口清单
    IF current_setting('app.skip_gap_maintenance', true) = 'on' THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    -- 按站咨询锁：与水位推进的同站对账互斥；锁随事务提交/回滚自动释放。
    -- UPDATE 改站号会同时碰两个站，按站号字典序统一加锁顺序，杜绝交叉死锁。
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
        -- 新时刻若为整点：观测已存在，消除该刻缺口（UPSERT 冲突也走此分支）
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

    -- DELETE：整点观测被删，若该刻已在水位内，重开原缺口
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
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS obs_gap_trg ON observations;
CREATE TRIGGER obs_gap_trg
    AFTER INSERT OR UPDATE OR DELETE ON observations
    FOR EACH ROW EXECUTE FUNCTION obs_gap_maintain_fn();

-- ============================================================================
-- 演示种子数据：最近 6 个自然月（UTC），8 个站点，每小时一条整数测值。
-- 全部写入默认分区（INSERT 会触发缺口维护，但此时尚无水位，清单保持为空）。
-- ============================================================================
INSERT INTO observations (station_id, ts, value)
SELECT 'ST' || lpad(s::text, 3, '0'),
       date_trunc('hour', t),
       (20 + 60 * random())::int
FROM generate_series(1, 8) AS s
CROSS JOIN generate_series(
        date_trunc('month', now()) - interval '5 months',
        now() - interval '1 hour',
        interval '1 hour'
    ) AS t
ON CONFLICT (station_id, ts) DO NOTHING;
