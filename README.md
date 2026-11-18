# 环境监测数仓 · 月分区在线迁移 + 站月缺测催补

两个子系统共用同一张 `observations` 观测表：

1. **月分区在线迁移**：把 `observations` 父表**默认分区**中的历史观测，按自然月（UTC）
   在线搬迁到新建的月分区。复制期间采集链路（独立连接）对该月的
   **新增 / 测值更正 / 删除**不中断；仅最终切换瞬间短暂锁表；失败或进程崩溃后可断点续迁；
   已完成月份重复操作不再搬迁。
2. **站月缺测催补**：采集员按站号、UTC 月份推进"催补水位"，把月初到水位之间缺少
   **同站同刻整点观测**的时刻持久化为缺口清单，安排设备补传。

## 数据模型

```sql
observations (station_id text, ts timestamptz, value integer,
              PRIMARY KEY (station_id, ts)) PARTITION BY RANGE (ts);
observations_default  -- 初始唯一分区（DEFAULT），历史数据都在这
```

元数据表：`migration_runs`（每月迁移状态机 + 进度断点）、
`migration_delta`（迁移期间触发器捕获的增量变更）。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`db` 服务首次启动会执行 `db/init.sql`：建父表/默认分区/元数据表，
并写入最近 6 个自然月 × 8 站点 × 每小时一条的整数测值（约 3 万行，全部在默认分区）。

### 本地（无 docker）

需要本机有 PostgreSQL 16 服务端二进制与 Python 3.11+，然后：

```bash
pip install -r app/requirements.txt
PGHOME=/path/to/pgsql PY=python3 bash scripts/dev_up.sh   # 初始化DB+种子+启动应用
```

## 网页操作

打开 `http://localhost:8000/` 是月分区迁移页，`http://localhost:8000/gaps` 是站月缺测催补页。

### 月分区迁移页（/）

1. **月份总览**：各月在默认分区的行数、已挂载月分区、迁移状态。
2. **迁移控制**：选月份 → 设批大小/批间休眠（慢复制）→ 开始迁移；进度条实时刷新。
   一次只迁一个月（进行中再发起会收到 409）。
3. **采集写入演示**：对选定月份手动或自动（可调间隔）做直接 SQL 增/改/删——
   每次都用**独立连接**写父表，模拟真实采集链路。
4. **父表查询**：迁移任意时刻查询父表，行数、主键重复数（恒为 0）、样本行。
5. **一致性校验**：父表行数 / 默认分区剩余（完成后为 0）/ 月分区行数三方核对。

### 站月缺测催补页（/gaps）

1. 选择**站号**与 **UTC 月份**，查询当前催补水位与持久缺口清单。
2. 输入该月某个 **UTC 整点**，把催补水位推进到该整点（**只进不退**，后退/相等/非整点/
   跨月均返回 400，且原水位与清单不变）。
3. 月初到水位之间，缺少**同站同刻整点观测**的整点进入持久缺口清单；
   **非整点记录不抵数**。
4. 采集端独立 SQL 写入区：可做整点/非整点插入、upsert、改时刻、删除，
   实时观察缺口随事务消警/重开。

迁移流程（`app/migrator.py`）：

| 阶段 | 做什么 | 是否锁写 |
|---|---|---|
| preparing | 建带 CHECK 的影子表、默认分区上挂增量触发器、登记运行记录 | 仅 DDL 瞬间 |
| waiting | 等触发器创建前开启的老事务结束（轮询 pg_stat_activity） | 否 |
| copying | 键集分页分批 `INSERT..ON CONFLICT` 复制，进度落库，可 sleep 放慢 | 否 |
| applying | 按 seq 顺序回放 `migration_delta`（insert/update→upsert，delete→删） | 否 |
| finalizing | **单事务**：SHARE 锁分区树 → 回放剩余增量 → 摘触发器 → 搬漏网行 → `ATTACH PARTITION` → 清理 → 置 done | **仅此短暂** |

正确性要点：

- 影子表在 `ATTACH` 前不属于父表，父表查询任何时刻都只见到一份数据——不重复、不缺失；
- 复制期间的所有变更由触发器落 `migration_delta`，回放幂等，可重复执行；
- 最终切换是**一个事务**，要么全部生效要么整体回滚，崩溃不会留下半吊子状态；
- 任意时刻进程死亡：影子表/增量/进度都在库里，重试从断点继续（影子表不清空、复制按
  最后主键续传、增量继续累积）；
- `migration_runs.status='done'` 的月份再次迁移直接拒绝（409），数据不变。

## 站月缺测催补的数据模型与正确性

新增两张持久化元数据表：

```sql
station_watermarks (station_id, month, watermark_ts, ...)  -- 每站每月一行，水位只进不退
station_gaps        (station_id, ts, month, opened_at,
                     PRIMARY KEY(station_id, ts))           -- 持久缺口清单，ts 必为整点
-- station_gaps(station_id, month) 索引：按站月查询直接命中，不逐次全月重算
```

缺口的自动维护完全由父表 `observations` 上的行级触发器 `obs_gap_trg` 完成
（PostgreSQL 会把父表触发器克隆到每个分区，含默认分区和后续 ATTACH 的月分区）：

| 采集端独立 SQL（各自独立事务） | 缺口清单变化 |
|---|---|
| INSERT 整点观测 | 该 (站,刻) 缺口**消警**（DELETE 清单行） |
| INSERT 非整点观测 | 不抵数，清单不变 |
| UPSERT 冲突（走 UPDATE 分支） | 整点同样消警 |
| UPDATE 改时刻 / 改站号 | 新刻消警；旧刻若已在水位内则**重开原缺口** |
| DELETE 整点观测 | 若该刻已在水位内，**重开原缺口** |
| 事务回滚 | 触发器效果随事务一并回滚，**清单不改变** |

水位推进（`app/gaps.py`）在**单事务**内完成：取按站咨询锁 → 锁水位行（`FOR UPDATE`）
→ 校验目标整点且不后退 → 更新水位 → 只对新增水位区间 `(旧水位, 目标]` 做整点序列
反连接 `observations` 的增量对账（缺观测即入清单，`ON CONFLICT DO NOTHING` 幂等）。
推进失败整体回滚，原水位与清单保持不变。

并发正确性要点：

- **按站咨询锁**串行化同站的"水位对账"与"采集写入"，消除反连接快照竞争，
  已提交结果既不误报（清单刻必有观测缺失）也不漏报（水位内缺测必在清单）；
  只锁本站，**其他站的采集写入与水位推进互不阻塞**。
- 咨询锁统一通过 `obs_gap_lock_station()` 用 `pg_try_advisory_xact_lock` + 退避自旋
  获取，而不是阻塞式加锁：本事务已持有时可安全重入，他人持有时等待但不进入死锁
  等待队列，避免"持锁事务内再写观测"的队首死锁；跨站 UPDATE 按站号字典序加双锁。
- 水位、缺口都在数据库里，**重启后按站月直接读表恢复**，查询命中
  `station_gaps(station_id, month)` 索引，不扫描 `observations`、不整月重算。
- 月分区迁移的 finalizing 事务内设 `SET LOCAL app.skip_gap_maintenance='on'`，
  搬迁只移动数据，**不制造虚假缺口，也不消掉真实缺口**；数据 ATTACH 进月分区后，
  克隆触发器继续对月分区上的迟到写入/删除做消警/重开。

## 自测

```bash
# 月分区迁移自测（需要已初始化的库；脚本自己拉起/崩溃/重启应用进程）
DATABASE_URL=postgresql://envmon@127.0.0.1:5544/envmon python3 scripts/selftest.py
# 站月缺测催补自测（进程内起真实 HTTP 服务，含迁移交错与重启持久化）
DATABASE_URL=postgresql://envmon@127.0.0.1:5544/envmon python3 scripts/selftest_gaps.py
```

覆盖：慢复制+并发直接 SQL 增删改后父表逐行一致；一次只迁一个月；已完成月份幂等；
copying/finalizing 故障注入后重试成功；复制进行中进程硬退出（`os._exit`）后
父表仍完整可查可写、重启后续迁完成；触发器/增量/默认分区残留清理检查。

## API 摘要

- `GET  /api/state` 月份总览与迁移进度
- `POST /api/migrate` `{month, batch_size, sleep_ms, fail_at}` 启动迁移（409=进行中/已完成）
- `GET  /api/query?month=` 父表查询（行数/重复数/样本）
- `POST /api/demo/write` `{op, month}` 独立连接直接 SQL 增/改/删
- `POST /api/demo/auto` `{enabled, month, interval_ms}` 自动采集写入开关
- `GET  /api/verify?month=` 一致性校验
- `POST /api/debug/crash` 进程硬退出（仅自测用）

### 站月缺测催补

- `GET  /gaps` 缺测催补网页
- `GET  /api/gaps/stations` 全部站号
- `POST /api/gaps/watermark` `{station_id, month, target_ts}` 推进水位到某 UTC 整点
  （400=后退/相等/非整点/跨月；失败回滚，原水位与清单不变）
- `GET  /api/gaps?station_id=&month=` 按站月查水位 + 持久缺口清单（索引直读，不重算）
- `GET  /api/gaps/summary?month=` 该月各站水位/缺口数概览
- `POST /api/gaps/collect` `{op, station_id, ts, value, new_ts}`
  采集端独立连接直接 SQL 写入（`insert`/`upsert`/`update_ts`/`delete`，随事务消警/重开；
  409=主键冲突等库错误，事务回滚清单不变；404=改删目标不存在）

## 已知边界（如实说明）

- 单应用进程内用互斥锁保证"一次只迁一个月"；多副本部署需改用数据库咨询锁。
- `ATTACH PARTITION` 需对父表短暂 `ACCESS EXCLUSIVE`，并会顺序扫描默认分区
  校验无越界行（演示数据量下为毫秒级）；默认分区极大时该扫描会变长，
  生产上可先把数据搬空再挂载以缩短。
- `CREATE TRIGGER` 与 `ATTACH` 等 DDL 本身需要瞬间锁，无法完全避免；
  除此之外复制/回放阶段完全不锁写。
- 种子数据与月份边界均按 UTC；父表查询按 `ts` 的 UTC 自然月切分。
- 缺口维护的按站咨询锁用 `pg_try_advisory_xact_lock` + 50ms 退避自旋，
  锁竞争激烈时写者会短暂空转等待（约 30s 超时上限），吞吐让位于正确性；
  正常采集节奏下等待为亚毫秒级。
- `station_watermarks` 的哨兵水位行为"月首前 1 小时"（表示该站月尚未推进），
  CHECK 约束据此放宽 1 小时；真实水位始终落在所属 UTC 月内。
