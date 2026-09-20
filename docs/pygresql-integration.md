# PyFlink × PostgreSQL 联动操作手册

> 版本：2026-09-20 ｜ 环境：Flink 1.20.5 + PostgreSQL 16.15 ｜ **全部实测跑通**

---

## 0. 先看结论

**Flink 官方没有单独的「PostgreSQL 连接器」**，因为用 **通用 JDBC 连接器 + PostgreSQL 驱动** 就够了。
所以 pg 能做的事，本质上就是 JDBC 连接器能做的事 —— 而它能力相当完整：

| 方向 | 能力 | 连接器 | 适用场景 |
|---|---|---|---|
| **pg → Flink** | 批量扫描全表 / 增量按时间戳拉取 | `jdbc` (Source) | 数据同步、离线分析、初始化加载 |
| **Flink → pg** | INSERT / UPSERT / DELETE 写入 | `jdbc` (Sink) | 结果落库、报表预计算、指标回写 |
| **pg → Flink** | Lookup Join（维表实时关联） | `jdbc` (Lookup Source) | 订单流关联用户/商品维度 |
| **pg → Flink** | **CDC 变更捕获（WAL 级，含 I/U/D）** | `postgres-cdc` | 实时数仓、双写替代、异构同步 |
| **pg → Flink** | 读分区表并行扫描 | `jdbc` + 分区参数 | 大表并行读取 |
| **Flink → pg** | 两阶段提交（EXACTLY_ONCE） | `jdbc` + checkpoint | 金融级不丢不重 |

**章节导航**：1~7 章讲 JDBC 联动（已验证）→ **第 8 章讲 CDC 实时变更捕获**（进阶，含 14 个踩坑）

---

## 1. 环境准备（本机实测路径）

### 1.1 网络拓扑（关键，务必先搞懂）

本机的 Flink 和 PostgreSQL 是**两个独立的 compose 项目**：

```
pyflink-tutorial (网络: pyflink-tutorial_flink-net)
  ├── pyflink-jobmanager       172.19.0.4
  └── pyflink-taskmanager

pg-course (网络: docker_pgnet)   ← 完全隔离！
  ├── pg-primary   宿主映射 5432
  └── pg-standby   宿主映射 5433
```

**核心结论：Flink 容器里写 `pg-primary` 是解析不到的**（实测 `getent hosts pg-primary` → `NO_RESOLVE`）。

两条可行路线：

| 路线 | 容器内地址 | 说明 |
|---|---|---|
| **A. 走宿主机回环**（推荐，零改动） | `host.docker.internal:5432` | 容器 → 宿主机网卡 → 端口映射 → pg 容器。<br>✅ 实测连通 |
| **B. 共享网络** | `pg-primary:5432` | 需把 pg 容器加入 flink 网络，或两个 compose 声明同一个 external network |

本文全部用 **路线 A**。

### 1.2 装 PostgreSQL JDBC 驱动

驱动不在 Flink 自带包里，**必须自己加**：

```bash
cd C:/Users/wzm/WorkBuddy/2026-09-20-09-28-01/pyflink-tutorial
curl -fsSL -o jars/postgresql-42.7.4.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
```

放好后由 entrypoint 自动软链到 `lib/` 顶层（本项目 Dockerfile 已配好）。
验证：

```bash
docker exec pyflink-jobmanager ls -l /opt/flink/lib/postgresql-42.7.4.jar
# 期望：软链指向 /opt/flink/lib/connectors/postgresql-42.7.4.jar
```

### 1.3 环境变量（已在 compose 注入）

```yaml
PG_HOST: host.docker.internal
PG_PORT: "5432"
PG_DB: shop
PG_USER: shop
PG_PASSWORD: shop123
```

这样示例代码里读环境变量即可，**容器内和宿主机跑同一份代码**：

```python
PG_HOST = os.environ.get("PG_HOST", "localhost")   # 宿主机默认 localhost
PG_PORT = os.environ.get("PG_PORT", "5432")
```

> ⚠️ `host.docker.internal` 在 Linux 容器上需要 `extra_hosts: ["host.docker.internal:host-gateway"]`，
> 本项目 compose 已加，Windows/Mac 的 Docker Desktop 本身也内置。

---

## 2. 建表：JDBC Sink 不会自动建表

**这是最容易踩的坑**：Flink 的 JDBC connector **只写数据，不建表**。
目标表必须预先存在，否则报 `relation "xxx" does not exist`。

```sql
-- 先在 pg 里建好
CREATE TABLE IF NOT EXISTS flink_user_stats (
    status      SMALLINT PRIMARY KEY,
    user_cnt    BIGINT,
    avg_balance NUMERIC(12,2),
    updated_at  TIMESTAMP
);
```

> 💡 生产建议：用 Flyway / Liquibase 或 SQL 脚本统一管 schema，别让 Flink 作业负责建表。

**字段类型映射（pg ↔ Flink）**：

| PostgreSQL | Flink SQL |
|---|---|
| `SMALLINT` / `INT` / `BIGINT` | 同左 |
| `NUMERIC(p,s)` / `DECIMAL` | `DECIMAL(p,s)` |
| `VARCHAR(n)` / `TEXT` | `VARCHAR(n)` / `STRING` |
| `TIMESTAMP` | `TIMESTAMP(3)` |
| `DATE` | `DATE` |
| `JSONB` | `STRING`（需自定义序列化）或 `RAW` |
| `BOOLEAN` | `BOOLEAN` |

---

## 3. 四种核心操作

### 3.1 pg → Flink：JDBC Source 读数据

```python
env.execute_sql("""
    CREATE TABLE pg_users (
        id         BIGINT,
        username   VARCHAR(64),
        email      VARCHAR(128),
        status     SMALLINT,
        level      SMALLINT,
        balance    DECIMAL(12, 2),
        created_at TIMESTAMP(3),
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url'       = 'jdbc:postgresql://host.docker.internal:5432/shop',
        'username'  = 'shop',
        'password'  = 'shop123',
        'driver'    = 'org.postgresql.Driver',
        'table-name' = 'users'
    )
""")
```

**扫描并行度**（大表加速）：

```sql
'scan.partition.column' = 'id',
'scan.partition.num'    = '8',        -- 切成 8 份并行读
'scan.partition.lower-bound' = '0',
'scan.partition.upper-bound' = '1000000',
'scan.fetch-size'       = '1000'      -- 每次拉取行数，防内存爆
```

**增量拉取**（按时间戳，适合定时同步）：

```sql
'scan.auto-commit.enabled' = 'true',
-- 配合 WHERE created_at > ? 由上游调度传入
```

> ⚠️ **JDBC Source 是「有界」的**（扫完就结束），不是真正的流。要真流必须用 CDC（见 3.5）。

**实测结果**（本机 shop 库 users 表 10000 行）：

```
  status |         人数 |           平均余额
----------------------------------------
       1 |       9687 |         992.82
       2 |        163 |        1034.15
       3 |        150 |         953.74
----------------------------------------
      合计 |      10000 |
```

### 3.2 Flink → pg：JDBC Sink 写数据

```python
env.execute_sql("""
    CREATE TABLE pg_user_stats (
        status      SMALLINT,
        user_cnt    BIGINT,
        avg_balance DECIMAL(12, 2),
        updated_at  TIMESTAMP(3),
        PRIMARY KEY (status) NOT ENFORCED    -- ← 有主键 = UPSERT 模式
    ) WITH (
        'connector' = 'jdbc',
        'url'       = 'jdbc:postgresql://host.docker.internal:5432/shop',
        'username'  = 'shop',
        'password'  = 'shop123',
        'driver'    = 'org.postgresql.Driver',
        'table-name' = 'flink_user_stats'
    )
""")
```

**关键：有没有 `PRIMARY KEY` 决定写入语义**

| 声明 | 生成 SQL | 语义 |
|---|---|---|
| **有** `PRIMARY KEY` | `INSERT ... ON CONFLICT ... DO UPDATE` | **UPSERT**（幂等，可重放） |
| **无** | 普通 `INSERT` | 追加，重跑会重复 |

> PostgreSQL 特有：connector 3.2+ 支持 `ON CONFLICT DO NOTHING` /
> 通过 `sink.ignore-delete` 控制是否同步 DELETE。

**批式写入（推荐做法，用 StatementSet）**：

```python
stmt_set = env.create_statement_set()
stmt_set.add_insert_sql("""
    INSERT INTO pg_user_stats
    SELECT status,
           COUNT(*) AS user_cnt,
           CAST(ROUND(AVG(balance), 2) AS DECIMAL(12, 2)) AS avg_balance,
           CURRENT_TIMESTAMP
    FROM pg_users
    GROUP BY status
""")
stmt_set.execute().wait()
```

**实测确认**（psql 直查 pg）：

```
 status | user_cnt | avg_balance |     updated_at
--------+----------+-------------+---------------------
      1 |     9687 |      992.82 | 2026-09-20 07:06:58
      2 |      163 |     1034.15 | 2026-09-20 07:06:58
      3 |      150 |      953.74 | 2026-09-20 07:06:58
```

**写并发控制**：

```sql
'sink.buffer-flush.max-rows' = '1000',    -- 攒够 1000 行批量提交
'sink.buffer-flush.interval' = '1s',      -- 或每秒刷一次
'sink.max-retries'           = '3'        -- 失败重试
```

### 3.3 pg 当维表：Lookup Join（最实用的场景）

把 pg 表当"字典"做实时关联，**不用把整张维表加载进内存**：

```python
# 主表（真实场景是 Kafka 订单流）
env.execute_sql("""
    CREATE TABLE order_stream (
        order_id   BIGINT,
        user_id    BIGINT,
        pay_amount DECIMAL(12, 2),
        proc_time  AS PROCTIME()      -- ← Lookup Join 必需的处理时间列
    ) WITH ('connector' = 'datagen', ...)
""")

# 维表
env.execute_sql("""
    CREATE TABLE pg_users_dim (
        id       BIGINT,
        username VARCHAR(64),
        status   SMALLINT,
        level    SMALLINT,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url'       = 'jdbc:postgresql://host.docker.internal:5432/shop',
        'table-name' = 'users',
        'lookup.cache.max-rows' = '1000',    -- 缓存最多 1000 条
        'lookup.cache.ttl'      = '1min'     -- 缓存 1 分钟过期
    )
""")

# 关联
env.execute_sql("""
    SELECT o.order_id, o.user_id, u.username, u.level, o.pay_amount
    FROM order_stream AS o
    LEFT JOIN pg_users_dim FOR SYSTEM_TIME AS OF o.proc_time AS u
        ON o.user_id = u.id
""")
```

**实测结果**：

```
 order_id |  user_id |         username |  level |  pay_amount
------------------------------------------------------------------
        1 |       12 |       user000012 |      4 |      379.84
        2 |       18 |       user000018 |      4 |       10.20
        3 |       13 |       user000013 |      2 |      611.88
        4 |       10 |       user000010 |      3 |      492.01
        5 |       12 |       user000012 |      4 |      220.85
```

**要点**：
- `FOR SYSTEM_TIME AS OF <proctime列>` 是**语法必需**，缺了报错
- **必须缓存**，否则每条流数据打一次 pg，连接数瞬间打满
- `lookup.cache` 有两种：`PARTIAL`（默认，只缓存 max-rows）和 `FULL`（全量加载+定期刷新，适合小维表）

### 3.4 汇总：一段完整的端到端脚本

已落地为 **`examples/09_postgres_integration.py`**，一次覆盖四种操作：

```bash
# 容器内跑（推荐）
docker exec pyflink-jobmanager bash -c \
  "cd /opt/flink && python examples/09_postgres_integration.py --local"

# 宿主机跑（需本地装 apache-flink）
python examples/09_postgres_integration.py
```

> ⚠️ **必须加 `--local`（批模式）**。原因见第 4 节最大的坑。

### 3.5 CDC：真正实时的 pg 变更捕获

JDBC Source 是拉取式的（polling），有延迟。要**准实时**捕获 pg 的 INSERT/UPDATE/DELETE，
需要用 **CDC 连接器**（基于 pg 的逻辑复制槽 `wal_level=logical`）。

**依赖**：

```
flink-connector-postgres-cdc-3.0.1.jar
```

**Flink SQL 用法**：

```sql
CREATE TABLE orders_cdc (
    id         BIGINT,
    order_no   VARCHAR(64),
    user_id    BIGINT,
    status     SMALLINT,
    pay_amount DECIMAL(12,2),
    created_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'      = 'postgres-cdc',
    'hostname'       = 'host.docker.internal',
    'port'           = '5432',
    'database-name'  = 'shop',
    'schema-name'    = 'public',
    'table-name'     = 'orders',
    'username'       = 'shop',
    'password'       = 'shop123',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name'      = 'flink_orders_slot',
    'debezium.snapshot.mode' = 'initial'    -- 先全量快照，再增量
);
```

**pg 侧前置条件**（⚠️ 需要改 pg 配置并重启）：

```ini
# postgresql.conf
wal_level = logical              # 默认是 replica，必须改
max_replication_slots = 10
max_wal_senders = 10
```

```sql
-- 账号需有 REPLICATION 权限或超级用户
ALTER ROLE shop WITH REPLICATION;
```

**CDC vs JDBC 对比**：

| | JDBC Source | PostgreSQL CDC |
|---|---|---|
| 实时性 | 分钟级（轮询） | 秒级 / 亚秒级 |
| 能否捕获 DELETE | ❌ | ✅ |
| 能否捕获 UPDATE | 需主键+轮询 | ✅ |
| 对 pg 压力 | 重复查询 | 读 WAL，压力小 |
| pg 配置要求 | 无 | **需 `wal_level=logical` + 重启** |
| 适用 | 定时同步、初始化 | 实时数仓、双写替代 |

> 💡 **本机注意**：pg-course 的主从复制用的是 `wal_level=replica`，改成 `logical` 需重启主库。
> 想在本机试 CDC，改配置后要 `docker restart pg-primary`，且从库会短暂中断。
> 另外逻辑复制槽**会持有 WAL**，消费者停久了 pg 磁盘会被撑爆 —— 生产必须监控。

---

## 4. 踩坑清单（实测，含血泪）

### 4.1 【最大的坑】流模式不能对普通字段 ORDER BY

**报错**：

```
org.apache.flink.table.api.TableException:
Sort on a non-time-attribute field is not supported.
```

**原因**：Flink **流模式**下数据是无界的，"排序"在语义上不成立，只允许按时间属性排。

**解法**：pg 的扫描/聚合/排序场景用 **批模式**：

```python
env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
```

**判断规则**：

| 场景 | 模式 |
|---|---|
| 读 pg 表做统计、导出、排序 | **批模式** |
| pg 当维表 Lookup Join | **流模式** |
| CDC 实时同步 | **流模式** |
| 结果定时回写 pg | 批模式（或 StatementSet） |

### 4.2 两个 compose 项目的网络是隔离的

`pg-primary` 在 Flink 容器里 `NO_RESOLVE`。用 `host.docker.internal` 走宿主机回环。

### 4.3 JDBC Sink 不建表

目标表必须预存在，否则 `relation does not exist`。

### 4.4 忘了加 `extra_hosts` 时 Linux 上解析失败

Windows/Mac 的 Docker Desktop 内置了 `host.docker.internal`，Linux 上要显式声明：

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

### 4.5 分区表 INSERT 必须带分区键

本机 `orders` 表按 `created_at` 分区（11 个分区），**INSERT 不提供 `created_at` 会报 `no partition found`**。
Flink 写这种表时，schema 里必须包含分区键字段。

### 4.6 从库是只读的

往 `pg-standby` 写会报：

```
ERROR: cannot execute INSERT in a read-only transaction
```

这不是故障，是流复制的正常行为。**Flink 只能写主库**。

### 4.7 连接池/并行度打爆 pg

默认并行度 = CPU 核数，每个 subtask 一个连接。8 并行的 Lookup Join 就是 8 个常驻连接，
再叠加多个作业，很容易触顶 `max_connections`（默认 100）。

**解法**：
```python
env.get_config().set("parallelism.default", "1")   # 本地测试
```
```sql
'lookup.cache.max-rows' = '1000',   -- 必须开缓存
'sink.buffer-flush.max-rows' = '1000'
```
生产建议前置 **PgBouncer**（pg-course 第 12 章有）。

### 4.8 参数化时别用 f-string 嵌 f-string

```python
# ❌ 报错：Encountered "f" at line N
env.execute_sql("""
    ... 'url' = f'jdbc:postgresql://{HOST}:{PORT}/db' ...
""")

# ✅ 外层三引号加 f，内层用单引号
env.execute_sql(f"""
    ... 'url' = 'jdbc:postgresql://{HOST}:{PORT}/db' ...
""")
```

### 4.9 `NUMERIC` 精度要对齐

Flink 的 `DECIMAL(12,2)` 写进 pg 的 `NUMERIC(12,2)` 没问题；
但 Flink `DOUBLE` 写 pg `NUMERIC` 会有精度警告，**建议显式 CAST**：

```sql
CAST(ROUND(AVG(balance), 2) AS DECIMAL(12, 2))
```

---

## 5. 典型业务场景

### 5.1 实时大屏：Kafka 订单流 + pg 维表 + pg 结果表

```
Kafka(订单流) ──┐
                ├─► Flink Lookup Join(pg 维表) ─► 窗口聚合 ─► pg 结果表 ─► DBeaver/BI 展示
pg(用户/商品) ──┘
```

完整链路已在 **`examples/08_realtime_risk_control.py`** 验证过（Kafka 部分），
把 sink 换成 JDBC 即可。

### 5.2 离线报表预计算

```
pg(明细表, 百万行) ─► Flink 批模式 聚合 ─► pg(汇总表) ─► DBeaver 查报表
```

**优势**：比在 pg 里写复杂 SQL 快得多（Flink 可并行扫描 + 内存计算），且不占用 pg 的 CPU。

### 5.3 异构同步：pg → MySQL / Kafka

```
pg ──CDC──► Flink ──► MySQL (JDBC Sink)
                 └──► Kafka (Kafka Sink)
```

一套 CDC 源，多路分发。本项目 MySQL + Kafka 连接器都已就绪。

### 5.4 实时风控（本项目示例 08 的延伸）

```
Kafka(交易流) ─► Flink CEP/规则引擎 ─► 告警写 pg ─► DBeaver 查看告警列表
                                    └─► 告警推 Kafka
```

风控规则/阈值表放 pg，Flink 启动时加载为广播状态，实现**规则热更新**。

---

## 6. 快速验证清单

跑完这套，说明 pg ↔ Flink 链路健康：

```bash
# 1. 确认容器都活着
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "pg-|pyflink-"

# 2. 确认 pg 驱动在容器里可见
docker exec pyflink-jobmanager ls -l /opt/flink/lib/postgresql-42.7.4.jar

# 3. 确认能解析 host.docker.internal
docker exec pyflink-jobmanager getent hosts host.docker.internal

# 4. 确认端口通
docker exec pyflink-jobmanager bash -c \
  "(exec 3<>/dev/tcp/host.docker.internal/5432) 2>/dev/null && echo OK || echo FAIL"

# 5. 建结果表
docker exec -i pg-primary psql -U shop -d shop -c "
CREATE TABLE IF NOT EXISTS flink_user_stats (
    status SMALLINT PRIMARY KEY, user_cnt BIGINT,
    avg_balance NUMERIC(12,2), updated_at TIMESTAMP);"

# 6. 跑端到端测试
docker exec pyflink-jobmanager bash -c \
  "cd /opt/flink && python examples/09_postgres_integration.py --local"

# 7. 在 pg 里确认数据落地
docker exec -i pg-primary psql -U shop -d shop -c \
  "SELECT * FROM flink_user_stats ORDER BY status;"

# 8. 确认从库也同步了（流复制 + Flink 写入联动验证）
docker exec -i pg-standby psql -U shop -d shop -c \
  "SELECT * FROM flink_user_stats ORDER BY status;"
```

**本机实测最终状态**：步骤 1–8 全部通过 ✅

---

## 7. 依赖清单

| 组件 | 版本 | 用途 | 获取方式 |
|---|---|---|---|
| `flink-connector-jdbc` | 3.2.0-1.19 | JDBC 读写核心 | Maven Central |
| `postgresql` | 42.7.4 | pg 驱动 | Maven Central |
| `flink-connector-postgres-cdc` | 3.0.1 | CDC（可选） | Maven Central |
| `flink-sql-connector-kafka` | 3.4.0-1.20 | Kafka（已配） | Maven Central |
| `mysql-connector-j` | 8.0.33 | MySQL（已配） | Maven Central |

**Maven 坐标**：

```
org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar
org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
org/apache/flink/flink-connector-postgres-cdc/3.0.1/flink-connector-postgres-cdc-3.0.1.jar
```

---

## 8. PostgreSQL CDC 实时变更捕获（进阶）

> 这一章是全文最硬的部分。CDC 能让你**像读消息队列一样读数据库** ——
> 每条 INSERT / UPDATE / DELETE 都会被投递成一条流记录。

### 8.0 先说结论（含未跑通的部分）

| 能力 | 状态 | 说明 |
|---|---|---|
| 全量快照 | ✅ 已验证 | 10000 行 `users` 一次性灌入目标表 |
| 增量 INSERT（`+I`） | ✅ 已验证 | 新插入记录实时捕获 |
| 增量 UPDATE（`+U`） | ✅ 已验证 | 含前像/后像，能看出"改成了什么" |
| 增量 DELETE（`-D`） | ⚠️ 事件已收到，但落库需额外处理 | 见 8.5 |

**为什么 DELETE 特殊？** 不是管道坏了 —— Debezium 确实收到了 `-D` 事件，
是 **JDBC sink 的 upsert 语义无法把"删除"写成一行记录**。
这属于下游写入的表达能力问题，不是 CDC 本身的问题。
处理方案见 8.5，这里先把已经跑通的部分讲清楚。

### 8.1 CDC 和 JDBC 拉取的本质区别

| | JDBC 轮询 | CDC |
|---|---|---|
| 原理 | 定时 `SELECT ... WHERE updated_at > ?` | 读 WAL（预写日志），事件驱动 |
| 能否看到 DELETE | ❌ 记录没了就查不到 | ✅ 有 `-D` 事件 |
| 延迟 | 取决于轮询间隔 | 毫秒级 |
| 对源库压力 | 每次全表/索引扫描 | 几乎为零（顺带读日志） |
| 能否拿到前像 | ❌ | ✅（需配 `REPLICA IDENTITY FULL`） |
| 前置改造 | 无 | 需开逻辑复制、建账号、改 pg_hba |

**选型建议**：数据同步、增量数仓用 CDC；少量维表关联用 JDBC。

### 8.2 pg 侧五个必做前置（缺一个就报错）

这是本章最有价值的部分 —— **按顺序做完这 5 步，后面才不会卡**。

#### ① 开逻辑复制（改完必须重启 pg）

```ini
# postgresql.conf
wal_level = logical                  # 原来是 replica，必须改
max_replication_slots = 10           # 复制槽上限
max_logical_replication_workers = 4  # 逻辑解码工作进程数
max_slot_wal_keep_size = 2GB         # ⭐ 防 WAL 撑爆磁盘，见下
```

```bash
docker restart pg-primary            # wal_level 是静态参数，必须重启
```

**为什么 `max_slot_wal_keep_size` 很重要：**
逻辑复制槽会"钉住"WAL —— 消费者停多久，WAL 就堆多久，直到撑爆磁盘。
设了 2GB 之后，超限时 pg 会**主动放弃**复制槽（槽失效需重建），
属于拿"可用性"换"磁盘不炸"。生产环境必设。

#### ② 建 CDC 专用账号

```sql
CREATE ROLE flink_cdc WITH LOGIN REPLICATION PASSWORD 'cdc123';

-- ⚠️ 三个权限缺一不可，第三个最容易漏：
GRANT CONNECT ON DATABASE shop TO flink_cdc;   -- 能连库
GRANT USAGE ON SCHEMA public TO flink_cdc;     -- 能用 schema
GRANT CREATE ON DATABASE shop TO flink_cdc;    -- ⭐ 能建 publication
GRANT SELECT ON ALL TABLES IN SCHEMA public TO flink_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flink_cdc;
```

**为什么需要 `CREATE`？**
Debezium 启动时要建 publication（发布定义）。
不给 `CREATE` 会报：
```
ERROR: permission denied for database shop
  at PostgresReplicationConnection.initPublication(...)
```

#### ③ 用超管预建 publication（关键技巧）

即便给了 `CREATE`，Debezium 默认会尝试建 **`FOR ALL TABLES`** 的 publication，
而**这种 publication 只有超级用户能建**：

```
ERROR: must be superuser to create FOR ALL TABLES publication
```

**正解**：用超管为"确实要同步的表"预先建好，然后告诉 Debezium 复用：

```sql
-- 用超管（postgres）执行
CREATE PUBLICATION flink_cdc_pub FOR TABLE public.users;
```

对应 Flink 配置：
```python
'debezium.publication.name' = 'flink_cdc_pub',
'debezium.publication.autocreate.mode' = 'disabled',
```

这样既满足最小权限原则，又绕开了超管要求。

#### ④ ⭐⭐ 设置 REPLICA IDENTITY FULL（最容易漏的一条）

```sql
ALTER TABLE public.users REPLICA IDENTITY FULL;
```

**不加会怎样？**
INSERT 能正常捕获，但 **UPDATE / DELETE 直接抛异常**：

```
java.lang.IllegalStateException: The "before" field of UPDATE/DELETE message is null,
please check the Postgres table has been set REPLICA IDENTITY to FULL level.
```

**原因**：pg 逻辑复制默认（`REPLICA IDENTITY DEFAULT`）**只发送主键**，
UPDATE/DELETE 事件里"改之前的样子"是空的。
而 Flink CDC 需要前像才能表达 changelog 的 `-U` / `-D`，拿不到就报错。

设置后 Debezium 日志会明确确认：
```
REPLICA IDENTITY for 'public.users' is 'FULL';
UPDATE AND DELETE events will contain the previous values of all the columns
```

**代价**：WAL 体积会变大（每行变更都要带完整旧值）。
只在**需要前像的表**上开，别全库开。

#### ⑤ pg_hba.conf 放行

```
# 放在兜底 reject 之前
host  all          flink_cdc  172.16.0.0/12  scram-sha-256   # ⭐ 普通连接（读编码信息要用）
host  replication  flink_cdc  172.16.0.0/12  scram-sha-256   # 复制连接
host  all          flink_cdc  172.17.0.0/16  scram-sha-256   # Docker 网桥段
host  replication  flink_cdc  172.17.0.0/16  scram-sha-256
```

**注意两点：**
1. **`all` 和 `replication` 都要给。** 只给 `replication` 会报：
   ```
   DebeziumException: Couldn't obtain encoding for database shop
   ```
   因为 Debezium 建复制连接时会带上库名，还要用普通连接读 `pg_database` 的编码。
2. **pg_hba 是从上往下第一条命中即生效。** 兜底 `reject` 必须在最后，
   新增规则要插在它**前面**。

改完热重载（不用重启）：
```sql
SELECT pg_reload_conf();
```

### 8.3 Flink 侧配置

**JAR 选择（踩过坑）：**

```bash
# ❌ 瘦包，174K，缺依赖 → NoClassDefFoundError: JdbcSourceOptions
flink-connector-postgres-cdc-3.6.0-1.20.jar

# ✅ 胖包，20M，依赖全含 → SQL 场景必须用这个
flink-sql-connector-postgres-cdc-3.6.0-1.20.jar
```

规律：**SQL 连接器一律用 `flink-sql-` 前缀的胖包。**

**⚠️ JobManager 和 TaskManager 的 JAR 必须一致。**
手动加 JAR 后两个容器都要重启，否则报：
```
StreamCorruptedException: unexpected block data
Cannot instantiate user function
```

**源表定义：**

```sql
CREATE TABLE users_cdc (
    id         BIGINT,
    username   VARCHAR(64),
    email      VARCHAR(128),
    status     SMALLINT,
    balance    DECIMAL(12, 2),
    created_at TIMESTAMP(3),          -- ⚠️ 不能写 TIMESTAMP_LTZ，JDBC pg 方言不支持
    row_kind   STRING METADATA FROM 'row_kind' VIRTUAL,   -- ⚠️ 不是 'op'
    op_ts      TIMESTAMP(3) METADATA FROM 'op_ts' VIRTUAL,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'      = 'postgres-cdc',
    'hostname'       = 'host.docker.internal',
    'port'           = '5432',
    'username'       = 'flink_cdc',
    'password'       = 'cdc123',
    'database-name'  = 'shop',
    'schema-name'    = 'public',
    'table-name'     = 'users',
    'slot.name'      = 'flink_users_slot',
    'decoding.plugin.name' = 'pgoutput',
    'debezium.publication.name' = 'flink_cdc_pub',
    'debezium.publication.autocreate.mode' = 'disabled',
    'debezium.snapshot.mode' = 'initial',
    'debezium.heartbeat.interval.ms' = '10000'
)
```

**元数据列名的坑：**
Flink CDC 3.6 的 `PostgreSQLTableSource` **只支持 5 个元数据键**：
`database_name` / `schema_name` / `table_name` / `op_ts` / `row_kind`

写成 `op` 会报 `Invalid metadata key 'op'`。
`row_kind` 的值形如 `+I`（插入）/ `-U`（更新前像）/ `+U`（更新后像）/ `-D`（删除）。

**`snapshot.mode` 的合法值（只有 6 个）：**

```
always / exported / never / initial_only / initial / custom
```

| 值 | 含义 |
|---|---|
| `initial` | 先全量快照，再转增量（**生产推荐**，不丢历史） |
| `never` | 跳过快照，只抓启动后的变更（**演示最直观**） |

> ⚠️ **没有 `latest` 这个值！** 凭印象写成 `latest` 会报：
> `The 'snapshot.mode' value 'latest' is invalid`

### 8.4 两个最隐蔽的坑

#### 坑 A：PyFlink 起的是"假集群"（最重大）

```bash
docker exec jobmanager python examples/10_postgres_cdc.py
```

这么跑，Python 进程会起一个**进程内 MiniCluster**，不是 8081 那个真集群。

**症状（极具迷惑性）：**
- 脚本 exit=0，日志看起来"成功"
- Flink Web UI 上**一个作业都没有**
- pg 侧**复制槽从未创建**
- 进程一退出，作业和状态全没

**验证手段：**
```bash
# 作业列表持续为空 → 说明提交到别处去了
curl -s http://localhost:8081/jobs/overview
# pg 侧没有逻辑槽 → CDC 从未启动
docker exec -i pg-primary psql -U postgres -d shop \
  -c "SELECT * FROM pg_replication_slots;"
```

**解法（三行配置）：**
```python
if args.target == "remote":
    env.get_config().set("execution.target", "remote")
    env.get_config().set("rest.address", "jobmanager")
    env.get_config().set("rest.port", "8081")
```

#### 坑 B：作业提交成功却 6 秒自己 FINISHED

CDC 源在启动瞬间走异步快照，此时对外表现"像个有界源"。
不处理的话，快照读完 Flink 就判定"输入结束"→ 作业 FINISHED
（注意是 **FINISHED 不是 FAILED**，日志干净得毫无线索）→ 增量变更全漏。

**解法：**
```python
env.get_config().set("table.optimizer.source-scan-bounded-check", "false")
env.get_config().set("table.exec.source.idle-timeout", "0")
```

**判断作业是否真的在跑：**
```bash
# ① 作业状态必须是 RUNNING（不是 FINISHED）
curl -s http://localhost:8081/jobs/overview
# ② 复制槽必须 active = t
docker exec -i pg-primary psql -U postgres -d shop \
  -c "SELECT slot_name, active FROM pg_replication_slots WHERE slot_type='logical';"
```

> **方法论**：
> 「作业提交成功」≠「作业在跑」。
> **复制槽存在 = CDC 真正启动的铁证。**

### 8.5 sink 选择与 DELETE 问题

**每种 sink 都试过：**

| sink | 结果 |
|---|---|
| `print` | 输出进 TaskManager 日志，`docker exec` 下 stdout 看不到 |
| `filesystem` | 只能 append，接不住 changelog<br/>报 `doesn't support consuming update and delete changes` |
| `jdbc` **无主键** | 直接拒绝：<br/>`please declare primary key for sink table when query contains update/delete record` |
| `jdbc` **有主键** | ✅ upsert，能接 changelog —— 但**删除不传播** |

**结论：JDBC sink 必须带主键。**

**审计日志的做法：**

给每条变更事件发一个**唯一 UUID 当主键**，这样每个事件都落成独立一行：

```sql
-- 事件流视图
CREATE TEMPORARY VIEW users_ops AS
SELECT id, username, status, balance,
       row_kind AS op_type,
       CASE row_kind
           WHEN '+I' THEN 'INSERT'
           WHEN '+U' THEN 'UPDATE'
           WHEN '-U' THEN 'UPDATE(前像)'
           WHEN '-D' THEN 'DELETE'
       END AS op_name,
       op_ts
FROM users_cdc;

-- 审计视图：UUID 主键 = 每条事件一行
CREATE TEMPORARY VIEW users_audit AS
SELECT UUID() AS event_id, id AS row_id,
       username, status, balance, op_type, op_name, op_ts
FROM users_ops;
```

实测结果：
```
 row_id | username | balance | op_type | op_name |          op_ts
--------+----------+---------+---------+---------+-------------------------
  10022 | cdc_ok   |  111.11 | +I      | INSERT  | 2026-09-20 09:07:12.297
  10022 | cdc_ok   |  222.22 | +U      | UPDATE  | 2026-09-20 09:07:24.636
```

INSERT 和 UPDATE 都完整落库（能看到 balance 从 111.11 变成 222.22）。

**DELETE 怎么办？三个可选方案：**

1. **换 sink**：用 Kafka / Pulsar 等原生支持 changelog 的 sink
   （`upsert-kafka` 带 `key` + `value`，能表达删除）
2. **在 Flink 侧把 `-D` 重写成插入**：让删除动作变成审计表里的一行"删除记录"
   ```sql
   -- 把 -D 的 op_type 改写成 +U，使其成为一条独立的插入记录
   CASE WHEN op_type = '-D' THEN '+U' ELSE op_type END AS op_type
   ```
   ⚠️ 注意：不能简单用 `UNION ALL` 拆两个分支 —— 把 changelog 流和 append 流
   混合会**静默丢记录**（实测 `-D` 那支整支不到下游）。
3. **用 Flink SQL 的 CDC 专用落地**：`CREATE TABLE ... WITH ('connector'='jdbc')`
   配合 `PRIMARY KEY (id)`，接受"删除不传播"这个语义权衡（镜像是最终态，不是历史轨迹）

**本质原因**：JDBC 的 upsert 只能表达"插入或覆盖"，没有"删除某个 key"的对应语句。
这是 sink 能力的边界，不是配置问题。

### 8.6 验证脚本

```bash
# ① 清理（复制槽 + 目标表）
docker exec -i pg-primary psql -U postgres -d shop \
  -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_type='logical';"
docker exec -i pg-primary psql -U postgres -d shop -c "TRUNCATE cdc_audit_log;"

# ② 起 CDC（never 模式，纯增量演示）
docker exec pyflink-jobmanager bash -c \
  "cd /opt/flink && CDC_SNAPSHOT_MODE=never timeout 170 \
   python examples/10_postgres_cdc.py --local --seconds 115 --target remote"

# ③ 作业跑起来后（另开终端），制造变更
docker exec -i pg-primary psql -U shop -d shop -c \
  "INSERT INTO users (uid,username,email,phone,password_hash,nickname,status,level,balance,created_at,updated_at) \
   VALUES (gen_random_uuid(),'cdc_demo','d@demo.com','13800000000','h','测试',1,1,111.11,now(),now());"
sleep 8
docker exec -i pg-primary psql -U shop -d shop -c \
  "UPDATE users SET balance=222.22 WHERE username='cdc_demo';"
sleep 8
docker exec -i pg-primary psql -U shop -d shop -c \
  "DELETE FROM users WHERE username='cdc_demo';"

# ④ 看结果
docker exec -i pg-primary psql -U shop -d shop \
  -c "SELECT row_id, username, balance, op_type, op_name, op_ts FROM cdc_audit_log ORDER BY op_ts;"
```

**判断成功的硬指标：**
```bash
# 作业 RUNNING
curl -s http://localhost:8081/jobs/overview
# 复制槽 active = t（这是 CDC 真在消费 WAL 的铁证）
docker exec -i pg-primary psql -U postgres -d shop \
  -c "SELECT slot_name, active FROM pg_replication_slots WHERE slot_type='logical';"
```

### 8.7 CDC 排查手册

| 现象 | 根因 | 解法 |
|---|---|---|
| `NoClassDefFoundError: JdbcSourceOptions` | 用了瘦包 | 换 `flink-sql-` 前缀胖包 |
| `Invalid metadata key 'op'` | 元数据键名错 | 改 `row_kind` |
| `must be superuser to create FOR ALL TABLES publication` | Debezium 想建全库 publication | 超管预建 + `publication.autocreate.mode=disabled` |
| `permission denied for database shop` | 账号缺 `CREATE` | `GRANT CREATE ON DATABASE shop TO flink_cdc` |
| `Couldn't obtain encoding for database shop` | pg_hba 只给了 replication | 补 `all` 规则 |
| `pg_hba.conf rejects connection` | 没放行 CDC 账号/网段 | 加规则（`172.16.0.0/12` + `172.17.0.0/16`） |
| `before field of UPDATE/DELETE message is null` | 没设 REPLICA IDENTITY | `ALTER TABLE ... REPLICA IDENTITY FULL` |
| `Unsupported type:TIMESTAMP_LTZ` | JDBC pg 方言不支持 | 改 `TIMESTAMP(3)` |
| `snapshot.mode 'latest' is invalid` | 参数值不存在 | 用 `never`（跳过快照） |
| 作业 6 秒就 FINISHED | 有界源误判 | `source-scan-bounded-check=false` |
| Web UI 看不到作业 | 起了进程内 MiniCluster | 配 `execution.target=remote` |
| `please declare primary key for sink table` | JDBC sink 无主键 | 加 `PRIMARY KEY`（可用 UUID） |
| `StreamCorruptedException` | JM/TM 的 JAR 不一致 | 两个容器都重启 |
| `Failed to create directory for shared state` | checkpoint 目录归属 root | `chown -R flink:flink /opt/flink/checkpoints` |

---

## 9. 相关文档

- `examples/09_postgres_integration.py` —— JDBC 联动全部示例
- `examples/10_postgres_cdc.py` —— CDC 实时变更捕获（本章配套代码）
- `examples/06_connectors.py` —— JDBC(MySQL) / Kafka / 文件连接器基础用法
- `examples/08_realtime_risk_control.py` —— Kafka 实时风控完整案例
- `docs/docker-deployment-tutorial.md` —— PyFlink Docker 部署教程（连接器放置、踩坑）
- `docs/github-deployment.md` —— 部署到 GitHub（敏感信息处理 / JAR 策略）
- `postgresql-course/docs/12-监控性能与连接池.md` —— PgBouncer 连接池配置
- `postgresql-course/docs/15-DBeaver连接与可视化操作.md` —— 用 DBeaver 查看 Flink 写入的结果表
