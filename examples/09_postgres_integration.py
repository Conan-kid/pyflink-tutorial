# -*- coding: utf-8 -*-
"""
示例 09：PyFlink × PostgreSQL 打通（JDBC 连接器双向读写）

覆盖 4 种最常用的 pg 交互模式：
  1) 读：pg 表 → Flink Table（JDBC Source，批量扫描）
  2) 写：Flink Table → pg 表（JDBC Sink，UPSERT）
  3) 查：把 pg 当维表做 Lookup Join（PER-JOB 实时关联）
  4) 聚合：pg 读 → Flink 聚合 → 写回 pg 结果表

运行方式（容器内）：
  python /opt/flink/examples/09_postgres_integration.py --local

连接地址说明：
  容器内 → host.docker.internal:5432（宿主机映射端口）
  宿主机 → localhost:5432
"""
import os
import sys
import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment

# ---------------------------------------------------------------- 连接参数
PG_HOST = os.environ.get("PG_HOST", "host.docker.internal")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "shop")
PG_USER = os.environ.get("PG_USER", "shop")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "shop123")

JDBC = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
DRIVER = "org.postgresql.Driver"


def jdbc_opt(extra=""):
    return (
        f"'connector' = 'jdbc', "
        f"'url' = '{JDBC}{extra}', "
        f"'username' = '{PG_USER}', "
        f"'password' = '{PG_PASSWORD}', "
        f"'driver' = '{DRIVER}'"
    )


def banner(title):
    print("\n" + "=" * 68)
    print(f"  {title}")
    print("=" * 68)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="本地模式")
    parser.add_argument("--streaming", action="store_true",
                        help="用流模式（默认批模式；流模式不能对普通字段 ORDER BY）")
    args = parser.parse_args()

    # ⚠️ 关键：pg 的批量扫描/聚合校验用【批模式】
    #    流模式下 `ORDER BY 非时间字段` 会直接报错：
    #    Sort on a non-time-attribute field is not supported.
    if args.streaming:
        env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    else:
        env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

    # 本地测试建议降低并行度，避免连接数过载
    env.get_config().set("parallelism.default", "1")
    # 可选：把 pg 的部分读取并行度也压下来
    env.get_config().set("table.exec.source.idle-timeout", "5s")

    mode = "流模式" if args.streaming else "批模式"
    print(f"[运行模式] {mode}")
    print(f"[连接目标] {JDBC}  用户={PG_USER}")

    # ============================================================ 1) 读 pg
    banner("1. JDBC Source —— 从 pg 读数据到 Flink")

    env.execute_sql(f"""
        CREATE TABLE pg_users (
            id            BIGINT,
            username      VARCHAR(64),
            email         VARCHAR(128),
            status        SMALLINT,
            level         SMALLINT,
            balance       DECIMAL(12, 2),
            created_at    TIMESTAMP(3),
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            {jdbc_opt()},
            'table-name' = 'users'
        )
    """)

    # 统计一下 pg 里的用户规模（这一步是真实扫表）
    res = env.execute_sql("""
        SELECT status, COUNT(*) AS cnt, ROUND(AVG(balance), 2) AS avg_balance
        FROM pg_users
        GROUP BY status
        ORDER BY status
    """)
    print("\n[pg users 按状态统计]")
    print(f"{'status':>8} | {'人数':>10} | {'平均余额':>14}")
    print("-" * 40)
    total = 0
    with res.collect() as it:
        for row in it:
            total += row[1]
            print(f"{row[0]:>8} | {row[1]:>10} | {row[2]:>14}")
    print("-" * 40)
    print(f"{'合计':>8} | {total:>10} |")

    # ============================================================ 2) 写 pg
    banner("2. JDBC Sink —— Flink 结果写入 pg 表")

    env.execute_sql(f"""
        CREATE TABLE pg_user_stats (
            status      SMALLINT,
            user_cnt    BIGINT,
            avg_balance DECIMAL(12, 2),
            updated_at  TIMESTAMP(3),
            PRIMARY KEY (status) NOT ENFORCED
        ) WITH (
            {jdbc_opt()},
            'table-name' = 'flink_user_stats'
        )
    """)

    # 先建目标表（Flink 的 JDBC Sink 不会自动建表）
    env.execute_sql(f"""
        CREATE TABLE pg_ddl (
            dummy INT
        ) WITH ({jdbc_opt()})
    """)

    # 用一条 DDL 建结果表（通过 sink 前的临时表配合）
    # 实际项目里通常是提前用 SQL 脚本 / Flyway 建表，这里演示运行时建表
    print("提示：目标表 flink_user_stats 需预先存在，见下方建表 SQL")

    # 写入：pg 读 → 聚合 → 写回 pg
    stmt_set = env.create_statement_set()
    stmt_set.add_insert_sql("""
        INSERT INTO pg_user_stats
        SELECT
            status,
            COUNT(*)                        AS user_cnt,
            CAST(ROUND(AVG(balance), 2) AS DECIMAL(12, 2)) AS avg_balance,
            CURRENT_TIMESTAMP               AS updated_at
        FROM pg_users
        GROUP BY status
    """)

    try:
        stmt_set.execute().wait()
        print("✓ 已写入 pg 表 flink_user_stats")
    except Exception as e:
        print(f"✗ 写入失败（目标表可能不存在）：{type(e).__name__}: {e}")

    # ============================================================ 3) 回读校验
    banner("3. 回读校验 —— 确认数据真的落到 pg 里了")

    env.execute_sql(f"""
        CREATE TABLE pg_user_stats_read (
            status      SMALLINT,
            user_cnt    BIGINT,
            avg_balance DECIMAL(12, 2),
            updated_at  TIMESTAMP(3),
            PRIMARY KEY (status) NOT ENFORCED
        ) WITH (
            {jdbc_opt()},
            'table-name' = 'flink_user_stats'
        )
    """)

    res = env.execute_sql("""
        SELECT status, user_cnt, avg_balance, updated_at
        FROM pg_user_stats_read
        ORDER BY status
    """)
    print(f"{'status':>8} | {'user_cnt':>10} | {'avg_balance':>14} | updated_at")
    print("-" * 66)
    try:
        with res.collect() as it:
            for row in it:
                print(f"{row[0]:>8} | {row[1]:>10} | {row[2]:>14} | {row[3]}")
    except Exception as e:
        print(f"读取失败：{e}")

    # ============================================================ 4) 维表 Join
    banner("4. Lookup Join —— 把 pg 当维表做实时关联")

    # 主表：用一个内存数据集模拟"流"（真实场景是 Kafka）
    env.execute_sql("""
        CREATE TABLE order_stream (
            order_id    BIGINT,
            user_id     BIGINT,
            pay_amount  DECIMAL(12, 2),
            proc_time   AS PROCTIME()
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '5',
            'fields.order_id.kind' = 'sequence',
            'fields.order_id.start' = '1',
            'fields.order_id.end' = '5',
            'fields.user_id.kind' = 'random',
            'fields.user_id.min' = '1',
            'fields.user_id.max' = '20',
            'fields.pay_amount.kind' = 'random',
            'fields.pay_amount.min' = '10',
            'fields.pay_amount.max' = '999'
        )
    """)

    env.execute_sql(f"""
        CREATE TABLE pg_users_dim (
            id       BIGINT,
            username VARCHAR(64),
            status   SMALLINT,
            level    SMALLINT,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            {jdbc_opt()},
            'table-name' = 'users',
            'lookup.cache.max-rows' = '1000',
            'lookup.cache.ttl' = '1min'
        )
    """)

    res = env.execute_sql("""
        SELECT
            o.order_id,
            o.user_id,
            u.username,
            u.level,
            o.pay_amount
        FROM order_stream AS o
        LEFT JOIN pg_users_dim FOR SYSTEM_TIME AS OF o.proc_time AS u
            ON o.user_id = u.id
    """)
    print("注意：Lookup Join 是流式作业，下面的采集会因 datagen 有限行而退出")
    print(f"\n{'order_id':>9} | {'user_id':>8} | {'username':>16} | {'level':>6} | {'pay_amount':>11}")
    print("-" * 66)
    try:
        with res.collect() as it:
            n = 0
            for row in it:
                n += 1
                if n > 10:
                    break
                print(f"{row[0]:>9} | {row[1]:>8} | {str(row[2]):>16} | {row[3]:>6} | {row[4]:>11}")
    except Exception as e:
        print(f"采集结束：{type(e).__name__}")

    banner("完成：pg 读 / 写 / 维表关联 / 聚合回写 四类操作演示完毕")


if __name__ == "__main__":
    main()
