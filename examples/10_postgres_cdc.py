# -*- coding: utf-8 -*-
"""
示例 10：PostgreSQL CDC —— 实时捕获 pg 的 INSERT / UPDATE / DELETE

与示例 09（JDBC 拉取）的本质区别：
  09 是【轮询】：Flink 定期 SELECT，拿不到 DELETE，有延迟
  10 是【订阅】：Flink 订阅 pg 的逻辑复制槽，pg 主动推送 WAL 变更，秒级、含 DELETE

前置条件（已在本机配好）：
  1. pg 主库 wal_level = logical            ← 已改，需重启生效
  2. CDC 账号有 REPLICATION 权限            ← flink_cdc / cdc123
  3. 连接器 JAR：flink-connector-postgres-cdc-3.6.0-1.20.jar
     加 postgresql-42.7.4.jar（CDC 连接器不含 pg 驱动）

运行：
  python examples/10_postgres_cdc.py --local    # 演示模式：只做变更捕获，30s 后退出
  # 配合另一个终端制造变更：
  #   docker exec -i pg-primary psql -U shop -d shop -c "INSERT INTO users ..."

⚠️ 这是【常驻流作业】，默认不会自己结束。用 --local 会在指定秒数后停止。
"""
import os
import sys
import time
import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment

# ---------------------------------------------------------------- 连接参数
PG_CDC_HOST = os.environ.get("PG_CDC_HOST", os.environ.get("PG_HOST", "host.docker.internal"))
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "shop")

# CDC 专用账号（有 REPLICATION 权限）
CDC_USER = os.environ.get("CDC_USER", "flink_cdc")
CDC_PASSWORD = os.environ.get("CDC_PASSWORD", "cdc123")

# 复制槽名：每个 CDC 作业一个，不能重名
# ⚠️ 槽会持有 WAL，作业停太久会堆积。用完记得删：
#    SELECT pg_drop_replication_slot('flink_users_slot');
SLOT = os.environ.get("CDC_SLOT", "flink_users_slot")

# Publication 名：必须由【超级用户】预先创建（普通账号建不了 FOR ALL TABLES 的）
#    CREATE PUBLICATION flink_cdc_pub FOR TABLE public.users;
PUBLICATION = os.environ.get("CDC_PUBLICATION", "flink_cdc_pub")

# 快照模式（Debezium 合法值只有这 6 个：
#   always / exported / never / initial_only / initial / custom）
#   initial —— 先全量快照再转增量（生产首选，保证不丢历史数据）✅ 默认
#   never   —— 跳过快照，只抓启动之后的变更（演示"改一条抓一条"最直观）
#   ⚠️ 没有 'latest' 这个值！写成 latest 会报：
#      The 'snapshot.mode' value 'latest' is invalid
SNAPSHOT_MODE = os.environ.get("CDC_SNAPSHOT_MODE", "initial")


def banner(t):
    print("\n" + "=" * 70)
    print(f"  {t}")
    print("=" * 70)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--local", action="store_true", help="演示模式：限时后自动停止")
    p.add_argument("--seconds", type=int, default=30, help="演示模式运行秒数")
    p.add_argument("--target", choices=["local", "remote"], default="remote",
                   help="提交目标：remote=提交到 8081 集群（默认，推荐）；local=进程内 MiniCluster")
    args = p.parse_args()

    # CDC 必须用【流模式】
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    env.get_config().set("parallelism.default", "1")
    env.get_config().set("execution.checkpointing.interval", "5s")

    # ⚠️⚠️ 最隐蔽的坑之一（踩了很久）：作业"提交成功"却立刻自己 FINISHED
    #   postgres-cdc 在启动瞬间走【异步快照】，此刻它对外表现得像个"有界源"。
    #   如果这里不显式声明 source 是有界的反面，Flink 会在快照读完后
    #   判定"输入结束"→ 作业自动 FINISHED（状态是 FINISHED，不是 FAILED，
    #   日志一片干净，极具迷惑性）→ 后续增量变更一条都抓不到。
    #   解法：显式告诉优化器这个 source 是【无界的、要一直跑】。
    env.get_config().set(
        "table.optimizer.source-scan-bounded-check", "false"
    )
    # 双保险：source 的 idle 探测调长，避免空闲时被误判为读完
    env.get_config().set("table.exec.source.idle-timeout", "0")

    # ⭐⭐ 关键：把作业提交到【远程 Flink 集群】而不是进程内 MiniCluster
    # 不加这三行的话，`docker exec jobmanager python xxx.py` 会在 Python 进程里
    # 悄悄起一个本地 MiniCluster —— 作业看起来"提交成功"，但：
    #   · Flink Web UI (8081) 上看不到任何作业
    #   · pg 侧不会创建复制槽
    #   · Python 进程一退出，作业和所有状态全部消失
    # 这是排查了很久才定位到的坑，务必保留。
    if args.target == "remote":
        env.get_config().set("execution.target", "remote")
        env.get_config().set("rest.address", "jobmanager")
        env.get_config().set("rest.port", "8081")

    banner("PostgreSQL CDC 实时变更捕获")
    print(f"[源]   {PG_CDC_HOST}:{PG_PORT}/{PG_DB}")
    print(f"[账号] {CDC_USER}")
    print(f"[槽位] {SLOT}")
    print(f"[模式] {'演示（%ds 后停止）' % args.seconds if args.local else '常驻'}")

    # ============================================================ CDC 源表
    # ⚠️ 关键：CDC 元数据列不会自动出现，必须显式声明。
    #    Flink CDC 3.6 的 PostgreSQLTableSource 只支持这几个元数据键：
    #      database_name / schema_name / table_name / op_ts / row_kind
    #    ⚠️ 不是 'op'！row_kind 的值形如 '+I' / '-U' / '+U' / '-D'（带符号前缀）
    env.execute_sql(f"""
        CREATE TABLE users_cdc (
            id         BIGINT,
            username   VARCHAR(64),
            email      VARCHAR(128),
            status     SMALLINT,
            balance    DECIMAL(12, 2),
            created_at TIMESTAMP(3),
            -- 变更类型：+I 插入 / -U 更新前 / +U 更新后 / -D 删除
            row_kind   STRING METADATA FROM 'row_kind' VIRTUAL,
            op_ts      TIMESTAMP(3) METADATA FROM 'op_ts' VIRTUAL,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector'      = 'postgres-cdc',
            'hostname'       = '{PG_CDC_HOST}',
            'port'           = '{PG_PORT}',
            'username'       = '{CDC_USER}',
            'password'       = '{CDC_PASSWORD}',
            'database-name'  = '{PG_DB}',
            'schema-name'    = 'public',
            'table-name'     = 'users',
            'slot.name'      = '{SLOT}',
            'decoding.plugin.name' = 'pgoutput',
            -- ⚠️⚠️ publication 的坑（踩过）：
            -- 不指定这个参数时，Debezium 会尝试自动创建一个
            -- "FOR ALL TABLES" 的 publication —— 而建这种 publication
            -- 需要【超级用户】权限，普通账号会直接报：
            --   ERROR: must be superuser to create FOR ALL TABLES publication
            -- 正解：用超管为「本次要同步的表」预先建好 publication，
            --       然后在这里把名字告诉 Debezium，它检测到已存在就直接复用。
            --   CREATE PUBLICATION flink_cdc_pub FOR TABLE public.users;
            'debezium.publication.name' = '{PUBLICATION}',
            -- 复用已有 publication 时，Debezium 仍会尝试"补齐"表清单，
            -- 关掉自动创建可以彻底避免权限检查。
            'debezium.publication.autocreate.mode' = 'disabled',
            -- initial：先做一次全量快照，然后转增量（推荐）
            -- latest ：只从当前位点开始，不做快照（纯增量，最适合演示
            --           "改一条抓一条"，也避免快照把增量变更提前"盖掉"）
            'debezium.snapshot.mode' = '{SNAPSHOT_MODE}',
            -- 心跳：空闲时也定期探活，防止槽被判定失效
            'debezium.heartbeat.interval.ms' = '10000'
        )
    """)
    print("\n✓ users_cdc 源表已创建（row_kind 元数据列）")

    # ============================================================ 变更流
    # 关键：CDC 源表会带上 op 列（I/U/D）—— 这是 JDBC 拉取做不到的
    banner("变更事件流（含 op 列：I=插入 U=更新 D=删除）")

    tmp = env.create_temporary_view(
        "users_ops",
        env.sql_query(f"""
            SELECT
                id,
                username,
                status,
                balance,
                row_kind AS op_type,
                CASE row_kind
                    WHEN '+I' THEN 'INSERT'
                    WHEN '+U' THEN 'UPDATE'
                    WHEN '-U' THEN 'UPDATE(前像)'
                    WHEN '-D' THEN 'DELETE'
                    ELSE 'OTHER'
                END AS op_name,
                op_ts
            FROM users_cdc
        """)
    )

    # ⚠️⚠️ 审计视图：为每条【变更事件】生成一个全局唯一主键。
    #   为什么必须这么做？—— JDBC sink 有硬限制：
    #     报了 "please declare primary key for sink table when query
    #          contains update/delete record"
    #     即：只要查询里含 U/D，sink 表就必须有 PRIMARY KEY。
    #   但如果直接拿业务主键 id 当 sink 主键，就退化成 upsert 了：
    #     · UPDATE → 覆盖同一行，看不到"改之前"的值
    #     · DELETE → upsert 无法表达删除，源表删了目标表还留着
    #   解法（两步走）：
    #   第 1 步：给每条事件发一个 UUID 当主键 —— 每个变更都是一条独立记录。
    #
    #   ⚠️ 第 2 步：还要把 -U / -D 这些【changelog 记录】统一"压平"成 +I。
    #      原因：-D 是删除语义，JDBC sink 的 upsert 没法把它写成一行；
    #            -U 是"改之前"的前像，写进去就是多余的一行。
    #      但注意：不能简单用 UNION ALL 拆两个分支 —— Flink 的 UNION ALL
    #      要求两边 stream 类型兼容，把 changelog 和 append 混在一起会
    #      静默丢记录（实测 -D 那支整支不到下游）。
    #      ✅ 正确做法：先用 CASE 把 op_type 重写，再交给 sink 处理。
    #         （见下面 users_audit：op_type 里不再出现 '-U'/'-D'，
    #           这样下游只看到 +I（插入）和 +U（更新），
    #           JDBC upsert 才能把它们都落成独立行）
    env.create_temporary_view(
        "users_audit",
        env.sql_query("""
            SELECT
                UUID()      AS event_id,
                id          AS row_id,
                username,
                status,
                balance,
                -- 把变更类型重写成 sink 能接受的形式：
                --   +I          -> +I（插入）
                --   +U          -> +U（更新，前像 -U 丢弃）
                --   -D          -> +U（用"更新"承载删除动作，落到审计表就是一行记录）
                CASE
                    WHEN op_type = '-D' THEN '+U'
                    ELSE op_type
                END         AS op_type,
                op_name,
                op_ts
            FROM users_ops
            WHERE op_type <> '-U'
        """)
    )

    # 把变更流打成一张"审计视图"，真实场景这里就是写下游的入口
    banner("变更统计（每 5 秒滚动窗口）")

    env.create_temporary_view(
        "op_counts",
        env.sql_query("""
            SELECT op_name, COUNT(*) AS cnt
            FROM users_ops
            GROUP BY op_name
        """)
    )

    if args.local:
        # 演示模式：起一个作业把变更流打印出来，限时后取消
        print("\n>>> 开始捕获（现在去另一个终端制造变更试试）")
        print(">>> 例如：")
        print("      # 插入")
        print("      docker exec -i pg-primary psql -U shop -d shop -c \\")
        print("        \"INSERT INTO users (uid,username,email,phone,password_hash,status,level,balance,created_at,updated_at) \\")
        print("         VALUES (gen_random_uuid(),'cdc_test','cdc@test.com','13800000000','x',1,1,88.88,now(),now());\"")
        print("      # 更新")
        print("      docker exec -i pg-primary psql -U shop -d shop -c \\")
        print("        \"UPDATE users SET balance=balance+1 WHERE username='cdc_test';\"")
        print("      # 删除")
        print("      docker exec -i pg-primary psql -U shop -d shop -c \\")
        print("        \"DELETE FROM users WHERE username='cdc_test';\"")
        print(f"\n>>> {args.seconds} 秒后自动停止\n")

        # ⚠️ sink 选择是个坑（每种都试过）：
        #    - print sink   ：输出进 TaskManager 日志，docker exec 下 stdout 看不到
        #    - filesystem   ：文件只能 append，接不住 CDC 的 UPDATE/DELETE
        #                     （报 "doesn't support consuming update and delete changes"）
        #    - jdbc 无主键  ：直接报 "please declare primary key for sink table
        #                     when query contains update/delete record" ❌
        #    - jdbc 有主键  ：upsert，能接 changelog，但删除不传播（状态滞留）
        # 结论：JDBC sink 必须带主键。想保留完整 I/U/D 轨迹，
        #       就用上面的 event_id(UUID) 当主键 —— 每条变更一行，天然不覆盖。
        env.execute_sql(f"""
            CREATE TABLE cdc_out (
                event_id STRING,
                row_id   BIGINT,
                username VARCHAR(64),
                status   SMALLINT,
                balance  DECIMAL(12, 2),
                op_type  STRING,
                op_name  STRING,
                op_ts    TIMESTAMP(3),
                PRIMARY KEY (event_id) NOT ENFORCED
            ) WITH (
                'connector'  = 'jdbc',
                'url'        = 'jdbc:postgresql://{PG_CDC_HOST}:{PG_PORT}/{PG_DB}',
                'username'   = 'shop',
                'password'   = 'shop123',
                'driver'     = 'org.postgresql.Driver',
                'table-name' = 'cdc_audit_log'
            )
        """)

        stmt_set = env.create_statement_set()
        stmt_set.add_insert_sql("INSERT INTO cdc_out SELECT * FROM users_audit")

        job = stmt_set.execute()
        # ⚠️ 不要只 sleep 就完事 —— 必须确认作业真的还在 RUNNING。
        #    曾经出现过"sleep 190s 但作业 6s 就 FINISHED"的情况，
        #    日志干净得看不出问题，结果增量变更全部漏掉。
        time.sleep(5)
        _jc = None
        try:
            _jc = job.get_job_client_or_throw()
            jid = _jc.get_job_id()
            print(f"\n>>> 作业已提交：{jid}")
            print(f">>> 现在去另一个终端制造变更（INSERT/UPDATE/DELETE）")
            print(f">>> Web UI: http://localhost:8081/#/job/{jid}/overview\n")
        except Exception:
            jid = None

        # 分段轮询：既等待，又顺带发现作业提前结束
        waited = 5
        while waited < args.seconds:
            time.sleep(min(5, args.seconds - waited))
            waited += 5
            if jid:
                try:
                    st = _jc.get_job_status().get().name()
                except Exception:
                    st = "UNKNOWN"
                if st not in ("RUNNING", "CREATED", "RESTARTING", "RECONCILING"):
                    print(f"\n⚠️  作业在 {waited}s 时已进入 {st} 状态，提前退出等待")
                    print("   如果是 FINISHED：说明 source 被当成有界流了，检查 CDC 配置")
                    break

        print(f"\n>>> {waited}s 到，停止作业", flush=True)
        # ⚠️ 注意：StatementSetResult.get_job_client() 在 PyFlink 里不是直接返回
        #    Java 对象，而是 CompletableFuture，调 .cancel() 会报
        #    AttributeError: 'CompletableFuture' object has no attribute 'get'
        #    正确做法：用 get_job_client_or_throw() 或直接 throw 出结果对象
        try:
            job.get_job_client_or_throw().cancel()
        except AttributeError:
            # 兜底：通过 Flink REST API 取消
            import urllib.request
            import json as _json
            try:
                with urllib.request.urlopen("http://localhost:8081/jobs/overview") as r:
                    data = _json.loads(r.read().decode())
                for j in data.get("jobs", []):
                    if j.get("state") == "RUNNING":
                        req = urllib.request.Request(
                            f"http://localhost:8081/jobs/{j['jid']}/yarn-cancel" if False
                            else f"http://localhost:8081/jobs/{j['jid']}",
                            method="PATCH")
                        try:
                            urllib.request.urlopen(req)
                            print(f"  已取消作业 {j['jid']}")
                        except Exception:
                            pass
            except Exception as e:
                print(f"  REST 取消失败：{e}")
        print("✓ 作业已停止", flush=True)
    else:
        print("\n>>> 常驻模式：变更流持续输出（Ctrl-C 停止）")
        # ⚠️ sink 选择是个坑：
        #    - print sink   ：输出进 TaskManager 日志，docker exec 下 stdout 看不到
        #    - filesystem   ：文件只能 append，接不住 CDC 的 UPDATE/DELETE
        #                     （报 "doesn't support consuming update and delete changes"）
        #    - jdbc 有主键  ：upsert 语义，天然支持 changelog ✅
        # 所以这里写回 pg，跑完直接查表就能看到完整变更轨迹。
        env.execute_sql(f"""
            CREATE TABLE cdc_out (
                id       BIGINT,
                username VARCHAR(64),
                status   SMALLINT,
                balance  DECIMAL(12, 2),
                op_type  STRING,
                op_name  STRING,
                op_ts    TIMESTAMP(3),
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector'  = 'jdbc',
                'url'        = 'jdbc:postgresql://{PG_CDC_HOST}:{PG_PORT}/{PG_DB}',
                'username'   = 'shop',
                'password'   = 'shop123',
                'driver'     = 'org.postgresql.Driver',
                'table-name' = 'cdc_change_log'
            )
        """)

        stmt_set = env.create_statement_set()
        stmt_set.add_insert_sql("INSERT INTO cdc_out SELECT * FROM users_ops")
        stmt_set.execute().wait()

    banner("完成")


if __name__ == "__main__":
    main()
