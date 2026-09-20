"""
示例 06 · 连接器实战：文件 + JDBC + Kafka
==========================================
前面示例都在用 from_elements 造数据，真实项目当然要接外部系统。
本示例演示 Flink SQL 里最常用的三个连接器。

【连接器使用三步曲】
    1. 在 CREATE TABLE 的 WITH 子句里声明连接器参数
    2. SQL 里像普通表一样 SELECT / INSERT
    3. 作业提交时把对应的 JAR 包带上（或放进 Flink 的 lib/ 目录）

【JAR 包从哪来？】
    Flink 官方不打进发行包，需要自己下：
      · flink-connector-kafka-*.jar
      · flink-connector-jdbc-*.jar + 对应的数据库驱动
      · flink-connector-files-*.jar
    下载地址：https://flink.apache.org/downloads/ （找到 Connectors 一节）
    本工程的 scripts/download_jars.sh 会自动帮你下。

【关于示例的可运行性】
    文件连接器：本示例可直接运行（有本地文件依赖，脚本会生成）
    JDBC 连接器：需要 MySQL，docker/docker-compose.yml 里有
    Kafka 连接器：需要 Kafka，docker/docker-compose.yml 里有
    没起服务时，本脚本会给出清晰提示而不是报一堆看不懂的 Java 异常。
"""

import os
import socket
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment

# 本示例的「是否具备运行条件」探测函数 ------------------------------

# ----------------------------------------------------------------------
# 连接地址解析：Docker 容器内用 compose 服务名，宿主机用 localhost
# ----------------------------------------------------------------------
def _conn_host(env_key, default):
    return os.environ.get(env_key, default)


KAFKA_BOOTSTRAP = _conn_host("KAFKA_BOOTSTRAP", "localhost:9092")
MYSQL_HOST = _conn_host("MYSQL_HOST", "localhost")
MYSQL_PORT = int(_conn_host("MYSQL_PORT", "3306"))


def port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """探测某个端口是否可达，用来判断依赖服务有没有起来。"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "1")

    # 处理时间语义：使用数据到达时的系统时钟（流处理默认）
    t_env.get_config().set("pipeline.time-characteristic", "ProcessingTime")

    work_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(work_dir, "..", "data")

    # ==================================================================
    # 【一】文件连接器 —— 读 CSD/CSV，写 CSV
    # ==================================================================
    print("\n" + "=" * 70)
    print("【一】文件连接器（Filesystem）")
    print("=" * 70)

    csv_path = os.path.join(data_dir, "sales.csv").replace("\\", "/")
    if not os.path.exists(csv_path):
        print(f"⚠ 找不到 {csv_path}")
        print("  请先运行：python scripts/generate_data.py")
    else:
        # CREATE TABLE 是 Flink SQL 声明外部表的语法，非常直观
        t_env.execute_sql(
            f"""
            CREATE TABLE sales_csv (
                order_id     STRING,
                user_name    STRING,
                category     STRING,
                amount       DOUBLE,
                order_time   TIMESTAMP(3)
            ) WITH (
                'connector'  = 'filesystem',
                'path'       = '{csv_path}',
                'format'     = 'csv',
                -- 关闭 changelog 文件，否则会多出目录
                'csv.disable-quote-character' = 'false'
            )
            """
        )

        print("已建表 sales_csv，做一次按品类聚合：")
        t_env.execute_sql(
            """
            SELECT category,
                   COUNT(1)     AS order_cnt,
                   SUM(amount)  AS total_sales
            FROM sales_csv
            GROUP BY category
            """
        ).print()

    # ==================================================================
    # 【二】JDBC 连接器 —— 写 MySQL（维表 JOIN 也用这个）
    # ==================================================================
    print("\n" + "=" * 70)
    print("【二】JDBC 连接器（MySQL）")
    print("=" * 70)

    if port_open(MYSQL_HOST, MYSQL_PORT):
        print("检测到 MySQL 已启动，建表并写入结果…")
        t_env.execute_sql(
            f"""
            CREATE TABLE category_summary (
                category    STRING,
                order_cnt   BIGINT,
                total_sales DOUBLE,
                PRIMARY KEY (category) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url'       = 'jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/flink_demo?useSSL=false&serverTimezone=UTC',
                'table-name'= 'category_summary',
                'username'  = 'root',
                'password'  = 'root123456'
            )
            """
        )
        # JDBC 结果表通常用 INSERT INTO 触发「流式写出」
        # 真实场景更常用 Kafka/CDC 做 sink，这里演示机制
        print("  ✓ category_summary 表已就绪（完整写入请见实战项目 08）")
    else:
        print("⚠ 未检测到 MySQL")
        print("  启动方式：cd docker && docker compose up -d mysql")
        print("  跳过 JDBC 部分，不影响其他示例。")

    # ==================================================================
    # 【三】Kafka 连接器 —— 流处理的标准入口
    # ==================================================================
    print("\n" + "=" * 70)
    print("【三】Kafka 连接器")
    print("=" * 70)

    if port_open(KAFKA_BOOTSTRAP.rsplit(":", 1)[0], int(KAFKA_BOOTSTRAP.rsplit(":", 1)[1])):
        print("检测到 Kafka 已启动，建表：")
        t_env.execute_sql(
            f"""
            CREATE TABLE kafka_orders (
                order_id   STRING,
                user_name  STRING,
                category   STRING,
                amount     DOUBLE,
                order_time TIMESTAMP(3),
                -- 事件时间列，配合 WATERMARK 实现乱序容忍
                WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = 'orders',
                'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
                'properties.group.id'          = 'pyflink-demo',
                'scan.startup.mode'            = 'earliest-offset',
                'format'                       = 'json',
                -- JSON 字段到表字段的映射，避免格式不匹配报错
                'json.ignore-parse-errors'     = 'true'
            )
            """
        )
        print("  ✓ kafka_orders 表已就绪")
        print("  跑实时作业请看：python 08_realtime_risk_control.py")
    else:
        print(f"⚠ 未检测到 Kafka（{KAFKA_BOOTSTRAP}）")
        print("  启动方式：cd docker && docker compose up -d kafka")

    print("\n" + "=" * 70)
    print("提示：完整可运行的实时管道见示例 08（需要 Kafka + MySQL）")
    print("=" * 70)


if __name__ == "__main__":
    main()
