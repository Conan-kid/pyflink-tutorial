"""
示例 01 · Hello World：批处理 WordCount（Table API 版）
=========================================================
学习目标
    1. 理解 ExecutionEnvironment 与 TableEnvironment 的分工
    2. 掌握「建表 → SQL 查询 → 打印结果」这条最短路径
    3. 熟悉 from_elements 这个调试利器（不用任何外部依赖就能造数据）

运行
    python 01_hello_wordcount.py

预期输出
    +I[flink, 3]
    +I[python, 2]
    ...
    +I 是 changelog 类型标记：+I = Insert（新增行）
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    # ------------------------------------------------------------------
    # 第 1 步：创建 TableEnvironment
    # ------------------------------------------------------------------
    # EnvironmentSettings.in_batch_mode()  → 批处理模式，有界数据，跑完就退出
    # EnvironmentSettings.in_streaming_mode() → 流处理模式，无限数据，一直跑
    #
    # 注意：PyFlink 里所有 Table API 操作都必须在 TableEnvironment 上做，
    #      它才是 Table API 的「入口」。StreamExecutionEnvironment 是给
    #      DataStream API 用的（示例 06 会用到）。
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # 一些常用配置，先埋个印象，后面示例会细讲
    # 本地跑的时候并行度不要贪大，否则启动开销比计算本身还久
    t_env.get_config().set("parallelism.default", "1")

    # ------------------------------------------------------------------
    # 第 2 步：造数据 —— 建一张「临时表」（temporary table）
    # ------------------------------------------------------------------
    # from_elements 把一个 Python list 变成一张 Flink 表。
    # 参数是「行的列表」，每条行是元组。
    # 这是学习阶段最好用的数据源：零依赖、秒级启动、结果可预期。
    t_env.create_temporary_view(
        "word_source",
        t_env.from_elements(
            [
                ("flink",),
                ("python",),
                ("flink",),
                ("sql",),
                ("python",),
                ("flink",),
                ("table",),
            ],
            # schema 声明列名和类型。
            # 类型名用 Flink 的 SQL 类型：STRING / INT / BIGINT / DOUBLE / TIMESTAMP(3)
            ["word"],
        ),
    )

    # ------------------------------------------------------------------
    # 第 3 步：写 SQL —— 复用你已有的 SQL 知识
    # ------------------------------------------------------------------
    # 这是 PyFlink 最爽的地方：GROUP BY / JOIN / OVER 全都是标准 SQL。
    # 你不需要为了流处理重学一套 DSL。
    result_table = t_env.sql_query(
        """
        SELECT word, COUNT(1) AS cnt
        FROM word_source
        GROUP BY word
        ORDER BY cnt DESC, word ASC
        """
    )

    # ------------------------------------------------------------------
    # 第 4 步：把结果拿出来看
    # ------------------------------------------------------------------
    # 方式 A：execute().print() —— 最简单，直接打到 stdout
    result_table.execute().print()

    # 方式 B（注释掉，供对比）：取回成 Python 对象自己处理
    # rows = [row for row in result_table.execute().collect()]
    # for row in rows:
    #     print(row)   # Row(word='flink', cnt=3)


if __name__ == "__main__":
    main()
