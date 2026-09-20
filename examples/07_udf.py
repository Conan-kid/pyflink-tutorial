"""
示例 07 · Python UDF：把非 SQL 能力塞进流处理
===============================================
当你需要做 SQL 做不到的事（调 Python 库、复杂正则、模型推理），
就该 Python UDF 上场了。Flink 支持三种 UDF：

    UDF   (ScalarFunction)  一行进一行出        —— 最常用
    UDAF  (AggregateFunction) 多行进一行出      —— 自定义聚合
    UDTF  (TableFunction)   一行进多行出        —— 一行拆成多行（如 explode）

【性能警告 —— 划重点】
    Python UDF 的数据要从 JVM 传到 Python 进程再传回来，
    比纯 SQL 慢 1~2 个数量级。
    所以：能写成 SQL 的，就别用 UDF。
    必须用 UDF 时，把过滤/聚合尽量放在 SQL 侧，缩小 UDF 的输入量。

运行
    python 07_udf.py
"""

import re

from pyflink.common import Row
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.types import DataTypes
from pyflink.table.udf import AggregateFunction, TableFunction, udaf, udf, udtf

# ----------------------------------------------------------------------
# 【极其重要的类型系统区分】
#
# PyFlink 里有两套类型系统，用错了就报
#   TypeError: Invalid returnType: returnType should be DataType or str
#
#   Table API 用（UDF / 建表 / 表转换）：
#     from pyflink.table.types import DataTypes
#     DataTypes.STRING() / DataTypes.DOUBLE() / DataTypes.BOOLEAN()
#     或者直接用字符串 "STRING" / "DOUBLE" / "BOOLEAN"
#
#   DataStream API 用（from_collection / 状态描述符）：
#     from pyflink.common.typeinfo import Types
#     Types.STRING() / Types.DOUBLE() / Types.BOOLEAN()
#
# UDF 的 result_type 属于 Table API 体系 → 必须用 DataTypes 或字符串。
# ----------------------------------------------------------------------


# ======================================================================
# ① 标量函数 UDF：一行进一行出
# ======================================================================
@udf(result_type="STRING")            # 用字符串最省事，等价于 DataTypes.STRING()
def mask_phone(phone: str) -> str:
    """手机号脱敏：13812345678 → 138****5678"""
    if not phone or len(phone) != 11:
        return phone or ""
    return phone[:3] + "****" + phone[-4:]


@udf(result_type="BOOLEAN")
def is_suspicious(amount: float, user_level: str) -> bool:
    """业务规则判定：金额异常或等级不符 → 可疑"""
    if user_level == "normal" and amount > 5000:
        return True
    if amount > 20000:
        return True
    return False


# ======================================================================
# ② 表函数 UDTF：一行进多行出（explode 的典型场景）
# ======================================================================
@udtf(result_types=["STRING"])        # 注意是复数 result_types
def split_tags(tag_str: str):
    """
    把 "数码|爆款|包邮" 拆成三行。
    这是 UDTF 最经典的用法，配合 LATERAL TABLE 使用。
    """
    if not tag_str:
        return
    for tag in tag_str.split("|"):
        tag = tag.strip()
        if tag:
            yield tag


# ======================================================================
# ③ 聚合函数 UDAF：多行进一行出
# ======================================================================
class WeightedAvg(AggregateFunction):
    """
    加权平均：sum(price*qty) / sum(qty)
    用 udaf() 包装后，可以在 SQL 里直接当聚合函数用。

    【Table API 的 UDAF 有三处极易写错，逐条对照官方示例】
      第 1 条 |accumulate 的参数顺序：accumulator 在前，input_row 在后。
              ── 注意这跟 DataStream API 的 AggregateFunction 正好相反
                 （那边是 add(value, accumulator)）！两套 API 各写各的。
      第 2 条 |accumulate 必须「原地修改」accumulator，不要 return 新对象。
              写成 `return acc + x` 会静默失效 —— 结果永远是初始值，
              不报错但答案错，最难查。
      第 3 条 |需要 retract（撤销，用于更新流）和 merge（窗口合并），
              流模式的聚合在数据更新时会调用它们，不实现可能算错。
    依据：PyFlink 自带的官方示例 pyflink/examples/table/basic_operations.py
    """

    def create_accumulator(self):
        # 累加器是个可索引对象：[加权总和, 权重总和]
        return Row(0.0, 0.0)

    def accumulate(self, accumulator, price, qty):
        """每来一行数据调用一次 —— 原地累加，不返回值"""
        if price is None or qty is None:
            return
        accumulator[0] += price * qty
        accumulator[1] += qty

    def retract(self, accumulator, price, qty):
        """撤销一条数据（流模式下数据被更新/删除时会用到）"""
        if price is None or qty is None:
            return
        accumulator[0] -= price * qty
        accumulator[1] -= qty

    def merge(self, accumulator, accumulators):
        """合并多个累加器（窗口合并 / 并行聚合结果汇总）"""
        for other in accumulators:
            accumulator[0] += other[0]
            accumulator[1] += other[1]

    def get_value(self, accumulator):
        """窗口结束时算出最终值"""
        return 0.0 if accumulator[1] == 0 else accumulator[0] / accumulator[1]

    def get_accumulator_type(self):
        # 累加器的类型声明，必须和 create_accumulator 返回的结构一致
        return DataTypes.ROW([
            DataTypes.FIELD("total", DataTypes.DOUBLE()),
            DataTypes.FIELD("weight", DataTypes.DOUBLE()),
        ])

    def get_result_type(self):
        return DataTypes.DOUBLE()


def build_env(mode: str):
    """建一个 TableEnvironment，并注册所有 UDF。"""
    settings = (
        EnvironmentSettings.in_batch_mode()
        if mode == "batch"
        else EnvironmentSettings.in_streaming_mode()
    )
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("parallelism.default", "1")

    # 注册 UDF 到 TableEnvironment，之后在 SQL 里就能直接调用
    #
    # 标量函数和表函数：直接传被装饰过的函数对象即可
    t_env.create_temporary_function("mask_phone", mask_phone)
    t_env.create_temporary_function("is_suspicious", is_suspicious)
    t_env.create_temporary_function("split_tags", split_tags)

    # 聚合函数：用 udaf() 包一层，并显式给出 input_types 和 result_type
    #
    # 【坑】@udaf 装饰器只能用在「函数」上，不能拿来装饰类。
    #      写成 @udaf(...) 加在 class 上会报：
    #        TypeError: Invalid function: not a function or callable
    #      类形式的 UDAF 必须显式调用 udaf() 包装。
    #
    # 【坑】不传 input_types 时，Flink 反查 Java 构造器会失败，报：
    #        Py4JException: Constructor ... PythonAggregateFunction(...) does not exist
    #      所以显式传 input_types 是最稳的做法。
    t_env.create_temporary_function(
        "weighted_avg",
        udaf(
            WeightedAvg(),
            input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()],
            result_type=DataTypes.DOUBLE(),
            accumulator_type=DataTypes.ROW([
                DataTypes.FIELD("total", DataTypes.DOUBLE()),
                DataTypes.FIELD("weight", DataTypes.DOUBLE()),
            ]),
        ),
    )
    return t_env


def make_views(t_env):
    """在两个环境里建同样的测试数据视图。"""
    users = t_env.from_elements(
        [
            ("u001", "13812345678", "vip"),
            ("u002", "13998765432", "normal"),
            ("u003", "13612341234", "normal"),
            ("u004", "15800001111", "vip"),
        ],
        ["user_id", "phone", "user_level"],
    )
    t_env.create_temporary_view("users", users)

    orders = t_env.from_elements(
        [
            ("o1", "u001", "数码|爆款|包邮", 8999.0, 1),
            ("o2", "u002", "包邮|清仓",       699.0, 3),
            ("o3", "u003", None,             25999.0, 1),
            ("o4", "u004", "数码|新品",       3599.0, 2),
            ("o5", "u002", "数码",            1299.0, 1),
        ],
        ["order_id", "user_id", "tags", "price", "qty"],
    )
    t_env.create_temporary_view("orders", orders)


def run_scalar_and_udtf(t_env):
    """标量 UDF + 表函数（批/流模式都支持）"""
    # ==================================================================
    # 【1】标量 UDF
    # ==================================================================
    print("\n" + "=" * 70)
    print("【1】标量 UDF：手机号脱敏 + 可疑订单标记")
    print("=" * 70)
    t_env.execute_sql(
        """
        SELECT o.order_id,
               u.user_id,
               mask_phone(u.phone)                      AS masked_phone,
               u.user_level,
               o.price,
               is_suspicious(o.price, u.user_level)     AS suspicious
        FROM orders o
        JOIN users u ON o.user_id = u.user_id
        ORDER BY o.order_id
        """
    ).print()

    # ==================================================================
    # 【2】表函数 UDTF + LATERAL TABLE
    # ==================================================================
    print("\n" + "=" * 70)
    print("【2】表函数 UDTF：把 tags 列炸开成多行")
    print("=" * 70)
    t_env.execute_sql(
        """
        SELECT o.order_id, t.tag
        FROM orders o,
             LATERAL TABLE(split_tags(o.tags)) AS t(tag)
        ORDER BY o.order_id, t.tag
        """
    ).print()

    # LATERAL TABLE 是 SQL 标准里的「横向连接」，
    # 把 UDTF 产生的每一行和原表的行关联起来，是数据展开的标准姿势。
    print("\n各标签出现次数：")
    t_env.execute_sql(
        """
        SELECT t.tag, COUNT(1) AS cnt
        FROM orders o,
             LATERAL TABLE(split_tags(o.tags)) AS t(tag)
        GROUP BY t.tag
        ORDER BY cnt DESC, t.tag
        """
    ).print()


def run_udaf(t_env):
    """聚合 UDAF（只在流模式可用）"""
    print("\n" + "=" * 70)
    print("【3】聚合 UDAF：加权平均价（按数量加权）")
    print("=" * 70)
    t_env.execute_sql(
        """
        SELECT weighted_avg(CAST(price AS DOUBLE), CAST(qty AS DOUBLE)) AS weighted_avg_price,
               COUNT(1) AS order_cnt
        FROM orders
        """
    ).print()

    # 对照一下：普通平均价（不加权）是多少？
    print("\n对照：普通算术平均价")
    t_env.execute_sql("SELECT AVG(price) AS simple_avg FROM orders").print()


def main():
    # ======================================================================
    # 本示例分两段跑，原因是一个 Flink 的硬限制：
    #
    #   ┌──────────────┬──────────┬──────────┐
    #   │ UDAF 类型     │ 批模式    │ 流模式    │
    #   ├──────────────┼──────────┼──────────┤
    #   │ 普通 UDAF     │ ✗ 不支持  │ ✓ 支持    │
    #   │ Pandas UDAF  │ ✓ 支持    │ ✗ 不支持  │
    #   └──────────────┴──────────┴──────────┘
    #
    # 报错原文：
    #   批模式：non-Pandas UDAFs are not supported in batch mode currently.
    #   流模式：Pandas UDAFs are not supported in streaming mode currently.
    #
    # 也就是说：**没有任何一种 Python UDAF 能同时跑批和流。**
    # UDF（标量）和 UDTF（表函数）倒是不受影响，两种模式都能用。
    #
    # 实际项目怎么办？
    #   · 批处理场景要自定义聚合 → 用 Pandas UDAF（配合批模式）
    #   · 流处理场景要自定义聚合 → 用普通 UDAF（配合流模式，即本例）
    #   · 或者绕开 UDAF：用 DataStream API 的 AggregateFunction，或拆成几步 SQL
    # ======================================================================

    # ---- 第一段：标量 UDF + UDTF（用批模式，输出有序好读）----
    print("\n" + "#" * 70)
    print("# 第一部分：标量 UDF 与表函数 UDTF（批模式）")
    print("#" * 70)
    batch_env = build_env("batch")
    make_views(batch_env)
    run_scalar_and_udtf(batch_env)

    # ---- 第二段：UDAF（必须用流模式）----
    print("\n" + "#" * 70)
    print("# 第二部分：聚合 UDAF（流模式，因为批模式不支持普通 UDAF）")
    print("#" * 70)
    stream_env = build_env("stream")
    make_views(stream_env)
    run_udaf(stream_env)


if __name__ == "__main__":
    main()
