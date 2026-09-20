"""
示例 04 · 四种窗口 + 三种聚合函数，一次学完
==============================================
本示例覆盖 PyFlink 窗口体系的全貌：

【窗口类型】
    1. TumblingWindow   滚动窗口    —— 窗口不重叠，如每 5 秒一个
    2. SlidingWindow    滑动窗口    —— 窗口重叠，如每 5 秒一个、每 2 秒滑动一次
    3. SessionWindow    会话窗口    —— 按「活跃间隙」切分，间隔超过阈值就断开
    4. GlobalWindow     全局窗口    —— 所有数据一个窗口，需要自定义 Trigger 才有意义

【聚合函数三档（性能递增，灵活度递减）】
    A. ReduceFunction    两两归约，输入输出同类型        —— 最简单
    B. AggregateFunction 增量聚合，有累加器             —— 推荐，支持不同类型进出
    C. ProcessWindowFunction 全量缓存 + 窗口元信息       —— 最灵活，但内存压力大

【黄金组合：AggregateFunction + ProcessWindowFunction】
    前者做增量计算（省内存），后者只补窗口元信息（不缓存全部数据）。
    这是生产环境的标准写法，示例最后会演示。

运行
    python 04_window_types.py
"""

import sys

from pyflink.common import Duration, Types
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
    ReduceFunction,
)
from pyflink.datastream.window import (
    SlidingProcessingTimeWindows,
    TumblingProcessingTimeWindows,
)

# ----------------------------------------------------------------------
# 【重要区分】Duration 和 Time 是两个不同的类，用错会报
#   AttributeError: 'Duration' object has no attribute 'to_milliseconds'
#
#   Duration  → 用在 WatermarkStrategy、allowed_lateness 等地方
#               （pyflink.common.Duration）
#   Time      → 用在窗口分配器 .of() 的参数里
#               （pyflink.common.time.Time）
#
# 记忆方法：窗口的 of() 要 Time，其他地方的超时/延迟参数要 Duration。
# ----------------------------------------------------------------------


# 数据：(传感器ID, 温度)
SENSOR_DATA = [
    ("s1", 20.1), ("s2", 21.5), ("s1", 22.3), ("s3", 19.8),
    ("s1", 25.0), ("s2", 23.1), ("s3", 20.5), ("s1", 27.2),
    ("s2", 24.8), ("s3", 22.0), ("s1", 26.1), ("s2", 25.5),
]


# ======================================================================
# A. ReduceFunction 示例：求每个传感器的最高温度
# ======================================================================
class MaxTempReducer(ReduceFunction):
    """
    ReduceFunction 要求输入输出类型完全一致。
    每次拿「当前累加值」和「新元素」比一比，返回新的累加值。
    """
    def reduce(self, value1, value2):
        # value 是元组 (sensor_id, temp)
        return value1 if value1[1] >= value2[1] else value2


# ======================================================================
# B. AggregateFunction 示例：求平均温度（增量计算，不缓存原始数据）
# ======================================================================
class AvgTempAggregator(AggregateFunction):
    """
    AggregateFunction 有三个方法：
      create_accumulator()      创建累加器（初始状态）
      add(value, accumulator)   把新元素并入累加器
      get_result(accumulator)   从累加器算出最终结果

    关键优势：只需要保存一个「累加器」对象，内存占用与数据量无关。
    即使窗口里有 1 亿条数据，也只占一个累加器的内存。

    【参数顺序的坑 —— 极易踩，且报错很难懂】
      PyFlink 的 add 签名是 add(value, accumulator)：**新元素在前，累加器在后**。
      这跟 Flink Java 版的 add(accumulator, value) 是反的！
      如果你照着 Java 文档写，就会把累加器当元素、元素当累加器，
      然后报一个跟真实原因完全无关的错误，比如：
        TypeError: can only concatenate str (not "int") to str

      依据（PyFlink 源码 fn_execution/state_impl.py）：
        accumulator = self._agg_function.add(v, accumulator)
    """

    def create_accumulator(self):
        # 累加器：(总和, 计数)  —— 任意 Python 可序列化对象都行
        return (0.0, 0)

    def add(self, value, acc):
        # value 在前，acc 在后（跟 Java 版相反，切记）
        total, count = acc
        return (total + value[1], count + 1)

    def get_result(self, acc):
        total, count = acc
        return total / count if count > 0 else 0.0

    def merge(self, acc1, acc2):
        """
        会话窗口 / 滑动窗口的合并场景会用到。
        比如两条流合并时，需要把两个累加器合并成一个。
        不实现也能跑，但生产建议实现。
        """
        return (acc1[0] + acc2[0], acc1[1] + acc2[1])


# ======================================================================
# C. 黄金组合：AggregateFunction + ProcessWindowFunction
# ======================================================================
class AvgWithWindowInfo(ProcessWindowFunction):
    """
    配合 AggregateFunction 使用时，elements 里装的不是原始数据，
    而是 AggregateFunction 的「结果值」（这里是单个 float）。

    这样既拿到了增量计算的低内存，又能补充窗口起止时间等元信息。
    """

    def process(self, key, context, elements):
        avg_temp = list(elements)[0]     # 已经算好的平均值
        w = context.window()
        yield (
            f"传感器={key} 窗口=[{w.start},{w.end}) "
            f"平均温度={avg_temp:.2f} 迟到标记={w.max_timestamp() > w.end}"
        )


# ======================================================================
# D. 会话窗口用的聚合器：统计会话内的事件数和时长
# ======================================================================
class SessionSummary(ProcessWindowFunction):
    def process(self, key, context, elements):
        items = list(elements)
        w = context.window()
        yield (
            f"传感器={key} 会话长度={w.end - w.start}ms "
            f"事件数={len(items)} 温度列表={[round(e[1], 1) for e in items]}"
        )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # Windows 上必须指定 Python 解释器，否则 Python Worker 起不来
    # （系统 PATH 里的 python 可能不是装了 PyFlink 的那个）
    env.set_python_executable(sys.executable)

    base = env.from_collection(
        SENSOR_DATA,
        type_info=Types.TUPLE([Types.STRING(), Types.DOUBLE()]),
    )

    # ------------------------------------------------------------------
    # 【1】滚动窗口 + ReduceFunction：每 3 个事件的窗口内找最高温
    #     这里用 ProcessingTime 会依实际时钟，为了结果可复现，
    #     我们用「计数窗口」思路近似 —— 实际项目用 EventTime 更常见
    # ------------------------------------------------------------------
    print("\n===== [1] 滚动窗口 + ReduceFunction（窗口内最高温度） =====")
    (
        base
        .key_by(lambda e: e[0])
        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(500)))
        .reduce(MaxTempReducer())
        .print()
    )

    # ------------------------------------------------------------------
    # 【2】滑动窗口 + AggregateFunction：每 500ms 滑一次、窗口 1s，求平均
    #
    # 注意这里用的是「处理时间」窗口。为什么不用事件时间？
    #   事件时间窗口需要先 assign_timestamps_and_watermarks，
    #   而本地小数据集跑得太快，watermark 还没推进作业就结束了，会一条结果都没有。
    #   示例 03 专门讲事件时间，这里用处理时间把窗口语义讲清楚就够了。
    # ------------------------------------------------------------------
    print("\n===== [2] 滑动窗口 + AggregateFunction（增量求平均） =====")
    (
        base
        .key_by(lambda e: e[0])
        .window(
            SlidingProcessingTimeWindows.of(
                Time.milliseconds(1000),   # 窗口大小
                Time.milliseconds(500),    # 滑动步长
            )
        )
        .aggregate(AvgTempAggregator())
        .print()
    )

    # ------------------------------------------------------------------
    # 【3】滚动窗口 + 黄金组合：增量聚合 + 窗口元信息
    # ------------------------------------------------------------------
    print("\n===== [3] 滚动窗口 + Agg + ProcessWindowFunction（黄金组合） =====")
    (
        base
        .key_by(lambda e: e[0])
        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(600)))
        # 两个参数：先做增量聚合，再用 ProcessWindowFunction 包装结果
        .aggregate(AvgTempAggregator(), AvgWithWindowInfo())
        .print()
    )

    env.execute("04-window-types")


if __name__ == "__main__":
    main()
