"""
示例 03 · 流处理第一课：时间语义与 Watermark
==============================================
这是 PyFlink 最核心、也最容易搞错的一课。请务必读完注释。

【为什么需要 Watermark？】
    流里的事件可能乱序到达。比如点击日志：
        事件时间 10:00:03 的日志，可能在 10:00:09 才到
        而 10:00:05 的日志，10:00:06 就到了
    如果按「到达时间」（处理时间）开窗，同一个用户的行为会被切到错误的窗口里。
    所以流处理要用「事件时间」（数据里自带的时间戳）开窗。

    但问题来了：窗口 [10:00:00, 10:00:05) 什么时候能「关闭」并输出结果？
    如果永远等下去，就成了无界等待。Watermark 就是答案 ——
    它是一句承诺：「我保证，不会再出现时间戳早于 T 的事件了。」
    Flink 收到 watermark T，就关闭所有 end_time <= T 的窗口，输出结果。

【Watermark 的两种策略】
    W1. 周期生成（默认，推荐）：每隔 N 毫秒扫一次，把「已见最大时间戳 - 延迟」作为 watermark
    W2. 标记生成：遇到特定标记才生成，用得少

【本示例的延迟容忍】
    out_of_orderness = 5 秒 → 允许事件迟到 5 秒，窗口多等 5 秒才关。这是准确性和延迟的权衡。

运行
    python 03_watermark_window.py
"""

import sys

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows


# ======================================================================
# 数据源：模拟一条「乱序的点击流」
# 格式：(用户ID, 页面, 事件时间毫秒)
# 注意观察时间戳：故意让一部分事件晚到，制造乱序
# ======================================================================
CLICK_EVENTS = [
    ("u1", "/home",  1000),   # 事件时间 第 1 秒
    ("u2", "/home",  1500),
    ("u1", "/sku",   2500),   # 第 2 秒
    ("u3", "/home",  3000),
    ("u1", "/cart",  4500),   # 第 4 秒
    ("u2", "/sku",   5000),
    ("u3", "/sku",   5500),
    ("u1", "/pay",   6500),   # 第 6 秒
    ("u2", "/cart",  7000),
    ("u3", "/cart",  8000),
    ("u1", "/home",  9500),   # 第 9 秒
    ("u2", "/pay",  10500),   # 第 10 秒
    # ↓ 下面两条是「迟到事件」：时间戳比它们前面的小
    #   如果没有 watermark 的延迟容忍，它们会被丢弃
    ("u3", "/pay",   4000),   # 迟到 4 秒，在容忍范围内（5秒）→ 应该被正确计入
    ("u1", "/sku",   2000),   # 迟到 7 秒 5，超出容忍 → 会被丢弃（这是设计行为，不是 bug）
]


class ClickTimestampAssigner(TimestampAssigner):
    """
    从数据里提取事件时间。

    【新手必踩的坑】
    你可能想直接写 lambda：
        .with_timestamp_assigner(lambda event, ts: event[2])

    在 PyFlink 1.20 里这会报：
        AttributeError: 'function' object has no attribute 'extract_timestamp'

    原因是 with_timestamp_assigner 要求一个 TimestampAssigner 的「实现类」，
    而不是普通函数。Flink 需要序列化这个对象分发到各个 TaskManager，
    普通 lambda 没法跨进程传递。

    所以正确姿势就是像下面这样定义一个小类。
    """

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        # value 是流里的元素（这里是元组），record_timestamp 是系统给的参考时间戳
        return value[2]


class FormatClick(MapFunction):
    """把元组转成更易读的字符串，顺便演示 MapFunction 的写法。"""

    def map(self, value):
        user, page, ts = value
        return f"用户={user} 页面={page} 事件时间={ts}ms"


class WindowResult(ProcessWindowFunction):
    """
    自定义窗口输出函数。
    用 ProcessWindowFunction 而不是简单的 ReduceFunction/AggregateFunction，
    因为它能拿到窗口的元信息（窗口起止时间），输出更丰富。

    签名：(key, context, elements) → iterator
      key      : 分组键（这里是用户ID）
      context  : 可以取 window() 拿到窗口起止时间
      elements : 这个窗口内该 key 的所有元素（可迭代对象）
    """

    def process(self, key, context, elements):
        events = list(elements)
        start = context.window().start
        end = context.window().end
        yield f"窗口[{start}, {end}) 用户={key} 点击次数={len(events)} 页面序列={[e[1] for e in events]}"


def main():
    # StreamExecutionEnvironment 是 DataStream API 的入口
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)   # 本地学习调 1 并行度，输出顺序更直观

    # ------------------------------------------------------------------
    # 【Windows 上必须配的一步】指定 Python 解释器绝对路径
    # ------------------------------------------------------------------
    # 不配这个会报：
    #   Failed to execute the command: python -c "import pyflink; ..."
    #   ModuleNotFoundError: No module named 'pyflink'
    #
    # 原因：Flink 启动 Python Worker 时调用的是裸命令 `python`，
    #      而系统 PATH 里的 python 可能不是装了 PyFlink 的那个（比如你的 3.13）。
    # 解法：显式告诉 Flink 用哪个解释器。用绝对路径最稳妥。
    #
    # 生产环境可以用 sys.executable 自动获取当前解释器：
    #   import sys; env.set_python_executable(sys.executable)
    # 但要注意：集群模式下每个 TaskManager 的 Python 路径可能不同，
    # 这时应该配 flink-conf.yaml 的 python.executable 项。
    env.set_python_executable(sys.executable)

    # ------------------------------------------------------------------
    # 1. 从集合创建流
    #    真实项目里这里是 env.add_source(KafkaSource...) 见示例 08
    # ------------------------------------------------------------------
    stream = env.from_collection(
        CLICK_EVENTS,
        # 显式声明类型，避免 Flink 类型推断出错
        type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()]),
    )

    # ------------------------------------------------------------------
    # 2. 分配时间戳 + 生成 Watermark（关键一步）
    # ------------------------------------------------------------------
    # for_bounded_out_of_orderness(Duration.of_seconds(5))：
    #   允许最多 5 秒的乱序。Flink 会以「已见最大事件时间 - 5秒」作为 watermark。
    # with_timestamp_assigner：告诉 Flink 从哪个字段取事件时间。
    #   注意要传 TimestampAssigner 的实现类实例，不能传 lambda（见上面类的注释）
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(ClickTimestampAssigner())

    timestamped = stream.assign_timestamps_and_watermarks(watermark_strategy)

    # ------------------------------------------------------------------
    # 3. 按用户分组 → 开 5 秒滚动窗口 → 统计
    # ------------------------------------------------------------------
    # key_by 之后，同一个 key 的数据会被路由到同一个并行子任务，
    # 所以同一用户的点击才能进到同一个窗口里
    result = (
        timestamped
        .key_by(lambda e: e[0])                                   # 按用户ID分组
        # 注意：窗口的 of() 要 Time，不是 Duration！
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))     # 5秒滚动窗口
        .process(WindowResult())
    )

    # ------------------------------------------------------------------
    # 4. 输出
    # ------------------------------------------------------------------
    result.print()

    # execute() 是真正触发执行的开关。
    # DataStream 作业默认是「流」语义，不会自己结束，
    # 但 from_collection 是有界源，处理完会自动退出。
    env.execute("03-watermark-tumbling-window")


if __name__ == "__main__":
    main()
