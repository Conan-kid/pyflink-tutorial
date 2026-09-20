"""
示例 05 · 有状态计算：KeyedState + Checkpoint 容错
====================================================
【为什么状态是 Flink 的看家本领？】
    Spark Streaming 是「微批」——攒 1 秒数据当成一个小批次处理。
    真正的流式引擎（Flink）是「逐条处理 + 状态常驻内存」。
    状态让 Flink 能做到微批做不到的事：
        · 跨事件累计（如用户累计消费额）
        · 去重（如 5 分钟内同一订单不重复处理）
        · CEP 复杂事件匹配（如 3 次登录失败后告警）
    而且状态是「可持久化 + 精确一次」的。

【两种状态】
    KeyedState    ：key_by 之后才能用，每个 key 一份状态（最常用）
    OperatorState ：算子级别，不依赖 key（如 Kafka source 的 offset）

【本示例演示】
    1. ValueState  —— 单值状态，做「累计消费额」
    2. ListState   —— 列表状态，做「最近 N 笔交易」
    3. MapState    —— 字典状态，做「各品类消费统计」
    4. Checkpoint  —— 开启容错，作业挂掉能自动恢复状态

【State TTL】
    用户 30 天不活跃，他的状态就该清掉，否则状态无限膨胀。
    StateTtlConfig 就是干这个的，示例里给了配置模板。

运行
    python 05_stateful.py
"""

import sys

from pyflink.common import Duration, Types
from pyflink.common.time import Time
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeContext,
    CheckpointingMode,
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import (
    ValueStateDescriptor,
    ListStateDescriptor,
    MapStateDescriptor,
    StateTtlConfig,
)


# 交易流水：(用户ID, 品类, 金额)
TRANSACTIONS = [
    ("alice", "手机",   5999.0),
    ("bob",   "耳机",    899.0),
    ("alice", "耳机",    399.0),
    ("carol", "笔记本", 8999.0),
    ("bob",   "手机",   4299.0),
    ("alice", "笔记本", 7599.0),
    ("carol", "耳机",    599.0),
    ("bob",   "笔记本", 6299.0),
]


class UserSpendingTracker(KeyedProcessFunction):
    """
    用三种状态追踪每个用户的消费行为。

    KeyedProcessFunction 的 open() 方法里才能拿到 RuntimeContext，
    也才能注册状态描述符 —— 这是新手常踩的坑：
    状态不能在 __init__ 里创建，必须在 open() 里创建。
    """

    def open(self, runtime_context: RuntimeContext):
        # --------------------------------------------------------------
        # 【ValueState】存单个标量：用户累计消费总额
        # --------------------------------------------------------------
        self.total_amount = runtime_context.get_state(
            ValueStateDescriptor("total_amount", Types.DOUBLE())
        )

        # --------------------------------------------------------------
        # 【ListState】存一个列表：用户最近 3 笔交易
        # 这里顺便演示 State TTL —— 30 天不访问自动清理
        #
        # 注意：new_builder 的参数是 pyflink.common.time.Time，
        #      不是 Duration（跟窗口类的要求一致）
        # --------------------------------------------------------------
        ttl_config = (
            StateTtlConfig.new_builder(Time.days(30))
            # 写时刷新「最后访问时间」（最常用的策略）
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            # 过期状态读出来返回 None，语义干净
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            # 增量清理：每次访问状态时顺带清理 10 个过期条目
            # 第二个参数 False = 不是每条记录都触发清理（避免性能开销过大）
            .cleanup_incrementally(10, False)
            .build()
        )
        list_desc = ListStateDescriptor("recent_txns", Types.STRING())
        list_desc.enable_time_to_live(ttl_config)
        self.recent_txns = runtime_context.get_list_state(list_desc)

        # --------------------------------------------------------------
        # 【MapState】存字典：各品类的消费金额
        # --------------------------------------------------------------
        self.by_category = runtime_context.get_map_state(
            MapStateDescriptor("by_category", Types.STRING(), Types.DOUBLE())
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        """
        每来一条交易就更新三种状态，并输出「累计快照」。
        注意：process_element 是生成器（yield），不是 return。
        """
        user, category, amount = value

        # 1. 更新累计总额
        prev_total = self.total_amount.value() or 0.0
        new_total = prev_total + amount
        self.total_amount.update(new_total)

        # 2. 追加到最近交易列表（只保留最近 3 笔）
        recent = self.recent_txns.get()
        recent_list = list(recent) if recent else []
        recent_list.append(f"{category}:{amount}")
        if len(recent_list) > 3:
            recent_list = recent_list[-3:]
        self.recent_txns.update(recent_list)

        # 3. 更新品类字典
        prev_cat = self.by_category.get(category) or 0.0
        self.by_category.put(category, prev_cat + amount)

        # 4. 输出当前用户的状态快照
        yield (
            f"用户={user} 本次={category}:{amount} "
            f"累计={new_total:.1f} "
            f"最近3笔={recent_list} "
            f"品类明细={dict(sorted(self.by_category.items()))}"
        )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # Windows 上必须指定 Python 解释器，否则 Python Worker 起不来
    env.set_python_executable(sys.executable)

    # ------------------------------------------------------------------
    # 【4】开启 Checkpoint —— 容错的生命线
    # ------------------------------------------------------------------
    # 每隔 1000ms 做一次 checkpoint，把状态快照存下来。
    # 作业挂掉重启时，会从最近的 checkpoint 恢复状态，做到「精确一次」。
    #
    # EXACTLY_ONCE：每条数据只影响状态一次（推荐，默认）
    # AT_LEAST_ONCE：可能重复，但吞吐更高
    env.enable_checkpointing(1000, CheckpointingMode.EXACTLY_ONCE)

    # 生产环境通常还要配状态后端和持久化路径，例如：
    #   env.set_state_backend(EmbeddedRocksDBStateBackend(...))
    #   config.set("state.checkpoints.dir", "s3://my-bucket/flink/checkpoints")
    # RocksDB 适合「状态远大于内存」的场景，HashMapStateBackend 适合小状态。

    # 从本地文件目录持续读（模拟流式接入）
    stream = env.from_collection(
        TRANSACTIONS,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.DOUBLE()]),
    )

    (
        stream
        .key_by(lambda e: e[0])            # 按用户分组，每个用户一份独立状态
        .process(UserSpendingTracker())
        .print()
    )

    env.execute("05-stateful-processing")


if __name__ == "__main__":
    main()
