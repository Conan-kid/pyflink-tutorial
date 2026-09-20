"""
示例 08 · 实战项目：实时交易风控系统（完整可运行）
====================================================
这是本教程的压轴项目，把前面所有知识点串成一条生产级管道。

【业务场景】
    电商平台需要实时监控交易，识别三类风险：
      R1. 高频交易   —— 同一用户 10 秒内下单 >= 3 笔
      R2. 大额异常   —— 单笔金额 > 20000
      R3. 累计超限   —— 24 小时内累计消费 > 50000

    识别出的风险实时写入告警表，同时输出到 Kafka 供下游消费。

【技术架构】
    Kafka(orders) ──> PyFlink 作业 ──┬──> MySQL(risk_alerts)  告警落库
                                      └──> Kafka(risk-alerts)  下游订阅
                     ↑
              规则配置表(MySQL) 通过维表 JOIN 动态加载

【运行前提】
    1. 启动 docker/docker-compose.yml 里的 Kafka + MySQL
    2. 运行 producer/produce_orders.py 持续灌数据
    3. 运行本脚本

    没起服务时，脚本会自动降级为「本地模拟模式」，用内存数据跑通全流程，
    保证你一定能看到效果。

运行
    python 08_realtime_risk_control.py          # 自动选择模式
    python 08_realtime_risk_control.py --local  # 强制本地模拟
"""

import argparse
import json
import os
import random
import socket
import sys
import time

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor


# ----------------------------------------------------------------------
# 连接地址解析：Docker 容器内用 compose 服务名，宿主机用 localhost
# ----------------------------------------------------------------------
def _conn_host(env_key, default):
    return os.environ.get(env_key, default)


KAFKA_BOOTSTRAP = _conn_host("KAFKA_BOOTSTRAP", "localhost:9092")
MYSQL_HOST = _conn_host("MYSQL_HOST", "localhost")
MYSQL_PORT = int(_conn_host("MYSQL_PORT", "3306"))


def set_python_env(env):
    """
    Windows 上必须配的一步：指定 Python 解释器。
    否则 Flink 启动 Python Worker 时用的是裸命令 `python`，
    找不到装了 PyFlink 的那个环境，报 ModuleNotFoundError: No module named 'pyflink'。
    """
    env.set_python_executable(sys.executable)


class OrderTimestampAssigner(TimestampAssigner):
    """
    从订单数据里提取事件时间。

    【坑】不能写 lambda！with_timestamp_assigner 要的是
    TimestampAssigner 的实现类，传 lambda 会报：
        AttributeError: 'function' object has no attribute 'extract_timestamp'
    """
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        # (order_id, user_id, category, amount, event_time)
        return value[4]


def port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ======================================================================
# 模拟数据生成器（本地模式用）
# ======================================================================
USERS = ["u1001", "u1002", "u1003", "u1004", "u1005"]
CATEGORIES = ["数码", "服饰", "食品", "家居", "图书"]

def gen_fake_orders(n: int = 60):
    """生成 n 条模拟订单，故意混入可疑交易。"""
    now = int(time.time() * 1000)
    orders = []
    for i in range(n):
        user = random.choice(USERS)
        amount = random.choice([
            random.uniform(50, 800),        # 正常小额
            random.uniform(1000, 9000),     # 正常大额
            random.uniform(10000, 30000),   # 可能触发 R2
        ])
        orders.append((
            f"o{1000 + i}",
            user,
            random.choice(CATEGORIES),
            round(amount, 2),
            now + i * 300,        # 事件时间：每 300ms 一条
        ))
    # 人为注入一个「高频交易」用户：u1003 连续快速下单
    for j in range(5):
        orders.append((
            f"o_risk_{j}",
            "u1003",
            "数码",
            2999.0,
            now + 4000 + j * 500,   # 500ms 一笔，10秒内 5 笔
        ))
    orders.sort(key=lambda x: x[4])
    return orders


# ======================================================================
# 核心风控逻辑：KeyedProcessFunction
# ======================================================================
class RiskDetector(KeyedProcessFunction):
    """
    按用户分组，维护两类状态：
        · 滑动窗口内的交易列表（自己实现，避免窗口和状态混用带来的理解成本）
        · 24 小时累计消费额

    检测到风险就以 JSON 字符串形式 yield 出来。
    """

    # 规则参数（真实项目应从配置中心或维表加载）
    HIGH_FREQ_COUNT = 3          # 10 秒内 >= 3 笔
    HIGH_FREQ_WINDOW_MS = 10_000
    LARGE_AMOUNT = 20_000.0      # 单笔 > 2 万
    DAILY_LIMIT = 50_000.0       # 24h 累计 > 5 万

    def open(self, rc: RuntimeContext):
        # 最近交易：(时间戳, 金额, 订单号) 的列表
        self.recent_txns = rc.get_list_state(
            ListStateDescriptor(
                "recent_txns",
                Types.TUPLE([Types.LONG(), Types.DOUBLE(), Types.STRING()]),
            )
        )
        # 24h 累计消费
        self.daily_total = rc.get_state(
            ValueStateDescriptor("daily_total", Types.DOUBLE())
        )
        # 定时器标识：记录最近一次注册的清理定时器，避免重复注册
        self.last_timer = rc.get_state(
            ValueStateDescriptor("last_timer", Types.LONG())
        )

    def _emit(self, rule_id: str, user: str, order_id: str, amount: float, detail: str):
        """构造告警 JSON"""
        return json.dumps({
            "rule_id": rule_id,
            "user_id": user,
            "order_id": order_id,
            "amount": amount,
            "detail": detail,
            "alert_time": int(time.time() * 1000),
        }, ensure_ascii=False)

    def process_element(self, value, ctx):
        # 数据格式：(order_id, user_id, category, amount, event_time)
        order_id, user, category, amount, event_time = value

        # --------------------------------------------------------------
        # 规则 R2：大额异常（单笔即可判定，不需要状态）
        # --------------------------------------------------------------
        if amount > self.LARGE_AMOUNT:
            yield self._emit(
                "R2-大额异常", user, order_id, amount,
                f"单笔金额 {amount:.2f} 超过阈值 {self.LARGE_AMOUNT:.0f}"
            )

        # --------------------------------------------------------------
        # 规则 R1：高频交易（需要维护最近 10 秒的交易）
        # --------------------------------------------------------------
        recent = self.recent_txns.get()
        records = list(recent) if recent else []

        # 清理超出窗口的旧记录
        records = [r for r in records if event_time - r[0] <= self.HIGH_FREQ_WINDOW_MS]
        records.append((event_time, amount, order_id))
        self.recent_txns.update(records)

        if len(records) >= self.HIGH_FREQ_COUNT:
            span = records[-1][0] - records[0][0]
            yield self._emit(
                "R1-高频交易", user, order_id, amount,
                f"{span}ms 内发生 {len(records)} 笔交易"
            )

        # 注册定时器：窗口过期后清理状态（避免状态无限增长）
        # 这是有状态流处理的一个重要实践 —— 状态一定要有「出口」
        last = self.last_timer.value()
        trigger_time = event_time + self.HIGH_FREQ_WINDOW_MS + 1000
        if last is None or trigger_time > last:
            ctx.timer_service().register_event_time_timer(trigger_time)
            self.last_timer.update(trigger_time)

        # --------------------------------------------------------------
        # 规则 R3：24 小时累计超限
        # --------------------------------------------------------------
        total = (self.daily_total.value() or 0.0) + amount
        self.daily_total.update(total)
        if total > self.DAILY_LIMIT:
            yield self._emit(
                "R3-累计超限", user, order_id, amount,
                f"24h 累计消费 {total:.2f} 超过限额 {self.DAILY_LIMIT:.0f}"
            )

    def on_timer(self, timestamp, ctx):
        """
        定时器触发：清理过期的状态。

        【大坑】on_timer 必须是「生成器」！

        即使你没有任何输出要发射，也必须让函数体里出现 yield，
        否则它返回 None，Flink 内部执行
            for result in results:
        就会报：
            TypeError: 'NoneType' object is not iterable
        而且这个错误发生在定时器回调里，堆栈很深，很难定位。

        正确做法：让函数体里出现 yield。
        即使没有任何业务输出要发射，也写一句 `if False: yield` 之类的占位。
        """
        recent = self.recent_txns.get()
        if recent:
            records = [
                r for r in recent
                if timestamp - r[0] <= self.HIGH_FREQ_WINDOW_MS + 1000
            ]
            if records:
                self.recent_txns.update(records)
            else:
                self.recent_txns.clear()

        # 关键：这个 yield 让 on_timer 成为生成器。
        # 清理定时器没有业务输出，所以条件永远为假，不会真的发射数据。
        if False:
            yield None


# ======================================================================
# 主流程
# ======================================================================
def build_local_job():
    """本地模拟模式：不依赖任何外部服务，一定能跑通。"""
    print("\n" + "=" * 70)
    print("模式：本地模拟（未检测到 Kafka，使用内存数据）")
    print("=" * 70 + "\n")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    set_python_env(env)
    env.enable_checkpointing(2000)

    orders = gen_fake_orders(60)
    print(f"已生成 {len(orders)} 条模拟订单，开始风控检测…\n")

    stream = env.from_collection(
        orders,
        type_info=Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(),
            Types.DOUBLE(), Types.LONG(),
        ]),
    )

    # 分配事件时间 + watermark（允许 3 秒乱序）
    timestamped = stream.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(3))
        .with_timestamp_assigner(OrderTimestampAssigner())
    )

    # 风控检测
    alerts = timestamped.key_by(lambda e: e[1]).process(RiskDetector())

    # 格式化输出
    def pretty(alert_json: str) -> str:
        a = json.loads(alert_json)
        return (
            f"[告警] {a['rule_id']:<12} "
            f"用户={a['user_id']} "
            f"订单={a['order_id']:<12} "
            f"金额={a['amount']:>10.2f}  "
            f"原因：{a['detail']}"
        )

    alerts.map(pretty).print()

    env.execute("08-realtime-risk-control-local")


def build_kafka_job():
    """
    Kafka 模式：从 Kafka 读订单流，检测后写回 Kafka。
    需要 docker/docker-compose.yml 里的 Kafka 已启动，
    并且 flink-connector-kafka JAR 在 classpath 上。
    """
    # 延迟导入：Kafka 连接器依赖额外的 JAR 包，
    # 放在函数内部可以让「本地模拟模式」在没装 JAR 时也能正常跑
    from pyflink.datastream.connectors.kafka import (
        KafkaSource,
        KafkaSink,
        KafkaRecordSerializationSchema,
        KafkaOffsetsInitializer,
    )
    from pyflink.common.serialization import SimpleStringSchema

    print("\n" + "=" * 70)
    print("模式：Kafka 实时管道")
    print("=" * 70 + "\n")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    set_python_env(env)
    env.enable_checkpointing(5000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("orders")
        .set_group_id("pyflink-risk-control")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # 从 Kafka 读到的是 JSON 字符串，需要解析成元组

    class ParseOrder(MapFunction):
        def map(self, raw: str):
            try:
                d = json.loads(raw)
                return (
                    str(d["order_id"]),
                    str(d["user_id"]),
                    str(d.get("category", "")),
                    float(d["amount"]),
                    int(d.get("event_time", int(time.time() * 1000))),
                )
            except Exception:
                # 脏数据返回 None，下游过滤掉
                # 生产环境应把脏数据写到专门的 DLQ（死信队列）
                return None

    # PyFlink 1.20 推荐用 from_source；旧的 add_source(source, name) 签名已不兼容
    kafka_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-orders")

    parsed = (
        kafka_stream
        .map(ParseOrder())
        .filter(lambda x: x is not None)
    )

    timestamped = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(3))
        .with_timestamp_assigner(OrderTimestampAssigner())
    )

    alerts = timestamped.key_by(lambda e: e[1]).process(RiskDetector())

    def pretty(alert_json: str) -> str:
        a = json.loads(alert_json)
        return (
            f"[告警] {a['rule_id']:<12} 用户={a['user_id']} "
            f"订单={a['order_id']:<10} 金额={a['amount']:>10.2f}  {a['detail']}"
        )

    formatted = alerts.map(pretty)
    formatted.print()

    # 同时写回 Kafka，供下游告警服务消费
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("risk-alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    alerts.map(lambda x: x).sink_to(sink)

    env.execute("08-realtime-risk-control-kafka")


def main():
    parser = argparse.ArgumentParser(description="实时交易风控")
    parser.add_argument("--local", action="store_true", help="强制本地模拟模式（内存数据，跑完即退出）")
    parser.add_argument(
        "--kafka",
        action="store_true",
        help="强制 Kafka 流模式（作业常驻运行，需 Ctrl-C 或 Flink Web UI 停止）",
    )
    args = parser.parse_args()

    kafka_ok = port_open(KAFKA_BOOTSTRAP.rsplit(":", 1)[0], int(KAFKA_BOOTSTRAP.rsplit(":", 1)[1]))

    # 默认策略：检测到 Kafka 就走真实流管道；--local 强制降级。
    # 注意：Kafka 模式是常驻流作业，不会自己结束 —— 批量测试请用 --local。
    if args.kafka or (kafka_ok and not args.local):
        if not kafka_ok and args.kafka:
            print(f"✗ 指定了 --kafka 但连不上 {KAFKA_BOOTSTRAP}")
            print("  启动方式：cd docker && docker compose up -d kafka")
            sys.exit(1)
        print(f"模式：Kafka 流处理（{KAFKA_BOOTSTRAP}）")
        print("提示：这是常驻流作业，不会自动退出。停止用 Ctrl-C 或 Flink Web UI。")
        build_kafka_job()
    else:
        if not args.local:
            print(f"提示：未检测到 Kafka（{KAFKA_BOOTSTRAP}），自动降级为本地模拟模式。")
            print("      想跑真 Kafka：cd docker && docker compose up -d kafka")
        build_local_job()


if __name__ == "__main__":
    main()
