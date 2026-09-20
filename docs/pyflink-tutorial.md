# PyFlink 从入门到生产 · 完整学习教程

> 配套工程：`pyflink-tutorial/`
> 版本基线：**Flink 1.20.0 / Python 3.11.9 / JDK 11**（本机已实测跑通全部 8 个示例）
> 难度曲线：8 个示例递进，从 WordCount 到实时风控系统
>
> **本教程的特色**：所有示例都在真实环境跑过，附录里的 22 个坑全部来自实测，
> 包括几个网上教程很少提、但一定会卡住你的问题（如 Windows 上 Python Worker 起不来、
> `add()` 参数顺序、`on_timer` 必须是生成器、UDAF 的批流限制等）。

---

## 目录

- [第 0 章 · 30 秒建立正确认知](#第-0-章)
- [第 1 章 · 环境搭建（含你本机的实测结论）](#第-1-章)
- [第 2 章 · 核心概念：为什么是 Flink 而不是 Spark](#第-2-章)
- [第 3 章 · Table API 与 SQL](#第-3-章)
- [第 4 章 · 时间语义与窗口](#第-4-章)
- [第 5 章 · 状态管理与容错](#第-5-章)
- [第 6 章 · 连接器](#第-6-章)
- [第 7 章 · Python UDF](#第-7-章)
- [第 8 章 · 生产实践与调优](#第-8-章)
- [第 9 章 · 实战项目：实时风控](#第-9-章)
- [附录 · 学习路线与常见坑](#附录)

---

<a name="第-0-章"></a>
## 第 0 章 · 30 秒建立正确认知

PyFlink 的本质是：**用 Python 写 Flink 作业的 API 层，底层跑的还是 Java 引擎。**

这不是一个"Python 版的流处理框架"，而是 Java Flink 的 Python 门面。理解这一点，后面很多困惑会自然消解：

| 现象 | 原因 |
|---|---|
| 为什么启动要好几秒 | 背后要拉起 JVM |
| 为什么 UDF 比 SQL 慢十倍 | Python 数据要跨进程传输 |
| 为什么某些 connector 要手动下 JAR | 那是 Java 的类库 |
| 为什么 Python 版本卡在 3.11 | Py4J 网关的兼容性约束 |

**两条编程路径，选哪条？**

| 路径 | 适合场景 | 推荐度 |
|---|---|---|
| **Table API / SQL** | 90% 的 ETL、聚合、实时数仓 | ⭐⭐⭐⭐⭐ 首选 |
| **DataStream API** | 复杂事件处理、自定义状态逻辑、非 SQL 语义 | ⭐⭐⭐ 按需 |

**我的建议**：先用 SQL 把 80% 的需求解决掉，剩下的用 UDF 填充，实在不行才下探到 DataStream API。很多新手一上来就啃 DataStream API，被状态和窗口绕晕，实际上业务根本用不到那么底层。

---

<a name="第-1-章"></a>
## 第 1 章 · 环境搭建

### 1.1 版本兼容性矩阵（先看这张表，能省你两小时）

| PyFlink | Flink | Python 支持 | Java 要求 |
|---|---|---|---|
| 2.0.x | 2.0 | 3.9 – 3.12 | **Java 11/17** |
| **1.20.x** | 1.20 | 3.9 – 3.11 | **Java 11/17** ← 本教程使用 |
| 1.19.x | 1.19 | 3.9 – 3.11 | **Java 11/17** |
| 1.18.x | 1.18 | 3.9 – 3.11 | **Java 11/17** |
| 1.17.x | 1.17 | 3.8 – 3.11 | Java 8/11 |
| 1.16.x | 1.16 | 3.8 – 3.10 | Java 8/11 |

**三个硬约束，踩了必报错：**
1. Python 超过 3.11 → Py4J 网关建立失败
2. Java 低于 11 → `UnsupportedClassVersionError`
3. PyFlink 和 Flink 大版本必须一致 → 否则序列化协议不匹配

### 1.2 关于你这台机器（含实测结论）

我实测了当前环境，结论比预想的好——**你的机器已经具备了跑 PyFlink 的全部条件**，只是配置没接上。

| 项 | 位置 | 状态 |
|---|---|---|
| **Python 3.11.9** | `D:\Python\python.exe` | ✓ 完美（PyFlink 最优版本） |
| **JDK 11.0.22 LTS** | `C:\Program Files\Java\jdk-11` | ✓ 已装，但没被用上 |
| JAVA_HOME | 指向 JDK 1.8 | ✗ 需要改指向 11 |
| **uv** | `C:\Users\wzm\.local\bin\uv.exe` | ✓ 已装（比 pip 快得多） |
| Docker | 已安装 | ✓ 备选方案 |
| Python 3.13.14 | 托管版本 | ✗ 不能用（见下） |

**关键发现 1：系统 JAVA_HOME 指向了 1.8，但机器上其实有 JDK 11。**

```
JAVA_HOME = C:\PROGRA~2\Java\JDK18~1.0_4   ← 这是 JDK 1.8，Flink 用不了
```

Flink 1.18+ 要求 Java 11/17。好消息是 `C:\Program Files\Java\jdk-11` 已经装好了，只要在跑 PyFlink 时把 `JAVA_HOME` 指向它就行——**不需要动系统全局配置**，本工程的 `scripts/env.sh` 已经处理好了。

**关键发现 2：Python 3.13 确实装不了 PyFlink（我试了）。**

在 3.13 上执行 `pip install apache-flink==1.18.1`，1 分半后失败：

```
ModuleNotFoundError: No module named 'pkg_resources'
ERROR: Failed to build 'apache-beam' when getting requirements to build wheel
```

原因：PyFlink 依赖 `apache-beam` 处理批式数据，而 Beam 5.x 的构建脚本用的是早已从 setuptools 移除的 `pkg_resources` API。Python 3.13 自带的新版 setuptools 里没有这个模块，构建环境直接中断。

这不是"配置问题"或者"少装个包"能绕过的——是上游依赖链的兼容性断层。**结论：不要在 3.13 上装 PyFlink。**

**所以：本机走「Python 3.11 + JDK 11 + uv」这条路，5 分钟能跑起来。**

```bash
# 一条命令装好（会自动检查并创建虚拟环境）
bash scripts/setup_uv.sh

# 加载环境变量后就能跑示例
source scripts/env.sh
python examples/01_hello_wordcount.py
```

如果你换了机器，本地装不了，那就用 Docker：

```bash
cd docker && docker compose up -d
docker compose exec jobmanager python /opt/flink/examples/01_hello_wordcount.py
```

### 1.3 方案 A：Docker（推荐）

```bash
cd pyflink-tutorial/docker
docker compose up -d
```

启动后可以访问：

| 服务 | 地址 | 用途 |
|---|---|---|
| Flink Web UI | http://localhost:8081 | 看作业状态、背压、checkpoint |
| Kafka UI | http://localhost:8082 | 看 topic 消息 |
| MySQL | localhost:3306 | root / root123456 |
| SQL Client | `docker compose exec sql-client ./bin/sql-client.sh` | 交互式探索 |

在容器里跑示例：

```bash
docker compose exec jobmanager python /opt/flink/examples/01_hello_wordcount.py
```

### 1.4 方案 B：本地安装（如果你想装）

```bash
# 1. 装 JDK 17
winget install EclipseAdoptium.Temurin.17.JDK

# 2. 装 Python 3.11（关键！不能用你现有的 3.13）
winget install Python.Python.3.11

# 3. 一键脚本会做检查和安装
bash scripts/setup.sh --check   # 先检查环境
bash scripts/setup.sh           # 再执行安装
```

脚本会在 `~/.workbuddy/binaries/pyflink-venv` 下建独立虚拟环境，不污染你的系统 Python。

### 1.5 验证安装

```bash
python -c "
import pyflink
print('PyFlink:', pyflink.__version__)
from pyflink.table import TableEnvironment, EnvironmentSettings
t = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
t.create_temporary_view('t', t.from_elements([('hello',), ('flink',)], ['w']))
t.sql_query('SELECT w, COUNT(1) c FROM t GROUP BY w').execute().print()
"
```

看到 `+I[flink, 1]` 和 `+I[hello, 1]` 就说明通了。

---

<a name="第-2-章"></a>
## 第 2 章 · 核心概念：为什么是 Flink

### 2.1 流处理 vs 微批

这是理解 Flink 价值的起点。

**Spark Streaming 的做法（微批）**：
```
数据流 ──> [攒1秒] ──> 当成一个小批次跑 ──> [攒1秒] ──> ...
```
延迟至少 1 秒起步，而且窗口边界是人为切的，跨批次的状态要靠外部存储。

**Flink 的做法（真流式）**：
```
数据流 ──> 每来一条就处理一条，状态常驻内存，延迟毫秒级
```

这个差异在三个场景下是决定性的：

1. **低延迟要求**：风控、告警、实时推荐 —— 秒级延迟就失去意义
2. **复杂事件匹配**：比如"3 次登录失败后 5 分钟内转账"这种跨事件的模式
3. **精确一次的状态**：微批的状态一致性要额外做很多工作，Flink 内建支持

### 2.2 Flink 的运行时骨架

```
┌─────────────────────────────────────────────────┐
│  Client（提交作业的地方）                          │
│  pyflink 脚本 → 生成 JobGraph → 提交给集群         │
└──────────────────┬──────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────┐
│  JobManager（大脑）                               │
│  · 把 JobGraph 翻译成 ExecutionGraph              │
│  · 调度 Task 到 TaskManager                       │
│  · 协调 Checkpoint（容错的核心）                   │
│  · 故障恢复（failover）                           │
└──────────────────┬──────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────┐
│  TaskManager（苦力）                              │
│  · 每个 TM 有若干 TaskSlot                       │
│  · 执行具体的算子（map/filter/join/...）           │
│  · 保存状态（state backend）                      │
│  · Python Worker 进程在这里被拉起                  │
└─────────────────────────────────────────────────┘
```

**关键点**：PyFlink 的 Python 代码运行在 TaskManager 里的 Python Worker 进程中，通过 Py4J 网关和 JVM 通信。所以：

- 你在 Python 里定义的状态，实际存在 JVM 侧的状态后端里
- Python Worker 挂掉，JVM 侧的算子不受影响（会被重启拉新进程）
- 数据在 Python 和 JVM 之间传输时，走 Arrow 列式格式以减少开销

### 2.3 算子链与并行度

```python
stream.map(f1).map(f2).filter(f3)
```

默认情况下这三个算子会被**链在一起**（operator chaining）放进同一个 Task 里执行，避免线程间传输开销。你可以用 `.disable_chaining()` 关掉，但通常不需要。

**并行度（parallelism）** 决定了有多少个并行的 Task 实例：

```python
env.set_parallelism(4)   # 这个算子会被切成 4 份并行跑
```

对 Python 学习者来说，开始阶段建议一律设成 1，原因：输出顺序可预测、日志好看、启动快。等真正做性能调优时再调。

### 2.4 数据交换的两种模式

| 模式 | 触发条件 | 代价 |
|---|---|---|
| **Forward** | 上下游并行度相同，不需要重分区 | 零开销 |
| **KeyBy / Rebalance / Broadcast** | 需要按 key 分区或打散 | 走网络，有序列化成本 |

`key_by()` 是分水岭——它把流变成 `KeyedStream`，之后才能用状态和窗口。同一个 key 一定会落到同一个并行子任务上，这是状态正确性的基础。

---

<a name="第-3-章"></a>
## 第 3 章 · Table API 与 SQL

对应示例：`examples/01_hello_wordcount.py`、`examples/02_table_api.py`

### 3.1 两个入口，别搞混

```python
# 入口 A：TableEnvironment —— Table API / SQL 用
from pyflink.table import EnvironmentSettings, TableEnvironment
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

# 入口 B：StreamExecutionEnvironment —— DataStream API 用
from pyflink.datastream import StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
```

**批模式还是流模式？**

```python
EnvironmentSettings.in_batch_mode()      # 有界数据，跑完退出。适合离线分析
EnvironmentSettings.in_streaming_mode()  # 无限数据，一直跑。适合实时
```

有个反直觉的点：**流模式可以跑批数据，批模式不能跑流数据。** 所以如果你想一套代码通吃，用流模式，Flink 对批输入会自动优化。

### 3.2 建表：三种数据来源

**① from_elements —— 调试神器**

```python
t_env.from_elements(
    [("flink",), ("python",)],     # 数据行
    ["word"],                       # 列名
)
```

零依赖、秒级启动，**学习阶段 90% 的时间应该用它**。别一上来就折腾 Kafka。

**② CREATE TABLE 连外部系统**

```python
t_env.execute_sql("""
    CREATE TABLE orders (
        order_id STRING,
        amount   DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'orders',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json'
    )
""")
```

**③ 从 DataStream 转过来**

```python
table = t_env.from_data_stream(datastream)   # 见示例 08
```

### 3.3 Table API ↔ SQL 对照表

这两者是**同一个东西的两种写法**，可以随时互转。

| 操作 | SQL | Table API |
|---|---|---|
| 选列 | `SELECT a, b` | `.select(t.a, t.b)` |
| 过滤 | `WHERE x > 10` | `.filter(t.x > 10)` |
| 分组 | `GROUP BY k` | `.group_by(t.k)` |
| 聚合 | `SUM(v)` | `t.v.sum` |
| 排序 | `ORDER BY v DESC` | `.order_by(t.v.desc)` |
| 限制 | `LIMIT 10` | `.limit(10)` |
| 去重 | `DISTINCT` | `.distinct()` |
| 别名 | `AS alias` | `.alias("alias")` |
| 插入 | `INSERT INTO` | `.execute_insert("sink")` |
| 转 SQL | — | `t_env.sql_query(f"SELECT * FROM {table}")` |

**聚合函数速查**（Table API）：

```python
t.col.sum      # 求和
t.col.count    # 计数
t.col.avg      # 平均
t.col.min      # 最小
t.col.max      # 最大
t.col.stddev_pop  # 总体标准差
t.col.var_pop     # 总体方差
t.col.count.distinct   # 去重计数
t.col.sum(0)          # 带初始值
```

### 3.4 惰性求值：新手最容易懵的地方

```python
result = (orders
    .filter(orders.amount > 100)
    .group_by(orders.category)
    .select(orders.category, orders.amount.sum))
# ↑ 到这里为止，一行数据都没算过！

result.execute().print()   # ← 这才真正提交作业开始计算
```

链式调用只是在**描述计算逻辑**（构建 DAG）。真正的执行被推迟到 `execute()`。所以：

- 中间变量赋值不会触发任何计算
- 同一个 `Table` 对象可以 `execute()` 多次（会起多个作业）
- 想复用结果，用 `create_temporary_view` 注册成视图

### 3.5 结果输出的几种方式

```python
# ① 打印到控制台（学习阶段最常用）
table.execute().print()

# ② 转成 Python 迭代器自己处理
with table.execute().collect() as rows:
    for row in rows:
        print(row[0], row[1])

# ③ 写入外部表
table.execute_insert("mysql_sink_table")

# ④ 只在流模式下用：把 changelog 转成 append 流再处理
table.to_changelog_stream()
```

**changelog 标记含义**（看输出时会一直见到）：

| 标记 | 含义 | 触发场景 |
|---|---|---|
| `+I` | Insert，新增行 | 普通插入 |
| `-U` | Update Before，更新前 | 聚合结果发生变化 |
| `+U` | Update After，更新后 | 聚合结果发生变化 |
| `-D` | Delete，删除行 | 删除操作 |

聚合查询在流模式下会持续输出 `-U/+U` 对，因为结果是动态变化的。这不是 bug。

---

<a name="第-4-章"></a>
## 第 4 章 · 时间语义与窗口

对应示例：`examples/03_watermark_window.py`、`examples/04_window_types.py`

**这是 PyFlink 最核心的一章，也是最容易搞错的一章。请慢读。**

### 4.1 三种时间概念

| 时间类型 | 含义 | 什么时候用 |
|---|---|---|
| **事件时间** Event Time | 数据里自带的时间戳 | 结果要求准确 → 首选 |
| **处理时间** Processing Time | 数据到达处理机的系统时钟 | 只看"最近N秒"不要求精确 |
| **摄入时间** Ingestion Time | 数据进入 Flink 的时间 | 极少用，已被事件时间替代 |

**举个例子说明差异：**

假设手机在隧道里，产生的点击日志 10:00:00 生成，10:00:30 才连上网络传到服务器。

- 用**事件时间**：这条日志属于 `[10:00:00, ...)` 的窗口 —— 正确
- 用**处理时间**：这条日志属于 `[10:00:30, ...)` 的窗口 —— 算错了

### 4.2 Watermark：乱序问题的解药

**问题**：窗口 `[10:00:00, 10:00:05)` 什么时候能关闭并输出结果？

如果永远等，就是无界等待。Watermark 给了一个答案：

> Watermark T 表示：「我保证，不会再出现事件时间 ≤ T 的数据了。」

Flink 收到 watermark T，就关闭所有 `end_time ≤ T` 的窗口。

**怎么生成 Watermark？**

```python
from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner

class MyTimestampAssigner(TimestampAssigner):
    """必须定义成类，不能写 lambda —— 见下面「两个必踩的坑」"""
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[2]          # 用第 3 个字段当事件时间

watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(5))
    .with_timestamp_assigner(MyTimestampAssigner())
)

stream = env.from_collection(data).assign_timestamps_and_watermarks(watermark_strategy)
```

`for_bounded_out_of_orderness(5秒)` 的意思是：**允许 5 秒的乱序**。Flink 会把「已见最大事件时间 − 5秒」作为 watermark。

#### 坑 1：`with_timestamp_assigner` 不能传 lambda

直觉写法（我在初稿里就是这么写的）：

```python
.with_timestamp_assigner(lambda event, ts: event[2])   # ✗ 报错
```

报错信息：

```
AttributeError: 'function' object has no attribute 'extract_timestamp'
```

**原因**：`with_timestamp_assigner` 要求一个 `TimestampAssigner` **实现类的实例**。Flink 需要把这个对象序列化后分发到各个 TaskManager，普通 lambda 没法跨进程传递。

**正确做法**：定义一个继承 `TimestampAssigner` 的小类，实现 `extract_timestamp` 方法（见上面示例）。

注意导入路径有点反直觉：`TimestampAssigner` 在 **`pyflink.common.watermark_strategy`** 模块里，不在 `pyflink.datastream.functions`。

#### 坑 2：`Duration` 和 `Time` 是两个不同的类

这个坑会让你怀疑人生，因为报错信息完全指错了方向。

```python
from pyflink.common import Duration          # 用于 Watermark、lateness
from pyflink.common.time import Time         # 用于窗口的 of()

# ✓ 正确
TumblingEventTimeWindows.of(Time.seconds(5))

# ✗ 错误
TumblingEventTimeWindows.of(Duration.of_seconds(5))
# AttributeError: 'Duration' object has no attribute 'to_milliseconds'
```

**怎么记**：

| 用在哪儿 | 用哪个类 |
|---|---|
| 窗口分配器 `.of()` 的参数 | **`Time`** |
| `StateTtlConfig.new_builder()` | **`Time`** |
| `WatermarkStrategy.for_bounded_out_of_orderness()` | `Duration` |
| `allowed_lateness()` | `Duration` |

一句话：**「开窗口、设 TTL」用 Time，「设延迟、设容忍」用 Duration。** 记不住就试一次，报错里那个 `has no attribute 'to_milliseconds'` 就是信号——说明你给的地方要 `Time`。

**这个参数怎么选？** 它是准确性和延迟的权衡：

| 设置 | 效果 |
|---|---|
| 太小（如 1ms） | 窗口关闭快、延迟低，但迟到数据全丢 |
| 太大（如 1小时） | 数据不丢，但结果要等 1 小时才出 |
| 经验值 | 观察数据乱序分布，取 P99 乱序程度 |

**迟到数据怎么办？**

```python
# 方式一：允许窗口在 watermark 之后继续处理迟到数据
.window(...).allowed_lateness(Duration.of_minutes(1))

# 方式二：把迟到数据收集到侧输出流
.window(...).side_output_late_data(late_tag)
```

### 4.3 四种窗口

| 窗口 | 图解 | 特点 | 典型场景 |
|---|---|---|---|
| **滚动** Tumbling | `[0-5) [5-10) [10-15)` | 不重叠，固定大小 | 每分钟统计 |
| **滑动** Sliding | `[0-5) [2-7) [4-9)` | 重叠，有步长 | 近5分钟实时看板 |
| **会话** Session | `[0-3) ... [8-12)` | 按空闲间隔切分 | 用户行为分析 |
| **全局** Global | `[0, ∞)` | 一个窗口装所有 | 需要自定义 Trigger |

```python
from pyflink.datastream.window import (
    TumblingEventTimeWindows, SlidingEventTimeWindows,
)

# 滚动窗口：5 秒一个
TumblingEventTimeWindows.of(Duration.of_seconds(5))

# 滑动窗口：窗口 10 秒，每 2 秒滑动一次
SlidingEventTimeWindows.of(Duration.of_seconds(10), Duration.of_seconds(2))
```

**窗口的完整调用链：**

```python
stream
    .key_by(lambda e: e[0])                        # ① 分组
    .window(TumblingEventTimeWindows.of(...))      # ② 开窗
    .aggregate(MyAggregator())                     # ③ 聚合
```

**注意：`key_by` 不是可选的。** 窗口算子必须作用在 `KeyedStream` 上，否则多个 key 的数据会混进同一个窗口。

### 4.4 三种聚合函数，选哪个？

| 类型 | 内存占用 | 灵活度 | 适用 |
|---|---|---|---|
| `ReduceFunction` | 极低 | 低（输入输出同类型） | 简单归约（求和、取最大） |
| `AggregateFunction` | **极低** | 中（有累加器） | **推荐默认选择** |
| `ProcessWindowFunction` | **高（缓存全量）** | 高（能拿窗口元信息） | 需要窗口起止时间等元数据 |

**内存差异有多大？** 假设窗口里有 100 万条数据：

- `AggregateFunction`：只存一个累加器对象（比如一个二元组）→ 几十字节
- `ProcessWindowFunction`：缓存 100 万条原始数据 → 可能几百 MB

#### 坑：`AggregateFunction.add()` 的参数顺序跟 Java 版是反的

这是本教程里最隐蔽的一个坑，因为它**不报错、但结果全错**，或者报一个跟真实原因毫无关系的错。

PyFlink 的实际签名是：

```python
def add(self, value, accumulator):     # ← 新元素在前，累加器在后！
    ...
```

依据（PyFlink 源码 `fn_execution/state_impl.py`）：

```python
accumulator = self._agg_function.add(v, accumulator)
```

而 Flink **Java 版**是 `add(accumulator, value)`。如果你照着 Java 文档写，就会：

1. 把累加器当元素、元素当累加器
2. 然后报出一个让人摸不着头脑的错误，比如：

```
TypeError: can only concatenate str (not "int") to str
```

（因为你的累加器是 `(float, int)` 元组，元素是 `(sensor_id, temp)` 元组，把前者当 value 拿去算温度就崩了）

**正确写法**：

```python
class AvgTempAggregator(AggregateFunction):
    def create_accumulator(self):
        return (0.0, 0)                      # (总和, 计数)

    def add(self, value, acc):               # value 在前，acc 在后
        total, count = acc
        return (total + value[1], count + 1)

    def get_result(self, acc):
        total, count = acc
        return total / count if count > 0 else 0.0

    def merge(self, acc1, acc2):             # 会话窗口合并时会用到
        return (acc1[0] + acc2[0], acc1[1] + acc2[1])
```

**注意**：这个坑只存在于 **DataStream API 的 AggregateFunction**。

Table API 的 UDAF 参数顺序又是反的（`accumulate(accumulator, input_row)`），见第 7 章。总之：**两套 API 的聚合参数顺序各写各的，写之前先看签名。**

### 4.5 黄金组合（生产环境标准写法）

```python
stream.key_by(...) \
    .window(...) \
    .aggregate(MyAggregator(), MyWindowResultFunction())
#             ↑增量计算         ↑只补窗口元信息，不缓存数据
```

`ProcessWindowFunction` 的 `elements` 里装的是 `AggregateFunction` 的**结果值**，而不是原始数据。这样既有增量计算的低内存，又有窗口元信息。

示例 04 里有完整实现，直接抄。

---

<a name="第-5-章"></a>
## 第 5 章 · 状态管理与容错

对应示例：`examples/05_stateful.py`

### 5.1 为什么状态是 Flink 的看家本领

有些计算**必须记住历史**：

- 用户累计消费额（跨事件累计）
- 5 分钟内同一订单不重复处理（去重）
- 3 次登录失败后告警（事件模式匹配）
- 实时特征（用户最近 10 次点击的品类分布）

这些用无状态的 map/filter 做不到。Flink 的状态是：

- **本地的**：存在 TaskManager 内存/RocksDB 里，访问是内存级速度
- **分区的**：每个 key 一份独立状态，天然支持并行
- **容错的**：通过 checkpoint 持久化，故障后可精确恢复

### 5.2 KeyedState 的三种形态

```python
from pyflink.datastream.state import (
    ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor,
)

class MyFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        # 单值：累计额、计数器、最近一次时间
        self.total = runtime_context.get_state(
            ValueStateDescriptor("total", Types.DOUBLE())
        )

        # 列表：最近 N 笔交易
        self.recent = runtime_context.get_list_state(
            ListStateDescriptor("recent", Types.STRING())
        )

        # 字典：各品类消费额
        self.by_cat = runtime_context.get_map_state(
            MapStateDescriptor("by_cat", Types.STRING(), Types.DOUBLE())
        )
```

**最重要的坑：状态必须在 `open()` 里创建，不能在 `__init__()` 里。**

原因：状态需要运行时上下文（RuntimeContext），而这个上下文只有在 `open()` 阶段才可用。在 `__init__` 里创建会报 `RuntimeContext not available`。

**API 速查：**

| 状态类型 | 读 | 写 | 清 |
|---|---|---|---|
| ValueState | `.value()` | `.update(v)` | `.clear()` |
| ListState | `.get()` | `.add(v)` / `.update(list)` | `.clear()` |
| MapState | `.get(k)` / `.items()` | `.put(k,v)` | `.remove(k)` / `.clear()` |

### 5.3 State TTL：防止状态无限膨胀

流处理是无限跑的，用户状态只增不减 → 迟早 OOM。

```python
from pyflink.common import Duration
from pyflink.datastream.state import StateTtlConfig

ttl_config = (
    StateTtlConfig
    .new_builder(Duration.of_days(30))                          # 30 天不访问就清
    .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)  # 写时刷新时间
    .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanup_incrementally(10, True)                            # 增量清理
    .build()
)

desc = ValueStateDescriptor("my_state", Types.DOUBLE())
desc.enable_time_to_live(ttl_config)
```

**`set_state_visibility` 两个选项的区别：**

- `NeverReturnExpired`：过期状态读出来是 None（**推荐**，语义干净）
- `ReturnExpiredIfNotCleanedUp`：清理前还能读到（可能读到旧值，慎用）

### 5.4 Checkpoint：容错的生命线

```python
from pyflink.datastream import CheckpointingMode

env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
```

每隔 60 秒，Flink 会把所有算子的状态做一次一致性快照。作业挂掉重启时，从最近快照恢复，做到「精确一次」。

**两种语义：**

| 模式 | 含义 | 代价 |
|---|---|---|
| `EXACTLY_ONCE` | 每条数据只影响状态一次 | 有对齐开销，延迟略高 |
| `AT_LEAST_ONCE` | 可能重复处理 | 吞吐更高 |

**状态后端怎么选？**

| 后端 | 状态放哪 | 适用 |
|---|---|---|
| `HashMapStateBackend` | JVM 堆内存 | 状态小（< 几百 MB），读写最快 |
| `EmbeddedRocksDBStateBackend` | 本地磁盘 | 状态远大于内存（TB 级），略慢 |

```python
from pyflink.datastream import EmbeddedRocksDBStateBackend
env.set_state_backend(EmbeddedRocksDBStateBackend())
```

**生产环境必备配置：**

```python
config = t_env.get_config()
config.set("state.checkpoints.dir", "s3://bucket/flink/checkpoints")
config.set("state.backend.incremental", "true")     # 增量 checkpoint，省带宽
config.set("execution.checkpointing.timeout", "10min")
config.set("execution.checkpointing.min-pause", "30s")   # 两次 ckpt 最小间隔
```

### 5.5 Checkpoint vs Savepoint

| | Checkpoint | Savepoint |
|---|---|---|
| 触发 | 自动，周期性的 | 手动，`flink savepoint <jobid>` |
| 用途 | 故障恢复 | **升级作业、改逻辑、迁移** |
| 生命周期 | 作业停止后通常删除 | 长期保留 |
| 触发方式 | 系统自动 | 用户主动 |

**实践建议**：每次上生产前手动打个 savepoint，改代码后从它恢复，这样能保证状态不丢。

### 5.6 定时器：时间驱动的逻辑

有状态流处理的一个重要能力——**不依赖数据到达，也能触发计算**。

```python
def process_element(self, value, ctx):
    # 注册一个事件时间定时器
    ctx.timer_service().register_event_time_timer(fire_time)

def on_timer(self, timestamp, ctx):
    # 时间到了，自动调用这里
    # 典型用途：清理过期状态、超时告警、会话结束判断
    ...
```

示例 08 的风控系统用它清理过期交易记录。这是防止状态泄漏的标准做法。

#### 坑：`on_timer` 必须是生成器

这个坑的报错信息极其误导：

```
TypeError: 'NoneType' object is not iterable
```

而且堆栈很深，指向 Beam 内部（`beam_operations_slow.py:150`），你很难联想到是自己的 `on_timer` 写错了。

**原因**：PyFlink 内部对定时器回调的处理是：

```python
for result in results:      # results 是 on_timer 的返回值
```

如果你的 `on_timer` 里**一个 `yield` 都没有**，它就是个普通函数，返回 `None`，于是 `for result in None` 崩了。

**正确写法**：即使没有业务输出要发射，也要让函数体里出现 `yield`：

```python
def on_timer(self, timestamp, ctx):
    # ... 清理状态的逻辑 ...

    # 关键：这个 yield 让 on_timer 成为生成器。
    # 清理定时器没有业务输出，所以条件永远为假。
    if False:
        yield None
```

对比一下 `process_element`——它本来就是生成器（用 `yield` 输出），所以没这个问题。**只有 `on_timer` 容易踩**，因为清理类逻辑往往"什么都不用输出"。

---

<a name="第-6-章"></a>
## 第 6 章 · 连接器

对应示例：`examples/06_connectors.py`

### 6.1 JAR 包问题（新手第一个拦路虎）

Flink 官方发行包**不包含**连接器 JAR。你需要自己下载放到 `<FLINK_HOME>/lib/` 目录。

**必须下载的常用 JAR：**

| 连接器 | Maven artifact | 大小 |
|---|---|---|
| Kafka | `flink-sql-connector-kafka-3.2.0-1.20.jar` | ~10MB |
| JDBC | `flink-connector-jdbc-3.2.0-1.19.jar` | ~200KB |
| MySQL 驱动 | `mysql-connector-j-8.0.33.jar` | ~2.5MB |
| 文件 | `flink-connector-files-1.20.0.jar` | ~2MB |
| Elasticsearch | `flink-sql-connector-elasticsearch7-3.1.0-1.19.jar` | ~30MB |

> ⚠️ 连接器 JAR 的版本号里最后那截（`-1.18` / `-1.19`）表示它编译时依赖的 Flink 大版本。
> **不需要和你的 Flink 完全同号**，但跨度别超过 1 个大版本。上表是按 Flink 1.20 配的。

**下载地址**：https://flink.apache.org/downloads/ → 页面底部的 "Additional Components"

**在 PyFlink 作业里动态加载 JAR**（不依赖集群配置）：

```python
t_env.get_config().set(
    "pipeline.jars",
    "file:///path/to/flink-sql-connector-kafka-3.0.2-1.18.jar;"
    "file:///path/to/flink-connector-jdbc-3.1.2-1.18.jar"
)
```

### 6.2 Kafka 连接器

**作为 Source：**

```sql
CREATE TABLE kafka_orders (
    order_id   STRING,
    user_id    STRING,
    amount     DOUBLE,
    order_time TIMESTAMP(3),
    -- 事件时间列 + Watermark 定义（就写在建表语句里，比 DataStream API 简洁）
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'my-consumer-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);
```

**`scan.startup.mode` 的四个选项：**

| 值 | 含义 |
|---|---|
| `earliest-offset` | 从头开始消费（开发调试用） |
| `latest-offset` | 从最新开始（只看新数据） |
| `group-offsets` | 从消费者组记录的位点继续（**生产默认**） |
| `timestamp` | 从指定时间戳开始（回溯某段时间） |

**作为 Sink** 时，加上 `'sink.partitioner'` 控制分区策略，常用 `'fixed'` 或 `'round-robin'`。

### 6.3 JDBC 连接器

```sql
CREATE TABLE mysql_sink (
    category    STRING,
    total_sales DOUBLE,
    PRIMARY KEY (category) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/flink_demo?useSSL=false&serverTimezone=UTC',
    'table-name' = 'category_summary',
    'username' = 'root',
    'password' = 'root123456',
    -- 关键调优参数
    'sink.buffer-flush.max-rows' = '1000',    -- 攒 1000 行批量提交
    'sink.buffer-flush.interval' = '2s',      -- 或每 2 秒提交一次
    'sink.max-retries' = '3'
);
```

**注意 `PRIMARY KEY ... NOT ENFORCED`**：Flink 不会真的去数据库校验主键，这只是告诉 Flink「这是 upsert 的键」，让它生成正确的 upsert 语句。**表在 MySQL 里必须真的有这个主键**，否则会报错。

**维表 JOIN**（用 MySQL 表补充流数据）：

```sql
-- 声明为 lookup 表
CREATE TABLE user_profile (
    user_id STRING,
    user_level STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = '...',
    'table-name' = 'user_profile',
    'lookup.cache.max-rows' = '10000',      -- 缓存 1 万行
    'lookup.cache.ttl' = '10min'            -- 缓存 10 分钟
);

-- 像普通表一样 JOIN，Flink 会自动走 lookup 查询
SELECT o.*, u.user_level
FROM orders o
JOIN user_profile FOR SYSTEM_TIME AS OF o.proc_time AS u
  ON o.user_id = u.user_id;
```

**`FOR SYSTEM_TIME AS OF o.proc_time`** 是流式维表 JOIN 的固定语法，表示「用订单处理时刻的维表版本」，别漏写。

### 6.4 文件连接器

```sql
CREATE TABLE csv_source (
    order_id   STRING,
    amount     DOUBLE,
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'filesystem',
    'path' = '/data/sales.csv',
    'format' = 'csv'
);
```

路径规则：**目录就读目录下所有文件；文件路径就只读那一个文件。**

格式支持：`csv` / `json` / `avro` / `parquet` / `orc` / `debezium-json` / `canal-json`。

监控文件变化（流模式下的目录扫描）：

```sql
'source.monitor-interval' = '10s'   -- 每 10 秒扫一次新文件
```

### 6.5 CDC 连接器（数据库实时同步）

这是 Flink 近年最火的应用场景之一——**用 Flink SQL 做数据库实时同步，替代 Canal/Debezium 的独立部署**。

```sql
-- MySQL CDC Source（需要 flink-sql-connector-mysql-cdc JAR）
CREATE TABLE mysql_binlog (
    id       BIGINT,
    name     STRING,
    amount   DECIMAL(10,2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root123456',
    'database-name' = 'flink_demo',
    'table-name' = 'orders'
);

-- 一条 SQL 完成实时同步
INSERT INTO doris_sink SELECT * FROM mysql_binlog;
```

CDC 连接器内置了「全量 + 增量」的自动切换：先全量快照，再无缝衔接到 binlog 增量，不需要锁表（默认有短暂的表锁，可用 `'debezium.snapshot.locking.mode' = 'none'` 关掉）。

---

<a name="第-7-章"></a>
## 第 7 章 · Python UDF

对应示例：`examples/07_udf.py`

### 7.1 三种 UDF 速览

| 类型 | 装饰器/基类 | 数据形态 | 典型用途 |
|---|---|---|---|
| 标量 ScalarFunction | `@udf` | 一行进一行出 | 格式化、脱敏、简单判定 |
| 表函数 TableFunction | `@udtf` | 一行进多行出 | explode、拆分、正则提取多个 |
| 聚合 AggregateFunction | `udaf()` | 多行进一行出 | 自定义聚合、加权平均 |

选型建议：能用 `@udf` 解决的就不上 `@udtf`，能用 SQL 内建函数的就别写 UDF。

### 7.2 两套类型系统，用错就报错

PyFlink 里有**两套完全独立的类型系统**，这是新手高频踩坑点：

| 类型系统 | 导入方式 | 用在哪 |
|---|---|---|
| **DataTypes** | `from pyflink.table.types import DataTypes` | Table API：UDF 的 result_type、建表、类型转换 |
| **Types** | `from pyflink.common.typeinfo import Types` | DataStream API：`from_collection`、状态描述符 |

**UDF 属于 Table API 体系**，所以 `result_type` 必须用 `DataTypes` 或字符串：

```python
from pyflink.common.typeinfo import Types          # ✗ 这个不能用在 UDF 上
from pyflink.table.types import DataTypes          # ✓ 用这个

@udf(result_type=Types.STRING())                   # ✗ 报错
def f(x): ...

@udf(result_type="STRING")                         # ✓ 字符串最省事
def f(x): ...

@udf(result_type=DataTypes.STRING())               # ✓ 也可以
def f(x): ...
```

用错的报错：

```
TypeError: Invalid returnType: returnType should be DataType or str but is String
```

（注意 `but is String` 这句话很有迷惑性——看起来它想要 String，实际上它收到的是 `Types.STRING()` 返回的对象，类名恰好叫 `String`）

**记忆法**：**做表格的时候用 `table.types`，做数据流的时候用 `common.typeinfo`。**

### 7.3 三种 UDF 的正确写法

```python
from pyflink.table.udf import udf, udtf, udaf
from pyflink.table.types import DataTypes
from pyflink.common import Row

# ---- ① 标量函数：用 @udf 装饰普通函数 ----
@udf(result_type="STRING")
def mask_phone(phone: str) -> str:
    return phone[:3] + "****" + phone[-4:]

# ---- ② 表函数：必须是生成器，result_types 是复数 ----
@udtf(result_types=["STRING"])
def split_tags(tag_str: str):
    for tag in (tag_str or "").split("|"):
        if tag.strip():
            yield tag.strip()

# ---- ③ 聚合函数：类形式，必须用 udaf() 显式包装 ----
class WeightedAvg(AggregateFunction):
    def create_accumulator(self):
        return Row(0.0, 0.0)

    def accumulate(self, accumulator, price, qty):
        # ★ 累加器在前，元素在后（跟 DataStream 版相反！）
        # ★ 原地修改，不要 return
        accumulator[0] += price * qty
        accumulator[1] += qty

    def retract(self, accumulator, price, qty):
        accumulator[0] -= price * qty
        accumulator[1] -= qty

    def merge(self, accumulator, accumulators):
        for other in accumulators:
            accumulator[0] += other[0]
            accumulator[1] += other[1]

    def get_value(self, accumulator):
        return 0.0 if accumulator[1] == 0 else accumulator[0] / accumulator[1]

    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("total", DataTypes.DOUBLE()),
            DataTypes.FIELD("weight", DataTypes.DOUBLE()),
        ])

    def get_result_type(self):
        return DataTypes.DOUBLE()
```

注册：

```python
# 标量、表函数：直接传被装饰过的函数对象
t_env.create_temporary_function("mask_phone", mask_phone)
t_env.create_temporary_function("split_tags", split_tags)

# 聚合函数：用 udaf() 包装，并显式给 input_types
t_env.create_temporary_function(
    "weighted_avg",
    udaf(
        WeightedAvg(),
        input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()],   # ← 别省
        result_type=DataTypes.DOUBLE(),
        accumulator_type=DataTypes.ROW([...]),
    ),
)
```

**UDTF 必须配合 `LATERAL TABLE` 使用：**

```sql
SELECT o.order_id, t.tag
FROM orders o,
     LATERAL TABLE(split_tags(o.tags)) AS t(tag)
```

#### UDAF 三个必踩的坑（都是实测出来的）

**坑 1：`@udaf` 装饰器不能用在类上**

```python
@udaf(result_type=DataTypes.DOUBLE())
class WeightedAvg(AggregateFunction):    # ✗ 报错
    ...
# TypeError: Invalid function: not a function or callable (__call__ is not defined)
```

装饰器只接受**函数或实例**。类形式的 UDAF 必须显式调用 `udaf(实例)` 包装。

**坑 2：不传 `input_types` 会导致 Java 构造器匹配失败**

```python
t_env.create_temporary_function("wavg", WeightedAvg())     # ✗ 报错
```

```
Py4JException: Constructor PythonAggregateFunction([...]) does not exist
```

原因是 Flink 要根据 `input_types` 去反查对应的 Java 构造器，传 None 就匹配不上。**显式传 `input_types` 是最稳的做法。**

**坑 3：`accumulate` 必须原地修改，不能 return**

```python
def accumulate(self, acc, price, qty):
    return (acc[0] + price * qty, acc[1] + qty)      # ✗ 静默失效！
```

**不报错，但结果永远是初始值**。这种"不报错但算错"的 bug 最难查。正确做法是原地修改（如上面示例的 `accumulator[0] += ...`）。

依据：PyFlink 自带官方示例 `pyflink/examples/table/basic_operations.py`。

### 7.4 UDAF 在批/流模式下的硬限制

这是实测发现的、**文档里写得很隐蔽**的一条限制：

| UDAF 类型 | 批模式 | 流模式 |
|---|---|---|
| 普通 UDAF（`func_type="general"`） | ✗ 不支持 | ✓ 支持 |
| Pandas UDAF（`func_type="pandas"`） | ✓ 支持 | ✗ 不支持 |

报错原文：

```
批模式下用普通 UDAF：
  TableException: non-Pandas UDAFs are not supported in batch mode currently.

流模式下用 Pandas UDAF：
  TableException: Pandas UDAFs are not supported in streaming mode currently.
```

**也就是说：没有任何一种 Python UDAF 能同时跑批和流。** 这是 Flink 的当前实现限制，不是配置问题。

**怎么应对：**

| 场景 | 方案 |
|---|---|
| 批处理要自定义聚合 | 用 Pandas UDAF（`func_type="pandas"`） |
| 流处理要自定义聚合 | 用普通 UDAF（`func_type="general"`，默认） |
| 想两栖 | 绕开 UDAF：用 DataStream API 的 `AggregateFunction`，或拆成几步 SQL |

好消息是 **UDF（标量）和 UDTF（表函数）不受这个限制**，批流都能用。

### 7.5 性能警告（划重点）

**Python UDF 比纯 SQL 慢 1~2 个数量级。**

原因：数据要从 JVM 序列化 → 传给 Python 进程 → 计算 → 再序列化传回 JVM。虽然 Flink 用了 Arrow 优化，但跨进程开销仍然可观。

**优化策略：**

| 策略 | 效果 |
|---|---|
| 能用 SQL 表达就别用 UDF | 最有效 |
| 把 `filter` 放在 UDF 之前 | 缩小输入量 |
| 用 `--python-executable` 指定 Python 环境 | 避免每次重装依赖 |
| 考虑用 `pandas_udf` 做向量化 | 批量处理，省 90% 传输开销 |

**向量化 UDF 示例：**

```python
from pyflink.table.udf import pandas_udf
import pandas as pd

@pandas_udf(result_type=Types.DOUBLE(), func_type="pandas")
def normalize(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()
```

一次处理一批数据（默认 1000 行），比逐行 UDF 快很多倍。**数据量大时首选这个。**

### 7.6 依赖管理：生产部署的大坑

UDF 里 `import` 的第三方库（如 `jieba`、`numpy`、`transformers`）必须在**所有 TaskManager 上**都存在。

**三种解法：**

```bash
# 方案 A：通过命令行指定 Python 环境
flink run -py job.py --python-executable /opt/conda/bin/python

# 方案 B：提交时附带 requirements（Flink 会自动建虚拟环境）
flink run -py job.py --pyFiles job_deps.zip

# 方案 C（最稳）：把依赖打进 Docker 镜像，TaskManager 直接用
```

生产环境推荐 C。方案 B 在离线集群上经常因为网络问题失败。

---

<a name="第-8-章"></a>
## 第 8 章 · 生产实践与调优

### 8.1 数据倾斜

**症状**：某个 Task 的吞吐远低于其他，Web UI 上背压（backpressure）标红。

**原因**：`key_by` 之后某个 key 的数据量远超其他（如 VIP 用户、热点商品）。

**解法：**

```sql
-- 解法一：加盐打散（两阶段聚合）
SELECT k, SUM(cnt) FROM (
    SELECT CONCAT(k, '_', CAST(RAND() * 10 AS INT)) AS k_salted,
           k, COUNT(1) AS cnt
    FROM source GROUP BY CONCAT(k, '_', CAST(RAND() * 10 AS INT)), k
) GROUP BY k;
```

用 `RAND()` 把热点 key 打散成 10 份并行算，再合并。**这是最常用的招。**

其他解法：
- 对热点 key 用 `rebalance()` 强制轮询（牺牲局部性换均匀）
- 如果 key 是数值型，用 `key_by` 取模后再加随机偏移
- 检查是否可以用 `Broadcast` 状态替代 join

### 8.2 反压（Backpressure）排查

**排查顺序：**

1. **Flink Web UI → Job → Backpressure** 标签，看哪些算子红了
2. 从**下游往上游**找——通常是下游慢了导致上游堵
3. 定位到具体算子后，看是外部系统慢（Kafka 写入、DB 写入）还是计算慢

**常见原因与对策：**

| 原因 | 对策 |
|---|---|
| Sink 写入 DB 慢 | 加 `sink.buffer-flush.max-rows`，批量提交 |
| Kafka 分区数不足 | 增加 topic 分区数 |
| UDF 太慢 | 换 `pandas_udf` 向量化 |
| 并行度不足 | 提高算子并行度 |
| GC 频繁 | 换 RocksDB 后端，调小状态 |

### 8.3 内存配置

Flink 的内存模型分几块，PyFlink 场景特别注意 **Python Worker 的内存不在 JVM 堆里**：

```yaml
# flink-conf.yaml
jobmanager.memory.process.size: 1600m
taskmanager.memory.process.size: 4096m
taskmanager.memory.managed.fraction: 0.4    # 状态后端可用内存占比
taskmanager.numberOfTaskSlots: 4
```

**Python Worker 内存控制：**

```python
# 限制 Python 侧的堆大小（防止 UDF 里加载大模型把内存撑爆）
env.set_python_executable("python3")
# 或通过 container 的 memory limit 控制
```

### 8.4 并行度设置

**核心公式：**

```
理想并行度 = Kafka 分区数（或数据源分片数）
```

并行度超过源分区数没有意义——多出来的 Task 会空转。

**算子级覆盖：**

```python
stream.map(f).set_parallelism(4)     # 这个算子单独用 4 并行度
```

**PyFlink 特别提醒**：每个并行子任务会拉起一个独立的 Python Worker 进程。并行度 16 意味着 16 个 Python 进程，内存占用是 Java 侧的数倍。**PyFlink 作业的并行度不要盲目调大。**

### 8.5 作业提交

```bash
# 本地运行（学习用）
python job.py

# 提交到 standalone 集群
flink run -m localhost:8081 -py job.py

# 提交到 YARN
flink run -m yarn-cluster -yjm 1024 -ytm 2048 -py job.py

# 提交到 K8s (需要 flink-kubernetes-operator)
kubectl apply -f flink-job.yaml
```

**生产建议**：用 **Application Mode**（`-t application`），而不是 Session Mode。Application Mode 下 JobManager 专属于该作业，资源隔离更好，且 `main()` 在集群里执行，客户端可以退出。

### 8.6 监控指标

关键指标，建议全部接入 Prometheus + Grafana：

| 指标 | 含义 | 告警阈值建议 |
|---|---|---|
| `numRecordsInPerSecond` | 输入速率 | 突降 50% 说明上游有问题 |
| `numRecordsOutPerSecond` | 输出速率 | 与输入速率对比看是否积压 |
| `currentInputWatermark` | 当前 watermark | 长时间不动说明某分区静默 |
| `lastCheckpointDuration` | 上次 ckpt 耗时 | > 1min 需要关注 |
| `lastCheckpointSize` | ckpt 大小 | 持续增长说明状态泄漏 |
| `numRestarts` | 重启次数 | > 0 就该查原因 |

**特别关注 watermark 不推进**——这通常意味着某个 Kafka 分区没数据，导致整个作业的 watermark 卡住，窗口永远不关闭。典型表现是"作业在跑但没输出"。

---

<a name="第-9-章"></a>
## 第 9 章 · 实战项目：实时风控系统

对应示例：`examples/08_realtime_risk_control.py`

### 9.1 业务需求

电商平台实时监控交易，识别三类风险：

| 规则 | 描述 | 阈值 |
|---|---|---|
| R1 | 高频交易 | 10 秒内同用户下单 ≥ 3 笔 |
| R2 | 大额异常 | 单笔金额 > 20000 |
| R3 | 累计超限 | 24 小时同用户累计消费 > 50000 |

### 9.2 架构

```
Kafka(orders)
     │
     ↓
┌─────────────────────────────────────┐
│  PyFlink 作业                        │
│  ┌───────────────────────────────┐  │
│  │ 1. Kafka Source               │  │
│  │ 2. JSON 解析 + 脏数据过滤      │  │
│  │ 3. 分配事件时间 + Watermark    │  │
│  │ 4. key_by(user_id)            │  │
│  │ 5. RiskDetector               │  │
│  │    ├─ ListState: 最近交易      │  │
│  │    ├─ ValueState: 24h累计      │  │
│  │    └─ Timer: 清理过期状态      │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │
       ┌───────┴────────┐
       ↓                ↓
  Kafka(alerts)    MySQL(risk_alerts)
   下游订阅           持久化查询
```

### 9.3 三个关键设计点

**① 为什么用 ListState 存最近交易，而不是用窗口？**

窗口是"固定时间片"语义，而高频检测是"任意滑动时间窗"。用 ListState 自己维护，可以精确实现"最近 10 秒内 N 笔"，比滑动窗口更灵活（滑动窗口的步长是固定的）。

**② 定时器的作用**

```python
ctx.timer_service().register_event_time_timer(trigger_time)
```

没有定时器，`ListState` 会无限增长——因为清理逻辑只在"新数据到达"时执行。如果用户不再下单，旧记录就永远留在状态里。**定时器保证状态能被独立清理**。

**③ 脏数据处理**

```python
.filter(lambda x: x is not None)   # 解析失败返回 None 被过滤掉
```

生产环境应该把脏数据写到独立的 DLQ（死信队列）而不是直接丢弃：

```python
class ParseOrder(MapFunction):
    def map(self, raw):
        try:
            return parse(raw)
        except Exception as e:
            # 侧输出到 DLQ
            ctx.output(dlq_tag, raw)
            return None
```

### 9.4 运行步骤

**方式一：本地模拟（零依赖）**

```bash
python examples/08_realtime_risk_control.py --local
```

会自动生成 60 条模拟订单（其中故意注入了高频交易用户），你立刻能看到告警输出。

**方式二：真实 Kafka 管道**

```bash
# 终端 1：启动依赖
cd docker && docker compose up -d kafka mysql

# 终端 2：灌数据
python scripts/produce_orders.py --rate 5

# 终端 3：跑风控作业
python examples/08_realtime_risk_control.py
```

打开 http://localhost:8082 可以在 Kafka UI 里看到 `risk-alerts` topic 的告警消息。

### 9.5 预期输出

```
[告警] R2-大额异常      用户=u1002 订单=o1015      金额=  25999.00  单笔金额 25999.00 超过阈值 20000
[告警] R1-高频交易      用户=u1003 订单=o_risk_2   金额=   2999.00  2500ms 内发生 3 笔交易
[告警] R1-高频交易      用户=u1003 订单=o_risk_3   金额=   2999.00  3000ms 内发生 4 笔交易
[告警] R3-累计超限      用户=u1002 订单=o1021      金额=  18900.00  24h 累计消费 54002.00 超过限额 50000
```

### 9.6 可以继续做的扩展

想练手的话，按这个顺序加功能：

1. **规则动态化**：把阈值放到 MySQL 维表，用 `FOR SYSTEM_TIME AS OF` 做维表 JOIN，改规则不用重启作业
2. **告警降噪**：同一用户 5 分钟内只发一条告警（用 ValueState 记录上次告警时间）
3. **告警分级**：按规则和金额分级，不同级别走不同下游
4. **模型推理**：在 UDF 里加载 sklearn 模型，做更精细的欺诈识别
5. **Exactly-Once 写入**：给 Kafka Sink 配上两阶段提交，保证告警不重不漏

---

<a name="附录"></a>
## 附录 · 学习路线与常见坑

### A.1 建议的 4 周学习路线

| 周 | 目标 | 对应示例 | 验收标准 |
|---|---|---|---|
| 第 1 周 | 环境 + Table API 基础 | 01, 02 | 能独立写聚合查询，理解惰性求值 |
| 第 2 周 | 时间语义与窗口 | 03, 04 | 能说清 Watermark 的作用，会用三种窗口 |
| 第 3 周 | 状态与容错 | 05 | 能写有状态算子，理解 checkpoint 恢复 |
| 第 4 周 | 连接器 + 实战 | 06, 07, 08 | 跑通端到端 Kafka 管道 |

**每天 1~2 小时足够。** 关键是每个示例都亲手跑一遍，然后改参数看结果变化——比如把 watermark 从 5 秒改成 0，观察迟到数据丢失。

### A.2 十六个最常见的坑

**这张表里的每一条都是我在这台机器上真跑出来的**，不是从文档抄的。

#### 环境类

| # | 现象 | 原因 | 解法 |
|---|---|---|---|
| 1 | `Py4JNetworkError` | Python 版本过高（>3.11） | 用 3.9–3.11 |
| 2 | `Failed to build 'apache-beam'` + `No module named 'pkg_resources'` | Python 3.12/3.13 的 setuptools 太新，Beam 构建脚本用不了 | 降到 3.11 |
| 3 | `UnsupportedClassVersionError` | Java 版本过低 | 升到 11/17 |
| 4 | `No module named 'pyflink'` | 没装 apache-flink | `pip install apache-flink==1.20.0` |
| 5 | **作业卡住不动，最后 Py4J 连接被重置** | **没配 `set_python_executable`，Flink 用了裸命令 `python`** | **见下面「Windows 专属坑」** |
| 6 | 中文全是乱码 `�ֻ�` | Windows 上 JVM 默认用 GBK | 设 `JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8` |

#### API 类

| # | 现象 | 原因 | 解法 |
|---|---|---|---|
| 7 | `'function' object has no attribute 'extract_timestamp'` | `with_timestamp_assigner` 传了 lambda | 定义 `TimestampAssigner` 子类 |
| 8 | `'Duration' object has no attribute 'to_milliseconds'` | 窗口 `.of()` / TTL 要 `Time`，给了 `Duration` | 换 `pyflink.common.time.Time` |
| 9 | `can only concatenate str (not "int") to str` | `AggregateFunction.add` 参数顺序反了 | `add(value, accumulator)` |
| 10 | `'NoneType' object is not iterable` | `on_timer` 里没有 `yield`，不是生成器 | 加一句 `if False: yield None` |
| 11 | `Object 'orders' not found` | `from_elements` 的 Table 没注册成视图就想在 SQL 里用 | 先 `create_temporary_view` |
| 12 | `Cannot resolve field [amount]` | `group_by` 后引用了原表的字段 | 只引用 `select` 里定义过的字段 |
| 13 | `Invalid returnType: ... should be DataType or str` | UDF 的 `result_type` 用了 `Types` | 改用 `DataTypes` 或字符串 |
| 14 | `Constructor PythonAggregateFunction([...]) does not exist` | UDAF 没传 `input_types` | 显式传 `input_types` |
| 15 | UDAF 结果永远是初始值 | `accumulate` 里用了 `return` | 改成原地修改累加器 |
| 16 | `non-Pandas UDAFs are not supported in batch mode` | 批模式不支持普通 UDAF | 换流模式，或用 Pandas UDAF |
| 17 | `RuntimeContext not available` | 状态描述符建在 `__init__` 里 | 移到 `open()` 里 |
| 18 | 窗口不输出结果（作业在跑但没数据） | Watermark 不推进 / 没设事件时间 | 检查是否调了 `assign_timestamps_and_watermarks` |
| 19 | `ClassNotFoundException: KafkaSource` | Connector JAR 没加载 | 下载 JAR 放进 `lib/` 或用 `pipeline.jars` |
| 20 | 作业跑着但没数据 | `scan.startup.mode` 是 latest 但数据已消费完 | 改成 `earliest-offset` |
| 21 | 内存持续增长最后 OOM | 状态没设 TTL | 配 `StateTtlConfig` |
| 22 | UDF 报第三方库 `ModuleNotFoundError` | 依赖没分发到 TaskManager | 打镜像或 `--pyFiles` |

#### Windows 专属坑：Python Worker 起不来

这是 Windows 上最容易卡住的一个坑，**网上教程几乎都没提**。

**症状**：作业提交后卡住不动，或者报：

```
java.io.IOException: Failed to execute the command:
    python -c "import pyflink;import os;print(...)"
output: ModuleNotFoundError: No module named 'pyflink'
```

**原因**：Flink 启动 Python Worker 时调用的是**裸命令 `python`**，走的是系统 PATH。如果你的 PATH 里第一个 `python` 不是装了 PyFlink 的那个环境（比如系统装了 Python 3.13），就会找不到模块。

**解法**：在代码里显式指定解释器。

```python
import sys
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_python_executable(sys.executable)      # ★ 加这一行
```

生产集群上用绝对路径更稳：

```python
env.set_python_executable("/opt/conda/envs/pyflink/bin/python")
```

或者在 `flink-conf.yaml` 里全局配置：

```yaml
python.executable: /opt/conda/envs/pyflink/bin/python
python.client.executable: /opt/conda/envs/pyflink/bin/python
```

**多环境用户的额外建议**：如果你机器上有多个 Python（像你这台有 3.10/3.11/3.13），强烈建议：

1. 单独建一个 PyFlink 专用虚拟环境（本工程的 `scripts/setup_uv.sh` 就是干这个的）
2. 每次跑作业前 `source scripts/env.sh` 把 PATH 切过去
3. 代码里同时加 `set_python_executable(sys.executable)` 双保险

### A.3 从 PyFlink 转到 Flink SQL 的时机

如果你发现：

- 逻辑全是 SQL 能表达的
- 不需要自定义状态
- 业务方想自己改规则

**那直接用 Flink SQL 客户端，别写 PyFlink。** PyFlink 的价值在于"需要 Python 生态"——比如调 ML 模型、用 pandas 做复杂处理。纯粹做 ETL 的话，SQL 更简单也更快。

### A.4 参考资料

| 资源 | 地址 | 说明 |
|---|---|---|
| 官方文档 | https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/ | 最权威，英文 |
| PyFlink 示例仓库 | https://github.com/apache/flink/tree/master/flink-python/pyflink/examples | 官方示例代码 |
| Flink SQL 文档 | https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/ | SQL 语法手册 |
| 连接器下载 | https://flink.apache.org/downloads/ | 各种 connector JAR |
| 中文社区 | https://flink-learning.apache.org/ | 中文教程与博客 |

### A.5 工程文件清单

```
pyflink-tutorial/
├── README.md                              ← 本项目说明
├── docs/
│   └── pyflink-tutorial.md                ← 本教程
├── examples/
│   ├── 01_hello_wordcount.py              ← Table API 入门
│   ├── 02_table_api.py                    ← Table API 链式操作
│   ├── 03_watermark_window.py             ← 时间语义与 Watermark
│   ├── 04_window_types.py                 ← 四种窗口 + 三种聚合
│   ├── 05_stateful.py                     ← 状态管理与 Checkpoint
│   ├── 06_connectors.py                   ← 文件 / JDBC / Kafka
│   ├── 07_udf.py                          ← 三种 Python UDF
│   └── 08_realtime_risk_control.py        ← 实战：实时风控
├── scripts/
│   ├── setup.sh                           ← 环境一键搭建
│   ├── generate_data.py                   ← 生成测试数据
│   └── produce_orders.py                  ← Kafka 数据生产端
├── docker/
│   └── docker-compose.yml                 ← Flink + Kafka + MySQL
└── data/
    ├── sales.csv                          ← 示例数据
    └── init.sql                           ← MySQL 建表语句
```
