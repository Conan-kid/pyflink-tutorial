# PyFlink 从入门到生产 · 完整学习教程

一套可以**真正跑起来**的 PyFlink 教程。10 个递进示例，从 WordCount 到 PostgreSQL CDC 实时变更捕获。

## 从 GitHub Clone 后怎么跑

```bash
git clone https://github.com/<你的用户名>/pyflink-tutorial.git
cd pyflink-tutorial

# ① 补下载连接器 JAR（仓库不存二进制，28MB，这一步必须做）
bash scripts/fetch_jars.sh

# ② 配置（可选，有默认值）
cp .env.example .env          # 然后按需修改里面的密码

# ③ 一键起集群 + 跑示例
bash scripts/docker_up.sh
bash scripts/docker_run.sh 01
```

> **前置要求**：Docker Desktop、内存 ≥ 8GB、磁盘 ≥ 5GB。
> **首次构建约 10 分钟**（装 Python 3.11 + PyFlink 约 400MB 依赖），之后走缓存十几秒。

## 快速开始

### 路线一：Docker 集群（推荐）

```bash
# 一键启动（构建镜像 + 起集群 + 等就绪）
bash scripts/docker_up.sh

# 需要 Kafka / MySQL 时
bash scripts/docker_up.sh --with-connectors

# 在集群里跑示例
bash scripts/docker_run.sh 01        # 单个
bash scripts/docker_run.sh all       # 全部

# 看状态 / 停止
bash scripts/docker_status.sh
bash scripts/docker_down.sh
```

启动后可以访问：
- Flink Web UI → http://localhost:8081
- Kafka UI → http://localhost:8082（需 `--with-connectors`）
- MySQL → `localhost:13306`（root / root123456）— 避开本机原生 MySQL 的 3306

> **关键**：官方 `flink:1.20` 镜像**不含 Python**，必须用 `docker/pyflink-image/Dockerfile` 构建自定义镜像。

**实测状态（2026-09-20）**：Flink 1.20.5 集群 + Kafka 3.7.0 + MySQL 8.0 全部就绪，
`bash scripts/docker_run.sh all` → 通过。

### 路线二：本地安装

```bash
# 先检查环境（会告诉你缺什么）
bash scripts/setup.sh --check

# 再执行安装
bash scripts/setup.sh
```

> **重要**：本地安装需要 **Python 3.9~3.11** 和 **Java 11/17**。
> Python 3.12/3.13 装了会失败（`apache-beam` 编译报错），这不是能绕过的配置问题。

### 零依赖试跑

不想装任何东西，可以直接跑这个（用内存数据，不需要 Flink 集群之外的任何服务）：

```bash
python scripts/generate_data.py                # 生成测试数据
python examples/08_realtime_risk_control.py --local   # 跑实战项目
```

## 示例清单

| # | 文件 | 主题 | 依赖 |
|---|---|---|---|
| 01 | `01_hello_wordcount.py` | Table API 入门、SQL 查询 | 无 |
| 02 | `02_table_api.py` | Table API 链式操作、聚合 | 无 |
| 03 | `03_watermark_window.py` | 时间语义、Watermark、滚动窗口 | 无 |
| 04 | `04_window_types.py` | 四种窗口 + 三种聚合函数 | 无 |
| 05 | `05_stateful.py` | KeyedState、State TTL、Checkpoint | 无 |
| 06 | `06_connectors.py` | 文件 / JDBC / Kafka 连接器 | 可选 |
| 07 | `07_udf.py` | 标量 UDF、表函数 UDTF、聚合 UDAF | 无 |
| 08 | `08_realtime_risk_control.py` | **实战：实时交易风控系统** | 可选 |
| 09 | `09_postgres_integration.py` | **PostgreSQL 联动：读/写/维表关联/聚合回写** | 需 pg-course |
| 10 | `10_postgres_cdc.py` | **PostgreSQL CDC：实时捕获 INSERT/UPDATE/DELETE** | 需 pg-course |

> **示例 09 / 10 说明**：需要外部 PostgreSQL（`pg-course` 项目的 `pg-primary`，宿主机端口 5432）。
> 两个 compose 项目网络隔离，容器内通过 `host.docker.internal:5432` 访问。
> pg 未启动时 `docker_run.sh` 会自动跳过，不算失败。
>
> **示例 10 的实测状态（2026-09-20）** —— 说实话，只跑通了一部分：
>
> | 能力 | 状态 |
> |---|---|
> | 全量快照（10000 行一次性灌入） | ✅ 已验证 |
> | 增量 INSERT（`+I`） | ✅ 已验证 |
> | 增量 UPDATE（`+U`，含前像对比） | ✅ 已验证 |
> | 增量 DELETE（`-D`）落库 | ❌ 未落库（JDBC sink 语义限制，见下） |
>
> DELETE 抓不到不是管道坏了 —— Debezium 确实收到了 `-D` 事件，
> 是 JDBC sink 的 upsert 无法把"删除"写成审计表里的一行。
> 完整原因和可选方案见 `docs/pygresql-integration.md` 的 CDC 章节。
>
> **跑示例 10 之前，pg 侧有 5 个必做前置**（缺一个就报错）：
> ```sql
> -- ① 开逻辑复制（改完要重启 pg）
> ALTER SYSTEM SET wal_level = 'logical';
> -- ② CDC 账号：REPLICATION + SELECT + 对 database 的 CREATE
> CREATE ROLE flink_cdc WITH LOGIN REPLICATION PASSWORD 'cdc123';
> GRANT CONNECT, CREATE ON DATABASE shop TO flink_cdc;
> GRANT USAGE ON SCHEMA public TO flink_cdc;
> GRANT SELECT ON ALL TABLES IN SCHEMA public TO flink_cdc;
> -- ③ 用超管建 publication（普通账号建不了 FOR ALL TABLES 的）
> CREATE PUBLICATION flink_cdc_pub FOR TABLE public.users;
> -- ④ ⭐ 必须！否则 UPDATE/DELETE 报 "before field is null"
> ALTER TABLE public.users REPLICA IDENTITY FULL;
> -- ⑤ pg_hba.conf 放行 flink_cdc（all + replication 都要），再 pg_reload_conf()
> ```
> ④ 是最容易漏的一条：不加它，INSERT 能过，UPDATE/DELETE 直接抛异常。

**建议按顺序读。** 每个文件都是自包含的，顶部的 docstring 讲清了学习目标和关键概念。

## 目录结构

```
pyflink-tutorial/
├── README.md                              ← 你在这里
├── LICENSE                                ← MIT
├── .gitignore / .gitattributes            ← 排除 JAR 和 .env，统一 LF 行尾
├── .env.example                           ← 配置模板（复制成 .env 用）
├── docs/
│   ├── pyflink-tutorial.md                ← 完整教程正文（推荐先读这个）
│   ├── pyflink-tutorial.html              ← HTML 版（浏览器阅读）
│   ├── docker-deployment-tutorial.md      ← Docker 部署教程（含存储位置全表）
│   ├── docker-deployment-tutorial.html    ← HTML 版
│   ├── pygresql-integration.md            ← PyFlink × PostgreSQL 联动手册 + CDC 章节
│   ├── pygresql-integration.html          ← HTML 版
│   ├── github-deployment.md               ← 部署到 GitHub 指南（敏感信息/JAR 策略）
│   └── github-deployment.html             ← HTML 版
├── examples/                              ← 10 个可运行示例
├── scripts/
│   ├── setup.sh / setup_uv.sh             ← 本机环境安装
│   ├── fetch_jars.sh                      ← ⭐ clone 后补下载 JAR（必须跑）
│   ├── env.sh                             ← 环境变量（本地跑必须 source）
│   ├── run_all.sh                         ← 本地批量跑示例
│   ├── docker_up.sh                       ← Docker 一键启动
│   ├── docker_run.sh                      ← Docker 里跑示例
│   ├── docker_status.sh                   ← Docker 集群状态
│   ├── docker_down.sh                     ← Docker 停止
│   ├── generate_data.py                   ← 生成测试数据
│   ├── produce_orders.py                  ← Kafka 数据生产端
│   └── build_html.py                      ← Markdown 转 HTML
├── docker/
│   ├── docker-compose.yml                 ← Flink 集群 + Kafka + MySQL
│   └── pyflink-image/Dockerfile           ← 自定义镜像（Python + PyFlink + 软链脚本）
├── jars/                                  ← 连接器 JAR（不进 Git，fetch_jars.sh 补）
└── data/                                  ← 测试数据 + 建表 SQL
```

## 推荐学习路线（4 周）

| 周 | 目标 | 示例 | 验收标准 |
|---|---|---|---|
| 1 | 环境 + Table API | 01, 02 | 能写聚合查询，理解惰性求值 |
| 2 | 时间语义与窗口 | 03, 04 | 能说清 Watermark 的作用 |
| 3 | 状态与容错 | 05 | 能写有状态算子，理解 checkpoint |
| 4 | 连接器 + 实战 | 06, 07, 08 | 跑通端到端 Kafka 管道 |

## 关键环境要求

| 组件 | 要求 | 说明 |
|---|---|---|
| Python | **3.9 – 3.11** | 3.12+ 会因 apache-beam 编译失败 |
| Java | **11 或 17** | Java 8 会报 UnsupportedClassVersionError |
| Flink | **1.20.x** | 与 PyFlink 大版本必须一致 |

版本兼容矩阵、22 个常见坑的排查表，见 `docs/pyflink-tutorial.md` 附录。

## 跑通实战项目

```bash
# 方式一：本地模拟（零依赖，一定能跑，跑完即退出）
python examples/08_realtime_risk_control.py --local

# 方式二：Docker 里的真实 Kafka 管道
bash scripts/docker_up.sh --with-connectors          # 起集群 + Kafka
cd docker
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders   # 终端 2：手工发消息
docker compose exec jobmanager \
  python /opt/flink/examples/08_realtime_risk_control.py --kafka   # 终端 3

# 方式三：本地 Python + Docker 里的 Kafka
cd docker && docker compose --profile connectors up -d kafka
python scripts/produce_orders.py --rate 5            # 终端 2
python examples/08_realtime_risk_control.py          # 终端 3
```

> ⚠️ **Kafka 模式是常驻流作业，不会自动退出**（这正是流处理的本意）。
> 停止用 `Ctrl-C`，或去 http://localhost:8081 点 Cancel。
> 批量测试请用 `--local`，`docker_run.sh all` 会自动加上。

预期能看到三类告警：高频交易（R1）、大额异常（R2）、累计超限（R3）。

## 相关文档

| 文档 | 内容 |
|---|---|
| `docs/pyflink-tutorial.html` | PyFlink 完整教程（概念 + 22 个实测坑） |
| `docs/docker-deployment-tutorial.html` | Docker 部署手册（含所有存储位置） |
| `docs/pygresql-integration.html` | PyFlink × PostgreSQL 联动 + CDC 实时捕获 |
| `docs/github-deployment.html` | 部署到 GitHub（敏感信息处理 / JAR 策略 / CI） |

## 许可

MIT License，见 [LICENSE](LICENSE)。示例代码随意取用，教程内容欢迎分享。
