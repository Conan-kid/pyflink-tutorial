> 本机实测环境：Windows 11 · Docker Desktop 4.89.0 · WSL2 后端
> 撰写时间：2026-09-20 ｜ **状态：全部部署完成，8 个示例 8/8 通过 ✅**
> 覆盖内容：两套部署 —— **pg-course**（PostgreSQL 主从）和 **pyflink-tutorial**（PyFlink 集群）

## 目录

- [0. 先把 Docker 数据挪到 D 盘](#0-先把-docker-数据挪到-d-盘)
- [1. 部署一：pg-course（PostgreSQL 16 主从复制）](#1-部署一pg-coursepostgresql-16-主从复制)
- [2. 部署二：pyflink-tutorial（PyFlink 集群）](#2-部署二pyflink-tutorialpyflink-集群)
- [3. 存储位置总表](#3-存储位置总表)
- [4. 常用命令速查](#4-常用命令速查)
- [5. 启动方式汇总](#5-启动方式汇总)
- [6. 故障排查](#6-故障排查)

---

## 0. 先把 Docker 数据挪到 D 盘

### 0.1 为什么

Docker Desktop 默认把所有数据（镜像、容器、卷）塞在 C 盘：

```
C:\Users\wzm\AppData\Local\Docker\wsl\disk\docker_data.vhdx   ← 镜像+容器+卷，都在这
C:\Users\wzm\AppData\Local\Docker\wsl\main\ext4.vhdx          ← WSL 发行版本体
```

本机迁之前 `docker_data.vhdx` 已经 **6.06 GB**，而 C 盘剩 290 GB。后面要拉 Flink（1 GB+）、Kafka、MySQL 等镜像，不挪迟早爆。

**D 盘剩 803 GB**，挪过去一劳永逸。

### 0.2 操作步骤

**第 1 步：完全退出 Docker Desktop**

```bash
# 用 Docker 自带的 CLI 优雅关闭（比任务管理器杀进程安全）
"C:\Program Files\Docker\Docker\DockerCli.exe" -Shutdown
```

等待约 15 秒，确认进程全退：

```powershell
Get-Process | Where-Object { $_.ProcessName -like "*docker*" }
Get-Service com.docker.service
# 期望：无 docker 进程，服务状态 Stopped
```

**第 2 步：备份原数据（务必做）**

```powershell
$src = "$env:LOCALAPPDATA\Docker\wsl"
$bak = "D:\DockerData\_backup_before_migrate"
New-Item -ItemType Directory -Force -Path $bak | Out-Null
Copy-Item "$src\disk" -Destination "$bak\disk" -Recurse -Force
Copy-Item "$src\main" -Destination "$bak\main" -Recurse -Force
```

> ⚠️ **绝不能复制正在运行的 vhdx**。虚拟磁盘挂载状态下复制，轻则文件损坏，重则整个 Docker 数据报废。必须先确认 Docker 完全停止。

**第 3 步：改配置文件**

文件：`C:\Users\wzm\AppData\Roaming\Docker\settings-store.json`

加一个 `CustomWslDistroDir` 字段（注意反斜杠要双写）：

```json
{
  "AutoStart": false,
  "CustomWslDistroDir": "D:\\DockerData",
  "DisplayedOnboarding": true,
  "EnableDockerAI": true,
  "InferenceCanUseGPUVariant": true,
  "LastContainerdSnapshotterEnable": 1788405088,
  "LicenseTermsVersion": 2,
  "SettingsVersion": 45,
  "UseContainerdSnapshotter": true
}
```

> 也可以用图形界面：Settings → Resources → Advanced → **Disk image location**。GUI 容错率更高，推荐新手走 GUI。

**第 4 步：把数据放到新位置**

Docker 期望的目录结构（**注意层级**）：

```
D:\DockerData\
├── disk\
│   └── docker_data.vhdx     ← 镜像/容器/卷数据
└── main\
    └── ext4.vhdx            ← WSL 发行版本体
```

```powershell
$target = "D:\DockerData"
New-Item -ItemType Directory -Force -Path "$target\disk" | Out-Null
New-Item -ItemType Directory -Force -Path "$target\main" | Out-Null
Copy-Item "$target\_backup_before_migrate\disk\docker_data.vhdx" -Destination "$target\disk\docker_data.vhdx" -Force
Copy-Item "$target\_backup_before_migrate\main\ext4.vhdx" -Destination "$target\main\ext4.vhdx" -Force
```

**第 5 步：启动验证**

```powershell
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
```

等 60~90 秒，然后：

```bash
docker info          # 能正常返回信息 = 成功
docker ps -a         # 原容器还在 = 数据没丢
```

**第 6 步：确认新位置生效 + 清理**

```powershell
# 确认 vhdx 已在 D 盘且时间戳是新的
Get-ChildItem "D:\DockerData" -Recurse -File | Select-Object FullName, Length, LastWriteTime
```

确认一切正常后，删掉 C 盘旧目录释放空间：

```powershell
Remove-Item "$env:LOCALAPPDATA\Docker\wsl" -Recurse -Force
```

> ⚠️ **先确认 Docker 在新位置跑起来了再删**。备份目录 `D:\DockerData\_backup_before_migrate` 可以再放几天，确认稳定后再删。

### 0.3 踩坑警告

| 坑 | 后果 | 正确做法 |
|---|---|---|
| 在 `~/.docker/daemon.json` 里设 `data-root` | **WSL2 模式下容器起不来**（`invalid rootfs` 错误） | WSL2 后端**不要动** `data-root`，用 `CustomWslDistroDir` |
| 复制运行中的 vhdx | 数据损坏，可能全丢 | 先 `-Shutdown`，确认进程退净 |
| 路径反斜杠没双写 | JSON 解析失败，配置不生效 | `"D:\\DockerData"` |
| 目标层级不对 | Docker 找不到数据，会新建空卷（看起来像"数据丢了"） | 严格按 `disk\` / `main\` 两层 |
| 网络驱动器 / 外置硬盘 | IO 慢，镜像拉取和容器启动明显卡 | 用本机内置盘（D 盘正好） |

### 0.4 本机实际情况（2026-09-20 实测通过 ✅）

| 项 | 结果 |
|---|---|
| vhdx 已复制到 `D:\DockerData` | ✅ |
| `settings-store.json` 已加 `CustomWslDistroDir` | ✅ |
| 启动后 `D:\DockerData\disk\docker_data.vhdx` 时间戳更新为**当前时间** | ✅ 证明 Docker 在用 D 盘 |
| 两个 pg 容器自动恢复 | ✅ 均 healthy |
| 迁盘后数据完整性 | ✅ `shop` 库 37 表 / 447MB 不变 |

**启动验证时踩到的两个坎（都已解决）**：

**坎 1：`wsl.exe` 被 WorkBuddy 安全策略拦截**

报错：
```
PROGRAM BLOCKED BY SECURITY POLICY
  - wsl.exe (C:\Windows\System32\wsl.exe)
```
解决：**WorkBuddy 客户端 → 左下角头像 → 设置 → 安全中心 → 命令黑名单 → 移除 `wsl.exe`**。
（注意是 WorkBuddy 自己的安全中心，不是 Windows 系统设置）

**坎 2：`com.docker.service` 需要管理员权限**

报错（`com.docker.backend.exe.log`）：
```
engine linux/wsl failed to start: checking WSL version:
executing wsl --version: fork/exec C:\WINDOWS\System32\wsl.exe: Access is denied.
```
原因：普通用户（Medium 完整性级别）无法启动 `com.docker.service`（`Start-Service` 报
「无法打开计算机上的服务」）。服务起不来 → backend 拿不到足够权限 → 调 wsl.exe 被拒。

解决：**右键 `Docker Desktop.exe` → 以管理员身份运行**。
一劳永逸的话：右键该 exe → 属性 → 兼容性 → 勾选「以管理员身份运行此程序」。

> 排查技巧：`Docker Desktop.exe.log` 里如果只有旧的 `backend process exited`、
> 没有新启动记录，说明进程起来立刻退了；真正的原因要去
> `com.docker.backend.exe.log` 里找。

---

## 1. 部署一：pg-course（PostgreSQL 16 主从复制）

### 1.1 这是什么

一套 PostgreSQL 学习环境，一主一从，演示**流式复制 + 复制槽 + WAL 归档**：

```
pg-primary (5432)  ──WAL 流复制──▶  pg-standby (5433, 只读)
       │
       └──WAL 归档──▶  /archive 卷（可用于 PITR 时间点恢复）
```

### 1.2 文件位置

| 内容 | 宿主机绝对路径 |
|---|---|
| **项目根目录** | `C:\Users\wzm\WorkBuddy\2026-09-02-17-21-42\postgresql-course\examples\docker\` |
| compose 配置 | 上述目录下的 `docker-compose.yml` |
| 主库配置 | `...\examples\docker\conf\postgresql.conf` |
| 认证配置 | `...\examples\docker\conf\pg_hba.conf` |
| 初始化脚本 | `...\examples\docker\initdb\00-init.sql` |
| 归档目录创建脚本 | `...\examples\docker\initdb\01-archive-dir.sh` |

### 1.3 启动

```bash
cd "C:/Users/wzm/WorkBuddy/2026-09-02-17-21-42/postgresql-course/examples/docker"

docker compose up -d          # 启动
docker compose ps             # 看状态
docker compose logs -f primary   # 跟踪主库日志
```

**首次启动会稍慢**（约 30~60 秒），因为从库要用 `pg_basebackup` 从主库拉全量数据。

### 1.4 连接

| 角色 | 地址 | 账号 |
|---|---|---|
| 主库（读写） | `localhost:5432` | `postgres` / `postgres` |
| 从库（只读） | `localhost:5433` | `postgres` / `postgres` |
| 复制用户 | 容器内部 | `replicator` / `replicator123` |
| 业务库 | `shop` | — |

命令行连接：

```bash
# 主库
docker exec -it pg-primary psql -U postgres -d shop

# 从库
docker exec -it pg-standby psql -U postgres -d shop
```

### 1.5 验证复制是否正常

```bash
# 1. 主库看复制状态 —— 期望 state = streaming
docker exec pg-primary psql -U postgres -c \
  "SELECT client_addr, state, sync_state, sent_lsn, replay_lsn FROM pg_stat_replication;"

# 2. 从库确认自己是只读副本 —— 期望 t
docker exec pg-standby psql -U postgres -c "SELECT pg_is_in_recovery();"

# 3. 复制槽 —— 期望 active = t
docker exec pg-primary psql -U postgres -c \
  "SELECT slot_name, slot_type, active FROM pg_replication_slots;"

# 4. 实测同步：主库写一条，从库应该能读到
docker exec pg-primary psql -U postgres -d shop -c \
  "CREATE TABLE _t(id int); INSERT INTO _t VALUES (1);"
sleep 2
docker exec pg-standby psql -U postgres -d shop -c "SELECT * FROM _t;"
docker exec pg-primary psql -U postgres -d shop -c "DROP TABLE _t;"

# 5. 从库写入应被拒绝 —— 期望报错 read-only transaction
docker exec pg-standby psql -U postgres -d shop -c "INSERT INTO _t VALUES (2);"
```

### 1.6 数据存储位置

| 数据 | Docker 卷名 | 宿主机实际路径（WSL2 内部） |
|---|---|---|
| 主库数据 | `docker_pg_primary_data` | `\\wsl$\docker-desktop-data\...\volumes\docker_pg_primary_data\_data` |
| 从库数据 | `docker_pg_standby_data` | 同上结构 |
| WAL 归档 | `docker_pg_archive` | 同上结构 |

查看卷在宿主机的路径：

```bash
docker volume inspect docker_pg_primary_data --format '{{.Mountpoint}}'
```

> 卷名为什么还是 `docker_pg_*` 而不是 `pg-course_pg_*`？见 [1.8 改名说明](#18-项目改名说明)。

### 1.7 项目改名说明（2026-09-20 完成）

**改之前**：项目名是目录名 `docker`（默认行为），有点敷衍。
**改之后**：`pg-course`。

**关键点：改项目名会导致卷名跟着变**，默认规则是 `<项目名>_<卷key>`：

```
改名前：docker_pg_primary_data
改名后：pg-course_pg_primary_data   ← 这是新建的空卷！数据"丢失"！
```

**解决方案**：在 compose 里用 `volumes.<key>.name` 把卷名**显式锁死**成原值，
并且加 `external: true` 告诉 Docker「这卷已经存在，别乱动」：

```yaml
name: pg-course        # ← 项目名

volumes:
  pg_primary_data:
    external: true                    # ← 已存在的卷，直接复用
    name: docker_pg_primary_data      # ← 锁死为原卷名，数据零搬迁
  pg_standby_data:
    external: true
    name: docker_pg_standby_data
  pg_archive:
    external: true
    name: docker_pg_archive

networks:
  pgnet:
    external: true
    name: docker_pgnet
```

> **为什么必须加 `external: true`**：只写 `name` 的话，Docker 仍会检查卷的
> `com.docker.compose.project` 标签，发现是旧项目 `docker` 创建的，就会告警：
>
> ```
> volume "docker_pg_primary_data" already exists but was created for project "docker"
> (expected "pg-course"). Use `external: true` to use an existing volume
> ```
>
> 加上 `external: true` 后 Docker 就不再校验归属，直接拿来用。

**改项目名的完整操作序列**（本机实测通过）：

```bash
cd "C:/Users/wzm/WorkBuddy/2026-09-02-17-21-42/postgresql-course/examples/docker"

# 1. 先改好 compose 文件（name + external 卷/网络）

# 2. 删掉旧容器 —— 注意用 docker rm 而不是 compose down！
#    因为旧容器属于项目 docker，而当前 compose 项目是 pg-course，
#    compose down 找不到它们，会报 container name already in use
docker rm -f pg-primary pg-standby

# 3. 确认数据卷还在（关键！）
docker volume ls          # 应看到 docker_pg_primary_data 等三个卷

# 4. 用新项目名启动
docker compose up -d
docker compose ls -a      # 期望看到 NAME = pg-course
```

> ⚠️ **第 2 步千万别用 `docker compose down -v`** —— `-v` 会删数据卷。
> 用 `docker rm -f <容器名>` 只删容器，卷安全无恙。

验证配置解析正确：

```bash
docker compose config --format json | python -c "
import sys, json
d = json.load(sys.stdin)
print('项目名:', d.get('name'))
print('容器名:', [s.get('container_name') for s in d.get('services', {}).values()])
for k, v in d.get('volumes', {}).items():
    print(f'  {k} -> {v.get(\"name\")} (external={v.get(\"external\")})')
"
```

期望输出：

```
项目名: pg-course
容器名: ['pg-primary', 'pg-standby']
  pg_archive -> docker_pg_archive (external=True)
  pg_primary_data -> docker_pg_primary_data (external=True)
  pg_standby_data -> docker_pg_standby_data (external=True)
```

**实测结果（2026-09-20）**：改名后重启，`shop` 库 37 张表 / 447MB 完好，
主从复制 `streaming`，`orders_202608` 主从都是 41134 行 —— **零数据丢失**。

### 1.8 停止与销毁

```bash
docker compose stop           # 停止容器，保留一切
docker compose down           # 停止并删除容器（数据卷保留）★ 常用
docker compose down -v        # 停止并删除容器 + 数据卷（⚠️ 数据全没）
docker compose restart primary   # 重启单个服务
```

### 1.9 已知"假故障"

**`pg_stat_archiver.failed_count` 数字很大**（本机是 507）——**这是历史遗留，不是故障**。

判断方法：比较失败时间和成功时间。

```bash
docker exec pg-primary psql -U postgres -x -c "SELECT * FROM pg_stat_archiver;"
```

```
last_failed_time    = 2026-09-03 15:56    ← 失败集中在这
last_archived_time  = 2026-09-04 09:44    ← 之后一直正常
archived_count      = 75
failed_count        = 507
```

失败全发生在**首次初始化阶段**（initdb 灌数据时归档目录还没就绪，Postgres 每 `archive_timeout`（300 秒）重试一次）。失败时间早于最后一次成功时间，就说明已恢复。

想清零重新统计：

```bash
docker exec pg-primary psql -U postgres -c "SELECT pg_stat_reset_shared('archiver');"
```

---

## 2. 部署二：pyflink-tutorial（PyFlink 集群）

### 2.1 这是什么

PyFlink 学习环境，JobManager + TaskManager 独立容器，配套 Kafka / MySQL 可选：

```
                    ┌──────────────────┐
                    │   JobManager     │  :8081（Web UI）
                    │  pyflink-1.20    │
                    └────────┬─────────┘
                             │ RPC
                    ┌────────▼─────────┐
                    │  TaskManager     │  4 个 slot
                    │  pyflink-1.20    │
                    └──────────────────┘
                             │
              ┌──────────────┴──────────────┐
        ┌─────▼─────┐                 ┌─────▼─────┐
        │  Kafka    │  :9092          │  MySQL    │  :3306
        └───────────┘                 └───────────┘
```

### 2.2 核心难点：官方镜像没有 Python

**这是最容易踩的坑。** 官方 `flink:1.20-scala_2.12-java17` 镜像里**只有 Java，没有 Python**，直接跑 PyFlink 会报 `python: command not found`。

所以必须自定义镜像。本项目的 `docker/pyflink-image/Dockerfile` 做了三件事：

1. 装 Python 3.11（**必须是 3.9~3.11**，3.12+ 装不上 PyFlink，原因见下）
2. 装 `apache-flink==1.20.0` + `kafka-python`
3. 下载连接器 JAR（Kafka / JDBC / MySQL）

**为什么 Python 只能用 3.9~3.11**：`apache-flink` 依赖 `apache-beam`，而后者的构建脚本用了 `pkg_resources` —— 这个 API 在 3.12+ 的 setuptools 里已被移除。在 Python 3.13 上装会直接失败：

```
ModuleNotFoundError: No module named 'pkg_resources'
ERROR: Failed to build 'apache-beam'
```

### 2.3 文件位置

| 内容 | 宿主机绝对路径 |
|---|---|
| **项目根目录** | `C:\Users\wzm\WorkBuddy\2026-09-20-09-28-01\pyflink-tutorial\` |
| Dockerfile | `...\pyflink-tutorial\docker\pyflink-image\Dockerfile` |
| compose 配置 | `...\pyflink-tutorial\docker\docker-compose.yml` |
| 示例代码 | `...\pyflink-tutorial\examples\01~08_*.py` |
| 辅助脚本 | `...\pyflink-tutorial\scripts\*.sh` |
| 数据文件 | `...\pyflink-tutorial\data\` |
| 连接器 JAR 存放 | `...\pyflink-tutorial\jars\` |

### 2.4 启动

```bash
cd "C:/Users/wzm/WorkBuddy/2026-09-20-09-28-01/pyflink-tutorial"

# 一键启动（构建镜像 + 起集群 + 等待就绪）
bash scripts/docker_up.sh

# 需要 Kafka / MySQL 时
bash scripts/docker_up.sh --with-connectors
```

**首次构建约 10 分钟**（要装 Python + PyFlink 约 400MB 依赖 + 下 JAR），之后构建走缓存只要十几秒，启动约 30 秒。

手动分步执行等价于：

```bash
cd docker
docker compose build                              # 构建自定义镜像
docker compose up -d jobmanager taskmanager       # 起 Flink 集群
docker compose --profile connectors up -d         # 额外起 Kafka + MySQL
```

### 2.5 访问入口

| 服务 | 地址 | 说明 |
|---|---|---|
| **Flink Web UI** | http://localhost:8081 | 看作业、TaskManager、checkpoint |
| **Kafka UI** | http://localhost:8082 | 看 topic 和消息（需 `--profile connectors`） |
| MySQL | `localhost:13306` | `root` / `root123456`，库 `flink_demo` |
| Kafka | `localhost:9092` | 容器内地址是 `kafka:9092` |

### 2.6 在集群里跑作业

**方式一：一条命令**

```bash
docker compose exec jobmanager python /opt/flink/examples/01_hello_wordcount.py
```

**方式二：交互式**

```bash
docker compose exec jobmanager bash
# 进入容器后
cd /opt/flink
python examples/01_hello_wordcount.py
```

**方式三：批量跑全部示例**

```bash
bash scripts/docker_run.sh all      # 跑 01~08
bash scripts/docker_run.sh 08       # 只跑 08
```

**方式四：Flink SQL 客户端**

```bash
docker compose --profile sql run --rm sql-client
```

> **注意**：容器内的路径是 `/opt/flink/examples/...`，不是 Windows 路径。因为 compose 把宿主机的 `examples/` 挂载到了容器的 `/opt/flink/examples`。

### 2.7 目录挂载映射（重要）

容器内外路径对应关系：

| 宿主机（Windows） | 容器内（Linux） | 用途 |
|---|---|---|
| `...\pyflink-tutorial\examples\` | `/opt/flink/examples` | 示例代码 |
| `...\pyflink-tutorial\scripts\` | `/opt/flink/scripts` | 辅助脚本 |
| `...\pyflink-tutorial\data\` | `/opt/flink/data` | 输入数据 / 输出结果 |
| `...\pyflink-tutorial\jars\` | `/opt/flink/lib/connectors` | 连接器 JAR |
| 卷 `flink-checkpoints` | `/opt/flink/checkpoints` | Checkpoint 数据 |
| 卷 `flink-savepoints` | `/opt/flink/savepoints` | Savepoint 数据 |

**改代码不用重建镜像** —— 因为是 bind mount，宿主机改完容器里立刻生效。

### 2.8 数据存储位置

```bash
# 查所有卷在宿主机的真实路径
docker volume ls --filter "name=pyflink-tutorial"
docker volume inspect pyflink-tutorial_flink-checkpoints --format '{{.Mountpoint}}'
```

| 卷名 | 容器内路径 | 存什么 |
|---|---|---|
| `pyflink-tutorial_flink-checkpoints` | `/opt/flink/checkpoints` | 作业 checkpoint |
| `pyflink-tutorial_flink-savepoints` | `/opt/flink/savepoints` | 手动 savepoint |
| `pyflink-tutorial_kafka-data` | `/var/lib/kafka/data` | Kafka 消息（含 topic） |
| `pyflink-tutorial_mysql-data` | `/var/lib/mysql` | MySQL 数据 |

> 卷名前缀 `pyflink-tutorial_` 来自 compose 的 `name: pyflink-tutorial`。

### 2.9 停止与销毁

```bash
bash scripts/docker_down.sh            # 停止，保留数据卷
bash scripts/docker_down.sh --purge    # 停止并清空数据卷（会二次确认）
```

等价命令：

```bash
cd docker
docker compose --profile connectors --profile sql down       # 停止
docker compose --profile connectors --profile sql down -v    # 停止 + 清空
```

### 2.10 profile 机制说明

compose 里用了 `profiles` 让可选服务默认不启动：

| 服务 | profile | 何时启动 |
|---|---|---|
| `jobmanager` | 无 | 默认启动 |
| `taskmanager` | 无 | 默认启动 |
| `sql-client` | `sql` | `--profile sql up` |
| `kafka` | `connectors` | `--profile connectors up` |
| `kafka-ui` | `connectors` | 同上 |
| `mysql` | `connectors` | 同上 |

**为什么要这样设计**：只跑示例 01~05 的话根本不需要 Kafka 和 MySQL，默认启动会白白吃 2 GB 内存、拉 1 GB 镜像。按需启动。

### 2.11 拉不到镜像怎么办（国内网络必看）

**症状**：

```
failed to authorize: failed to fetch anonymous token:
Get "https://auth.docker.io/token?...": Bad Gateway
```

**原因**：Docker Hub 在国内访问不稳定，认证服务经常 502。

**解决：配镜像加速源**

编辑 `C:\Users\wzm\.docker\daemon.json`：

```json
{
  "builder": {
    "gc": {
      "defaultKeepStorage": "20GB",
      "enabled": true
    }
  },
  "experimental": false,
  "registry-mirrors": [
    "https://docker.1ms.run",
    "https://docker.xuanyuan.me",
    "https://dockers.xuanyuan.me"
  ]
}
```

> ⚠️ **WSL2 后端绝对不要在这里加 `data-root`** —— 会让容器起不来。
> 只加 `registry-mirrors` 是安全的。

改完必须**重启 Docker Desktop** 才生效（`DockerCli.exe -Shutdown` 再启动）。

验证是否生效：

```bash
docker info | grep -A 5 "Registry Mirrors"
# 应看到配置的三个镜像源
```

**备用方案：走代理**

如果本机开了代理（如 Clash 在 7890 端口），可以给 Docker 配 HTTP 代理：

```json
{
  "proxies": {
    "http-proxy": "http://127.0.0.1:7890",
    "https-proxy": "http://127.0.0.1:7890"
  }
}
```

> 实测：本机直连 Docker Hub 报 502，走 `127.0.0.1:7890` 代理返回 200。
> 但镜像加速源比代理更稳，建议优先用加速源。

**怎么判断是网络问题还是配置问题**：

```bash
curl -sS -o /dev/null -w "%{http_code}\n" https://auth.docker.io/token
# 000 或 502 = 网络不通
# 200 = 网络正常，问题在别处
```

### 2.12 本机实测记录（2026-09-20）

**镜像构建**

```bash
cd docker
docker compose build
# 期望最后一行：naming to docker.io/library/pyflink-tutorial:1.20
```

构建耗时约 **6 分钟**（大部分时间花在 `pip install apache-flink`，约 400 MB 依赖）。

验证镜像里的环境：

```bash
docker run --rm pyflink-tutorial:1.20 python --version
# Python 3.11.x

docker run --rm pyflink-tutorial:1.20 python -c "import pyflink; print(pyflink.__version__)"
# 1.20.0

docker run --rm pyflink-tutorial:1.20 ls /opt/flink/lib/connectors
# flink-sql-connector-kafka-3.2.0-1.20.jar
# flink-connector-jdbc-3.2.0-1.19.jar
# mysql-connector-j-8.0.33.jar
```

**集群启动**

```bash
cd docker
docker compose up -d jobmanager taskmanager
docker compose ps
```

期望两个容器都是 `Up`，jobmanager 显示 `(healthy)`。

**验证清单**

| # | 检查项 | 命令 | 期望 |
|---|---|---|---|
| 1 | JobManager 存活 | `curl -s http://localhost:8081/overview` | 返回 JSON，`taskmanagers` ≥ 1 |
| 2 | TaskManager 注册 | `curl -s http://localhost:8081/taskmanagers` | 含 `"taskmanagers":[{"id":...}]` |
| 3 | Slot 数量 | 上面返回里的 `slotsNumber` | 4 |
| 4 | Python 可用 | `docker compose exec jobmanager python --version` | Python 3.11.x |
| 5 | PyFlink 可导入 | `docker compose exec jobmanager python -c "import pyflink; print(pyflink.__version__)"` | 1.20.0 |
| 6 | 中文编码 | `docker compose exec jobmanager env \| grep JAVA_TOOL_OPTIONS` | `-Dfile.encoding=UTF-8` |
| 7 | 示例已挂载 | `docker compose exec jobmanager ls /opt/flink/examples` | 8 个 `.py` 文件 |

**跑示例**

```bash
# 单个
docker compose exec -T jobmanager python /opt/flink/examples/01_hello_wordcount.py

# 全部（推荐，逐个报告成败）
bash scripts/docker_run.sh all
```

`docker_run.sh` 的判定逻辑：**捕获 stdout+stderr，只要出现 `Traceback` / `Error` / `Exception` 就判失败**。

> ⚠️ **示例 08 有特殊处理**：它检测到 Kafka 就会起**常驻流作业**（永不退出）。
> 批量脚本会自动给 08 加 `--local` 参数走本地模拟，否则会一直挂住。
> 想手动跑真 Kafka 流作业：
>
> ```bash
> docker compose exec jobmanager python /opt/flink/examples/08_realtime_risk_control.py --kafka
> # 这是常驻作业，用 Ctrl-C 或 Flink Web UI 停止
> ```

**实测结果（2026-09-20，8/8 通过）**：

| 示例 | 内容 | 输出行数 | 说明 |
|---|---|---|---|
| `01_hello_wordcount.py` | DataStream WordCount | 9 | flink=3 / python=2 / sql=1 / table=1 |
| `02_table_api.py` | Table API / SQL | 55 | |
| `03_watermark_window.py` | 事件时间 + 水位线 | 7 | |
| `04_window_types.py` | 滚动/滑动/会话窗口 | 3 | |
| `05_stateful.py` | Keyed State + Checkpoint | 8 | |
| `06_connectors.py` | 文件 + JDBC + Kafka | 418 | 三段连接器**全部走真实路径** |
| `07_udf.py` | Python UDF / UDTF | 70 | |
| `08_realtime_risk_control.py` | 实时风控综合案例 | 98 | R1/R2/R3 规则均触发 |

**连接器真实验证**：

```bash
# MySQL 里确实建出了表
docker compose exec mysql mysql -uroot -proot123456 -e "SHOW TABLES;" flink_demo
# 输出：category_summary / orders / risk_alerts / risk_rules

# Kafka 连接正常
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**输出落到哪**

示例默认写到容器内 `/opt/flink/data/`，对应宿主机：

```
C:\Users\wzm\WorkBuddy\2026-09-20-09-28-01\pyflink-tutorial\data\
```

### 2.13 连接器 JAR 的正确放置方式（重要）

**Python 侧和 Java 侧的连接器是两回事**：

| 连接器类型 | 用什么 | 放哪 |
|---|---|---|
| Table / SQL 连接器 | `CREATE TABLE ... WITH ('connector'='kafka')` | JAR 放进 Flink 的 `lib/` |
| DataStream 连接器 | `KafkaSource.builder()` 等 Python API | 同上 |

**坑点：Flink 只扫描 `lib/` 顶层，不会递归子目录。**

本项目把宿主机 `jars/` 挂载到容器的 `/opt/flink/lib/connectors/`。如果把 JAR 只放在
这个子目录里，Table SQL 能识别（Flink 有额外扫描），但 **DataStream 的 `KafkaSource.builder()`
会报**：

```
TypeError: Could not found the Java class
'org.apache.flink.connector.kafka.source.KafkaSource.builder'.
The Java dependencies could be specified via command line argument '--jarfile'
or the config option 'pipeline.jars'
```

**解决**：容器启动时把 `lib/connectors/*.jar` 软链到 `lib/` 顶层。

Dockerfile 里做了这件事，并通过 compose 的 `entrypoint` 生效：

```dockerfile
RUN printf '%s\n' \
      '#!/bin/bash' \
      'set -e' \
      'if [ -d /opt/flink/lib/connectors ]; then' \
      '  for j in /opt/flink/lib/connectors/*.jar; do' \
      '    [ -e "$j" ] || continue' \
      '    ln -sf "$j" "/opt/flink/lib/$(basename "$j")"' \
      '  done' \
      'fi' \
      'exec /docker-entrypoint.sh "$@"' \
      > /opt/flink/link-connectors.sh \
    && chmod +x /opt/flink/link-connectors.sh
```

```yaml
# docker-compose.yml
x-flink-common: &flink-common
  entrypoint: ["/opt/flink/link-connectors.sh"]   # ← 启动前先建软链
```

**为什么不用「直接把 jars/ 挂到 lib/」**：那样宿主机空目录会覆盖掉镜像自带的
`flink-dist` / `flink-table-planner` 等核心 JAR，Flink 直接起不来。

**验证软链生效**：

```bash
docker compose exec jobmanager ls -la /opt/flink/lib/*.jar | grep -E "kafka|jdbc|mysql"
# 应看到三个 lrwxrwxrwx 开头的软链，指向 lib/connectors/ 下的实际文件
```

**放新 JAR 的正确姿势**：丢进宿主机的 `jars/` 目录，然后 `docker compose restart jobmanager taskmanager`。
不用重新构建镜像。

### 2.14 示例代码怎么同时适配「宿主机」和「容器」

示例里的 Kafka / MySQL 地址不能写死 `localhost`：

- 在**宿主机**跑 → `localhost` 正确
- 在**容器**里跑 → `localhost` 指的是容器自己，必须用 compose 服务名 `kafka` / `mysql`

所以示例统一用环境变量解析：

```python
def _conn_host(env_key, default):
    return os.environ.get(env_key, default)

KAFKA_BOOTSTRAP = _conn_host("KAFKA_BOOTSTRAP", "localhost:9092")
MYSQL_HOST = _conn_host("MYSQL_HOST", "localhost")
MYSQL_PORT = int(_conn_host("MYSQL_PORT", "3306"))
```

compose 里给 Flink 服务注入容器内地址：

```yaml
environment:
  KAFKA_BOOTSTRAP: kafka:9092     # 容器内用服务名
  MYSQL_HOST: mysql
  MYSQL_PORT: "3306"
```

> **另一个坑**：在 f-string 三引号 SQL 里，地址不要写成嵌套的 `f"..."`，
> 直接写 `'jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/...'` 即可。
> 嵌套双引号 f-string 会被当成 SQL 文本的一部分，
> Flink 解析器会报 `Encountered "f" at line N`。

---

## 3. 存储位置总表

### 3.1 Docker 本体

| 内容 | 路径 |
|---|---|
| **Docker 数据根目录** | `D:\DockerData\` |
| 镜像 + 容器 + 卷数据 | `D:\DockerData\disk\docker_data.vhdx` |
| WSL 发行版本体 | `D:\DockerData\main\ext4.vhdx` |
| 迁移前备份 | `D:\DockerData\_backup_before_migrate\` |
| Docker Desktop 配置 | `C:\Users\wzm\AppData\Roaming\Docker\settings-store.json` |
| Docker 引擎配置 | `C:\Users\wzm\.docker\daemon.json` |
| 日志 | `C:\Users\wzm\AppData\Local\Docker\log\` |

### 3.2 pg-course 项目

| 内容 | 路径 |
|---|---|
| 项目目录 | `C:\Users\wzm\WorkBuddy\2026-09-02-17-21-42\postgresql-course\examples\docker\` |
| 主库数据卷 | `docker_pg_primary_data` |
| 从库数据卷 | `docker_pg_standby_data` |
| WAL 归档卷 | `docker_pg_archive` |
| 网络 | `docker_pgnet` |
| 配置文件 | `...\docker\conf\{postgresql.conf, pg_hba.conf}` |
| 初始化 SQL | `...\docker\initdb\00-init.sql` |

### 3.3 pyflink-tutorial 项目

| 内容 | 路径 |
|---|---|
| 项目目录 | `C:\Users\wzm\WorkBuddy\2026-09-20-09-28-01\pyflink-tutorial\` |
| Dockerfile | `...\pyflink-tutorial\docker\pyflink-image\Dockerfile` |
| checkpoint 卷 | `pyflink-tutorial_flink-checkpoints` |
| savepoint 卷 | `pyflink-tutorial_flink-savepoints` |
| Kafka 数据卷 | `pyflink-tutorial_kafka-data` |
| MySQL 数据卷 | `pyflink-tutorial_mysql-data` |
| 网络 | `pyflink-tutorial_flink-net` |

---

## 4. 常用命令速查

### 4.1 全局

```bash
docker info                  # Docker 引擎信息（判断是否在运行）
docker version               # 客户端/服务端版本
docker system df             # 磁盘占用总览
docker system df -v          # 详细占用（含卷）
docker system prune -af      # ⚠️ 清理所有未使用镜像/容器/网络
```

### 4.2 容器

```bash
docker ps                    # 运行中的容器
docker ps -a                 # 所有容器（含已停止）
docker logs -f <容器名>       # 跟踪日志
docker exec -it <容器名> bash # 进入容器
docker stats                 # 实时资源占用
docker inspect <容器名>       # 详细配置
docker restart <容器名>       # 重启
```

### 4.3 镜像

```bash
docker images                # 所有镜像
docker rmi <镜像ID>           # 删除镜像
docker image prune -a        # 清理未使用镜像
```

### 4.4 卷

```bash
docker volume ls                                    # 所有卷
docker volume inspect <卷名> --format '{{.Mountpoint}}'  # 卷在宿主机的路径
docker volume rm <卷名>                              # 删除卷（⚠️ 数据没）
```

### 4.5 compose

```bash
# 必须在含 docker-compose.yml 的目录下执行
docker compose up -d                 # 启动
docker compose ps                    # 状态
docker compose logs -f <服务名>       # 日志
docker compose stop <服务名>          # 停止单个
docker compose restart <服务名>       # 重启单个
docker compose down                  # 停止 + 删容器
docker compose down -v               # 停止 + 删容器 + 删卷（⚠️）
docker compose config                # 校验并展开配置
docker compose config --services     # 列出服务
docker compose ls -a                 # 列出所有 compose 项目
```

---

## 5. 启动方式汇总

### 5.1 前提：Docker Desktop 要开着

本机 `AutoStart: false`，**开机不会自启**。两种方式：

```powershell
# 方式一：图形界面
# 开始菜单搜 "Docker Desktop" 点开

# 方式二：命令行
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
```

等 60~90 秒，用 `docker info` 确认就绪。

> 想让它开机自启：Docker Desktop → Settings → General → 勾选 "Start Docker Desktop when you sign in"。

### 5.2 启动 pg-course

```bash
cd "C:/Users/wzm/WorkBuddy/2026-09-02-17-21-42/postgresql-course/examples/docker"
docker compose up -d
docker compose ps
```

### 5.3 启动 pyflink-tutorial

```bash
cd "C:/Users/wzm/WorkBuddy/2026-09-20-09-28-01/pyflink-tutorial"
bash scripts/docker_up.sh                       # 只起 Flink
bash scripts/docker_up.sh --with-connectors     # 加 Kafka + MySQL
```

### 5.4 全部停掉

```bash
cd "C:/Users/wzm/WorkBuddy/2026-09-02-17-21-42/postgresql-course/examples/docker" && docker compose down
cd "C:/Users/wzm/WorkBuddy/2026-09-20-09-28-01/pyflink-tutorial/docker" && docker compose --profile connectors --profile sql down
```

或者直接退出 Docker Desktop（容器不会丢，只是停止）。

---

## 6. 故障排查

### 6.1 wsl.exe 被安全策略拦截

**症状**：

```
PROGRAM BLOCKED BY SECURITY POLICY
The sandbox prevented a program on the configured Program Blacklist from starting:
  - wsl.exe (C:\Windows\System32\wsl.exe)
```

Docker Desktop 启动时必须调用 `wsl.exe`（WSL2 后端），被拦就会**启动失败**——表现为双击 Docker Desktop 后进程闪一下就没了，`docker info` 报：

```
failed to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine
```

**解决**：打开 **安全中心 → 命令安全 → 程序黑名单**，把 `wsl.exe` 移除。

> 这个拦截无法从命令侧绕过，必须手动改配置。

### 6.2 容器起不来 / 提示 invalid rootfs

**原因**：在 `~/.docker/daemon.json` 里设了 `data-root`。WSL2 后端不支持这个参数。

**解决**：删掉 `daemon.json` 里的 `data-root`，重启 Docker Desktop。

正确的 `daemon.json`（**不含 data-root**）：

```json
{
  "builder": {
    "gc": {
      "defaultKeepStorage": "20GB",
      "enabled": true
    }
  },
  "experimental": false
}
```

### 6.3 改项目名后数据"丢了"

**原因**：默认卷名是 `<项目名>_<卷key>`，改项目名 = 指向新空卷。

**解决**：用 `volumes.<key>.name` 锁死原卷名。详见 [1.7](#17-项目改名说明2026-09-20-完成)。

**已经"丢"了的补救**：原卷其实还在，只是没被使用。

```bash
docker volume ls                              # 找到原来的卷名
docker run --rm -v 旧卷名:/from -v 新卷名:/to alpine \
  sh -c "cd /from && cp -av . /to"
```

### 6.4 PyFlink 报 `python: command not found`

**原因**：用了官方 `flink:1.20` 镜像（没有 Python）。

**解决**：用本项目的自定义 Dockerfile 构建镜像。检查：

```bash
docker compose exec jobmanager python --version      # 期望 Python 3.11.x
docker compose exec jobmanager python -c "import pyflink; print(pyflink.__version__)"
```

### 6.5 PyFlink 中文乱码

**原因**：JVM 默认字符集不是 UTF-8。

**解决**：Dockerfile 里已加 `ENV JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"`。手动验证：

```bash
docker compose exec jobmanager env | grep JAVA_TOOL_OPTIONS
```

### 6.6 端口冲突

本机已占用端口：

| 端口 | 被谁占 |
|---|---|
| 5432 | pg-primary |
| 5433 | pg-standby |
| 8081 | pyflink-jobmanager |
| 8082 | pyflink-kafka-ui |
| 9092 | pyflink-kafka |
| 13306 | pyflink-mysql（**避让** 3306） |
| 3306 | ⚠️ **本机原生 MySQL 服务**（Windows 服务 `mysqld`），非容器 |

> **3306 是本机的既有 MySQL 服务**，所以容器的 MySQL 映射到 **13306**。
> 容器内部仍然是 3306，Flink 容器通过服务名 `mysql:3306` 访问，不受影响。

**启动前先查**：

```bash
docker ps --format "table {{.Names}}\t{{.Ports}}"
netstat -ano | findstr ":5432"      # Windows 查端口占用
```

冲突了就改 compose 里的端口映射，比如 `"15432:5432"`。

### 6.7 Docker 磁盘满了

```bash
docker system df              # 先看占用
docker system df -v           # 看细节（哪个卷/镜像大）

docker container prune        # 清理已停止容器
docker image prune -a         # 清理未使用镜像
docker builder prune          # 清理构建缓存
docker volume prune           # ⚠️ 清理未使用卷（会删数据！先确认）
```

### 6.8 查看启动失败原因

```powershell
# 日志目录
$env:LOCALAPPDATA\Docker\log\host\
├── Docker Desktop.exe.log      # 主程序
├── com.docker.backend.exe.log  # 后端引擎
├── monitor.log                 # 监控
├── httpproxy.log               # 网络代理
└── DockerCli.exe.log           # CLI 操作

# 看最新日志
Get-ChildItem "$env:LOCALAPPDATA\Docker\log\host\" | Sort-Object LastWriteTime -Descending | Select-Object -First 5
Get-Content "$env:LOCALAPPDATA\Docker\log\host\Docker Desktop.exe.log" -Tail 30
```

### 6.9 构建镜像时 JAR 下载 404

**症状**：`docker compose build` 输出里出现

```
>>> 下载 mysql-connector-j-8.0.33.jar
curl: (22) The requested URL returned error: 404
```

**原因**：Maven 坐标写错了。MySQL 官方驱动在 Maven Central 上的 groupId 是 **`com.mysql`**，不是 `mysql`：

| ❌ 错误 | ✅ 正确 |
|---|---|
| `mysql/mysql-connector-j/8.0.33/...` | `com/mysql/mysql-connector-j/8.0.33/...` |

判断方法：直接在浏览器打开完整 URL，404 就是坐标错，能下载就是对的。

**兜底方案**：JAR 下载失败不会阻塞构建（Dockerfile 里做了容错）。手动下载后放进宿主机：

```
C:\Users\wzm\WorkBuddy\2026-09-20-09-28-01\pyflink-tutorial\jars\
```

这个目录挂载到容器的 `/opt/flink/lib/connectors`，**放进去重启容器就生效，不用重新构建镜像**。

### 6.10 构建时校验脚本把自己卡死

**症状**：

```
[7/7] RUN python --version && python -c "import pyflink; print(pyflink.__version__)" && java -version
AttributeError: module 'pyflink' has no attribute '__version__'
ERROR: process did not complete successfully: exit code: 1
```

**原因**：`pyflink` 模块**没有** `__version__` 属性 —— 这是写校验代码时想当然了。更坑的是，
这个校验写在 `RUN` 里且用了 `&&`，校验一挂整个镜像构建就失败，前面 10 分钟的 pip 安装全白费。

**正确写法**：查已安装的**分发包**版本，而不是模块属性：

```dockerfile
RUN python -c "from importlib.metadata import version; print(version('apache-flink'))"
# 1.20.0
```

**教训**：构建期做自检是好事，但**校验代码本身必须先在交互环境里验证过**。
至少在自检外面套上 `|| true`，让它只警告不阻塞。

### 6.11 Kafka 镜像拉不下来（免费加速节点繁忙）

**症状**：

```
Image bitnami/kafka:3.7 Error error from registry: 免费节点当前繁忙，请稍后重试。
如果需要稳定高速的镜像拉取服务，可使用轩辕镜像专业版…
```

**原因**：`bitnami/kafka` 这个仓库在国内免费加速源上常年拥堵（实测连试 5 个标签全失败）。

**解决：换 Apache 官方镜像**

```yaml
kafka:
  image: apache/kafka:3.7.0     # ← 而不是 bitnami/kafka:3.7
```

Apache Kafka 现在有官方 Docker 镜像，实测**能正常拉下来**（`apache/kafka:3.7.0` 和 `:latest` 都可以）。

**注意环境变量名完全不同**，不能直接照搬 Bitnami 的配置：

| 项 | Bitnami | Apache 官方 |
|---|---|---|
| 节点 ID | `KAFKA_CFG_NODE_ID` | `KAFKA_NODE_ID` |
| 角色 | `KAFKA_CFG_PROCESS_ROLES` | `KAFKA_PROCESS_ROLES` |
| 监听器 | `KAFKA_CFG_LISTENERS` | `KAFKA_LISTENERS` |
| 数据目录 | `/bitnami/kafka` | `/var/lib/kafka/data` |
| 工具脚本 | `kafka-topics.sh`（在 PATH） | `/opt/kafka/bin/kafka-topics.sh` |

还需要显式给一个 `CLUSTER_ID`（KRaft 首次格式化用）。

**被加速源卡住的其他镜像**：`provectuslabs/kafka-ui` 实测能拉；`mysql:8.0` 实测能拉。
真拉不动时，用 `docker pull` 单个重试几次，通常能过。

### 6.12 端口被本机原生服务占用

**症状**：

```
Error response from daemon: ports are not available:
exposing port TCP 0.0.0.0:3306 -> 127.0.0.1:0:
listen tcp 0.0.0.0:3306: bind: Only one usage of each socket address is normally permitted.
```

**诊断**：

```bash
netstat -ano | findstr ":3306"
#   TCP    0.0.0.0:3306    LISTENING    7744
```

拿到 PID 后查是什么进程（PowerShell）：

```powershell
Get-Process -Id 7744 | Select-Object Id, ProcessName, Path | Format-List
```

本机结果：`mysqld` —— **Windows 上装了原生 MySQL 服务**，常年占着 3306。

**解决**：容器映射到别的端口，别去动用户的原生服务。

```yaml
mysql:
  ports:
    - "13306:3306"     # 宿主机 13306 → 容器 3306
```

改完 `docker compose --profile connectors up -d` 即可。宿主机连 `localhost:13306`，
容器之间仍走 `mysql:3306`，互不影响。

### 6.13 软链没生效导致 Kafka 连接器找不到

见 [2.13 连接器 JAR 的正确放置方式](#213-连接器-jar-的正确放置方式重要)。
如果改了 JAR 但容器里 `lib/` 顶层没有对应软链，重启即可：

```bash
docker compose restart jobmanager taskmanager
```

### 6.14 重要提醒：别信 Git Bash 的管道

在 Git Bash 里，管道会把 UTF-8 中文当二进制处理，`grep` / `tail` 可能报 `Binary file matches` 或显示成空。**这会让你误判命令结果**。

```bash
# ❌ 不可靠
docker exec pg-primary psql -U postgres -c "SELECT ..." | grep 中文

# ✅ 可靠：重定向到文件再读
docker exec pg-primary psql -U postgres -c "SELECT ..." > /tmp/out.txt 2>&1
# 然后用 Read 工具读，或：
python -c "import io; print(io.open('/tmp/out.txt', encoding='utf-8').read())"
```

---

## 附录：本机实测记录（2026-09-20）

| 项 | 结果 |
|---|---|
| Docker Desktop 版本 | 4.89.0 |
| 原数据位置 | `C:\Users\wzm\AppData\Local\Docker\wsl\`（6.15 GB） |
| 新数据位置 | `D:\DockerData\`（已复制） |
| PostgreSQL 版本 | 16.15（Debian 13） |
| 主从复制状态 | `streaming` / `async`，`sent_lsn == replay_lsn` |
| 数据一致性 | `orders_202608` 主从都是 41134 行 ✓ |
| shop 库规模 | 37 张表 / 447 MB，`payments` 17.4 万行 |
| 从库只读验证 | 写入被拒（`read-only transaction`）✓ |
| WAL 归档 | 75 个成功；507 次失败是初始化期历史遗留 |
| **PyFlink 镜像** | 自定义 `pyflink-tutorial:1.20`（Python 3.11 + PyFlink 1.20.0） |
| 镜像内依赖 | py4j 0.10.9.7 / apache-beam 2.48.0 / pandas 2.3.3 |
| Flink 集群版本 | 1.20.5，1 个 TaskManager，4 个 slot |
| 连接器 JAR | Kafka 3.4.0-1.20 / JDBC 3.2.0-1.19 / MySQL 8.0.33 |
| **示例测试结果** | **8 / 8 全部通过**（示例 06 三段连接器均走真实路径） |
| Kafka 镜像 | `apache/kafka:3.7.0`（`bitnami/kafka` 加速源拉不动） |
| MySQL 端口 | 宿主机 13306 → 容器 3306（3306 被本机原生 mysqld 占用） |

### 本次踩坑清单（按发生顺序）

| # | 问题 | 根因 | 解法 |
|---|---|---|---|
| 1 | MySQL 驱动 404 | Maven 坐标写成 `mysql/...` | 改 `com/mysql/mysql-connector-j/...` |
| 2 | Kafka 连接器 404 | `3.2.0-1.20` 版本不存在 | 用 `3.4.0-1.20` |
| 3 | 构建失败 | 校验用了 `pyflink.__version__`（不存在） | 改 `importlib.metadata.version()` |
| 4 | 镜像里 JAR 被覆盖 | `jars/` 空目录 bind mount 盖住镜像内容 | 从镜像 `docker cp` 出来填充 |
| 5 | 构建期 JAR 下载失败不报错 | `fetch()` 用 `\|\| echo` 吞掉了非零退出码 | 改成 if/else 明确提示 |
| 6 | 加速源拉不动 Kafka | `bitnami/kafka` 免费节点常年繁忙 | 换 `apache/kafka:3.7.0` |
| 7 | 3306 端口冲突 | 本机原生 MySQL 服务占用 | 容器映射到 13306 |
| 8 | 示例连不上服务 | 容器内 `localhost` 指向容器自己 | 环境变量 + compose 服务名 |
| 9 | Flink 报 `Encountered "f"` | f-string 三引号 SQL 里嵌套了 `f"..."` | 内层去掉 f，用单引号 |
| 10 | Kafka 类找不到 | JAR 在 `lib/connectors/` 子目录 | 启动脚本软链到 `lib/` 顶层 |
| 11 | `add_source` 签名不存在 | PyFlink 1.20 API 变更 | 改用 `env.from_source(...)` |
| 12 | 示例 08 批量测试挂住 | Kafka 流作业常驻不退出 | 批量脚本自动加 `--local` |
