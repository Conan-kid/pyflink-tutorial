#!/usr/bin/env bash
# ============================================================
# PyFlink Docker 集群管理脚本
# ============================================================
# 用法：
#   bash scripts/docker_up.sh              # 构建镜像 + 起 Flink 集群
#   bash scripts/docker_up.sh --with-connectors   # 额外起 Kafka + MySQL
#   bash scripts/docker_run.sh 01          # 在集群里跑第 01 个示例
#   bash scripts/docker_run.sh all         # 在集群里跑全部示例
#   bash scripts/docker_status.sh          # 看集群状态
#   bash scripts/docker_down.sh            # 停止
# ============================================================
set -e

PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_DIR="$PROJ_ROOT/docker"

echo "============================================================"
echo " PyFlink Docker 环境"
echo "============================================================"

# ---------- 0. 检查 Docker ----------
if ! docker info >/dev/null 2>&1; then
    echo "✗ Docker 未运行。请先启动 Docker Desktop。"
    exit 1
fi
echo "✓ Docker 已就绪"

# ---------- 1. 检查目录 ----------
for d in examples scripts data jars; do
    if [ ! -d "$PROJ_ROOT/$d" ]; then
        mkdir -p "$PROJ_ROOT/$d"
        echo "  · 创建目录 $d"
    fi
done

# ---------- 2. 构建镜像 ----------
# 关键：官方 flink 镜像没有 Python，必须用自定义镜像
echo ""
echo "[1/3] 构建自定义镜像（python3.11 + PyFlink 1.20 + 连接器）"
echo "      首次构建约 5-10 分钟，请耐心等待…"
cd "$DOCKER_DIR"
docker compose build

# ---------- 3. 启动 ----------
echo ""
echo "[2/3] 启动 Flink 集群"
if [ "$1" = "--with-connectors" ]; then
    echo "      含 Kafka + MySQL"
    docker compose --profile connectors up -d
else
    docker compose up -d jobmanager taskmanager
fi

# ---------- 4. 等待就绪 ----------
echo ""
echo "[3/3] 等待 JobManager 就绪…"
for i in $(seq 1 30); do
    if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
        echo "  ✓ 集群就绪（用时约 $((i*3)) 秒）"
        break
    fi
    sleep 3
    if [ "$i" -eq 30 ]; then
        echo "  ⚠ 等待超时，请查看日志：docker compose logs jobmanager"
    fi
done

echo ""
echo "============================================================"
echo " 集群已启动"
echo "============================================================"
echo ""
echo "  Flink Web UI   →  http://localhost:8081"
if [ "$1" = "--with-connectors" ]; then
echo "  Kafka UI       →  http://localhost:8082"
echo "  MySQL          →  localhost:3306  (root / root123456)"
fi
echo ""
echo " 提交作业："
echo "   docker compose exec jobmanager python /opt/flink/examples/01_hello_wordcount.py"
echo ""
echo " 进入容器："
echo "   docker compose exec jobmanager bash"
echo ""
