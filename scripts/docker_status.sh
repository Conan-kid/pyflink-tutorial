#!/usr/bin/env bash
# ============================================================
# PyFlink Docker 集群状态查看
# ============================================================
set -e

PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJ_ROOT/docker"

echo "============================================================"
echo " PyFlink Docker 集群状态"
echo "============================================================"
echo ""

if ! docker info >/dev/null 2>&1; then
    echo "✗ Docker 未运行"
    exit 1
fi

echo "--- 容器 ---"
docker compose ps -a 2>/dev/null || echo "（无容器）"
echo ""

echo "--- 镜像 ---"
docker images pyflink-tutorial:1.20 --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}" 2>/dev/null || echo "（镜像未构建）"
echo ""

echo "--- 数据卷 ---"
docker volume ls --filter "name=pyflink-tutorial" --format "table {{.Name}}\t{{.Driver}}" 2>/dev/null
echo ""

echo "--- JobManager REST API ---"
if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
    echo "✓ 可访问 http://localhost:8081"
    curl -s http://localhost:8081/overview 2>/dev/null | \
        python -c "import sys,json;d=json.load(sys.stdin);print(f\"  TaskManager 数：{d.get('taskmanagers',0)}\");print(f\"  Slot 总数：{d.get('slots-total',0)}\");print(f\"  可用 Slot：{d.get('slots-available',0)}\");print(f\"  运行中作业：{d.get('jobs-running',0)}\")" 2>/dev/null || true
else
    echo "✗ JobManager 不可访问（未启动？）"
fi
echo ""

echo "--- 磁盘占用 ---"
docker system df 2>/dev/null | head -6
echo ""
