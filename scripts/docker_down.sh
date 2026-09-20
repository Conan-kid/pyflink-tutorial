#!/usr/bin/env bash
# ============================================================
# 停止 PyFlink Docker 集群
# ============================================================
# 用法：
#   bash scripts/docker_down.sh          # 停止，保留数据卷
#   bash scripts/docker_down.sh --purge  # 停止并删除数据卷
# ============================================================
set -e

PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJ_ROOT/docker"

echo "============================================================"
echo " 停止 PyFlink Docker 集群"
echo "============================================================"

if [ "$1" = "--purge" ]; then
    echo ""
    echo "⚠️  警告：将删除所有数据卷（checkpoints / savepoints / kafka / mysql 数据）"
    read -p "确认继续？输入 yes 继续：" ans
    if [ "$ans" != "yes" ]; then
        echo "已取消。"
        exit 0
    fi
    docker compose --profile connectors --profile sql down -v
    echo "✓ 已停止并清空数据"
else
    docker compose --profile connectors --profile sql down
    echo "✓ 已停止（数据卷保留）"
fi
echo ""
