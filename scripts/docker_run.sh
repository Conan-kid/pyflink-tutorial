#!/usr/bin/env bash
# ============================================================
# 在 Docker 集群里运行示例
# ============================================================
# 用法：
#   bash scripts/docker_run.sh 01     # 跑示例 01
#   bash scripts/docker_run.sh 08     # 跑示例 08
#   bash scripts/docker_run.sh all    # 跑全部
# ============================================================
set -e

PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_DIR="$PROJ_ROOT/docker"
TARGET="${1:-all}"

if ! docker info >/dev/null 2>&1; then
    echo "✗ Docker 未运行。请先启动 Docker Desktop。"
    exit 1
fi

cd "$DOCKER_DIR"

if ! docker compose ps --status running 2>/dev/null | grep -q jobmanager; then
    echo "✗ JobManager 未运行。先执行：bash scripts/docker_up.sh"
    exit 1
fi

run_one() {
    local num="$1"
    local f
    f=$(ls "$PROJ_ROOT/examples/${num}_"*.py 2>/dev/null | head -1)
    if [ -z "$f" ]; then
        echo "  [${num}] ✗ 找不到示例文件"
        return 1
    fi
    local base
    base=$(basename "$f")
    printf "  [%s] %-42s" "$num" "$base"

    # 示例 08 默认会连 Kafka 起常驻流作业（永不退出），批量跑时必须加 --local，
    # 否则会一直挂在这里。想跑真 Kafka 请单独执行：
    #   docker compose exec jobmanager python /opt/flink/examples/08_realtime_risk_control.py --kafka
    # 示例 09 依赖外部 pg 容器（pg-course 项目，网络隔离，走 host.docker.internal）。
    # pg 没起或结果表不存在时直接跳过，不算失败。
    local extra=""
    if [ "$num" = "08" ]; then
        extra="--local"
    fi

    if [ "$num" = "09" ]; then
        # 检查 pg 是否可达
        if ! docker compose exec -T jobmanager bash -c \
             "(exec 3<>/dev/tcp/host.docker.internal/5432) 2>/dev/null"; then
            echo "SKIP  (pg 未运行或端口不通，先启动 pg-course)"
            return 0
        fi
        # 检查结果表是否存在
        local tbl
        tbl=$(docker exec -i pg-primary psql -U shop -d shop -t -A -c \
              "SELECT to_regclass('public.flink_user_stats');" 2>/dev/null || echo "")
        if [ -z "$tbl" ] || [ "$tbl" = "" ]; then
            echo "SKIP  (flink_user_stats 表不存在，见文档第 2 节建表 SQL)"
            return 0
        fi
        extra="--local"
    fi

    local out
    out=$(docker compose exec -T jobmanager python "/opt/flink/examples/$base" $extra 2>&1) || true

    # 检查是否有 Traceback / Error
    if echo "$out" | grep -qE "Traceback|Error:|Exception:"; then
        echo "FAIL"
        echo "$out" | tail -15 | sed 's/^/        /'
        return 1
    else
        local lines
        lines=$(echo "$out" | grep -vc '^$' || echo 0)
        echo "OK    (${lines} 行输出)"
        return 0
    fi
}

echo "============================================================"
echo " 在 Docker 集群中运行 PyFlink 示例"
echo "============================================================"
echo ""

PASS=0
FAIL=0

if [ "$TARGET" = "all" ]; then
    for num in 01 02 03 04 05 06 07 08 09; do
        if run_one "$num"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
    done
else
    if run_one "$TARGET"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
fi

echo ""
echo "============================================================"
echo " 结果：通过 $PASS / 失败 $FAIL"
echo "============================================================"
echo ""
echo " 查看集群作业：http://localhost:8081/#/job/running"
echo ""

[ "$FAIL" -eq 0 ]
