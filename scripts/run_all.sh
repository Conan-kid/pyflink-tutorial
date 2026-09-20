#!/usr/bin/env bash
# ============================================================
# 批量运行并验证所有示例
# ============================================================
# 用法：bash scripts/run_all.sh
# ============================================================

export JAVA_HOME="C:/Program Files/Java/jdk-11"
export PATH="$JAVA_HOME/bin:D:/pyflink-envs/pyflink-1.20/Scripts:$PATH"
export PYTHONIOENCODING=utf-8
export PYTHONUTF8=1
export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"

PY="D:/pyflink-envs/pyflink-1.20/Scripts/python.exe"
ROOT="C:/Users/wzm/WorkBuddy/2026-09-20-09-28-01/pyflink-tutorial"
OUTDIR="$ROOT/data/_test"
mkdir -p "$OUTDIR"

cd "$ROOT" || exit 1

PASS=0
FAIL=0
FAILED_LIST=""

run_one() {
    local num="$1"
    local script="$2"
    local extra="${3:-}"
    local name=$(basename "$script" .py)
    local out="$OUTDIR/${num}.txt"

    printf "  [%s] %-42s " "$num" "$name"

    if timeout 300 $PY "$script" $extra > "$out" 2>"$OUTDIR/${num}.err"; then
        local rows=$(grep -c "rows in set\|^\[告警\|^\[结果\|^窗口\|^传感器\|^用户=" "$out" 2>/dev/null || echo 0)
        local size=$(stat -c%s "$out" 2>/dev/null || echo 0)
        echo "OK    (${size} 字节, ${rows} 条输出)"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        FAIL=$((FAIL + 1))
        FAILED_LIST="$FAILED_LIST $num"
    fi
}

echo "============================================================"
echo " 运行所有 PyFlink 示例"
echo "============================================================"
echo ""

run_one 01 "examples/01_hello_wordcount.py"
run_one 02 "examples/02_table_api.py"
run_one 03 "examples/03_watermark_window.py"
run_one 04 "examples/04_window_types.py"
run_one 05 "examples/05_stateful.py"
run_one 06 "examples/06_connectors.py"
run_one 07 "examples/07_udf.py"
run_one 08 "examples/08_realtime_risk_control.py" "--local"

echo ""
echo "============================================================"
echo " 结果：通过 $PASS / 失败 $FAIL"
if [ -n "$FAILED_LIST" ]; then
    echo " 失败的示例：$FAILED_LIST"
    for n in $FAILED_LIST; do
        echo ""
        echo "--- 示例 $n 的错误信息（末尾20行）---"
        tail -20 "$OUTDIR/${n}.err" | grep -v "UserWarning\|pkg_resources\|^  import"
    done
fi
echo "============================================================"
echo ""
echo "详细输出在：$OUTDIR/"
