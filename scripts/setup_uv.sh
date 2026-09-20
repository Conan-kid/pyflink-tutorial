#!/usr/bin/env bash
# ============================================================
# PyFlink 环境安装（uv 版 · 本机专用）
# ============================================================
# 前提（本机已满足）：
#   · Python 3.11.9  → D:\Python\python.exe
#   · JDK 11         → C:\Program Files\Java\jdk-11
#   · uv             → C:\Users\wzm\.local\bin\uv.exe
#
# 用法：
#   bash scripts/setup_uv.sh          # 安装
#   bash scripts/setup_uv.sh --check  # 只检查
# ============================================================
set -e

UV="C:/Users/wzm/.local/bin/uv.exe"
PY311="D:/Python/python.exe"
JDK11="C:/Program Files/Java/jdk-11"
VENV="D:/pyflink-envs/pyflink-1.20"
FLINK_VER="1.20.0"

echo "============================================================"
echo " PyFlink 环境安装（uv）"
echo "============================================================"

echo ""
echo "[1/4] Python 3.11"
if [ -x "$PY311" ]; then
    echo "  ✓ $("$PY311" --version 2>&1)  @ $PY311"
else
    echo "  ✗ 未找到 $PY311"
    echo "    安装：winget install Python.Python.3.11"
    exit 1
fi

echo ""
echo "[2/4] JDK 11"
if [ -x "$JDK11/bin/java.exe" ]; then
    echo "  ✓ $("$JDK11/bin/java.exe" -version 2>&1 | head -1)  @ $JDK11"
else
    echo "  ✗ 未找到 $JDK11"
    echo "    安装：winget install EclipseAdoptium.Temurin.11.JDK"
    exit 1
fi

echo ""
echo "[3/4] uv"
if [ -x "$UV" ]; then
    echo "  ✓ uv 已就绪"
else
    echo "  ✗ 未找到 uv @ $UV"
    echo "    安装：powershell -c \"irm https://astral.sh/uv/install.ps1 | iex\""
    exit 1
fi

echo ""
echo "[4/4] 虚拟环境"
if [ -x "$VENV/Scripts/python.exe" ]; then
    echo "  ✓ 已存在 $VENV"
else
    echo "  · 创建 $VENV"
    "$UV" venv "$VENV" --python "$PY311"
fi

if [ "${1:-}" = "--check" ]; then
    echo ""
    echo "============================================================"
    echo " 环境检查通过。执行 bash scripts/setup_uv.sh 安装依赖。"
    echo "============================================================"
    exit 0
fi

echo ""
echo "安装依赖（apache-flink 含大量 JAR，约 5-10 分钟）…"
VIRTUAL_ENV="$VENV" "$UV" pip install "apache-flink==$FLINK_VER"

echo ""
echo "安装 kafka-python（数据生产端需要）…"
VIRTUAL_ENV="$VENV" "$UV" pip install kafka-python

echo ""
echo "验证安装…"
export JAVA_HOME="$JDK11"
export PATH="$JAVA_HOME/bin:$PATH"
export PYTHONIOENCODING=utf-8

"$VENV/Scripts/python.exe" -c "
import pyflink
print('  PyFlink:', pyflink.__version__)

from pyflink.table import TableEnvironment, EnvironmentSettings
t = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
t.get_config().set('parallelism.default', '1')
t.create_temporary_view('t', t.from_elements([('hello',), ('flink',), ('hello',)], ['w']))
print('  最小作业结果：')
t.sql_query('SELECT w, COUNT(1) AS c FROM t GROUP BY w').execute().print()
print('  ✓ 环境验证通过')
"

echo ""
echo "============================================================"
echo " 安装完成"
echo "============================================================"
echo ""
echo "使用方式："
echo "  source scripts/env.sh"
echo "  python examples/01_hello_wordcount.py"
echo ""
