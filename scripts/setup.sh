#!/usr/bin/env bash
# ============================================================
# PyFlink 环境一键搭建（Windows / Git Bash）
# ============================================================
# 用法：
#   bash scripts/setup.sh          # 完整安装
#   bash scripts/setup.sh --check  # 只检查环境，不安装
# ============================================================
set -e

VENV_DIR="C:/Users/wzm/.workbuddy/binaries/pyflink-venv"
PY311=""
MODE="${1:-install}"

echo "============================================================"
echo " PyFlink 环境检查"
echo "============================================================"

# ---------- 1. 检查 Java ----------
echo ""
echo "[1/4] 检查 Java 运行时…"
if command -v java >/dev/null 2>&1; then
    JAVA_VER=$(java -version 2>&1 | head -1)
    echo "  当前：$JAVA_VER"
    if echo "$JAVA_VER" | grep -qE '"1\.8|"9|"10'; then
        echo "  ✗ 版本过低！Flink 1.18+ 要求 Java 11 或 17。"
        echo "    解决方式（任选其一）："
        echo "      A. 装 JDK 17：winget install EclipseAdoptium.Temurin.17.JDK"
        echo "      B. 直接用 Docker（推荐，见下面第 5 步）"
        JAVA_OK=0
    else
        echo "  ✓ Java 版本满足要求"
        JAVA_OK=1
    fi
else
    echo "  ✗ 未找到 java 命令"
    JAVA_OK=0
fi

# ---------- 2. 检查 Python ----------
echo ""
echo "[2/4] 检查 Python…"
PY_OK=0
for cand in "C:/Users/wzm/.workbuddy/binaries/python/versions/3.11.9/python.exe" \
            "C:/Users/wzm/.workbuddy/binaries/python/versions/3.10.11/python.exe" \
            "D:/Python310/python.exe"; do
    if [ -x "$cand" ]; then
        V=$("$cand" --version 2>&1)
        echo "  发现：$cand → $V"
        if echo "$V" | grep -qE "3\.(9|10|11)\."; then
            PY311="$cand"
            PY_OK=1
            break
        fi
    fi
done

if [ "$PY_OK" -eq 0 ]; then
    echo "  ✗ 未找到 Python 3.9~3.11（PyFlink 官方只支持到 3.11）"
    echo "    当前系统 Python 是 3.13，PyFlink 装了也会报 Py4J 兼容错误。"
    echo "    解决方式（任选其一）："
    echo "      A. 装 Python 3.11：winget install Python.Python.3.11"
    echo "      B. 直接用 Docker（推荐，见下面第 5 步）"
fi

# ---------- 3. 检查 Docker ----------
echo ""
echo "[3/4] 检查 Docker…"
if command -v docker >/dev/null 2>&1; then
    if docker info >/dev/null 2>&1; then
        echo "  ✓ Docker 运行中"
        DOCKER_OK=1
    else
        echo "  ⚠ Docker 已安装但未启动（打开 Docker Desktop 即可）"
        DOCKER_OK=0
    fi
else
    echo "  ✗ 未找到 Docker"
    DOCKER_OK=0
fi

# ---------- 4. 检查 PyFlink ----------
echo ""
echo "[4/4] 检查 PyFlink 安装状态…"
if [ -x "$VENV_DIR/bin/python" ] || [ -x "$VENV_DIR/Scripts/python.exe" ]; then
    echo "  ✓ 已存在虚拟环境 $VENV_DIR"
else
    echo "  · 尚未创建虚拟环境"
fi

if [ "$MODE" = "--check" ]; then
    echo ""
    echo "============================================================"
    echo " 检查完毕。执行 bash scripts/setup.sh 开始安装。"
    echo "============================================================"
    exit 0
fi

# ---------- 5. 执行安装 ----------
echo ""
echo "============================================================"
echo " 开始安装"
echo "============================================================"

if [ "$JAVA_OK" -eq 0 ] || [ "$PY_OK" -eq 0 ]; then
    echo ""
    echo "本地环境不满足条件，推荐走 Docker 方案："
    echo ""
    echo "  cd docker"
    echo "  docker compose up -d"
    echo ""
    echo "容器内已预装 Java 17 + Python 3.11 + PyFlink，开箱即用。"
    echo ""
    read -p "是否仍要继续本地 venv 安装？(y/N) " ans
    if [ "$ans" != "y" ] && [ "$ans" != "Y" ]; then
        echo "已取消。请按上面提示使用 Docker 方案。"
        exit 0
    fi
fi

if [ -z "$PY311" ]; then
    echo "✗ 没有可用的 Python 3.9~3.11，无法继续。"
    exit 1
fi

echo ""
echo "创建虚拟环境：$VENV_DIR"
"$PY311" -m venv "$VENV_DIR"

PIP="$VENV_DIR/Scripts/pip.exe"
[ -x "$PIP" ] || PIP="$VENV_DIR/bin/pip"

echo ""
echo "安装依赖（这一步会下载约 300MB，请耐心等待）…"
"$PIP" install --upgrade pip

# apache-flink 自带 PyFlink + 大部分 Python 依赖
# 版本对应关系：apache-flink 1.19.x ↔ Flink 1.19，1.20.x ↔ Flink 1.20
"$PIP" install "apache-flink==1.20.0"
"$PIP" install kafka-python

echo ""
echo "验证安装…"
"$VENV_DIR/Scripts/python.exe" -c "
import pyflink
from pyflink.table import TableEnvironment, EnvironmentSettings
print('  ✓ PyFlink 版本：', pyflink.__version__)
t = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
t.create_temporary_view('t', t.from_elements([('hello',), ('flink',)], ['w']))
t.sql_query('SELECT w, COUNT(1) c FROM t GROUP BY w').execute().print()
print('  ✓ 最小作业跑通了')
" 2>&1 | tail -20

echo ""
echo "============================================================"
echo " 安装完成"
echo "============================================================"
echo ""
echo "激活环境："
echo "  source $VENV_DIR/Scripts/activate     # Git Bash"
echo ""
echo "跑第一个示例："
echo "  python examples/01_hello_wordcount.py"
echo ""
