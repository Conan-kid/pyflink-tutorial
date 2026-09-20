#!/usr/bin/env bash
# ============================================================
# PyFlink 环境变量（本机实测配置）
# ============================================================
# 用法：
#   source scripts/env.sh          # 加载环境
#   python examples/01_hello_wordcount.py
#
# 或者直接在命令前拼上：
#   bash -c "source scripts/env.sh && python examples/01_hello_wordcount.py"
# ============================================================

# ---- Java 17/11（Flink 必需，本机 1.8 不能用）----
export JAVA_HOME="C:/Program Files/Java/jdk-11"
export PATH="$JAVA_HOME/bin:$PATH"

# ---- PyFlink 虚拟环境（Python 3.11.9，用 uv 创建）----
export PYFLINK_VENV="D:/pyflink-envs/pyflink-1.20"
export PATH="$PYFLINK_VENV/Scripts:$PATH"

# ---- 编码设置（重要！不加会中文乱码）----
# Windows 上 JVM 默认用 GBK，导致 SQL 里的中文和输出全是乱码。
# 这三个变量缺一不可：
export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"   # 让 JVM 用 UTF-8 处理字符串
export PYTHONIOENCODING=utf-8                       # Python 的 stdout 编码
export PYTHONUTF8=1                                 # Python 3.7+ 的 UTF-8 模式

echo "✓ PyFlink 环境已加载"
echo "   Java    : $("$JAVA_HOME/bin/java.exe" -version 2>&1 | head -1)"
echo "   Python  : $("$PYFLINK_VENV/Scripts/python.exe" --version 2>&1)"
"$PYFLINK_VENV/Scripts/python.exe" -c "
import importlib.metadata as m
print('   PyFlink : ' + m.version('apache-flink'))
" 2>/dev/null || echo "   PyFlink : 未安装（跑 scripts/setup_uv.sh）"
