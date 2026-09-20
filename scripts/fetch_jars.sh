#!/usr/bin/env bash
# ============================================================================
#  下载 Flink 连接器 JAR
# ----------------------------------------------------------------------------
#  为什么需要这个脚本？
#    JAR 包体积大（CDC 胖包单个 20MB），不适合放进 Git 仓库，
#    所以 .gitignore 里排除了 jars/*.jar。
#    克隆仓库后跑一次这个脚本，就能把连接器补齐。
#
#  用法：
#    bash scripts/fetch_jars.sh              # 下载全部
#    bash scripts/fetch_jars.sh pg           # 只下载 PostgreSQL 相关
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
JARS_DIR="$PROJECT_ROOT/jars"
OPT_DIR="$PROJECT_ROOT/jars-optional"

MAVEN="https://repo1.maven.org/maven2"

mkdir -p "$JARS_DIR" "$OPT_DIR"

# fetch <maven-相对路径> <目标文件>
fetch() {
    local rel="$1" dest="$2"
    if [ -f "$dest" ] && [ -s "$dest" ]; then
        echo "  ✓ 已存在，跳过：$(basename "$dest")"
        return 0
    fi
    echo "  ↓ 下载：$(basename "$dest")"
    if ! curl -fsSL --retry 3 --connect-timeout 20 "$MAVEN/$rel" -o "$dest"; then
        echo "  ✗ 下载失败：$rel" >&2
        rm -f "$dest"
        return 1
    fi
    local sz
    sz=$(du -h "$dest" | cut -f1)
    echo "    完成（$sz）"
}

TARGET="${1:-all}"

if [ "$TARGET" = "all" ] || [ "$TARGET" = "kafka" ]; then
    echo "── Kafka SQL 连接器 ──"
    fetch "org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar" \
          "$JARS_DIR/flink-sql-connector-kafka-3.4.0-1.20.jar"
fi

if [ "$TARGET" = "all" ] || [ "$TARGET" = "mysql" ]; then
    echo "── MySQL JDBC 驱动 ──"
    fetch "com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" \
          "$JARS_DIR/mysql-connector-j-8.0.33.jar"
fi

if [ "$TARGET" = "all" ] || [ "$TARGET" = "pg" ]; then
    echo "── Flink JDBC 连接器 ──"
    fetch "org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar" \
          "$JARS_DIR/flink-connector-jdbc-3.2.0-1.19.jar"

    echo "── PostgreSQL JDBC 驱动（放 jars-optional）──"
    # ⚠️ 放 optional 目录：CDC 胖包里已自带 pg 驱动，两个一起放会版本冲突。
    #    只有跑示例 09（JDBC 读写 pg）时才需要，届时手动加到 lib/。
    fetch "org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" \
          "$OPT_DIR/postgresql-42.7.4.jar"

    echo "── PostgreSQL CDC 连接器（胖包，20MB）──"
    # ⚠️ 必须用 flink-sql- 前缀的胖包！瘦包（flink-connector-postgres-cdc）
    #    缺 flink-connector-cdc-base 依赖，会报 NoClassDefFoundError: JdbcSourceOptions
    fetch "org/apache/flink/flink-sql-connector-postgres-cdc/3.6.0-1.20/flink-sql-connector-postgres-cdc-3.6.0-1.20.jar" \
          "$JARS_DIR/flink-sql-connector-postgres-cdc-3.6.0-1.20.jar"
fi

echo ""
echo "✓ 完成。当前连接器："
ls -lh "$JARS_DIR"/*.jar 2>/dev/null | awk '{printf "    %-55s %s\n", $9, $5}'
if ls "$OPT_DIR"/*.jar >/dev/null 2>&1; then
    echo "  jars-optional（按需手动加载）："
    ls -lh "$OPT_DIR"/*.jar 2>/dev/null | awk '{printf "    %-55s %s\n", $9, $5}'
fi
