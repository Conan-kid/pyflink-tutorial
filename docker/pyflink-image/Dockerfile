# ============================================================
# PyFlink 运行时镜像
# ============================================================
# 基础镜像：官方 Flink 1.20（Java 17 自带，但【没有 Python】）
# 本镜像在上面补齐：
#   1. Python 3.11（PyFlink 只支持 3.9~3.11，3.12+ 装不上）
#   2. apache-flink==1.20.0（含 py4j / apache-beam / pandas）
#   3. 常用连接器 JAR（Kafka / JDBC / MySQL / Files）
# ============================================================
FROM flink:1.20-scala_2.12-java17

# ---------- 环境变量 ----------
# 中文不乱码：JVM 默认会用容器 locale，显式指定 UTF-8
ENV JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"
ENV PYTHONIOENCODING=utf-8
ENV PYTHONUTF8=1
ENV LANG=C.UTF-8

# ---------- 1. 装 Python 3.11 ----------
# Debian bookworm 自带 python3.11，直接装即可
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.11 \
        python3.11-dev \
        python3.11-venv \
        python3-pip \
        build-essential \
        curl \
        vim \
        less && \
    ln -sf /usr/bin/python3.11 /usr/local/bin/python && \
    ln -sf /usr/bin/python3.11 /usr/local/bin/python3 && \
    rm -rf /var/lib/apt/lists/*

# ---------- 2. 装 PyFlink ----------
# 用国内镜像加速；apache-flink 会连带装 py4j / apache-beam / pandas / numpy
# 注意：apache-flink 依赖体积约 400MB，网络抖动容易失败，加长超时 + 重试
RUN python -m pip install --no-cache-dir --upgrade \
        -i https://mirrors.aliyun.com/pypi/simple/ \
        --trusted-host mirrors.aliyun.com \
        pip setuptools wheel && \
    python -m pip install --no-cache-dir \
        --timeout 120 --retries 5 \
        -i https://mirrors.aliyun.com/pypi/simple/ \
        --trusted-host mirrors.aliyun.com \
        "apache-flink==1.20.0" && \
    python -m pip install --no-cache-dir \
        --timeout 120 --retries 5 \
        -i https://mirrors.aliyun.com/pypi/simple/ \
        --trusted-host mirrors.aliyun.com \
        kafka-python

# ---------- 3. 连接器 JAR ----------
# 从 Maven 中央仓库拉；失败也不阻塞构建（JAR 可以后期放 jars/ 目录挂载进去）
ARG MAVEN_BASE=https://repo1.maven.org/maven2
RUN set -eux; \
    mkdir -p /opt/flink/lib/connectors; \
    fetch() { \
      url="$1"; out="$2"; \
      echo ">>> 下载 $out"; \
      if curl -fsSL --retry 3 --retry-delay 2 -o "/opt/flink/lib/connectors/$out" "$url"; then \
        echo "    OK  $out"; \
      else \
        rm -f "/opt/flink/lib/connectors/$out"; \
        echo "!!! 下载失败：$out"; \
        echo "    可手动下载后放入宿主机 jars/ 目录（会挂载到 /opt/flink/lib/connectors）"; \
        echo "    $url"; \
      fi; \
    }; \
    fetch "$MAVEN_BASE/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar" \
          "flink-sql-connector-kafka-3.4.0-1.20.jar"; \
    fetch "$MAVEN_BASE/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar" \
          "flink-connector-jdbc-3.2.0-1.19.jar"; \
    fetch "$MAVEN_BASE/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" \
          "mysql-connector-j-8.0.33.jar"; \
    fetch "$MAVEN_BASE/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" \
          "postgresql-42.7.4.jar"; \
    fetch "$MAVEN_BASE/org/apache/flink/flink-connector-postgres-cdc/3.6.0-1.20/flink-connector-postgres-cdc-3.6.0-1.20.jar" \
          "flink-connector-postgres-cdc-3.6.0-1.20.jar"; \
    echo "---- 连接器目录 ----"; \
    ls -lh /opt/flink/lib/connectors/

# ---------- 4. 工作目录 ----------
# 把示例和脚本放进来，容器启动即可用
WORKDIR /opt/flink
RUN mkdir -p /opt/flink/examples /opt/flink/data /opt/flink/scripts \
             /opt/flink/checkpoints /opt/flink/savepoints

# ---------- 5. 连接器自动挂载脚本 ----------
# 背景：连接器 JAR 放在 lib/connectors/ 子目录里，但 Flink 只扫描 lib/ 顶层，
#       所以必须把它们软链到 lib/ 才能被 classpath 识别。
#       宿主机 jars/ 目录挂载到 lib/connectors，放新 JAR 进去重启即生效。
RUN printf '%s\n' \
      '#!/bin/bash' \
      'set -e' \
      '# 把 connectors 子目录下的 JAR 软链到 lib/ 顶层（Flink 只扫这里）' \
      'if [ -d /opt/flink/lib/connectors ]; then' \
      '  for j in /opt/flink/lib/connectors/*.jar; do' \
      '    [ -e "$j" ] || continue' \
      '    ln -sf "$j" "/opt/flink/lib/$(basename "$j")"' \
      '  done' \
      'fi' \
      '# 继续走官方 entrypoint（jobmanager / taskmanager / help）' \
      'exec /docker-entrypoint.sh "$@"' \
      > /opt/flink/link-connectors.sh \
    && chmod +x /opt/flink/link-connectors.sh

# 验证 Python 环境（构建时就暴露问题，别等到运行才发现）
# 注意：pyflink 模块本身没有 __version__ 属性，要查已安装分发包的版本
RUN python --version && \
    python -c "import pyflink; print('pyflink 模块导入 OK ->', pyflink.__file__)" && \
    python -c "from importlib.metadata import version; [print(n, version(n)) for n in ('apache-flink','py4j','apache-beam','pandas')]" && \
    java -version 2>&1 | head -1

# 默认命令与官方镜像一致，由 compose 覆盖
CMD ["help"]
