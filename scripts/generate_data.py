#!/usr/bin/env python
"""
生成示例用的测试数据
"""
import csv
import os
import random
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

USERS = ["alice", "bob", "carol", "dave", "erin"]
CATEGORIES = ["手机", "笔记本", "耳机", "平板", "配件"]

def gen_sales_csv(path: str, rows: int = 200):
    """生成销售明细 CSV，供文件连接器示例使用。"""
    base = datetime(2026, 9, 1, 0, 0, 0)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for i in range(rows):
            w.writerow([
                f"o{i:04d}",
                random.choice(USERS),
                random.choice(CATEGORIES),
                round(random.uniform(99, 9999), 2),
                (base + timedelta(minutes=i * 7)).strftime("%Y-%m-%d %H:%M:%S"),
            ])
    print(f"  ✓ 已生成 {path} ({rows} 行)")


def gen_init_sql(path: str):
    """生成 MySQL 建表语句，供实战项目使用。"""
    sql = """-- PyFlink 实战项目：初始化数据库
CREATE DATABASE IF NOT EXISTS flink_demo
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE flink_demo;

-- 订单流（Kafka 之外的备选落地表）
CREATE TABLE IF NOT EXISTS orders (
    order_id   VARCHAR(32)  PRIMARY KEY,
    user_id    VARCHAR(32)  NOT NULL,
    category   VARCHAR(32),
    amount     DECIMAL(12,2) NOT NULL,
    event_time BIGINT       NOT NULL,
    INDEX idx_user_time (user_id, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 风控告警表（PyFlink 作业的 Sink）
CREATE TABLE IF NOT EXISTS risk_alerts (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    rule_id    VARCHAR(32)   NOT NULL,
    user_id    VARCHAR(32)   NOT NULL,
    order_id   VARCHAR(32),
    amount     DECIMAL(12,2),
    detail     VARCHAR(512),
    alert_time BIGINT,
    INDEX idx_user (user_id),
    INDEX idx_rule (rule_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 风控规则配置表（维表 JOIN 的动态规则来源）
CREATE TABLE IF NOT EXISTS risk_rules (
    rule_id     VARCHAR(32) PRIMARY KEY,
    rule_name   VARCHAR(64),
    threshold   DECIMAL(12,2),
    window_ms   BIGINT,
    enabled     TINYINT DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO risk_rules VALUES
    ('R1', '高频交易', 3,     10000,  1),
    ('R2', '大额异常', 20000, NULL,   1),
    ('R3', '累计超限', 50000, 86400000, 1);

-- 品类汇总表（示例 06 的 JDBC Sink 目标）
CREATE TABLE IF NOT EXISTS category_summary (
    category    VARCHAR(32) PRIMARY KEY,
    order_cnt   BIGINT,
    total_sales DECIMAL(14,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(sql)
    print(f"  ✓ 已生成 {path}")


if __name__ == "__main__":
    print("生成测试数据…")
    gen_sales_csv(os.path.join(DATA_DIR, "sales.csv"))
    gen_init_sql(os.path.join(DATA_DIR, "init.sql"))
    print("完成。")
