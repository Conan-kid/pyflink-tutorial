#!/usr/bin/env python
"""
Kafka 订单数据生产端
=====================
持续往 topics:orders 灌入模拟订单，供 PyFlink 实时风控作业消费。

用法：
    python scripts/produce_orders.py                 # 持续生产，每秒 2 条
    python scripts/produce_orders.py --rate 10       # 每秒 10 条
    python scripts/produce_orders.py --count 200     # 只发 200 条后退出
    python scripts/produce_orders.py --users 200     # 200 个用户（压测用）

建议开两个终端：
    终端 A：python scripts/produce_orders.py
    终端 B：python examples/08_realtime_risk_control.py
"""

import argparse
import json
import random
import time

from kafka import KafkaProducer

CATEGORIES = ["数码", "服饰", "食品", "家居", "图书", "美妆", "运动"]

# 风险注入概率：让示例能稳定产出告警
P_LARGE_AMOUNT = 0.06      # 6% 概率产生大额订单（触发 R2）
P_HIGH_FREQ_USER = 0.10    # 10% 的用户是「高频薅羊毛」型


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",          # 保证不丢
        linger_ms=10,        # 攒 10ms 再发，提升吞吐
    )


def make_order(seq: int, user_pool: list) -> dict:
    user = random.choice(user_pool)

    # 高频用户：用极小金额密集下单，触发 R1
    if user.startswith("hot_") or random.random() < P_HIGH_FREQ_USER:
        user = random.choice([u for u in user_pool if u.startswith("hot_")] or user_pool)
        amount = round(random.uniform(10, 200), 2)
    elif random.random() < P_LARGE_AMOUNT:
        amount = round(random.uniform(20000, 60000), 2)     # 触发 R2 / R3
    else:
        amount = round(random.uniform(50, 8000), 2)

    return {
        "order_id": f"o{int(time.time() * 1000) % 10_000_000}{seq:04d}",
        "user_id": user,
        "category": random.choice(CATEGORIES),
        "amount": amount,
        "event_time": int(time.time() * 1000),
    }


def main():
    ap = argparse.ArgumentParser(description="Kafka 订单生产者")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--rate", type=float, default=2.0, help="每秒条数")
    ap.add_argument("--count", type=int, default=0, help="发送总数，0 表示无限")
    ap.add_argument("--users", type=int, default=20, help="普通用户数")
    args = ap.parse_args()

    user_pool = [f"u{1000 + i}" for i in range(args.users)]
    # 加入 3 个「高频用户」，保证 R1 规则能被触发
    user_pool += ["hot_A", "hot_B", "hot_C"]

    print(f"连接 Kafka: {args.bootstrap}")
    try:
        producer = build_producer(args.bootstrap)
    except Exception as e:
        print(f"✗ 连接失败：{e}")
        print("  请先启动 Kafka：cd docker && docker compose up -d kafka")
        return

    print(f"开始生产 → topic={args.topic} rate={args.rate}/s users={len(user_pool)}")
    print("按 Ctrl+C 停止\n")

    interval = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0
    alert_candidates = {"large": 0}
    try:
        while args.count == 0 or sent < args.count:
            order = make_order(sent, user_pool)
            producer.send(args.topic, key=order["user_id"], value=order)
            sent += 1

            if order["amount"] > 20000:
                alert_candidates["large"] += 1

            if sent % 20 == 0:
                producer.flush()
                print(
                    f"  已发送 {sent} 条 | 其中大额订单 {alert_candidates['large']} 条"
                )

            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\n收到中断信号")
    finally:
        producer.flush()
        producer.close()
        print(f"\n完成。共发送 {sent} 条订单。")


if __name__ == "__main__":
    main()
