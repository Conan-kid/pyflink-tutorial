"""
示例 02 · Table API 的「链式操作」
=====================================
学习目标
    1. 搞清楚 Table API 和 SQL 的对应关系（同一个东西的两种写法）
    2. 掌握 from_elements 声明多列 schema 的方法
    3. 理解 Table API 的惰性求值：链式调用只是「搭管道」，execute() 才真正开跑

运行
    python 02_table_api.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.get_config().set("parallelism.default", "1")

    # ------------------------------------------------------------------
    # 造一批「订单」数据：订单ID、用户、类别、金额、是否退款
    # ------------------------------------------------------------------
    orders = t_env.from_elements(
        [
            ("o001", "alice", "手机", 5999.0, False),
            ("o002", "bob", "手机", 4299.0, False),
            ("o003", "alice", "耳机", 899.0, False),
            ("o004", "carol", "笔记本", 8999.0, False),
            ("o005", "bob", "耳机", 1299.0, True),   # 退款单
            ("o006", "alice", "笔记本", 7599.0, False),
            ("o007", "dave", "手机", 1999.0, False),
            ("o008", "carol", "耳机", 599.0, False),
            ("o009", "bob", "笔记本", 6299.0, False),
            ("o010", "dave", "耳机", 399.0, True),    # 退款单
        ],
        ["order_id", "user_name", "category", "amount", "is_refund"],
    )

    # ------------------------------------------------------------------
    # 【练习 1】筛选 + 选列 —— 对应 SQL 的 WHERE + SELECT
    # ------------------------------------------------------------------
    print("\n===== 练习 1：有效订单（未退款）的订单号与金额 =====")
    (
        orders
        .filter(orders.is_refund == False)            # WHERE is_refund = false
        .select(orders.order_id, orders.amount)       # SELECT order_id, amount
        .execute()
        .print()
    )

    # 等价 SQL 写法，二选一即可：
    # t_env.sql_query(f"SELECT order_id, amount FROM {orders} WHERE is_refund = false")

    # ------------------------------------------------------------------
    # 【练习 2】新增计算列 —— 对应 SQL 的 SELECT expr AS alias
    # ------------------------------------------------------------------
    print("\n===== 练习 2：计算含税金额（按 10% 税率）=====")
    (
        orders
        .filter(orders.is_refund == False)
        # 在 Table API 里，算术运算是直接写 Python 表达式的
        .select(
            orders.user_name,
            orders.category,
            orders.amount,
            # 注意：Table API 的算术表达式不会自动推断别名，
            # 必须显式 .alias()，否则列名会变成一串难看的自动生成名字
            (orders.amount * 1.1).alias("amount_with_tax"),
        )
        .execute()
        .print()
    )

    # ------------------------------------------------------------------
    # 【练习 3】分组聚合 —— 对应 SQL 的 GROUP BY
    # ------------------------------------------------------------------
    print("\n===== 练习 3：各类别有效订单的销售额与单数 =====")
    (
        orders
        .filter(orders.is_refund == False)
        .group_by(orders.category)                            # GROUP BY category
        .select(
            orders.category,
            orders.amount.sum.alias("total_sales"),            # SUM(amount)
            orders.amount.count.alias("order_cnt"),            # COUNT(amount)
            orders.amount.avg.alias("avg_amount"),             # AVG(amount)
            orders.amount.max.alias("max_amount"),             # MAX(amount)
        )
        .execute()
        .print()
    )

    # 聚合函数速查（Table API 写法）：
    #   table.col.sum / count / avg / min / max / stddev_pop / var_pop
    #   还有 .sum(0) 这种带初始值的形式，以及 .distinct 去重：orders.col.count.distinct

    # ------------------------------------------------------------------
    # 【练习 4】排序 + 取 TopN
    # ------------------------------------------------------------------
    # 踩坑提醒：group_by 之后再 select，得到的是「新表」，
    # 排序时只能引用 select 里定义过的字段，不能再引用原始的 orders.amount。
    # 所以正确姿势是：先把聚合结果存成变量，再对它排序。
    print("\n===== 练习 4：销售额 Top 3 用户 =====")
    user_totals = (
        orders
        .filter(orders.is_refund == False)
        .group_by(orders.user_name)
        .select(orders.user_name, orders.amount.sum.alias("total_sales"))
    )
    (
        user_totals
        .order_by(user_totals.total_sales.desc)     # 引用新表里的 total_sales
        .limit(3)
        .execute()
        .print()
    )

    # 对比：用 SQL 写就没有作用域烦恼，字段由 SQL 自己管理。
    #
    # 【重要】想在 SQL 里引用这个 Table，必须先注册成视图！
    #   from_elements 返回的是一个 Table 对象，SQL 解析器不认识它，
    #   直接写 FROM orders 会报 "Object 'orders' not found"。
    #   必须先 create_temporary_view 给它一个 SQL 可见的名字。
    t_env.create_temporary_view("orders_view", orders)
    print("\n用 SQL 写同样的逻辑（更省心）：")
    t_env.sql_query(
        """
        SELECT user_name, SUM(amount) AS total_sales
        FROM orders_view
        WHERE is_refund = false
        GROUP BY user_name
        ORDER BY total_sales DESC
        LIMIT 3
        """
    ).execute().print()


if __name__ == "__main__":
    main()
