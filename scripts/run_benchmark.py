#!/usr/bin/env python3
import argparse
import time
import mysql.connector


QUERIES = {
    "join_range": """
        SELECT SQL_NO_CACHE o.order_id, o.order_amount, r.region_name, s.segment_name
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN regions r ON c.region_id = r.region_id
        JOIN segments s ON c.segment_id = s.segment_id
        WHERE o.order_id BETWEEN %s AND %s
    """,
    "denorm_range": """
        SELECT SQL_NO_CACHE order_id, order_amount, region_name, segment_name
        FROM orders_denorm
        WHERE order_id BETWEEN %s AND %s
    """,
    "join_filter": """
        SELECT SQL_NO_CACHE o.order_id, o.order_amount, r.region_name, s.segment_name
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN regions r ON c.region_id = r.region_id
        JOIN segments s ON c.segment_id = s.segment_id
        WHERE o.order_date BETWEEN %s AND %s AND c.segment_id = %s
    """,
    "denorm_filter": """
        SELECT SQL_NO_CACHE order_id, order_amount, region_name, segment_name
        FROM orders_denorm
        WHERE order_date BETWEEN %s AND %s AND segment_id = %s
    """,
    "join_agg": """
        SELECT SQL_NO_CACHE r.region_name, s.segment_name, COUNT(*) AS cnt, SUM(o.order_amount) AS total
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN regions r ON c.region_id = r.region_id
        JOIN segments s ON c.segment_id = s.segment_id
        WHERE o.order_date BETWEEN %s AND %s
        GROUP BY r.region_name, s.segment_name
    """,
    "denorm_agg": """
        SELECT SQL_NO_CACHE region_name, segment_name, COUNT(*) AS cnt, SUM(order_amount) AS total
        FROM orders_denorm
        WHERE order_date BETWEEN %s AND %s
        GROUP BY region_name, segment_name
    """,
}


def time_query(cur, sql, params, repeats=5, warmup=1):
    times = []
    for _ in range(warmup):
        cur.execute(sql, params)
        cur.fetchall()
    for _ in range(repeats):
        start = time.time()
        cur.execute(sql, params)
        cur.fetchall()
        times.append(time.time() - start)
    return times


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    args = parser.parse_args()

    cnx = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    cur = cnx.cursor()

    range_params = (100000, 150000)
    filter_params = ("2021-01-01", "2022-01-01", 3)
    agg_params = ("2021-01-01", "2022-01-01")

    for name, sql in QUERIES.items():
        params = (
            range_params
            if "range" in name
            else filter_params
            if "filter" in name
            else agg_params
        )
        times = time_query(cur, sql, params, repeats=args.repeats, warmup=args.warmup)
        avg = sum(times) / len(times)
        print(name, times, "avg", avg)

    cur.close()
    cnx.close()


if __name__ == "__main__":
    main()
