#!/usr/bin/env python3
import argparse
import os
import mysql.connector


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--data", default="./bench/data")
    parser.add_argument("--truncate", action="store_true")
    args = parser.parse_args()

    cnx = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        allow_local_infile=True,
    )
    cur = cnx.cursor()

    cur.execute("SET GLOBAL local_infile=1")
    cnx.commit()

    if args.truncate:
        for table in ["orders_denorm", "orders", "customers", "segments", "regions"]:
            cur.execute(f"TRUNCATE TABLE {table}")
        cnx.commit()

    load_sql = {
        "regions": "LOAD DATA LOCAL INFILE '{path}' INTO TABLE regions FIELDS TERMINATED BY ','",
        "segments": "LOAD DATA LOCAL INFILE '{path}' INTO TABLE segments FIELDS TERMINATED BY ','",
        "customers": "LOAD DATA LOCAL INFILE '{path}' INTO TABLE customers FIELDS TERMINATED BY ','",
        "orders": "LOAD DATA LOCAL INFILE '{path}' INTO TABLE orders FIELDS TERMINATED BY ','",
        "orders_denorm": "LOAD DATA LOCAL INFILE '{path}' INTO TABLE orders_denorm FIELDS TERMINATED BY ','",
    }

    for table, tmpl in load_sql.items():
        path = os.path.abspath(os.path.join(args.data, f"{table}.csv"))
        sql = tmpl.format(path=path)
        print(f"Loading {table}...")
        cur.execute(sql)
        cnx.commit()

    cur.close()
    cnx.close()


if __name__ == "__main__":
    main()
