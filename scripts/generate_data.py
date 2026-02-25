#!/usr/bin/env python3
import argparse
import csv
import os
import random
from datetime import datetime, timedelta


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500000)
    parser.add_argument("--out", type=str, default="./bench/data")
    args = parser.parse_args()

    random.seed(42)
    os.makedirs(args.out, exist_ok=True)

    # Dimension sizes
    regions = 10
    segments = 5
    customers = 100000

    # Regions
    with open(os.path.join(args.out, "regions.csv"), "w", newline="") as f:
        w = csv.writer(f)
        for i in range(1, regions + 1):
            w.writerow([i, f"Region-{i}"])

    # Segments
    with open(os.path.join(args.out, "segments.csv"), "w", newline="") as f:
        w = csv.writer(f)
        for i in range(1, segments + 1):
            w.writerow([i, f"Segment-{i}"])

    # Customers (region/segment assignment is deterministic for stable joins)
    base_dt = datetime(2020, 1, 1)
    with open(os.path.join(args.out, "customers.csv"), "w", newline="") as f:
        w = csv.writer(f)
        for customer_id in range(1, customers + 1):
            region_id = ((customer_id - 1) % regions) + 1
            segment_id = ((customer_id - 1) % segments) + 1
            created_at = base_dt + timedelta(days=customer_id % 1000)
            w.writerow(
                [
                    customer_id,
                    region_id,
                    segment_id,
                    created_at.strftime("%Y-%m-%d %H:%M:%S"),
                ]
            )

    # Orders + denormalized
    with (
        open(os.path.join(args.out, "orders.csv"), "w", newline="") as fo,
        open(os.path.join(args.out, "orders_denorm.csv"), "w", newline="") as fd,
    ):
        wo = csv.writer(fo)
        wd = csv.writer(fd)

        for order_id in range(1, args.rows + 1):
            customer_id = random.randint(1, customers)
            region_id = ((customer_id - 1) % regions) + 1
            segment_id = ((customer_id - 1) % segments) + 1
            order_amount = round(random.uniform(10, 500), 2)
            order_date = base_dt + timedelta(days=random.randint(0, 365 * 3))

            wo.writerow(
                [
                    order_id,
                    customer_id,
                    order_amount,
                    order_date.strftime("%Y-%m-%d"),
                ]
            )

            wd.writerow(
                [
                    order_id,
                    customer_id,
                    region_id,
                    f"Region-{region_id}",
                    segment_id,
                    f"Segment-{segment_id}",
                    order_amount,
                    order_date.strftime("%Y-%m-%d"),
                    (base_dt + timedelta(days=customer_id % 1000)).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                ]
            )

    print(f"Generated data in {args.out}")


if __name__ == "__main__":
    main()
