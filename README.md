# MySQL 8 Join vs Single-Table Benchmark (Prototype)

A small prototype to compare the cost of a 4-table JOIN versus a single-table SELECT in MySQL 8 with about 500k base rows.

## Scope
- Normalized: orders + customers + regions + segments (4-table JOIN)
- Denormalized: single orders_denorm table

## Prerequisites
- Docker
- Python 3

## Setup
```bash
cd /Users/terui/workspace/mysql-join-benchmark
docker compose up -d
python3 -m pip install -r requirements.txt
python3 scripts/generate_data.py --rows 500000 --out ./bench/data
python3 scripts/load_data.py --host 127.0.0.1 --port 3306 --user root --password root --database bench --truncate
python3 scripts/run_benchmark.py --host 127.0.0.1 --port 3306 --user root --password root --database bench
```

## What It Runs
1. Range SELECT by order_id
2. Filtered SELECT by order_date + segment_id
3. Aggregation by region + segment

Each query runs with warmup and repeats to show average timings.

## Notes
- Run benchmarks twice to compare cold vs warm cache.
- If you want to reset the schema, run: `docker compose down -v` and start again.
- For deeper insight, run `EXPLAIN ANALYZE` on the queries in `scripts/run_benchmark.py`.

## Files
- `initdb/schema.sql` schema definition
- `scripts/generate_data.py` synthetic CSV generator
- `scripts/load_data.py` bulk loader
- `scripts/run_benchmark.py` query runner
