"""Silver: load one day of bronze quotes into lake.silver.quotes_daily.

Idempotent: a rerun replaces that day's partition (overwritePartitions).
Fails loudly, writing nothing, when the day is empty or too dirty.
"""
import argparse
import time

from lakehouse.spark import get_spark
from lakehouse.transforms import clean_quotes, select_day, split_valid

SOURCE = "lake.bronze.quotes"
TARGET = "lake.silver.quotes_daily"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
  quote_id BIGINT, request_id BIGINT, user_id BIGINT,
  insurer_id INT, product STRING, price BIGINT,
  created_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES ('format-version' = '2')
"""

LAST_COMMIT = f"""
SELECT operation,
  summary['added-records']   AS added,
  summary['deleted-records'] AS deleted,
  summary['total-records']   AS total
FROM {TARGET}.snapshots
ORDER BY committed_at DESC LIMIT 1
"""


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--day", required=True)
    # Share of invalid rows above which the run fails instead of loading.
    p.add_argument("--max-invalid-ratio", type=float, default=0.01)
    # Seconds to keep the driver (and its Spark UI) alive after the work.
    p.add_argument("--linger", type=int, default=0)
    return p.parse_args()


def main():
    args = parse_args()
    spark = get_spark(f"quotes-daily-{args.day}")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql(DDL)

    day = clean_quotes(select_day(spark.table(SOURCE), args.day)).cache()
    valid, invalid = split_valid(day)
    total, bad = day.count(), invalid.count()

    # An empty or dirty day is a broken upstream, not a day to load.
    if total == 0:
        raise SystemExit(f"no rows for {args.day} in {SOURCE}")
    if bad / total > args.max_invalid_ratio:
        raise SystemExit(f"{bad}/{total} invalid rows for {args.day}")

    valid.writeTo(TARGET).overwritePartitions()
    print(f"loaded {total - bad} rows for {args.day} ({bad} invalid skipped)")
    spark.sql(LAST_COMMIT).show(truncate=False)

    time.sleep(args.linger)
    spark.stop()


if __name__ == "__main__":
    main()
