"""Bronze: generate synthetic quotes into lake.bronze.quotes.

Pods can't see files on your laptop, so the demo creates its source data
inside the cluster. Idempotent: deterministic rows + overwritePartitions.
"""
import argparse

from lakehouse.spark import get_spark
from lakehouse.transforms import generate_quotes

TABLE = "lake.bronze.quotes"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  quote_id BIGINT, request_id BIGINT, user_id BIGINT,
  insurer_id INT, product STRING, price BIGINT,
  created_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES ('format-version' = '2')
"""


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=2_000_000)
    p.add_argument("--start-day", default="2026-08-01")
    p.add_argument("--days", type=int, default=7)
    return p.parse_args()


def main():
    args = parse_args()
    spark = get_spark("generate-quotes")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql(DDL)

    quotes = generate_quotes(spark, args.rows, args.start_day, args.days)
    quotes.writeTo(TABLE).overwritePartitions()

    spark.sql(
        f"SELECT to_date(created_at) AS day, count(*) AS quotes "
        f"FROM {TABLE} GROUP BY 1 ORDER BY 1"
    ).show(100)
    spark.stop()


if __name__ == "__main__":
    main()
