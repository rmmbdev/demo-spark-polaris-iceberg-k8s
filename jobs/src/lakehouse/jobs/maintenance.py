"""Table maintenance: compact small files, then expire old snapshots.

The routine work of a lakehouse team, usually scheduled nightly.
"""
import argparse
import datetime as dt

from lakehouse.spark import get_spark


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--table", default="silver.quotes_daily",
                   help="namespace.table inside the `lake` catalog")
    p.add_argument("--retain-last", type=int, default=3)
    return p.parse_args()


def count(spark, sql):
    return spark.sql(sql).first()[0]


def main():
    args = parse_args()
    spark = get_spark(f"maintenance-{args.table}")
    table = f"lake.{args.table}"
    # Procedure arguments must be literals, so compute "now" here (UTC).
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    before = count(spark, f"SELECT count(*) FROM {table}.files")
    spark.sql(f"""
        CALL lake.system.rewrite_data_files(
          table => '{args.table}',
          options => map('min-input-files', '2'))
    """).show(truncate=False)

    spark.sql(f"""
        CALL lake.system.expire_snapshots(
          table => '{args.table}',
          older_than => TIMESTAMP '{now}',
          retain_last => {args.retain_last})
    """).show(truncate=False)

    after = count(spark, f"SELECT count(*) FROM {table}.files")
    snapshots = count(spark, f"SELECT count(*) FROM {table}.snapshots")
    print(f"{table}: data files {before} -> {after}, snapshots kept {snapshots}")
    spark.stop()


if __name__ == "__main__":
    main()
