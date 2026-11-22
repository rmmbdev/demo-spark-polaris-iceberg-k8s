import argparse

from lakehouse.spark import get_spark

TABLE = "lake.bronze.quotes"


def parse_args():
    p = argparse.ArgumentParser()
    return p.parse_args()


def main():
    args = parse_args()
    spark = get_spark("show-count")

    spark.sql(
        f"SELECT count(*) AS quotes "
        f"FROM {TABLE}"
    ).show(100)
    spark.stop()


if __name__ == "__main__":
    main()
