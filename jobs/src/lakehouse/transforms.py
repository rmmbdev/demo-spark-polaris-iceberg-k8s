"""Pure DataFrame -> DataFrame transforms, unit-tested without a cluster.

Timestamps are built from strings cast in Spark, so they follow the session
time zone (UTC in the jobs and tests), never the machine's local zone.

Jobs (lakehouse/jobs/*.py) only wire these together with reads and writes.
"""
import datetime as dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PRODUCTS = ["third_party", "body", "fire", "travel"]
QUOTES_PER_REQUEST = 5
N_INSURERS = 20
MIN_PRICE = 1_000_000
MAX_PRICE = 10_000_000

QUOTE_COLUMNS = [
    "quote_id",
    "request_id",
    "user_id",
    "insurer_id",
    "product",
    "price",
    "created_at",
]


def generate_quotes(
    spark: SparkSession, rows: int, start_day: str, days: int
) -> DataFrame:
    """Synthetic insurance quotes, spread evenly over `days` days.

    Deterministic: the same arguments give the same rows, so reruns of the
    generator job are idempotent.
    """
    seconds = days * 86_400
    start = F.lit(f"{start_day} 00:00:00").cast("timestamp")
    request_id = F.floor(F.col("id") / QUOTES_PER_REQUEST)
    product_index = F.pmod(F.xxhash64(request_id, F.lit(1)), F.lit(len(PRODUCTS)))
    price_steps = (MAX_PRICE - MIN_PRICE) // 1_000
    price = MIN_PRICE + F.pmod(F.xxhash64("id", F.lit(2)), F.lit(price_steps)) * 1_000
    offset_seconds = F.floor(F.col("id") * seconds / rows)
    created_at = F.timestamp_seconds(F.unix_timestamp(start) + offset_seconds)

    quotes = spark.range(rows).select(
        F.col("id").alias("quote_id"),
        request_id.alias("request_id"),
        F.pmod(F.xxhash64(request_id, F.lit(3)), F.lit(500_000)).alias("user_id"),
        (F.pmod(F.col("id"), F.lit(N_INSURERS)) + 1).cast("int").alias("insurer_id"),
        F.element_at(F.array(*[F.lit(p) for p in PRODUCTS]), (product_index + 1).cast("int"))
        .alias("product"),
        price.cast("bigint").alias("price"),
        created_at.alias("created_at"),
    )
    return quotes


def select_day(quotes: DataFrame, day: str) -> DataFrame:
    """Rows whose `created_at` falls on `day` (session time zone).

    A range on the raw column, not `to_date(created_at) == day`, so the
    filter can be pushed down to Iceberg's file statistics.
    """
    next_day = dt.date.fromisoformat(day) + dt.timedelta(days=1)
    start = F.lit(f"{day} 00:00:00").cast("timestamp")
    end = F.lit(f"{next_day} 00:00:00").cast("timestamp")
    is_on_day = (F.col("created_at") >= start) & (F.col("created_at") < end)
    return quotes.filter(is_on_day)


def clean_quotes(quotes: DataFrame) -> DataFrame:
    """Schema contract for silver quotes."""
    return quotes.select(
        "quote_id",
        "request_id",
        "user_id",
        "insurer_id",
        "product",
        "price",
        "created_at",
    )


def is_valid_quote():
    has_known_product = F.col("product").isin(PRODUCTS)
    has_positive_price = F.col("price") > 0
    has_keys = F.col("quote_id").isNotNull() & F.col("created_at").isNotNull()
    return has_known_product & has_positive_price & has_keys


def split_valid(quotes: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valid, invalid) rows. Invalid rows are counted, never silently dropped."""
    valid = is_valid_quote()
    return quotes.filter(valid), quotes.filter(~valid | valid.isNull())
