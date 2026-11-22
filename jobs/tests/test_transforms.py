import datetime as dt

from lakehouse.transforms import (
    PRODUCTS,
    QUOTE_COLUMNS,
    clean_quotes,
    generate_quotes,
    select_day,
    split_valid,
)

SCHEMA = (
    "quote_id long, request_id long, user_id long, insurer_id int, "
    "product string, price long, created_at timestamp"
)


def quote(quote_id, product="body", price=5_000_000, ts="2026-08-02 10:00:00"):
    # Timezone-aware (UTC): a naive datetime would be read in the laptop's
    # local zone and shift across the day boundary (e.g. Asia/Tehran).
    created_at = dt.datetime.fromisoformat(ts).replace(tzinfo=dt.timezone.utc)
    return (quote_id, quote_id // 5, 1, 1, product, price, created_at)


def test_generate_is_deterministic_and_spread_over_days(spark):
    a = generate_quotes(spark, rows=1_000, start_day="2026-08-01", days=4)
    b = generate_quotes(spark, rows=1_000, start_day="2026-08-01", days=4)

    assert a.columns == QUOTE_COLUMNS
    assert a.exceptAll(b).count() == 0
    days = {r[0] for r in a.selectExpr("to_date(created_at)").distinct().collect()}
    assert days == {dt.date(2026, 8, d) for d in (1, 2, 3, 4)}
    assert {r[0] for r in a.select("product").distinct().collect()} <= set(PRODUCTS)


def test_select_day_uses_half_open_range(spark):
    quotes = spark.createDataFrame(
        [
            quote(1, ts="2026-08-01 23:59:59"),
            quote(2, ts="2026-08-02 00:00:00"),
            quote(3, ts="2026-08-02 23:59:59"),
            quote(4, ts="2026-08-03 00:00:00"),
        ],
        SCHEMA,
    )
    ids = sorted(r.quote_id for r in select_day(quotes, "2026-08-02").collect())
    assert ids == [2, 3]


def test_split_valid_keeps_bad_rows_countable(spark):
    quotes = spark.createDataFrame(
        [quote(1), quote(2, product="car_loan"), quote(3, price=0),
         quote(4, product=None)],
        SCHEMA,
    )
    valid, invalid = split_valid(quotes)
    assert [r.quote_id for r in valid.collect()] == [1]
    assert sorted(r.quote_id for r in invalid.collect()) == [2, 3, 4]


def test_clean_quotes_is_the_schema_contract(spark):
    quotes = spark.createDataFrame([quote(1)], SCHEMA).withColumn("extra", 1)
    assert clean_quotes(quotes).columns == QUOTE_COLUMNS
