import pytest

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,DoubleType,DateType, StructType,TimestampType
import retail_analysis.databricks.notebooks.create_enriched_orders as m



# ─────────────────────────────────────────────
# _prepare_orders TESTS
# ─────────────────────────────────────────────
@pytest.mark.gold
@pytest.mark.parametrize(
    "rows, expected_qty, expected_price, expected_profit",
    [
        (
            [("2024-01-01", "2024-01-03", 2, 100, 10),
             ("2024-01-01", "2024-01-03", 3, 200, 5)],
            5, 300, 15.0
        ),
    ]
)
def test_prepare_orders_aggregation(spark, rows, expected_qty, expected_price, expected_profit):
    """
    Ensures aggregation logic correctly sums quantity, price,
    and profit across grouped rows.
    """
    data = [
        (1, r[0], r[1], "Standard", 10, 101, 0.1, r[2], r[3], r[4])
        for r in rows
    ]

    df = spark.createDataFrame(data, [
        "order_id","order_date","ship_date","ship_mode",
        "customer_id","product_id","discount",
        "quantity","price","profit"
    ])

    result = m._prepare_orders(df).collect()[0].asDict()

    assert result["quantity"] == expected_qty
    assert result["price"] == expected_price
    assert result["profit"] == pytest.approx(expected_profit)



@pytest.mark.gold
@pytest.mark.parametrize(
    "order_date, ship_date, expected_order, expected_ship",
    [
        ("1/1/2024", "2/1/2024", date(2024, 1, 1), date(2024, 1, 2)),
        ("01/01/2024", "02/01/2024", date(2024, 1, 1), date(2024, 1, 2)),

        ("2024-01-01", "2024-01-02", None, None),
        ("2024/01/01", "2024/01/02", None, None),

        ("invalid", "not_a_date", None, None),
    ]
)
def test_prepare_orders_date_parsing(spark, order_date, ship_date, expected_order, expected_ship):
    """
    Validate date parsing using DATE_FMT = "d/M/yyyy".

    Ensures:
    - valid formats are correctly parsed to date
    - invalid formats result in NULL
    """
    df = spark.createDataFrame([
        (1, order_date, ship_date, "Standard", 1, 101, 0.0, 1, 100, 10)
    ], [
        "order_id","order_date","ship_date","ship_mode",
        "customer_id","product_id","discount",
        "quantity","price","profit"
    ])

    row = m._prepare_orders(df).first().asDict()

    assert row["order_date"] == expected_order
    assert row["ship_date"] == expected_ship






# ─────────────────────────────────────────────
# _prepare_customers TESTS
# ─────────────────────────────────────────────

@pytest.mark.gold
def test_prepare_customers_expected_columns(spark):
    """
    Verifies that _prepare_customers returns exactly the expected columns
    with correct aliasing and no extra fields.
    """
    df = spark.createDataFrame([
        (1, "Alice", "India", "Consumer", "Chennai", "TN", "South", "EXTRA")
    ], [
        "customer_id","customer_name","country","segment",
        "city","state","region","extra_col"
    ])

    result = m._prepare_customers(df)

    expected_columns = [
        "customer_id",
        "customer_name",
        "country"
    ]

    assert result.columns == expected_columns

# ─────────────────────────────────────────────
# _prepare_products TESTS
# ─────────────────────────────────────────────


@pytest.mark.gold
def test_prepare_products_expected_columns(spark):
    """
    Verifies that _prepare_products returns exactly the expected columns
    and excludes any unrelated fields.
    """
    df = spark.createDataFrame([
        (101, "Laptop", "Tech", "Computers", 1000.0, "EXTRA")
    ], [
        "product_id","product_name","category",
        "sub_category","price_per_product","extra_col"
    ])

    result = m._prepare_products(df)

    expected_columns = [
        "product_id",
        "category",
        "sub_category"
    ]

    assert result.columns == expected_columns

