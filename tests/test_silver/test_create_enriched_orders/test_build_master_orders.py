import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
import retail_analysis.databricks.notebooks.create_enriched_orders as m
from retail_analysis.databricks.notebooks.create_enriched_orders import build_master_orders 
from retail_analysis.databricks.utils.schema import ORDERS_SCHEMA, CUSTOMER_SCHEMA, PRODUCTS_SCHEMA
from datetime import date


import retail_analysis.databricks.notebooks.create_enriched_orders as module

from retail_analysis.databricks.utils.util import read_data, merge_delta_upsert, enforce_schema


# ─────────────────────────────────────────────
# Monkeypatch preparers (isolate function logic)
# ─────────────────────────────────────────────
@pytest.fixture(autouse=True)
def patch_preparers(monkeypatch):
    monkeypatch.setattr(module, "_prepare_orders", lambda df: df)
    monkeypatch.setattr(module, "_prepare_customers", lambda df: df)
    monkeypatch.setattr(module, "_prepare_products", lambda df: df)




# ─────────────────────────────────────────────
# Base Fixtures
# ─────────────────────────────────────────────
@pytest.fixture
def customers_df(spark):
    return spark.createDataFrame([
        (1, "Alice", "India", "Consumer", "Chennai", "TN", "South"),
    ], ["customer_id", "customer_name", "country", "segment", "customer_city", "customer_state", "region"])


@pytest.fixture
def products_df(spark):
    return spark.createDataFrame([
        (101, "Laptop", "Tech", "Computers", 1000.0),
    ], ["product_id", "product_name", "category", "sub_category", "price_per_product"])


# ─────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────
def get_row(df):
    return df.first().asDict()



# ─────────────────────────────────────────────
# TESTS
# ─────────────────────────────────────────────
@pytest.mark.gold
@pytest.mark.parametrize(
    "qty, price, discount, expected",
    [
        (1, 100.0, 0.0, 100.0),
        (2, 100.0, 0.1, 90.0),
        (5, 50.0, 0.2, 40.0),
    ]
)
def test_total_revenue_uses_quantity(spark, customers_df, products_df, qty, price, discount, expected):
    """
    Ensures total_revenue correctly accounts for quantity, price, and discount.
    Detects bug where quantity was ignored in calculation.
    """

    orders = spark.createDataFrame([
        (1, date(2024,1,1), date(2024,1,3), "Standard", 1, 101, qty, price, discount, 10)
    ], ["order_id","order_date","ship_date","ship_mode","customer_id","product_id","quantity","price","discount","profit"])

    result = m.build_master_orders(spark, orders, customers_df, products_df)
    row = get_row(result)

    assert row["total_revenue"] == pytest.approx(expected)


@pytest.mark.gold
@pytest.mark.parametrize(
    "order_date, ship_date, expected_year, expected_month, expected_days",
    [
        (date(2024, 5, 10), date(2024, 5, 13), 2024, 5, 3),
        (None, date(2024, 1, 2), None, None, None),  # None propagation
        (None, None, None,None,None),
        (date(2024,1,5),date(2024,1,3), 2024,1,-2),
    ],
)
def test_date_fields_correctly_parsed_and_calculated(spark, customers_df, products_df, order_date, ship_date, expected_year, expected_month, expected_days):
    """
    Validates correct extraction of order_year, order_month,
    and computation of days_to_ship.
    """
    from datetime import date
    schema =  StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True)
    ])
    
    orders = spark.createDataFrame([
        (1, order_date, ship_date, "Standard", 1, 101, 8,  100.0, 0.1, 10.0)
    ], schema)

    result = build_master_orders(spark, orders, customers_df, products_df)
    row = get_row(result)

    assert row["order_year"] == expected_year
    assert row["order_month"] == expected_month
    assert row["days_to_ship"] == expected_days


@pytest.mark.gold
def test_left_join_missing_dimension_data(spark):
    """
    Ensures left joins preserve order rows even when
    customer or product records are missing.
    Dimension fields should be NULL.
    """
    from datetime import date

    orders = spark.createDataFrame([
        (1, date(2024,1,1), date(2024,1,2), "Standard", 999, 888, 1, 100, 0, 5)
    ], ["order_id","order_date","ship_date","ship_mode","customer_id","product_id","quantity","price","discount","profit"])

    customers = spark.createDataFrame([], "customer_id INT, customer_name STRING, country STRING, segment STRING, customer_city STRING, customer_state STRING, region STRING")
    products = spark.createDataFrame([], "product_id INT, product_name STRING, category STRING, sub_category STRING, price_per_product DOUBLE")

    row = get_row(m.build_master_orders(spark, orders, customers, products))

    assert row["customer_name"] is None
    assert row["customer_id"] == 999
    assert row["product_id"] == 888


@pytest.mark.gold
def test_output_schema_and_columns(spark, customers_df, products_df):
    """
    Verifies that the output DataFrame has the expected columns
    and schema structure in correct order.
    """
    from datetime import date

    orders = spark.createDataFrame([
        (1, date(2024,1,1), date(2024,1,2), "Standard", 1, 101, 1, 100, 0, 5)
    ], ["order_id","order_date","ship_date","ship_mode","customer_id","product_id","quantity","price","discount","profit"])

    df = m.build_master_orders(spark, orders, customers_df, products_df)

    expected_cols = [
        "order_id","order_date","ship_date","ship_mode",
        "order_year","order_month","days_to_ship",
        "customer_id","customer_name","country",
        "product_id", "category","sub_category",
        "quantity","order_price","discount","profit","total_revenue","_updated_at"
    ]

    assert df.columns == expected_cols


@pytest.mark.gold
def test_updated_at_column_exists_and_not_null(spark, customers_df, products_df):
    """
    Ensures metadata column _updated_at is populated
    and not null for all rows.
    """
    from datetime import date

    orders = spark.createDataFrame([
        (1, date(2024,1,1), date(2024,1,2), "Standard", 1, 101, 1, 100, 0, 5)
    ], ["order_id","order_date","ship_date","ship_mode","customer_id","product_id","quantity","price","discount","profit"])

    row = get_row(m.build_master_orders(spark, orders, customers_df, products_df))

    assert row["_updated_at"] is not None

