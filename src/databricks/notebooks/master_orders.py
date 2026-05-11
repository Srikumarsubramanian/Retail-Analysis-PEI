"""
Gold Layer — Master Orders
==========================
Joins Bronze orders with Silver dimension tables (customers, products)
to produce a fully enriched, analytics-ready master table.

Key guarantees:
  - Date strings parsed into DateType.
  - Derived columns: ``order_year``, ``order_month``, ``days_to_ship``.
  - Left joins preserve all order rows even if a dimension key is missing.
  - Canonical column ordering for downstream consumption.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from src.databricks.utils.constants import (
    BRONZE_DELTA_PATH,
    SILVER_DELTA_PATH,
    GOLD_DELTA_PATH,
)
from src.databricks.utils.logger import get_logger
from src.databricks.utils.schema import ORDERS_SCHEMA, CUSTOMER_SCHEMA, PRODUCTS_SCHEMA
from src.databricks.utils.util import read_data, merge_delta_upsert, enforce_schema

log = get_logger(__name__)

DATE_FMT = "d/M/yyyy"
MERGE_KEYS = [
    "order_id", "order_date", "ship_date",
    "ship_mode", "customer_id", "product_id", "discount",
]



# ─────────────────────────────────────────────
# Read
# ─────────────────────────────────────────────
def _read_sources(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Read bronze orders and silver dimension tables.

    Future Actions
    -----------
    filter at source once we get last_update timestamps for each file at source
    """
    log.info("Reading bronze orders and silver dimension tables")
    orders    = read_data(spark, f"{BRONZE_DELTA_PATH}","orders",   format="delta")
    orders    = enforce_schema(orders, ORDERS_SCHEMA, "Orders Bronze")
    customers = read_data(spark, f"{SILVER_DELTA_PATH}","customer", format="delta")
    customers = enforce_schema(customers, CUSTOMER_SCHEMA, "Customers Silver")
    products  = read_data(spark, f"{SILVER_DELTA_PATH}","products", format="delta")
    products  = enforce_schema(products, PRODUCTS_SCHEMA, "Products Silver")
    log.info("Successfully read bronze orders and silver dimension tables")
    return orders, customers, products


# ─────────────────────────────────────────────
# Prepare dimensions
# ─────────────────────────────────────────────
def _prepare_orders(orders: DataFrame) -> DataFrame:
    """Parse date strings and round profit."""


    return (
        orders
        .drop("row_id")
        .withColumn("order_date", F.to_date(F.col("order_date"), DATE_FMT))
        .withColumn("ship_date",  F.to_date(F.col("ship_date"),  DATE_FMT))
        .groupBy(MERGE_KEYS)
        .agg(
            F.sum("quantity").alias("quantity"),
            F.sum("price").alias("price"),
            F.round(F.sum("profit"), 2).alias("profit"),
        )
    )

def _prepare_customers(customers: DataFrame) -> DataFrame:
    return customers.select(
        F.col("customer_id").alias("cust_customer_id"),
        F.col("customer_name"),
        F.col("country"),
        F.col("segment"),
        F.col("city").alias("customer_city"),
        F.col("state").alias("customer_state"),
        F.col("region"),
    )


def _prepare_products(products: DataFrame) -> DataFrame:
    return products.select(
        F.col("product_id").alias("prod_product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("sub_category"),
        F.col("price_per_product"),
    )


# ─────────────────────────────────────────────
# Build master table
# ─────────────────────────────────────────────
def build_master_orders(spark: SparkSession,orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """Build the enriched master orders table.

    Steps
    -----
    1. Load bronze orders and silver customers / products.
    2. Parse date strings into ``DateType``.
    3. Left-join orders → customers and orders → products.
    4. Derive ``order_year``, ``order_month``, ``days_to_ship``.
    5. Return desired column order.
    """
    

    orders   = _prepare_orders(orders)

    cust_dim = F.broadcast(_prepare_customers(customers))
    prod_dim = F.broadcast(_prepare_products(products))

    enriched = (
        orders
        .join(cust_dim, orders["customer_id"] == cust_dim["cust_customer_id"], how="left")
        .join(prod_dim, orders["product_id"]  == prod_dim["prod_product_id"],  how="left")
        .withColumn("order_year",   F.year(F.col("order_date")))
        .withColumn("order_month",  F.month(F.col("order_date")))
        .withColumn("days_to_ship", F.datediff(F.col("ship_date"), F.col("order_date")))
        ##quantity is already part of the price field in orders
        .withColumn("total_revenue", F.round( F.col("price") * (1 - F.col("discount")), 2))
        .withColumn("_updated_at", F.current_timestamp())
    )

    master = enriched.select(
        # Order identifiers and logistics
        F.col("order_id"),
        F.col("order_date"),
        F.col("ship_date"),
        F.col("ship_mode"),
        F.col("order_year"),
        F.col("order_month"),
        F.col("days_to_ship"),
        # Customer dimension
        F.col("customer_id"),
        F.col("customer_name"),
        F.col("country"),
        F.col("segment"),
        F.col("customer_city"),
        F.col("customer_state"),
        F.col("region"),
        # Product dimension
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("sub_category"),
        F.col("price_per_product"),
        # Measures
        F.col("quantity"),
        F.col("price").alias("order_price"),
        F.col("discount"),
        F.col("profit"),
        F.col("total_revenue"),
        # Metadata
        F.col("_updated_at")
    )

    return master


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
def run_master_orders(spark: SparkSession) -> DataFrame:
    """Build and persist the master orders table."""
    log.info("Building master orders table")
    orders, customers, products = _read_sources(spark)
    master_orders = build_master_orders(spark,orders, customers, products)
    master_orders = master_orders.repartition("order_year")
    merge_delta_upsert(
        spark, 
        master_orders, 
        "master_orders", 
        GOLD_DELTA_PATH, 
        merge_keys=["order_id", "product_id"],
        partition_cols=["order_year"]
    )
    log.info(f"Successfully built master orders table and saved to {GOLD_DELTA_PATH}/master_orders")


def main():
    # spark = get_spark()

    run_master_orders(spark)


    # spark.read.format("delta").load(f"{GOLD_DELTA_PATH}/master_orders").show()
    # # Z-ORDER on high-cardinality join/filter columns for downstream query speed
    # from delta.tables import DeltaTable
    # master_path = f"{GOLD_DELTA_PATH}/master_orders"
    # if DeltaTable.isDeltaTable(spark, master_path):
    #     spark.sql(f"OPTIMIZE delta.`{master_path}` ZORDER BY (customer_id, product_id)")
    #     log.info("Z-ORDER optimized master_orders on customer_id, product_id")

    # log.info("Gold master orders pipeline completed successfully")



if __name__ == "__main__":
    main()