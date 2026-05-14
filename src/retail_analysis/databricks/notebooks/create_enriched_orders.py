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


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


from retail_analysis.databricks.utils.constants import (
    BRONZE_DELTA_PATH,
    SILVER_DELTA_PATH,
    GOLD_DELTA_PATH,
)
from retail_analysis.databricks.utils.logger import get_logger
from retail_analysis.databricks.utils.schema import ORDERS_SCHEMA, CUSTOMER_SCHEMA, PRODUCTS_SCHEMA
from retail_analysis.databricks.utils.util import read_data, merge_delta_upsert, enforce_schema, optimize_delta_zorder
from retail_analysis.databricks.utils.transforms import round_currency
from retail_analysis.databricks.utils.custom_exceptions import PipelineError, ReadError, WriteError, TransformError

log = get_logger(__name__)

DATE_FMT = "d/M/yyyy"




# ---------------------------------------------
# Internal helpers
# ---------------------------------------------


# ---------------------------------------------
# Read
# ---------------------------------------------
def _read_sources(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Read bronze orders and silver dimension tables.

    Future Actions
    -----------
    filter at source once we get last_update timestamps for each file at source
    """
    try:
        log.info("Reading bronze orders and silver dimension tables")
        orders    = read_data(spark, f"{BRONZE_DELTA_PATH}","orders",   format="delta")
        orders    = enforce_schema(orders, ORDERS_SCHEMA, "Orders Bronze")
        customers = read_data(spark, f"{SILVER_DELTA_PATH}","customers", format="delta")
        customers = enforce_schema(customers, CUSTOMER_SCHEMA, "Customers Silver")
        products  = read_data(spark, f"{SILVER_DELTA_PATH}","products", format="delta")
        products  = enforce_schema(products, PRODUCTS_SCHEMA, "Products Silver")
        log.info("Successfully read bronze orders and silver dimension tables")
        return orders, customers, products
    except Exception as e:
        log.exception("Error reading bronze orders and silver dimension tables")
        raise ReadError("Error reading bronze orders and silver dimension tables") from e


# ---------------------------------------------
# Prepare data
# ---------------------------------------------

def _prepare_orders(orders: DataFrame) -> DataFrame:
    """Parse date strings and round profit."""
    try:
        GROUPING_KEYS = [
        "order_id", "order_date", "ship_date",
        "ship_mode", "customer_id", "product_id", "discount",
    ]
        return (
            orders
            .withColumn("order_date", F.to_date(F.col("order_date"), DATE_FMT))
            .withColumn("ship_date",  F.to_date(F.col("ship_date"),  DATE_FMT))
            .groupBy(GROUPING_KEYS)
            .agg(
                F.sum("quantity").alias("quantity"),
                F.sum("price").alias("price"),
                round_currency(F.sum("profit"), 2).alias("profit"),
            )
        )
    except Exception as e:
        log.exception("Error preparing orders")
        raise TransformError("Error preparing orders") from e

def _prepare_customers(customers: DataFrame) -> DataFrame:
    try:
        return customers.select(
            F.col("customer_id"),
            F.col("customer_name"),
            F.col("country")
        )
    except Exception as e:
        log.exception("Error preparing customers")
        raise TransformError("Error preparing customers") from e


def _prepare_products(products: DataFrame) -> DataFrame:
    try:
        return products.select(
            F.col("product_id"),
            F.col("category"),
            F.col("sub_category")
        )
    except Exception as e:
        log.exception("Error preparing products")
        raise TransformError("Error preparing products") from e


# ---------------------------------------------
# Build master table
# ---------------------------------------------

def build_master_orders(
    spark: SparkSession,
    orders: DataFrame,
    customers: DataFrame,
    products: DataFrame
) -> DataFrame:
    """Build the enriched master orders table."""
    try:
        # Keep the aliases explicit so downstream column references are unambiguous.
        o = _prepare_orders(orders).alias("o")
        c = F.broadcast(_prepare_customers(customers)).alias("c")
        p = F.broadcast(_prepare_products(products)).alias("p")

        enriched = (
            o.join(c, F.col("o.customer_id") == F.col("c.customer_id"), how="left")
             .join(p, F.col("o.product_id") == F.col("p.product_id"), how="left")
             .withColumn("order_year", F.year(F.col("o.order_date")))
             .withColumn("order_month", F.month(F.col("o.order_date")))
             .withColumn("days_to_ship", F.datediff(F.col("o.ship_date"), F.col("o.order_date")))
             .withColumn("total_revenue", F.round(F.col("o.price") * (1 - F.col("o.discount")), 2))
             .withColumn("_updated_at", F.current_timestamp())
        )

        master = enriched.select(
            # Order identifiers and logistics
            F.col("o.order_id").alias("order_id"),
            F.col("o.order_date").alias("order_date"),
            F.col("o.ship_date").alias("ship_date"),
            F.col("o.ship_mode").alias("ship_mode"),
            F.col("order_year"),
            F.col("order_month"),
            F.col("days_to_ship"),
            # Customer dimension
            F.col("o.customer_id").alias("customer_id"),
            F.col("c.customer_name").alias("customer_name"),
            F.col("c.country").alias("country"),
            # Product dimension
            F.col("o.product_id").alias("product_id"),

            F.col("p.category").alias("category"),
            F.col("p.sub_category").alias("sub_category"),
            # Measures
            F.col("o.quantity").alias("quantity"),
            F.col("o.price").alias("order_price"),
            F.col("o.discount").alias("discount"),
            F.col("o.profit").alias("profit"),
            F.col("total_revenue"),
            # Metadata
            F.col("_updated_at"),
        )

        return master
    except Exception as e:
        log.exception("Error building master orders table")
        raise TransformError("Error building master orders table") from e


# ---------------------------------------------
# Orchestration
# ---------------------------------------------
def run_master_orders(spark: SparkSession) -> DataFrame:
    """Build and persist the master orders table."""
    log.info("Building master orders table")
    try:
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
        
        # Optimize with Z-Order for downstream aggregation reads
        optimize_delta_zorder(
            spark,
            GOLD_DELTA_PATH,
            "master_orders",
            ["customer_id", "product_id"],
            log
        )
        
        log.info(f"Successfully built master orders table and saved to {GOLD_DELTA_PATH}/master_orders")
        return master_orders
    except Exception as e:
        log.exception(f"Error building master orders table")
        raise PipelineError("Error building master orders table") from e


# ---------------------------------------------
# Entry point
# ---------------------------------------------

if __name__ == "__main__":
    #for local run
    # from retail_analysis.databricks.utils.spark_session import get_spark
    # spark = get_spark()
    run_master_orders(spark)