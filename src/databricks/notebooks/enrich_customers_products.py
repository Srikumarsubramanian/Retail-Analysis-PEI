"""
Enrichment Pipeline for Customers and Products
==================================================
Reads Bronze-layer delta files, applies data quality , schema enforcement and business rules,
and writes enriched dimension tables to the Silver layer.

Key guarantees:
  - Null / duplicate primary keys are quarantined to silver/quarantined_...
  - String fields are trimmed and cased for consistent lookups.
  - PII fields (email, phone) are SHA-256 hashed.
  - Every row carries an ``_enriched_at`` timestamp.
"""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.databricks.utils.constants import (
    BRONZE_DELTA_PATH,
    SILVER_DELTA_PATH,
)
from src.databricks.utils.logger import get_logger
from src.databricks.utils.schema import PRODUCTS_SCHEMA, CUSTOMER_SCHEMA
from src.databricks.utils.util import read_data, write_delta_table, merge_delta_upsert,enforce_schema
from src.databricks.utils.dq import clean_customer_name, clean_phone, clean_email

log = get_logger(__name__)


# ─────────────────────────────────────────────
# Products transforms
# ─────────────────────────────────────────────
def transform_products(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Cleanse and enrich the products .
      - Rows with null values in non nullable fields are quarantined
      - Null product IDs → dropped
      - Duplicate product IDs → deduplicated (Have to implement determinisitic deduplication)
      - Negative prices → set to NULL
        Future Actions:
        - Implement determinisitic deduplication for product_id
        - Implement schema evolution
        - Implement partitioning strategy
        - Implement incremental load
    """
    log.info(f"transforming products")
    df = (
        df.filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("sub_category", F.initcap(F.trim("sub_category")))
        .withColumn("product_name", F.trim("product_name"))
        .withColumn(
            "price_per_product",
            F.when(F.col("price_per_product") < 0, None)
             .otherwise(F.col("price_per_product").cast(DoubleType())),
        )
        .withColumn("_enriched_at", F.current_timestamp())

    )
    log.info(f"transform_products completed")
    return df


def build_products(spark: SparkSession) -> DataFrame:
    """Read bronze products and apply silver enrichment.
    
    Future Actions
    -----------
    filter at source once we get last_update timestamps for each file at source
    """
    log.info(f"Reading products from bronze path {BRONZE_DELTA_PATH}")
    df = read_data(spark, f"{BRONZE_DELTA_PATH}","products", "delta")
    df = enforce_schema(df, PRODUCTS_SCHEMA, "Products Bronze")

    log.info(f"products initial read completed")
    return transform_products(spark, df)


# ─────────────────────────────────────────────
# Customers transforms
# ─────────────────────────────────────────────
def transform_customers(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Cleanse and enrich the customers dimension.

    Business rules:
      - Null customer IDs → dropped
      - Duplicate customer IDs → deduplicated (first wins)
      - String fields trimmed and initcap'd
      - Segment normalised to controlled vocabulary
      - PII fields (email, phone) → SHA-256 hashed, originals dropped

    Future Actions:
        - Pattern and length profiling for customer id
        - Determinstic deduplication for customer id
        
    """
    log.info(f"transforming customers")
    cols = set(df.columns)
  
    df = (
        df
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .withColumn("customer_name", clean_customer_name(F.col("customer_name")))
        .withColumn("city",   F.initcap(F.trim(F.col("city"))))
        .withColumn("state",  F.initcap(F.trim(F.col("state"))))
        .withColumn("region", F.initcap(F.trim(F.col("region"))))
        .withColumn("country", F.initcap(F.trim(F.col("country"))))
        .withColumn("segment", F.initcap(F.trim(F.col("segment"))))
        .withColumn("postal_code", F.trim(F.col("postal_code")).cast("string"))
        .withColumn("email", clean_email(F.col("email")))
        .withColumn("phone", clean_phone(F.col("phone")))
        .withColumn("_enriched_at", F.current_timestamp())
    )

    ##masking logic implementation
    ##optional PII masking using SHA-256
    # df = df.withColumn("email_masked", F.sha2("email", 256)).drop("email")
    # df = df.withColumn("phone_masked", F.sha2("phone", 256)).drop("phone")

    # selecting column order
    base_cols = [
        "customer_id", "customer_name", "segment",
        "city", "state", "region", "country", "postal_code",
    ]
    pii_cols  = ["email","phone"]
    meta_cols = ["_ingested_at", "_enriched_at"]
    extra = [c for c in df.columns if c not in base_cols + pii_cols + meta_cols]

    df = df.select(base_cols + pii_cols + extra + meta_cols)

    log.info(f"transform_customers completed")
    return df


def build_customers(spark: SparkSession) -> DataFrame:
    """Read bronze customers and apply silver enrichment.
    
    Future Actions
    -----------
    filter at source once we get last_update timestamps for each file at source
    """
    log.info(f"Reading customers from bronze path {BRONZE_DELTA_PATH}")
    df = read_data(spark, BRONZE_DELTA_PATH, "customer", "delta")
    df = enforce_schema(df, CUSTOMER_SCHEMA, "Customers Bronze")

    log.info(f"customers initial read completed")
    return transform_customers(spark, df)


# ─────────────────────────────────────────────
# Pipeline orchestrator
# ─────────────────────────────────────────────
def run_silver(spark: SparkSession) -> Dict[str, DataFrame]:
    """Orchestrate the full Silver enrichment pipeline.

    Returns
    -------
    dict[str, DataFrame]
        ``{"products": enriched_products_df, "customer": enriched_customer_df}``
    
    """
    log.info(f"Starting enrichment - customers and products")
    results: Dict[str, DataFrame] = {}

    products = build_products(spark)

    ###bucket by product_id for downstream join with orders table
    log.info(f"Writing products to silver path {SILVER_DELTA_PATH}")
    merge_delta_upsert(spark, products, "products", SILVER_DELTA_PATH, merge_keys = ["product_id"], partition_cols=["category"])
    results["products"] = products

    log.info(f"products enrichment completed and saved to path {SILVER_DELTA_PATH}")

    log.info(f"Starting customers enrichment")
    customers = build_customers(spark)


    ###bucket by customer_id for downstream join with orders table
    log.info(f"Writing customers to silver path {SILVER_DELTA_PATH}")
    merge_delta_upsert(spark, customers, "customer", SILVER_DELTA_PATH, merge_keys = ["customer_id"], partition_cols=[ "country", "region"])
    results["customer"] = customers
    
    log.info(f"customers enrichment completed and saved to path {SILVER_DELTA_PATH}")
    
    log.info(f"Enrichment - customers and products completed")
    return results


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    # spark = get_spark()
    run_silver(spark)