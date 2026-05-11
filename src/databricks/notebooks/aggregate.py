"""
Gold Layer — Profit Aggregation
================================
Aggregates the master orders table into a profit summary at the
grain of: ``order_year, category, sub_category, customer_id``.

  - Rows with null profit are filtered before aggregation.
  - Profit margin is null-safe (returns NULL when revenue = 0).
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from src.databricks.utils.constants import GOLD_DELTA_PATH
from src.databricks.utils.logger import get_logger
from src.databricks.utils.schema import MASTER_ORDERS_SCHEMA
from src.databricks.utils.util import read_data, enforce_schema, merge_delta_upsert

log = get_logger(__name__)


# ─────────────────────────────────────────────
# Read
# ─────────────────────────────────────────────
def _read_master_orders(spark: SparkSession) -> DataFrame:
    """Read the gold-layer master orders table."""
    log.info(f"Reading master orders from {GOLD_DELTA_PATH}/master_orders")
    df = read_data(spark, GOLD_DELTA_PATH,"master_orders","delta")
    return df
# ─────────────────────────────────────────────
# Filter
# ─────────────────────────────────────────────
def _filter_valid_orders(df: DataFrame) -> DataFrame:
    """Remove rows where profit is null."""
    return df.filter(F.col("profit").isNotNull())


# ─────────────────────────────────────────────
# Aggregate
# ─────────────────────────────────────────────
def build_profit_aggregate(df: DataFrame) -> DataFrame:
    """Aggregate profit metrics.

    Grain: order_year, category, sub_category, customer_id

    Output columns:
      - total_profit
      - total_revenue
      - _computed_at (timestamp)
    """

    log.info("Aggregating master_orders")
    agg_df = df.groupBy(
            "order_year",
            "category",
            "sub_category",
            "customer_id", 
            "customer_name",
            "country",
            "region",
        ).agg(
            F.round(F.sum("profit"), 2).alias("total_profit"),
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
        )
    agg_df = agg_df.withColumn("_updated_at", F.current_timestamp())
    return agg_df


# ─────────────────────────────────────────────
# Pipeline orchestrator
# ─────────────────────────────────────────────
def run_profit_aggregate(spark: SparkSession) -> DataFrame:
    """Orchestrate the aggregation pipeline.

    Returns
    -------
    DataFrame
        The aggregated profit table.
    """

    df = _read_master_orders(spark)
    df = enforce_schema(df, MASTER_ORDERS_SCHEMA, "Master Orders Read")
    df = _filter_valid_orders(df)
    agg_df = build_profit_aggregate(df)


    merge_delta_upsert(
        spark,
        agg_df,
        table="profit_by_year_category_customer",
        base_path=GOLD_DELTA_PATH,
        merge_keys=["order_year", "category", "sub_category", "customer_id"],
        partition_cols=["order_year"],
    )
    log.info(f"Aggregated data written to gold layer: {GOLD_DELTA_PATH}/profit_by_year_category_customer")
    # Z-ORDER on high-cardinality filter columns for KPI queries
    from delta.tables import DeltaTable
    agg_path = f"{GOLD_DELTA_PATH}/profit_by_year_category_customer"
    if DeltaTable.isDeltaTable(spark, agg_path):
        spark.sql(f"OPTIMIZE delta.`{agg_path}` ZORDER BY (customer_id, category)")
        log.info("Z-ORDER optimized aggregate on customer_id, category")

    log.info("Aggregate pipeline completed successfully")
    return agg_df


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
def main() -> DataFrame:
    """Standalone entry point."""
    # spark = get_spark()
    return run_profit_aggregate(spark)


if __name__ == "__main__":
    main()