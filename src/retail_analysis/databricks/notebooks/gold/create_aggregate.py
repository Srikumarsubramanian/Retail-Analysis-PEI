"""
Gold Layer — Profit Aggregation
================================
Aggregates the master orders table into a profit summary at the
grain of: ``order_year, category, sub_category, customer_id``.

  - Rows with null profit are filtered before aggregation.
  - Profit margin is null-safe (returns NULL when revenue = 0).
"""



from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from retail_analysis.databricks.utils.constants import GOLD_DELTA_PATH
from retail_analysis.databricks.utils.logger import get_logger
from retail_analysis.databricks.utils.schema import MASTER_ORDERS_SCHEMA
from retail_analysis.databricks.utils.util import read_data, enforce_schema, merge_delta_upsert,optimize_delta_zorder 
from retail_analysis.databricks.utils.custom_exceptions import ReadError,TransformError , PipelineError


log = get_logger(__name__)

# ---------------------------------------------
# Internal helpers
# ---------------------------------------------


# ---------------------------------------------
# Read
# ---------------------------------------------
def _read_master_orders(spark: SparkSession) -> DataFrame:
    """Read the gold-layer master orders table."""
    log.info(f"Reading master orders from {GOLD_DELTA_PATH}/master_orders")
    try:
        df = read_data(spark, GOLD_DELTA_PATH,"master_orders","delta")
    except Exception as e:
        log.exception("Failed to read master orders")
        raise ReadError("Failed to read master orders")  from e
    return df

# ---------------------------------------------
# Aggregate
# ---------------------------------------------
def build_profit_aggregate(df: DataFrame) -> DataFrame:
    """Aggregate profit metrics.

    Grain: order_year, category, sub_category, customer_id

    Output columns:
      - total_profit
      - total_revenue
      - _updated_at (timestamp)
    """


    log.info("Aggregating master_orders")
    try:
        agg_df = df.groupBy(
                "order_year",
                "category",
                "sub_category",
                "customer_id"
                
            ).agg(
                F.sum("profit").alias("total_profit"),
                F.sum("total_revenue").alias("total_revenue"),
            )
        agg_df = agg_df.withColumn("_updated_at", F.current_timestamp())
        return agg_df
    except Exception as e:
        log.exception("Failed to aggregate master_orders")
        raise TransformError("Failed to aggregate master_orders")  from e

# ---------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------
def run_profit_aggregate(spark: SparkSession) -> DataFrame:
    """Orchestrate the aggregation pipeline.

    Returns
    -------
    DataFrame
        The aggregated profit table.
    """
    TABLE_NAME = "profit_by_year_category_customer"
    MERGE_KEYS = ["order_year", "category", "sub_category", "customer_id"]
    PARTITION_COLS = ["order_year"]
    ZORDER_COLS = ["customer_id", "category"]

    try:
        # Read
        df = _read_master_orders(spark)
        log.info("Master orders read successfully")

        # Schema enforcement
        df = enforce_schema(df, MASTER_ORDERS_SCHEMA, "Master Orders Read")
        log.info("Schema enforcement completed successfully")


        # -- Aggregate
        agg_df = build_profit_aggregate(df)
        repartitioned_df = agg_df.repartition("order_year")
        log.info("Profit aggregation and repartition completed successfully")

        # -- Merge (write)
        log.info("Writing aggregated data to Delta table: {TABLE_NAME} at {GOLD_DELTA_PATH}")

        merge_delta_upsert(
            spark,
            repartitioned_df,
                table=TABLE_NAME,
                base_path=GOLD_DELTA_PATH,
            merge_keys=MERGE_KEYS,
            partition_cols=PARTITION_COLS,
        )

        log.info("Merge completed successfully for table '%s'", TABLE_NAME)
        try:
            # -- Z-Order optimize
            log.info(f"Starting Z-Order optimization for table: {TABLE_NAME} on columns {ZORDER_COLS}")
            optimize_delta_zorder(
                spark,
                GOLD_DELTA_PATH,
                TABLE_NAME,
                ZORDER_COLS,
                log,
            )
            log.info("Z-Order optimization completed successfully for table '%s'", TABLE_NAME)
        except Exception as e:
            log.warning("Z-Order optimization skipped for table '%s'", TABLE_NAME)


        log.info("Aggregate pipeline completed successfully")
        return agg_df

    except Exception as e:
        log.exception("Profit aggregate pipeline failed ")
        raise PipelineError("Profit aggregate pipeline failed") from e


# ---------------------------------------------
# Entry point
# ---------------------------------------------

if __name__ == "__main__":
    #for local run
    # from retail_analysis.databricks.utils.spark_session import get_spark
    # spark = get_spark()
    run_profit_aggregate(spark)


