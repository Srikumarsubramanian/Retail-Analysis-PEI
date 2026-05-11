"""
Gold Layer — Reporting KPIs
============================
Builds multiple reporting views from the pre-computed aggregate table.

"""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.databricks.utils.constants import GOLD_DELTA_PATH
from src.databricks.utils.logger import get_logger
from src.databricks.utils.schema import PROFIT_AGGREGATE_SCHEMA
from src.databricks.utils.util import read_data, enforce_schema

log = get_logger(__name__)


# ─────────────────────────────────────────────
# Read
# ─────────────────────────────────────────────
def _read_aggregate_table(spark: SparkSession) -> DataFrame:
    """Read the pre-computed aggregate table used by reporting views."""
    df = read_data(spark, GOLD_DELTA_PATH,"profit_by_year_category_customer", "delta")
    return enforce_schema(df, PROFIT_AGGREGATE_SCHEMA, "Profit Aggregate Read")

def show_kpis(spark: SparkSession):
    # ─────────────────────────────────────────────
    # 1. Profit by Year
    # Grain: 1 row per order_year
    # ─────────────────────────────────────────────
    log.info("Calculating Profit by Year")
    spark.sql("""
        SELECT
            order_year,
            ROUND(SUM(total_profit),  2) AS profit
        FROM profit_aggregate
        GROUP BY order_year

    """).show()

    # ─────────────────────────────────────────────
    # 2. Profit by Year + Product Category
    # Grain: 1 row per (order_year, category)
    # ─────────────────────────────────────────────
    log.info("Calculating Profit by Year + Product Category")
    spark.sql("""
        SELECT
            order_year,
            category,
            ROUND(SUM(total_profit),  2) AS profit
        FROM profit_aggregate
        GROUP BY order_year, category
    """).show()

    # ─────────────────────────────────────────────
    # 3. Profit by Customer
    # Grain: 1 row per customer_id
    # ─────────────────────────────────────────────
    log.info("Calculating Profit by Customer")
    spark.sql("""
        SELECT
            customer_id,
            ROUND(SUM(total_profit),  2)  AS profit
        FROM profit_aggregate
        GROUP BY customer_id
    """).show()

    # ─────────────────────────────────────────────
    # 4. Profit by Customer + Year
    # Grain: 1 row per (customer_id, order_year)
    # ─────────────────────────────────────────────
    
    spark.sql("""
        SELECT
            customer_id,
            order_year,
            ROUND(SUM(total_profit),  2)  AS profit
        FROM profit_aggregate
        GROUP BY customer_id, order_year
    """).show()


def run_reporting(spark: SparkSession) -> Dict[str, DataFrame]:
    """Orchestrate report creation.

    Parameters
    ----------
    spark : SparkSession
        Required for reading the aggregate table.
    """
    log.info("Running KPI calculations")
    agg = _read_aggregate_table(spark).cache()
    agg.createOrReplaceTempView("profit_aggregate")
    log.info("Loaded aggregate source table")

    show_kpis(spark)
    agg.unpersist()
    log.info("Reporting pipeline completed successfully")
    
    return 


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
def main() -> Dict[str, DataFrame]:
    """Standalone entry point."""
    return run_reporting(spark)


if __name__ == "__main__":
    main()