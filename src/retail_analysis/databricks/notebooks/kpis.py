"""
Gold Layer — Reporting KPIs
============================
Builds multiple reporting views from the pre-computed aggregate table.

"""



from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retail_analysis.databricks.utils.constants import GOLD_DELTA_PATH
from retail_analysis.databricks.utils.logger import get_logger
from retail_analysis.databricks.utils.schema import PROFIT_AGGREGATE_SCHEMA
from retail_analysis.databricks.utils.util import read_data, enforce_schema
from retail_analysis.databricks.utils.custom_exceptions import ReportingError, KPIQueryError, ReadError, PipelineError

log = get_logger(__name__)



# ---------------------------------------------
# Internal helpers
# ---------------------------------------------

# ---------------------------------------------
# Read
# ---------------------------------------------
def _read_aggregate_table(spark: SparkSession) -> DataFrame:
    """Read the pre-computed aggregate table used by reporting views."""
    try:
        df = read_data(spark, GOLD_DELTA_PATH,"profit_by_year_category_customer", "delta")
        return enforce_schema(df, PROFIT_AGGREGATE_SCHEMA, "Profit Aggregate Read")
    except Exception as e:
        log.exception("Error reading aggregate table")
        raise ReadError("Error reading aggregate table") from e


# ---------------------------------------------
# KPI Calculations
# ---------------------------------------------
def build_kpis(spark: SparkSession):
    # ---------------------------------------------
    # 1. Profit by Year
    # Grain: 1 row per order_year
    # ---------------------------------------------
    try:
        log.info("Calculating Profit by Year")
        spark.sql("""
            SELECT
                order_year,
                ROUND(SUM(total_profit),  2) AS profit
            FROM profit_aggregate
            GROUP BY order_year

        """).show()

        # ---------------------------------------------
        # 2. Profit by Year + Product Category
        # Grain: 1 row per (order_year, category)
        # ---------------------------------------------
        log.info("Calculating Profit by Year + Product Category")
        spark.sql("""
            SELECT
                order_year,
                category,
                ROUND(SUM(total_profit),  2) AS profit
            FROM profit_aggregate
            GROUP BY order_year, category
        """).show()

        # ---------------------------------------------
        # 3. Profit by Customer
        # Grain: 1 row per customer_id
        # ---------------------------------------------
        log.info("Calculating Profit by Customer")
        spark.sql("""
            SELECT
                customer_id,
                ROUND(SUM(total_profit),  2)  AS profit
            FROM profit_aggregate
            GROUP BY customer_id
        """).show()

        # ---------------------------------------------
        # 4. Profit by Customer + Year
        # Grain: 1 row per (customer_id, order_year)
        # ---------------------------------------------
        log.info("Calculating Profit by Customer and Year")
        spark.sql("""
            SELECT
                customer_id,
                order_year,
                ROUND(SUM(total_profit),  2)  AS profit
            FROM profit_aggregate
            GROUP BY customer_id, order_year
        """).show()
    except Exception as e:
        log.exception("Error calculating KPIs")
        raise KPIQueryError("Error calculating KPIs") from e



# ---------------------------------------------
# Orchestration
# ---------------------------------------------
def run_reporting(spark: SparkSession) -> Dict[str, DataFrame]:
    """Orchestrate report creation.

    Parameters
    ----------
    spark : SparkSession
        Required for reading the aggregate table.
    """
    try:
        agg = None
        log.info("Running KPI calculations")
        agg = _read_aggregate_table(spark).cache()
        agg.createOrReplaceTempView("profit_aggregate")
        log.info("Loaded aggregate source table")

        build_kpis(spark)
        log.info("Reporting pipeline completed successfully")
        return True
    except Exception as e:
        log.exception("Reporting pipeline failed")
        raise ReportingError("Reporting pipeline failed") from e
    finally:
        if agg is not None:
            try:
                agg.unpersist()
            except Exception as e:
                log.exception("Failed to unpersist aggregate DataFrame", exc_info=True)

    


# ---------------------------------------------
# Entry point
# ---------------------------------------------

if __name__ == "__main__":
    ##for local run
    # from retail_analysis.databricks.utils.spark_session import get_spark
    # spark = get_spark()
    run_reporting(spark)
