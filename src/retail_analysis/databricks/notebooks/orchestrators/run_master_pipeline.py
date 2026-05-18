"""
End-to-end pipeline orchestrator.

Runs the full medallion pipeline:
  Bronze (ingestion) → Silver (enrichment) → Gold (master + aggregate) → Reporting (KPIs)


"""


import time
from typing import Any, Callable, Dict



from retail_analysis.databricks.utils.setup_logging.logger import get_logger
from retail_analysis.databricks.utils.setup_exceptions.custom_exceptions import PipelineError
log = get_logger(__name__)

def _run_stage(name: str, fn: Callable[..., Any], *args: Any) -> Any:
    """Run a pipeline stage with timing and error logging."""
  
    log.info(f"Starting: {name}")
    t0 = time.perf_counter()
    try:
        result = fn(*args)
        log.info(f"Completed: {name} ({time.perf_counter() - t0:.1f}s)")
        return result
    except Exception as e:
        log.exception(f"FAILED: {name} ({time.perf_counter() - t0:.1f}s)")
        raise PipelineError(f"Pipeline failed at: {name} due to \n {e}") from e


def run_pipeline(spark,config_path: str = "src\retail_analysis\templates\configs\etl.yml") -> Dict[str, Any]:
    """Execute the full medallion pipeline.

    Parameters
    ----------
    config_path : str
        Path to the ETL YAML config.

    Returns
    -------
    dict
        Results from each pipeline stage.
    """
    # Lazy imports avoid circular dependencies and keep module
    # load time fast when only a subset of stages is needed.
    try:
            
        from retail_analysis.databricks.notebooks.bronze.create_raw_tables import run_bronze
        from retail_analysis.databricks.notebooks.silver.create_enriched_customers_products import run_silver
        from retail_analysis.databricks.notebooks.silver.create_enriched_orders import run_master_orders
        from retail_analysis.databricks.notebooks.gold.create_aggregate import run_profit_aggregate
        from retail_analysis.databricks.notebooks.gold.create_sql_kpis import run_reporting


        results: Dict[str, Any] = {}

        results["bronze"] = _run_stage(
            "Bronze Ingestion",
            run_bronze,
            spark,
            config_path,
        )

        results["silver"] = _run_stage(
            "Silver Enrichment",
            run_silver,
            spark,
        )

        results["gold_master"] = _run_stage(
            "Gold — Master Orders",
            run_master_orders,
            spark
        )

        results["gold_aggregate"] = _run_stage(
            "Gold — Profit Aggregation",
            run_profit_aggregate,
            spark,
        )

        results["reporting"] = _run_stage(
            "Reporting KPIs",
            run_reporting,
            spark,
        )

        log.info("Master Pipeline completed successfully")
        return results
    except Exception as e:
        log.exception("Master Pipeline failed")
        raise PipelineError(f"Master Pipeline failed") from e


if __name__ == "__main__":
    ##for local run
    from retail_analysis.databricks.utils.util_funcs.spark_session import get_spark
    spark = get_spark()
    config_path = r"src/retail_analysis/templates/configs/etl.yml"
    run_pipeline(spark, config_path)