"""
Ingestion Pipeline
=========================
Reads raw source files (CSV, JSON, Excel) and persists them as
normalised Delta tables in the Bronze layer.

Key guarantees:
  - Column names are lowercased and sanitised to be compatible with delta file naming standards.
  - Every row carries an ``_ingested_at`` timestamp.
  - Failures on individual files are logged and re-raised with context.
"""


from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from retail_analysis.databricks.utils.constants import (
    SOURCE_BASE_PATH,
    BRONZE_DELTA_PATH,
)
from retail_analysis.databricks.utils.custom_exceptions import ConfigError, TransformError , WriteError, ReadError, PipelineError
from retail_analysis.databricks.utils.logger import get_logger
from retail_analysis.databricks.utils.transforms import normalise_columns, add_ingestion_metadata
from retail_analysis.databricks.utils.util import (
    ensure_files_exist,
    load_and_validate_config,
    read_data,
    write_delta_table,
)

log = get_logger(__name__)


# ---------------------------------------------
# Internal helpers
# ---------------------------------------------
def _read_bronze_source(
    spark: SparkSession,
    source_path: str,
    file_name: str,
) -> DataFrame:
    """Read a single source file with format-appropriate options."""
    try:
        file_ext = file_name.rsplit(".", 1)[-1].lower()

        options: dict = {}
        if file_ext == "csv":
            options["header"] = "true"
            options["escape"]= '"'

        
        elif file_ext == "json":
            options["multiLine"] = "true"
            options["dateFormat"] = "d/M/yyyy"

        df_raw = read_data(spark, source_path, file_name, format=file_ext, **options)

        return df_raw
    
    except Exception as e:
        log.exception(f"Reading raw bronze file {file_name} failed")
        raise ReadError(f"Reading raw bronze file {file_name} failed") from e


def _transform_bronze(df: DataFrame) -> DataFrame:
    """Apply Bronze-layer standardisation (column names + metadata)."""
    try:
        df = normalise_columns(df)
        df = add_ingestion_metadata(df)
        return df
    except Exception as e:
        log.exception("Failed to transform data in Bronze layer")
        raise TransformError("Failed to transform data in Bronze layer") from e


def _write_bronze(
    spark: SparkSession,
    df: DataFrame,
    file_name: str,
    bronze_path: str,
    mode: str = "overwrite",
    partition_cols: List[str] = None,
) -> None:

    """
    Writes a DataFrame to the Bronze Delta layer.
    Mode is set to overwrite by default.
    """
    try:
        
        file_name_without_extension= file_name.rsplit(".", 1)[0]
        options = {"overwriteSchema": "true"} if mode == "overwrite" else {}
        write_delta_table(
            spark,
            df,
            file_name=file_name_without_extension,
            base_path=bronze_path,
            mode=mode,
            partition_by=partition_cols,
            **options
        )
    
    except Exception as e:
        log.exception(f"Writing bronze file {file_name} failed")
        raise WriteError(f"Writing bronze file {file_name} failed") from e




# ---------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------
def run_bronze(
    spark: SparkSession,
    config_path: str = "src/retail_analysis/configs/etl.yml",

) -> Dict[str, DataFrame]:
    """Orchestrate the full Bronze ingestion pipeline.

    Parameters
    ----------
    spark : SparkSession
    config_path : str
        Path to the YAML config file that has Source details to be ingested.

    Returns
    -------
    dict[str, DataFrame]
        Mapping of ``file_name → ingested DataFrame``.

    Raises
    ------
    ConfigError
        If the config is missing or the ``files`` key is empty.
    FileNotFoundError
        If any expected source files are absent on disk.
    RuntimeError
        If ingestion of any individual file fails.
    """
    # Load config
    config = load_and_validate_config(path=config_path)
    files: List[str] = config.get("files") or []
    if not files:
        log.critical("Missing or empty 'files' key in config")
        raise ConfigError("The 'files' key is missing or empty")



    # Validate file existence
    present, missing = ensure_files_exist(SOURCE_BASE_PATH, files)

    if missing:
        log.critical(
            "Missing required source files",
            extra={"missing": missing},
        )
        raise FileValidationError(f"Missing {len(missing)} file(s): {missing}")

    log.info("All source files verified", extra={"count": len(present)})



    # Ingest each file
    results: Dict[str, DataFrame] = {}

    for file_name in files:
        try:    
            log.info(f"Ingesting {file_name}")

            df = _read_bronze_source(spark, SOURCE_BASE_PATH, file_name)
            df = _transform_bronze(df)

            _write_bronze(spark, df, file_name, BRONZE_DELTA_PATH, mode="overwrite" )

            results[file_name] = df
            log.info(f"Ingestion completed: {file_name}")
        except Exception as e:
            log.exception(f"Ingestion failed: {file_name}")
            raise PipelineError(f"Pipeline failed on file: {file_name}") from e

    log.info(f"Bronze ingestion pipeline completed successfully. Total files ingested: {len(results)}")
    return results


# ---------------------------------------------
# Entry point
# ---------------------------------------------

if __name__ == "__main__":
    """Standalone entry point for notebook"""
    ##for local run
    # from retail_analysis.databricks.utils.spark_session import get_spark
    # spark = get_spark()
    run_bronze(spark)