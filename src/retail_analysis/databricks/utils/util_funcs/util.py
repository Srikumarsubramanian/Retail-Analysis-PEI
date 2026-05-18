"""
Generic I/O and validation utilities.

This module provides data readers, writers, and pre-flight checks
that are format-agnostic. Transform logic has been moved to
``transforms.py``; use that module for column-level operations.
"""

from typing import List, Optional, Sequence, Tuple

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


from retail_analysis.databricks.utils.setup_exceptions.custom_exceptions import ConfigError, DataQualityError, WriteError ,ReadError , TransformError,PipelineError
from retail_analysis.databricks.utils.setup_logging.logger import get_logger
from retail_analysis.templates.constants import QUARANTINE_PATH

log = get_logger(__name__)

# Initialize global dbutils for module usage
# ---------------------------------------------
# Get dbutils
# ---------------------------------------------


def get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        return dbutils
    except Exception as e:
        log.error(f"Failed to get dbutils: {e}")
        return None
# from src.databricks.utils.util import get_dbutils

# dbutils = get_dbutils(spark)


# ---------------------------------------------
# Config
# ---------------------------------------------
def load_and_validate_config(path: str) -> dict:
    """Load a YAML config file and return the parsed dict.

    Raises
    ------
    ConfigError
        If the file is missing, unreadable, or contains invalid YAML.
    """
    try:
        with open(path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError as e:
        raise ConfigError(f"Config file not found: {path}") from e
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in {path}") from e
    except Exception as e:
        raise ConfigError(f"Failed to load config") from e

    if config is None:
        log.exception(f"Failed to load config from {path}")
        raise ConfigError(f"Config is empty: {path}")
    if not isinstance(config, dict):
        log.exception(f"Failed to load config from {path}")
        raise ConfigError(f"expected dict: found {type(config).__name__}: {path}")

    log.info(f"Config loaded successfully from {path}")
    return config


# ---------------------------------------------
# File Validation
# ---------------------------------------------
def ensure_files_exist(
    source_path: str,
    files: List[str],
    raise_on_missing: bool = False,
) -> Tuple[List[str], List[str]]:
    import os

    present, missing = [], []
    for file_name in files:
        full_path = os.path.join(source_path, file_name)
        if os.path.isfile(full_path):
            present.append(file_name)
            log.debug("Source file found", extra={"file": file_name, "path": full_path})
        else:
            missing.append(file_name)
            log.warning("Source file MISSING", extra={"file": file_name, "expected_path": full_path})

    if missing and raise_on_missing:
        raise FileNotFoundError(
            f"{len(missing)} file(s) missing:\n" + "\n".join(missing)
        )

    return present, missing

# def ensure_files_exist(source_path: str, files: list[str], raise_on_missing: bool = False):
#     """
#     Check that every expected source file exists on disk.

#     Parameters
#     ----------
#     source_path : str
#         Directory containing source files.
#     files : list[str]
#         Expected file names.
#     raise_on_missing : bool
#         If ``True``, raise ``FileNotFoundError`` when any file is absent.

#     Returns
#     -------
#     (present, missing)
#         Lists of file names.
# """
    
#     present, missing = [], []

#     try:
#         existing_files = {f.name for f in dbutils.fs.ls(source_path)}
#     except Exception:
#         existing_files = set()

#     for file_name in files:
#         if file_name in existing_files:
#             present.append(file_name)
#         else:
#             missing.append(file_name)

#     if missing and raise_on_missing:
#         raise FileNotFoundError(f"{len(missing)} file(s) missing:\n" + "\n".join(missing))

#     return present, missing



# ---------------------------------------------
# Read
# ---------------------------------------------
def read_data(
    spark: SparkSession,
    path: str,
    file_name: str,
    format: str,
    schema: Optional[StructType] = None,
    mode: str = "PERMISSIVE",
    **options,
) -> DataFrame:
    """Read data into a Spark DataFrame.

    Supports CSV, JSON, Parquet, Delta, and Excel (via crealytics spark-excel).

    Parameters
    ----------
    spark : SparkSession
    path : str
        File or directory path to read.
    format : str
        File format (csv, json, parquet, delta, xlsx/excel/xls).
    schema : StructType, optional
        If provided, enforces this schema on read instead of relying on
        type inference.  Catches schema drift at ingestion time.
    mode : str, optional
    In PERMISSIVE mode corrupt records are quarantined to  quarantine folder.
        Ignored for Excel, Parquet, and Delta formats.
    **options
        Additional Spark reader options forwarded to the underlying reader.

    Returns
    -------
    DataFrame
    """
    fmt = format.lower()
    source_file_path = f"{path}/{file_name}"
    try:

        # -- Excel: no permissive / quarantine support --
        if fmt in ("xlsx", "excel", "xls"):
            reader = spark.read.format("com.crealytics.spark.excel").option("header", "true")
            for key, value in options.items():
                reader = reader.option(key, value)
            return reader.load(source_file_path)

        # -- CSV / JSON / Parquet / Delta --
        reader = spark.read.format(fmt)
        if schema:
            reader = reader.schema(schema)
        if fmt in ("csv", "json"):
            reader = reader.option("mode", mode)
        if fmt == "json":
            reader = reader.option("multiline", "true")
            reader = reader.option("primitivesAsString", "true")

        # Enable corrupt-record capture for CSV/JSON in PERMISSIVE mode
        is_permissive = fmt in ("csv", "json") and mode.upper() == "PERMISSIVE"
        if is_permissive:
            reader = reader.option("mode", "PERMISSIVE")
            reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")

        for key, value in options.items():
            reader = reader.option(key, value)

        df_raw = reader.load(source_file_path)

        # -- Quarantine corrupt rows (CSV / JSON permissive only) --
        if is_permissive and "_corrupt_record" in df_raw.columns:
            df_bad = df_raw.filter(F.col("_corrupt_record").isNotNull())
            df_good = df_raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

            

            if df_bad.take(1):
                quarantine_file_path = f"{QUARANTINE_PATH}/{file_name}"
                df_bad.write.mode("append").format('fmt').save(quarantine_file_path)

                log.warning(
                    f"Corrupt records quarantined from {source_file_path} to {quarantine_file_path}"
                )

            return df_good

        return df_raw
    except Exception as e:
        log.exception(f"Failed to read data from {source_file_path}")
        raise ReadError(f"Failed to read data from {source_file_path}") from e



def enforce_schema(
    df: DataFrame,
    expected_schema: StructType,
    label: str,
) -> DataFrame:
    """Enforce a strict schema, ignoring extra columns, and quarantine bad rows.

    Validates that all expected columns exist. Extra columns are ignored.
    Casts columns to expected types.
    Checks for nulls in columns marked as non-nullable (nullable=False) in the schema.
    If any non-nullable column is null, the row is written to a quarantine table
    in the Silver layer instead of failing the pipeline.

    Raises
    ------
    DataQualityError
        If any required column is missing from the DataFrame.
    """
    from retail_analysis.templates.constants import SILVER_DELTA_PATH
    
    actual_cols = set(df.columns)
    missing = [f.name for f in expected_schema.fields if f.name not in actual_cols]
    
    if missing:
        raise DataQualityError(
            f"{label} is missing {len(missing)} expected column(s): {missing}. "
            f"Actual columns: {sorted(actual_cols)}"
        )
        
    # 1. Select and cast ONLY expected columns + metadata (e.g., _ingested_at)
    select_exprs = [
        F.col(f.name).cast(f.dataType).alias(f.name)
        for f in expected_schema.fields
    ]
    
        
    df_cast = df.select(*select_exprs)
    
    # 2. Check for non-nullable columns being null
    non_nullable_cols = [f.name for f in expected_schema.fields if not f.nullable]
    
    if non_nullable_cols:
        # Build condition: A row is BAD if ANY non_nullable_col is null
        cond = F.col(non_nullable_cols[0]).isNull()
        for c in non_nullable_cols[1:]:
            cond = cond | F.col(c).isNull()
            
        bad_rows = df_cast.filter(cond)
        good_rows = df_cast.filter(~cond)

        quarantine_path  = f"{QUARANTINE_PATH}/{label.replace(' ', '_').lower()}"
            
            # Add quarantine metadata and write to quarantine table
        
        bad_rows\
            .withColumn("_quarantined_at", F.current_timestamp())\
            .write\
            .format("delta")\
            .mode("append")\
            .save(quarantine_path)
        
        return good_rows
        
    return df_cast




# ---------------------------------------------
# Write
# ---------------------------------------------
def write_delta_table(
    spark: SparkSession,
    df: DataFrame,
    file_name: str,
    base_path: str,
    mode: str = "error",
    partition_by: Optional[List[str]] = None,
    **options,
) -> None:
    """Persist a DataFrame as a Delta table.

    Parameters
    ----------
    spark : SparkSession
    df : DataFrame
    file_name : str
        file_name (becomes a subdirectory of *base_path*).
    base_path : str
        Root directory for this layer's Delta tables.
    mode : str
        Spark write mode (``append``, ``overwrite``, ``error``).
    partition_by : list[str], optional
        Columns to partition by.  Enables predicate push-down on common
        filter columns (e.g. ``order_year``) for large datasets.
    **options : kwargs
        Additional options to pass to the Spark writer.

    Raises
    ------
    WriteError
        If the write operation fails.
    """
    target_path = f"{base_path}/{file_name}"
    try:
        writer = (
            df.write
            .format("delta")
            .mode(mode)
            .options(**options)
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)
    except Exception as exc:
        log.exception(f"Delta write of {file_name} failed to {target_path} with mode {mode}")
        raise WriteError(f"Failed to write table '{file_name}'") from exc

    log.info(
        f"Delta write of {file_name} completed to {target_path} with mode {mode}",
    )



def merge_delta_upsert(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    base_path: str,
    merge_keys: List[str],
    partition_cols: Optional[List[str]] = None,
    incremental_filter: Optional[str] = None,   # e.g. "order_year >= 2024"
    enable_schema_evolution: bool = False,
    deduplicate_source: bool = False,
   **options,
) -> None:
    """Upsert into a Delta table: update matching rows, insert new ones. \
    Falls back to an overwrite-create if no Delta table exists at the path.

    Parameters
    ----------
    spark : SparkSession
    df : DataFrame
    table : str
        The name of the Delta table to upsert into.
    base_path : str
        The base path for the Delta table.
    merge_keys : list[str]
        The keys to use for merging.
    partition_cols : list[str], optional
        Columns to partition by.
    incremental_filter : str, optional
        Incremental filter to apply to the source DataFrame.
    enable_schema_evolution : bool, optional
        Enable schema evolution.
    deduplicate_source : bool, optional
        Deduplicate the source DataFrame.
    **options : kwargs
        Additional options to pass to the Spark writer.

    Raises
    ------
    WriteError
        If the merge operation fails.
    """
    from delta.tables import DeltaTable

    if not merge_keys:
        raise WriteError("merge_keys must contain at least one column")

    path = f"{base_path}/{table}"

    try:
        # ---------------------------------------------
        # 1. Deduplicate source 
        # ---------------------------------------------
        if deduplicate_source:
            # before_count = df.count()
            df = df.dropDuplicates(merge_keys)
            # after_count = df.count()
            log.info(f"[UPSERT] Deduplicated source")
        # ---------------------------------------------
        # 2. Optional incremental filter (performance)
        # ---------------------------------------------
        if incremental_filter:
            log.info(f"[UPSERT] Applying incremental filter: {incremental_filter}")
            df = df.filter(incremental_filter)

        # ---------------------------------------------
        # 3. Build merge condition
        # ---------------------------------------------
        merge_condition = " AND ".join(
            [f"target.{col} = source.{col}" for col in merge_keys]
        )

        # ---------------------------------------------
        # 4. Merge or Create
        # ---------------------------------------------
        if DeltaTable.isDeltaTable(spark, path):

            delta_table = DeltaTable.forPath(spark, path)

            merge_builder = (
                delta_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
            )

            if enable_schema_evolution:
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

            log.info(f"[UPSERT] Executing merge on {table}")

            merge_builder.execute()

            log.info(f"[UPSERT] Merge completed for {table}")

        else:
            log.info(f"[UPSERT] Table does not exist. Creating new Delta table at {path}")

            writer = df.write.format("delta").mode("overwrite")

            if partition_cols:
                log.info(f"[UPSERT] Applying partitioning: {partition_cols}")
                writer = writer.partitionBy(*partition_cols)

            if options:
                writer = writer.options(**options)

            writer.save(path)

            log.info(f"[UPSERT] Table created at {path}")

    except Exception as exc:
        log.exception(f"[UPSERT] Failed for {table} at {path}")
        raise WriteError(
            f"Delta upsert failed for table '{table}' at {path}"
        ) from exc


from delta.tables import DeltaTable

def optimize_delta_zorder(
    spark,
    base_path:str,
    table_name: str,
    zorder_cols: list[str],
    log=None,
) -> bool:
    """
    Run OPTIMIZE ZORDER BY on a Delta table path if it exists.

    Returns:
        True if optimization was run, False otherwise.
    """

    table_path = f"{base_path}/{table_name}"
    if not DeltaTable.isDeltaTable(spark, table_path):
        if log:
            log.info(f"Skipping OPTIMIZE: not a Delta table at {table_path}")
        return False

    cols_sql = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({cols_sql})")

    if log:
        log.info(f"Z-ORDER optimized {table_path} on {cols_sql}")

    return True




