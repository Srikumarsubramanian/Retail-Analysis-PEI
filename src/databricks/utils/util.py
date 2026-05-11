"""
Generic I/O and validation utilities.

This module provides data readers, writers, and pre-flight checks
that are format-agnostic. Transform logic has been moved to
``transforms.py``; use that module for column-level operations.
"""

# from __future__ import annotations

import os
from typing import List, Optional, Sequence, Tuple

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.databricks.utils.custom_exceptions import ConfigError, DataQualityError, WriteError ,IngestionError
from src.databricks.utils.logger import get_logger
from src.databricks.utils.constants import QUARANTINE_PATH

log = get_logger(__name__)


# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
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
        raise ConfigError(f"Invalid YAML in {path}: {e}") from e
    except Exception as e:
        raise ConfigError(f"Failed to load config: {e}") from e

    if config is None:
        raise ConfigError(f"Config file is empty: {path}")

    log.info("Config loaded successfully", extra={"path": path})
    return config


# ─────────────────────────────────────────────
# File Validation
# ─────────────────────────────────────────────
def ensure_files_exist(
    source_path: str,
    files: List[str],
    raise_on_missing: bool = False,
) -> Tuple[List[str], List[str]]:
    """Check that every expected source file exists on disk.

    Parameters
    ----------
    source_path : str
        Directory containing source files.
    files : list[str]
        Expected file names.
    raise_on_missing : bool
        If ``True``, raise ``FileNotFoundError`` when any file is absent.

    Returns
    -------
    (present, missing)
        Lists of file names.
    """
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


# ─────────────────────────────────────────────
# Read
# ─────────────────────────────────────────────
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
        Parse mode for CSV/JSON: ``PERMISSIVE`` (default), ``DROPMALFORMED``,``FAILFAST``.  In PERMISSIVE mode corrupt records are quarantined to a ``quarantine/`` subfolder alongside the Target path.
        Ignored for Excel, Parquet, and Delta formats.
    **options
        Additional Spark reader options forwarded to the underlying reader.

    Returns
    -------
    DataFrame
    """
    try:
        fmt = format.lower()
        source_file_path = f"{path}/{file_name}"


        # ── Excel: no permissive / quarantine support ──
        if fmt in ("xlsx", "excel", "xls"):
            reader = spark.read.format("com.crealytics.spark.excel")
            reader = reader.option("header", "true")
            for key, value in options.items():
                reader = reader.option(key, value)
            return reader.load(source_file_path)

        # ── CSV / JSON / Parquet / Delta ──
        reader = spark.read.format(fmt)
        if schema is not None:
            reader = reader.schema(schema)
        if fmt in ("csv", "json"):
            reader = reader.option("mode", mode)

        # Enable corrupt-record capture for CSV/JSON in PERMISSIVE mode
        is_permissive = fmt in ("csv", "json") and mode.upper() == "PERMISSIVE"
        if is_permissive:
            reader = reader.option("mode", "PERMISSIVE")
            reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")

        for key, value in options.items():
            reader = reader.option(key, value)

        df_raw = reader.load(source_file_path)

        # ── Quarantine corrupt rows (CSV / JSON permissive only) ──
        if is_permissive and "_corrupt_record" in df_raw.columns:
            df_bad = df_raw.filter(F.col("_corrupt_record").isNotNull())
            df_good = df_raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

            bad_count = df_bad.count()

            if bad_count > 0:
                quarantine_file_path = f"{QUARANTINE_PATH}/{file_name}"
                df_bad.write.mode("append").format(fmt).save(quarantine_file_path)

                log.warning(
                    "Corrupt records quarantined",
                    extra={
                        "source": path,
                        "quarantine_path": quarantine_file_path,
                        "count": bad_count,
                    },
                )

            return df_good

        return df_raw
    except Exception as e:
        log.error(f"Failed to read data from {source_file_path} {e}")
        raise IngestionError(f"Failed to read data from {source_file_path} {e}")

# def validate_schema(
#     df: DataFrame,
#     expected_cols: Sequence[str],
#     label: str = "DataFrame",
# ) -> None:
#     """Verify that a DataFrame contains all expected columns.

#     Raises
#     ------
#     DataQualityError
#         If any expected column is missing from the DataFrame.
#     """
#     actual = set(df.columns)
#     missing = [c for c in expected_cols if c not in actual]
#     if missing:
#         raise DataQualityError(
#             f"{label} is missing {len(missing)} expected column(s): {missing}. "
#             f"Actual columns: {sorted(actual)}"
#         )


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
    from src.databricks.utils.constants import SILVER_DELTA_PATH
    
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
    
        
    df_cast = df.select(*select_exprs).cache()
    
    # 2. Check for non-nullable columns being null
    non_nullable_cols = [f.name for f in expected_schema.fields if not f.nullable]
    
    if non_nullable_cols:
        # Build condition: A row is BAD if ANY non_nullable_col is null
        cond = F.col(non_nullable_cols[0]).isNull()
        for c in non_nullable_cols[1:]:
            cond = cond | F.col(c).isNull()
            
        bad_rows = df_cast.filter(cond)
        good_rows = df_cast.filter(~cond)
        
        # Action to count bad rows (safe due to .cache())
        bad_count = bad_rows.count()
        if bad_count > 0:
            log.warning(f"Quarantining {bad_count} bad rows from {label}")
            quarantine_path  = f"{QUARANTINE_PATH}/{label.replace(' ', '_').lower()}"
            
            # Add quarantine metadata and write to quarantine table
            (
                bad_rows
                .withColumn("_quarantined_at", F.current_timestamp())
                .withColumn("_quarantine_source", F.lit(label))
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(quarantine_path)
            )
            
        return good_rows
        
    return df_cast


# ─────────────────────────────────────────────
# Write
# ─────────────────────────────────────────────
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
        log.exception("Delta write failed", extra={"file_name": file_name, "target_path": target_path})
        raise WriteError(f"Failed to write table '{file_name}' to {target_path}") from exc

    log.info(
        "Delta write completed",
        extra={"file_name": file_name, "target_path": target_path, "mode": mode, "partition_by": partition_by},
    )


def merge_delta_upsert(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    base_path: str,
    merge_keys: list[str],
    partition_cols: Optional[List[str]] = None,
    **options,
) -> None:
    """Upsert into a Delta table: update matching rows, insert new ones.

    Falls back to an overwrite-create if no Delta table exists at the path.

    Raises
    ------
    WriteError
        If the merge operation fails.
    """
    from delta.tables import DeltaTable
    if not merge_keys:
        raise ValueError("merge_keys must contain at least one column")

    # Build merge condition for multiple keys
    merge_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in merge_keys]
    )


    path = f"{base_path}/{table}"
    try:
        if DeltaTable.isDeltaTable(spark, path):
            (
                DeltaTable.forPath(spark, path)
                .alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            log.info(f"Delta upsert completed for file {table} at {path} with merge keys {merge_keys}")
        else:
            writer = df.write.format("delta")

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            if options:
                writer = writer.options(**options)
            writer.save(path)
            log.info(f"Delta upsert completed for file {table} at {path}")
    except Exception as exc:
        log.exception(f"Delta upsert failed for file {table} at {path}\n Error: {exc} \n ")
        raise WriteError(f"Failed to upsert table '{table}' at {path} due to \n {exc} \n ") from exc
    
