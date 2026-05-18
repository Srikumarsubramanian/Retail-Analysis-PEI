"""
Reusable DataFrame transforms.

All functions are pure (DataFrame in → DataFrame out) with no I/O,
making them easy to unit-test without mocking.
"""

import re
from pyspark.sql import DataFrame , Column
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from retail_analysis.databricks.utils.setup_exceptions.custom_exceptions import TransformError
from retail_analysis.databricks.utils.setup_logging.logger import get_logger



log = get_logger(__name__)


def normalise_columns(df: DataFrame) -> DataFrame:
    """Lower-case all column names and replace non-alphanumeric chars with underscores."""

    aliases = []
    seen = {}
    
    try:
        for old_col in df.columns:
            new_col = re.sub(r"[^a-zA-Z0-9_]", "_", old_col.strip().lower())
            new_col = re.sub(r"_+", "_", new_col).strip("_")

            ##if col name is empty string
            if new_col == "":
                new_col = "col"

            ##rename columns if there are duplicates
            if new_col in seen:
                seen[new_col] += 1
                new_col = f"{new_col}_{seen[new_col]}"
            else:
                seen[new_col] = 0

            aliases.append(df[old_col].alias(new_col))

        return df.select(aliases)
    except Exception as e:
        log.PipelineError("Failed to normalise columns")
        raise TransformError("Failed to normalise columns") from e


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Stamp every row with the current ingestion timestamp."""
    try:
        return df.withColumn("_ingested_at", F.current_timestamp())
    except Exception as e:
        log.exception("Failed to add ingestion metadata")
        raise TransformError("Failed to add ingestion metadata") from e



def round_currency(
    column: Column,
    scale: int = 2,
) -> Column:
    """
    Enterprise-grade rounding for financial columns.

    Parameters
    ----------
    column : Column
        Input numeric column
    scale : int
        Number of decimal places (default: 2)


    Returns
    -------
    Column
        Rounded column with Decimal precision
    """
    if not isinstance(column, Column):
        raise TypeError("Input must be a PySpark Column")

    try:                
        # Apply rounding
        rounded = F.round(column, scale)

        return F.when(F.isnan(rounded), None).otherwise(rounded)
    except Exception as e:
        log.exception("Failed to round currency")
        raise TransformError("Failed to round currency") from e