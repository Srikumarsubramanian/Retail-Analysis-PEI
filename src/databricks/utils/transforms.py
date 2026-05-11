"""
Reusable DataFrame transforms.

All functions are pure (DataFrame in → DataFrame out) with no I/O,
making them easy to unit-test without mocking.
"""

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


import re
from pyspark.sql import DataFrame


def normalise_columns(df: DataFrame) -> DataFrame:
    """Lower-case all column names and replace non-alphanumeric chars with underscores."""

    aliases = []
    seen = {}

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


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Stamp every row with the current ingestion timestamp."""
    return df.withColumn("_ingested_at", F.current_timestamp())
