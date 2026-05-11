"""
Data Cleaning and Validation Utilities



"""

from __future__ import annotations

from pyspark.sql import DataFrame

from pyspark.sql import functions as F
from pyspark.sql.column import Column

from src.databricks.utils.logger import get_logger

log = get_logger(__name__)


EMAIL_PATTERN = r"^[a-z0-9._%+-]+@[a-z0-9-]+(\.[a-z0-9-]+)+$"

def clean_customer_name(column: Column) -> Column:
    """
    Clean and standardize customer name values.

    Rules
    -----
    - Trim leading/trailing spaces
    - Remove numeric characters
    - Replace invalid special characters with spaces
    - Collapse multiple spaces
    - Convert to title case
    - Preserve alphabets, apostrophes, hyphens
    """

    #remove numbers
    cleaned = (
        F.regexp_replace(
            F.trim(column),
            r"\d+",
            ""
        )
    )

    #remove invalid special characters
    cleaned = F.regexp_replace(
        cleaned,
        r"[^A-Za-z\s'\-]",
        " "
    )

    #collapse multiple spaces
    cleaned = F.regexp_replace(
        cleaned,
        r"\s+",
        " "
    )

    #convert to title case
    cleaned = F.initcap(cleaned)

    #trim spaces at last
    cleaned = F.trim(cleaned)

    # Must contain at least one alphabetic character
    has_alpha = cleaned.rlike(r"[A-Za-z]")

    return F.when(
        cleaned.isNull() | (~has_alpha),
        F.lit(None)
    ).otherwise(cleaned)


def clean_phone(column: Column) -> Column:
    """
    Clean and standardize phone numbers to an E.164-like format.

    Rules
    -----
    - Trim leading/trailing spaces
    - Extract extension into separate column
    - Preserve leading '+'
    - Remove all other non-digit characters
    - Keep country codes intact
    - Validate digit count between 7 and 15
    - If value does not meet the criteria, return NULL
    """

    # 1. Remove extension part from phone value
    cleaned = F.regexp_replace(
        F.trim(column),
        r"(?i)\s*(?:ext\.?|x)\s*\d+$",
        ""
    )

    # 2. Remove separators and formatting chars
    cleaned = F.regexp_replace(
        cleaned,
        r"[()\s.\-]",
        ""
    )

    # 3. Remove invalid characters (preserve leading +)
    cleaned = F.regexp_replace(
        cleaned,
        r"(?!^\+)[^0-9]",
        ""
    )

    # 4. Validation
    digit_count = F.length(F.regexp_replace(cleaned, r"\D", ""))
    is_valid = (
        cleaned.rlike(r"^\+?\d+$") &
        digit_count.between(7, 15)
    )

    return F.when(
        column.isNull() |
        (F.trim(column) == "") |
        (~is_valid),
        F.lit(None)
    ).otherwise(cleaned)


def extract_phone_extension(column: Column) -> Column:
    """Extract numeric extension from a phone string (e.g., 'x1234' -> '1234')."""
    extension = F.regexp_extract(
        column,
        r"(?i)(?:ext\.?|x)\s*(\d+)$",
        1
    )
    return F.when(extension != "", extension).otherwise(F.lit(None))



def clean_email(column: Column) -> Column:
    """
    Clean and validate email values.

    Rules
    -----
    - Trim leading/trailing spaces
    - Convert to lowercase
    - Validate email format
    - Return NULL for invalid/empty emails
    """

    cleaned = F.lower(F.trim(column))

    is_valid = cleaned.rlike(EMAIL_PATTERN)

    return F.when(
        column.isNull() |
        (cleaned == "") |
        (~is_valid),
        F.lit(None)
    ).otherwise(cleaned)




