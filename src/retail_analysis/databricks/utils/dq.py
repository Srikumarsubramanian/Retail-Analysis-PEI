"""
Data Cleaning and Validation Utilities
"""

from retail_analysis.databricks.utils.custom_exceptions import DataQualityError,PipelineError

__all__ = [
    "clean_customer_name",
    "clean_phone",
    "extract_phone_extension",
    "clean_email",
]


from pyspark.sql import DataFrame

from pyspark.sql import functions as F
from pyspark.sql.column import Column

from retail_analysis.databricks.utils.logger import get_logger
from retail_analysis.databricks.utils.constants import EMAIL_PATTERN

log = get_logger(__name__)




def clean_customer_name(column: Column) -> Column:
    """
    Clean and standardize customer name values.

    Rules
    -----
    - Trim leading/trailing spaces
    - Remove numeric characters
    - Collapse multiple spaces
    - Convert to title case
    - Preserve alphabets, apostrophes, hyphens

    Future Actions
    -----
    - Remove special chars other than apostrophe and hypen - need unicode support
    """
    try:
                
        #remove numbers
        cleaned = (
            F.regexp_replace(
                F.trim(column),
                r"\d+",
                ""
            )
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



        return F.when(
            cleaned.isNull() | (F.trim(cleaned) == ""), F.lit(None)
        ).otherwise(cleaned)

    except Exception as e:
        raise DataQualityError("Failed to clean customer name")  from e

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
        Extract numeric extension from a raw phone string (e.g., 'x1234' -> '1234').

    Must be called on the original column *before* clean_phone(), which
    strips the extension during cleaning.
    """
    try:
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
        has_plus = cleaned.startswith("+")
        cleaned = F.regexp_replace(cleaned, r"[^\d]", "")
        cleaned = F.when(has_plus, F.concat(F.lit("+"), cleaned)).otherwise(cleaned)

        # 4. Validation
        digit_count = F.length(F.regexp_replace(cleaned, r"[^\d]", ""))
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
    except Exception as e:
        raise DataQualityError("Failed to clean phone number")  from e


def extract_phone_extension(column: Column) -> Column:
    """Extract numeric extension from a phone string (e.g., 'x1234' -> '1234')."""
    try:
        extension = F.regexp_extract(
            column,
            r"(?i)(?:ext\.?|(?<!\w)x)\s*(\d+)$",
            1
        )
        return F.when(extension != "", extension).otherwise(F.lit(None))
    except Exception as e:
        raise DataQualityError("Failed to extract phone extension")  from e



def clean_email(column: Column) -> Column:
    """
    Clean and validate email values.

    Rules
    -----
    - Trim leading/trailing spaces
    - Convert to lowercase
    - Validate email format
    """
    try:
        cleaned = F.lower(F.trim(column))

        is_valid = cleaned.rlike(EMAIL_PATTERN)

        return F.when(
            column.isNull() |
            (cleaned == "") |
            (~is_valid),
            F.lit(None)
        ).otherwise(cleaned)
    except Exception as e:
        raise DataQualityError("Failed to clean email")  from e

