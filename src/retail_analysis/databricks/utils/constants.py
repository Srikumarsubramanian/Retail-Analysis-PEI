"""
Pipeline constants — path and database configuration.

Provides all layer paths and database names used across the ETL pipeline.
"""


# -- Layer paths --------------------------------------
SOURCE_BASE_PATH = "d:/PEI/retail-analysis/temp/source"
BRONZE_DELTA_PATH = "d:/PEI/retail-analysis/temp/delta/ecommerce/bronze"
SILVER_DELTA_PATH = "d:/PEI/retail-analysis/temp/delta/ecommerce/silver"
GOLD_DELTA_PATH = "d:/PEI/retail-analysis/temp/delta/ecommerce/gold"

QUARANTINE_PATH = "d:/PEI/retail-analysis/temp/quarantine"


LOG_DIR = "d:/PEI/retail-analysis/temp/logs"

EMAIL_PATTERN = r"^[a-z0-9._%+-]+@[a-z0-9-]+(\.[a-z0-9-]+)+$"