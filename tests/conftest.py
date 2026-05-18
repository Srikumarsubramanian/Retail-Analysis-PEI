import pytest
from retail_analysis.databricks.utils.util_funcs.spark_session import get_spark

@pytest.fixture(scope="session")
def spark():
    """Provides an actual SparkSession for the entire test session."""
    spark_session = get_spark()
    yield spark_session
    spark_session.stop()
