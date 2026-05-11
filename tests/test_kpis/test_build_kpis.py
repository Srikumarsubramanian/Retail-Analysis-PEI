import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from retail_analysis.databricks.notebooks import kpis as rpt
from retail_analysis.databricks.utils.custom_exceptions import KPIQueryError


@pytest.fixture()
def build_kpis_setup():
    spark = MagicMock(name="spark")

    df = MagicMock(name="df")
    df.show = MagicMock()

    spark.sql = MagicMock(return_value=df)

    return SimpleNamespace(
        spark=spark,
        df=df,
    )

@pytest.mark.reporting
def test_build_kpis_success(build_kpis_setup):
    s = build_kpis_setup

    rpt.build_kpis(s.spark)


    assert s.spark.sql.call_count == 4

    assert s.df.show.call_count == 4

@pytest.mark.reporting
def test_build_kpis_failure(build_kpis_setup):
    s = build_kpis_setup

    s.spark.sql.side_effect = Exception("Error calculating KPIs")

    with pytest.raises(KPIQueryError, match="Error calculating KPIs"):
        rpt.build_kpis(s.spark)

