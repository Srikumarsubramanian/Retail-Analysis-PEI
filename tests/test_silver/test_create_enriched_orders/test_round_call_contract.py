
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

import retail_analysis.databricks.notebooks.silver.create_enriched_orders as m
from pyspark.sql.column import Column

from retail_analysis.databricks.utils.setup_exceptions.custom_exceptions import TransformError


@pytest.fixture()
def prepare_orders_setup(monkeypatch):
    df = MagicMock(name="orders_df")


    df.withColumn.return_value = df
    df.groupBy.return_value = df
    df.agg.return_value = df
    mock_round = MagicMock(name="round_currency")
    
    mock_col = MagicMock(name="col", spec=Column)
    mock_to_date = MagicMock(name="to_date", return_value=mock_col)

    monkeypatch.setattr(m, "round_currency", mock_round)
    monkeypatch.setattr(m.F, "col", MagicMock(return_value=mock_col))
    monkeypatch.setattr(m.F, "to_date", mock_to_date)
    monkeypatch.setattr(m.F, "sum", MagicMock(return_value=mock_col))
    return SimpleNamespace(
        df=df,
        mock_round=mock_round,
    )

@pytest.mark.unit
def test_prepare_orders_calls_round_currency(prepare_orders_setup):
    """
    Validate that round_currency is called correctly.

    Ensures:
    - round_currency is invoked
    - called with Column (F.sum("profit"))
    - called with scale=2
    """
    s = prepare_orders_setup
    s.df.columns = [
        "order_id", "order_date", "ship_date",
        "ship_mode", "customer_id", "product_id",
        "discount", "quantity", "price", "profit"
    ]

    m._prepare_orders(s.df)

    s.mock_round.assert_called_once()

    args, kwargs = s.mock_round.call_args

    assert isinstance(args[0], Column)

    assert args[1] == 2





@pytest.mark.unit
def test_prepare_orders_raises_transform_error_on_failure(prepare_orders_setup):
    """
    Validate that _prepare_orders wraps internal errors
    into TransformError.
    """
    s = prepare_orders_setup

    s.df.withColumn.side_effect = Exception(" Some Error")


    with pytest.raises(TransformError) as exc_info:
        m._prepare_orders(s.df)

