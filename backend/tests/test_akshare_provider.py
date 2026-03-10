from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.app.services.akshare_provider import AkshareProvider, infer_publish_date


class FakeAkshareModule:
    def __init__(self, dataframe: pd.DataFrame | None = None, error: Exception | None = None):
        self.dataframe = dataframe
        self.error = error

    def stock_financial_abstract_new_ths(self, symbol: str, indicator: str = "按报告期") -> pd.DataFrame:
        assert indicator == "按报告期"
        if self.error is not None:
            raise self.error
        assert self.dataframe is not None
        return self.dataframe


def _financial_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"report_date": "2025-06-30", "metric_name": "index_weighted_avg_roe", "value": 18.5},
            {"report_date": "2025-06-30", "metric_name": "calculate_operating_income_total_yoy_growth_ratio", "value": 22.1},
            {"report_date": "2025-06-30", "metric_name": "calculate_parent_holder_net_profit_yoy_growth_ratio", "value": 16.4},
            {"report_date": "2025-06-30", "metric_name": "parent_holder_net_profit", "value": 240.0},
            {"report_date": "2025-06-30", "metric_name": "basic_eps", "value": 2.0},
            {"report_date": "2025-06-30", "metric_name": "calc_per_net_assets", "value": 12.0},
            {"report_date": "2025-06-30", "metric_name": "assets_debt_ratio", "value": 41.2},
            {"report_date": "2025-06-30", "metric_name": "index_per_operating_cash_flow_net", "value": 3.5},
            {"report_date": "2025-03-31", "metric_name": "index_full_diluted_roe", "value": 17.8},
            {"report_date": "2025-03-31", "metric_name": "calculate_operating_income_total_yoy_growth_ratio", "value": 18.9},
            {"report_date": "2025-03-31", "metric_name": "calculate_parent_holder_net_profit_yoy_growth_ratio", "value": 12.3},
            {"report_date": "2025-03-31", "metric_name": "parent_holder_net_profit", "value": 180.0},
            {"report_date": "2025-03-31", "metric_name": "basic_eps", "value": 1.5},
            {"report_date": "2025-03-31", "metric_name": "calc_per_net_assets", "value": 11.0},
            {"report_date": "2025-03-31", "metric_name": "assets_debt_ratio", "value": 39.6},
            {"report_date": "2025-03-31", "metric_name": "index_per_operating_cash_flow_net", "value": 2.2},
        ]
    )


def test_fetch_financial_series_maps_abstract_new_metrics():
    provider = AkshareProvider(akshare_module=FakeAkshareModule(_financial_rows()))

    rows = provider.fetch_financial_series("600519")

    assert len(rows) == 2
    first = rows[0]
    assert first.code == "600519"
    assert first.report_date == "2025-03-31"
    assert first.publish_date == infer_publish_date(date(2025, 3, 31)).isoformat()
    assert first.roe == 17.8
    assert first.revenue_yoy == 18.9
    assert first.profit_yoy == 12.3
    assert first.net_profit == 180.0
    assert first.operating_cashflow == pytest.approx(264.0)
    assert first.debt_ratio == 39.6
    assert first.basic_eps == 1.5
    assert first.per_net_assets == 11.0
    assert first.pe_ttm == 0.0
    assert first.pb == 0.0


def test_fetch_financial_series_raises_for_empty_payload():
    provider = AkshareProvider(akshare_module=FakeAkshareModule(pd.DataFrame()))

    with pytest.raises(RuntimeError, match="600519: 财务接口返回空数据"):
        provider.fetch_financial_series("600519")


def test_fetch_financial_series_raises_for_missing_metrics():
    dataframe = _financial_rows().query("metric_name != 'basic_eps'")
    provider = AkshareProvider(akshare_module=FakeAkshareModule(dataframe))

    with pytest.raises(RuntimeError, match="600519: 财务接口缺少关键指标 basic_eps"):
        provider.fetch_financial_series("600519")


def test_fetch_financial_series_surfaces_request_errors():
    provider = AkshareProvider(akshare_module=FakeAkshareModule(error=ValueError("upstream broke")))

    with pytest.raises(RuntimeError, match="600519: 财务接口请求失败 upstream broke"):
        provider.fetch_financial_series("600519")
