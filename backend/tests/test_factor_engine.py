from __future__ import annotations

from datetime import date, timedelta

from backend.app.models import StrategyConfig
from backend.app.services.factor_engine import compute_snapshot, evaluate_filters, rank_snapshot
from backend.tests.support import StubAkshareProvider


def build_maps():
    provider = StubAkshareProvider()
    end_date = date.today()
    start_date = end_date - timedelta(days=provider.lookback_days)
    stocks = provider.fetch_universe()
    benchmark_stock, benchmark_bars = provider.fetch_benchmark_series(start_date, end_date)
    stocks.append(benchmark_stock)
    meta_map = {
        stock.code: {
            "code": stock.code,
            "name": stock.name,
            "industry": stock.industry,
            "board": stock.board,
            "security_type": stock.security_type,
            "listing_date": stock.listing_date,
            "is_st": int(stock.is_st),
            "is_suspended": int(stock.is_suspended),
        }
        for stock in stocks
    }
    price_map: dict[str, list[dict]] = {}
    for stock in provider.fetch_universe():
        for bar in provider.fetch_price_bars(stock.code, start_date, end_date):
            price_map.setdefault(bar.code, []).append(
                {
                    "trade_date": bar.trade_date,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "turnover": bar.turnover,
                    "is_suspended": int(bar.is_suspended),
                }
            )
    for bar in benchmark_bars:
        price_map.setdefault(bar.code, []).append(
            {
                "trade_date": bar.trade_date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "turnover": bar.turnover,
                "is_suspended": int(bar.is_suspended),
            }
        )
    financial_map = {
        item.code: {
            "code": item.code,
            "report_date": item.report_date,
            "publish_date": item.publish_date,
            "roe": item.roe,
            "revenue_yoy": item.revenue_yoy,
            "profit_yoy": item.profit_yoy,
            "operating_cashflow": item.operating_cashflow,
            "net_profit": item.net_profit,
            "pe_ttm": item.pe_ttm,
            "pb": item.pb,
            "debt_ratio": item.debt_ratio,
        }
        for stock in provider.fetch_universe()
        for item in provider.fetch_financial_series(stock.code)
    }
    return provider.fetch_price_bars("600519", start_date, end_date)[-1].trade_date, meta_map, price_map, financial_map


def test_snapshot_marks_risk_flags_and_missing_data():
    as_of_date, meta_map, price_map, financial_map = build_maps()
    del financial_map["600519"]
    snapshot = compute_snapshot(meta_map, price_map, financial_map, as_of_date)

    assert "缺少财务快照" in snapshot["600519"].basic_reasons
    assert "ST 风险" in snapshot["600001"].basic_reasons
    assert "上市未满 120 个交易日" in snapshot["301501"].basic_reasons
    assert "最新交易日停牌" in snapshot["000725"].basic_reasons
    assert snapshot["600001"].current_price < 5


def test_rankings_change_when_weights_change():
    as_of_date, meta_map, price_map, financial_map = build_maps()
    snapshot = compute_snapshot(meta_map, price_map, financial_map, as_of_date)

    quality_config = StrategyConfig(
        name="Quality",
        description="",
        weights={"roe": 1, "cashflow_quality": 1, "debt_ratio": 0.5},
        rebalance={"top_n": 10},
    )
    momentum_config = StrategyConfig(
        name="Momentum",
        description="",
        weights={"ma_trend": 1, "relative_strength_60d": 1, "volume_surge_20d": 1},
        rebalance={"top_n": 10},
    )

    quality_rank = rank_snapshot(snapshot, quality_config)
    momentum_rank = rank_snapshot(snapshot, momentum_config)

    quality_codes = [item["code"] for item in quality_rank[:5]]
    momentum_codes = [item["code"] for item in momentum_rank[:5]]

    assert quality_rank
    assert momentum_rank
    assert "600001" not in quality_codes
    assert "301501" not in quality_codes
    assert quality_codes != momentum_codes


def test_legacy_strategy_shape_and_earnings_window_filter():
    as_of_date, meta_map, price_map, financial_map = build_maps()
    code = "600519"
    financial_map[code]["publish_date"] = as_of_date
    snapshot = compute_snapshot(meta_map, price_map, financial_map, as_of_date)

    legacy_config = StrategyConfig.model_validate(
        {
            "name": "Legacy",
            "description": "",
            "weights": {"roe": 1, "ma_trend": 1},
            "top_n": 8,
            "min_listing_days": 120,
            "min_avg_turnover_20d": 80_000_000,
        }
    )

    assert legacy_config.rebalance.top_n == 8
    assert legacy_config.stock_pool.min_avg_turnover_20d == 80_000_000
    assert "处于财报事件窗口" in evaluate_filters(snapshot[code], legacy_config)


def test_min_price_and_listing_filters_apply_to_low_quality_names():
    as_of_date, meta_map, price_map, financial_map = build_maps()
    snapshot = compute_snapshot(meta_map, price_map, financial_map, as_of_date)
    config = StrategyConfig(name="Strict", description="")

    assert "股价低于 5 元" not in evaluate_filters(snapshot["000725"], config)
    assert "上市未满 120 个交易日" in evaluate_filters(snapshot["301501"], config)
