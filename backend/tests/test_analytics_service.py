from __future__ import annotations

import time

from backend.app.config import AppSettings
from backend.app.db import Repository
from backend.app.models import StrategyConfig
from backend.app.services.analytics_service import AnalyticsService
from backend.app.services.strategy_service import StrategyService, default_strategy_config
from backend.app.services.sync_service import SyncService
from backend.tests.support import StubAkshareProvider


def build_services(tmp_path):
    data_dir = tmp_path / "analytics-data"
    settings = AppSettings(
        host="127.0.0.1",
        port=8124,
        data_dir=data_dir,
        db_path=data_dir / "gupiao.db",
        research_db_path=data_dir / "gupiao_research.db",
    )
    repository = Repository(settings.db_path)
    repository.init_db()
    sync_service = SyncService(repository, settings, provider=StubAkshareProvider())
    response = sync_service.start_or_resume_sync()
    deadline = time.time() + 10
    while time.time() < deadline:
        current = sync_service.get_run(response.run_id)
        if current and current.status != "running":
            break
        time.sleep(0.05)
    else:
        raise AssertionError("sync did not complete in time")
    analytics_service = AnalyticsService(repository, settings.benchmark_code)
    strategy_service = StrategyService()
    return repository, strategy_service, analytics_service


def test_default_rankings_and_internal_validation_cache_are_consistent(tmp_path):
    repository, strategy_service, analytics_service = build_services(tmp_path)
    config = strategy_service.get_strategy()
    latest_trade_date = repository.get_latest_trade_date()

    ranked_date, rows, payload = analytics_service.get_ranked_rows(config)
    shortlist_date, shortlist, shortlist_payload = analytics_service.shortlist(config)

    assert ranked_date == latest_trade_date
    assert shortlist_date == latest_trade_date
    assert rows
    assert len(shortlist) <= config.rebalance.top_n
    assert payload["validation_summary"].current_model_health in {"healthy", "degraded", "insufficient"}
    assert shortlist_payload["block_reasons"] == payload["block_reasons"]

    cached = repository.get_validation_cache(latest_trade_date)
    assert cached is not None
    assert cached["config_key"] == analytics_service._config_key(default_strategy_config())
    assert repository.get_latest_model_health() is not None


def test_strategy_weights_change_main_analytics_rankings(tmp_path):
    repository, _strategy_service, analytics_service = build_services(tmp_path)
    latest_trade_date = repository.get_latest_trade_date()
    quality_config = StrategyConfig(
        name="Quality",
        description="",
        weights={"roe": 1, "cashflow_quality": 1, "profit_yoy": 1, "debt_ratio": 0.5},
        rebalance={"top_n": 8},
    )
    momentum_config = StrategyConfig(
        name="Momentum",
        description="",
        weights={"ma_trend": 1, "relative_strength_60d": 1, "volume_surge_20d": 1, "liquidity": 0.5},
        rebalance={"top_n": 8},
    )

    _, quality_rows, _ = analytics_service.get_ranked_rows(quality_config, as_of_date=latest_trade_date, limit=5)
    _, momentum_rows, _ = analytics_service.get_ranked_rows(momentum_config, as_of_date=latest_trade_date, limit=5)

    assert quality_rows
    assert momentum_rows
    assert [row["code"] for row in quality_rows] != [row["code"] for row in momentum_rows]


def test_negative_growth_filters_apply_to_samples_shortlist_and_analysis(tmp_path):
    repository, _strategy_service, _analytics_service = build_services(tmp_path)
    latest_trade_date = repository.get_latest_trade_date()
    with repository.connect() as connection:
        connection.execute(
            "UPDATE financial_snapshot SET revenue_yoy = -12.0, profit_yoy = -8.0 WHERE code = '600519'"
        )

    analytics_service = AnalyticsService(repository, "000300.SH")
    config = StrategyConfig(
        name="Strict Growth",
        description="",
        weights={"revenue_yoy": 1, "profit_yoy": 1, "roe": 1},
        stock_pool={
            "exclude_negative_revenue_yoy": True,
            "exclude_negative_profit_yoy": True,
            "min_avg_turnover_20d": 80_000_000,
            "min_price": 5,
            "min_listing_days": 120,
        },
        rebalance={"top_n": 8},
    )

    _analysis_date, _entry, analysis, _payload = analytics_service.get_analysis_for_code(
        "600519",
        config,
        as_of_date=latest_trade_date,
    )
    sample_codes = {sample.code for sample in analytics_service._training_samples(config, as_of_date=latest_trade_date)}
    shortlist, _regime = analytics_service.backtest_shortlist(config, as_of_date=latest_trade_date)

    assert analysis is not None
    assert "营收同比为负" in analysis["ineligible_reasons"]
    assert "利润同比为负" in analysis["ineligible_reasons"]
    assert "600519" not in sample_codes
    assert "600519" not in {row["code"] for row in shortlist}


def test_custom_config_does_not_overwrite_persisted_default_cache(tmp_path):
    repository, strategy_service, analytics_service = build_services(tmp_path)
    latest_trade_date = repository.get_latest_trade_date()

    analytics_service.get_ranked_rows(strategy_service.get_strategy(), as_of_date=latest_trade_date, limit=5)
    default_cache = repository.get_validation_cache(latest_trade_date)

    custom_config = StrategyConfig(
        name="Focused",
        description="",
        weights={"ma_trend": 1, "relative_strength_60d": 1, "roe": 0.2},
        rebalance={"top_n": 6},
    )
    analytics_service.get_ranked_rows(custom_config, as_of_date=latest_trade_date, limit=5)
    post_custom_cache = repository.get_validation_cache(latest_trade_date)

    assert default_cache is not None
    assert post_custom_cache == default_cache
