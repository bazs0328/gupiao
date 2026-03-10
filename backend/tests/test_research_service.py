from __future__ import annotations

from datetime import date, timedelta

from backend.app.config import AppSettings
from backend.app.db import Repository
from backend.app.research_db import ResearchRepository
from backend.app.services.research_execution import simulate_trade
from backend.app.services.research_service import ResearchService
from backend.tests.support import StubResearchProvider


def make_research_service(tmp_path):
    data_dir = tmp_path / "research-data"
    settings = AppSettings(
        host="127.0.0.1",
        port=8125,
        data_dir=data_dir,
        db_path=data_dir / "gupiao.db",
        research_db_path=data_dir / "gupiao_research.db",
    )
    business_repository = Repository(settings.db_path)
    business_repository.init_db()
    research_repository = ResearchRepository(settings.research_db_path)
    research_repository.init_db()
    service = ResearchService(
        research_repository,
        business_repository,
        settings.benchmark_code,
        provider=StubResearchProvider(),
    )
    return settings, research_repository, service


def test_research_refresh_builds_diagnostics_and_ready_parameter(tmp_path):
    _settings, repository, service = make_research_service(tmp_path)

    run = service.refresh_blocking("rebuild")
    diagnostics = service.get_diagnostics()

    assert run["status"] == "success"
    assert diagnostics.status in {"ready", "limited"}
    assert diagnostics.sample_count > 0
    assert diagnostics.source_quality_summary
    assert diagnostics.factor_drift
    assert repository.get_latest_parameter() is not None
    assert service.get_active_strategy_config() is not None


def test_research_repository_respects_listing_st_and_delisting_state(tmp_path):
    _settings, repository, service = make_research_service(tmp_path)
    service.refresh_blocking("rebuild")

    before_listing = repository.get_meta_snapshot((date.today() - timedelta(days=401)).isoformat())
    during_st = repository.get_meta_snapshot((date.today() - timedelta(days=60)).isoformat())
    after_delist = repository.get_meta_snapshot(date.today().isoformat())

    assert "688001" not in before_listing
    assert during_st["600001"]["is_st"] == 1
    assert "688001" not in after_delist


def test_publish_date_gate_and_quality_gate_keep_estimated_rows_out_of_headline_stats(tmp_path):
    _settings, repository, service = make_research_service(tmp_path)
    service.refresh_blocking("rebuild")

    visible = repository.get_visible_financials(as_of_date=(date.today() - timedelta(days=200)).isoformat())
    diagnostics = service.get_diagnostics()

    assert visible
    assert any(row["publish_date_quality"] == "estimated" for row in visible.values()) or diagnostics.headline_sample_count <= diagnostics.sample_count
    assert repository.count_headline_samples() < repository.count_samples()


def test_execution_simulator_handles_stop_loss_and_gap_logic():
    trading_dates = ["2026-03-06", "2026-03-09", "2026-03-10", "2026-03-11"]
    date_index = {trade_date: index for index, trade_date in enumerate(trading_dates)}
    bar_lookup = {
        "AAA": {
            "2026-03-09": {"open": 10.0, "high": 10.2, "low": 9.8, "close": 10.1, "is_suspended": 0},
            "2026-03-10": {"open": 9.0, "high": 9.3, "low": 8.8, "close": 9.1, "is_suspended": 0},
            "2026-03-11": {"open": 9.2, "high": 9.4, "low": 9.0, "close": 9.3, "is_suspended": 0},
        },
        "000300.SH": {
            "2026-03-09": {"open": 100.0, "high": 101.0, "low": 99.5, "close": 100.5, "is_suspended": 0},
            "2026-03-10": {"open": 100.4, "high": 101.2, "low": 100.0, "close": 100.8, "is_suspended": 0},
            "2026-03-11": {"open": 100.8, "high": 101.3, "low": 100.4, "close": 101.0, "is_suspended": 0},
        },
    }

    result = simulate_trade(
        code="AAA",
        signal_date="2026-03-06",
        trading_dates=trading_dates,
        date_index=date_index,
        bar_lookup=bar_lookup,
        benchmark_code="000300.SH",
        holding_period_days=2,
        stop_loss=9.2,
        take_profit=10.8,
        commission_bps=5,
        slippage_bps=8,
        stamp_duty_bps=10,
    )

    assert result is not None
    assert result.exit_reason == "stop_loss_gap"
    assert result.exit_trade_date == "2026-03-10"
    assert result.excess_return < 0
