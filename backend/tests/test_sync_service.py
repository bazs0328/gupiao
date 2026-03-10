from __future__ import annotations

from pathlib import Path

from backend.app.config import AppSettings
from backend.app.db import Repository
from backend.app.services.provider import ProviderFinancial
from backend.app.services.sync_service import SyncService
from backend.tests.support import StubAkshareProvider


def test_derive_financial_valuations_uses_publish_date_price_and_caps_invalid_inputs(tmp_path):
    data_dir = Path(tmp_path) / "sync-service-data"
    settings = AppSettings(
        host="127.0.0.1",
        port=8125,
        data_dir=data_dir,
        db_path=data_dir / "gupiao.db",
        research_db_path=data_dir / "gupiao_research.db",
    )
    repository = Repository(settings.db_path)
    repository.init_db()
    service = SyncService(repository, settings, provider=StubAkshareProvider())

    price_history = [
        ("2025-01-02", 10.0),
        ("2025-01-06", 12.0),
    ]
    rows = [
        ProviderFinancial(
            code="600519",
            report_date="2024-12-31",
            publish_date="2025-01-05",
            roe=18.0,
            revenue_yoy=12.0,
            profit_yoy=10.0,
            operating_cashflow=300.0,
            net_profit=240.0,
            pe_ttm=0.0,
            pb=0.0,
            debt_ratio=38.0,
            basic_eps=2.0,
            per_net_assets=5.0,
        ),
        ProviderFinancial(
            code="600519",
            report_date="2024-09-30",
            publish_date="2025-01-05",
            roe=18.0,
            revenue_yoy=12.0,
            profit_yoy=10.0,
            operating_cashflow=300.0,
            net_profit=240.0,
            pe_ttm=0.0,
            pb=0.0,
            debt_ratio=38.0,
            basic_eps=-1.0,
            per_net_assets=5.0,
        ),
        ProviderFinancial(
            code="600519",
            report_date="2024-06-30",
            publish_date="2025-01-01",
            roe=18.0,
            revenue_yoy=12.0,
            profit_yoy=10.0,
            operating_cashflow=300.0,
            net_profit=240.0,
            pe_ttm=0.0,
            pb=0.0,
            debt_ratio=38.0,
            basic_eps=2.0,
            per_net_assets=0.0,
        ),
    ]

    derived = service._derive_financial_valuations(rows, price_history)

    assert derived[0].pe_ttm == 5.0
    assert derived[0].pb == 2.0
    assert derived[1].pe_ttm == 10000.0
    assert derived[1].pb == 2.0
    assert derived[2].pe_ttm == 10000.0
    assert derived[2].pb == 10000.0
