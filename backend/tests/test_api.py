from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
import time

from fastapi.testclient import TestClient

from backend.app.config import AppSettings
from backend.app.db import Repository
from backend.app.main import create_app
from backend.tests.support import StubAkshareProvider, StubResearchProvider, wait_for_sync_completion


class BrokenFinancialProvider(StubAkshareProvider):
    def fetch_financial_series(self, symbol: str):
        super().fetch_financial_series(symbol)
        return []


def make_settings(tmp_path):
    data_dir = tmp_path / "data"
    return AppSettings(
        host="127.0.0.1",
        port=8123,
        data_dir=data_dir,
        db_path=data_dir / "gupiao.db",
        research_db_path=data_dir / "gupiao_research.db",
    )


def make_client(tmp_path, provider=None, settings=None, research_provider=None):
    app_settings = settings or make_settings(tmp_path)
    return TestClient(
        create_app(
            app_settings,
            provider=provider or StubAkshareProvider(),
            research_provider=research_provider,
        )
    )


def test_create_app_serves_frontend_bundle(tmp_path):
    frontend_dir = tmp_path / "frontend"
    frontend_dir.mkdir()
    (frontend_dir / "index.html").write_text("<html><body>frontend shell</body></html>", encoding="utf-8")

    settings = make_settings(tmp_path)
    client = TestClient(create_app(settings, frontend_dir=frontend_dir, provider=StubAkshareProvider()))

    assert client.get("/").status_code == 200
    assert "frontend shell" in client.get("/rankings-overview").text
    assert client.get("/health").status_code == 200


def test_sync_rank_detail_report_and_watchlist_flow(tmp_path):
    settings = make_settings(tmp_path)
    client = make_client(tmp_path, settings=settings)

    sync_response = client.post("/sync/eod")
    assert sync_response.status_code == 200
    sync_payload = sync_response.json()
    assert sync_payload["status"] == "running"
    assert sync_payload["sync_mode"] == "initial_build"
    terminal = wait_for_sync_completion(client, sync_payload["run_id"])

    assert terminal["progress_ratio"] == 1
    assert terminal["stocks_synced"] >= 10
    assert terminal["latest_trade_date"]
    with sqlite3.connect(settings.db_path) as connection:
        financial_count = connection.execute("SELECT COUNT(*) FROM financial_snapshot").fetchone()[0]
        validation_count = connection.execute("SELECT COUNT(*) FROM validation_cache").fetchone()[0]
        model_health_count = connection.execute("SELECT COUNT(*) FROM model_health_snapshot").fetchone()[0]
    assert financial_count > 0
    assert validation_count == 0
    assert model_health_count == 0

    status_payload = client.get("/data/status").json()
    assert status_payload["recommendation_status"] == "ready"
    assert status_payload["sync_mode"] == "initial_build"
    assert "trust_level" not in status_payload
    assert "paper_performance" not in status_payload
    assert "research_status" in status_payload
    assert "parameter_version" in status_payload
    assert "research_refresh_message" in status_payload

    rankings_payload = client.get("/rankings", params={"limit": 12}).json()
    assert rankings_payload["items"]
    assert "strategy_id" not in rankings_payload
    assert "strategy_name" not in rankings_payload
    assert rankings_payload["parameter_version"] == "default"
    assert rankings_payload["parameter_source"] == "default"
    top_code = rankings_payload["items"][0]["code"]

    detail_payload = client.get(f"/stocks/{top_code}").json()
    assert detail_payload["code"] == top_code
    assert detail_payload["factor_scores"]
    assert detail_payload["contribution_breakdown"]
    assert "recent_journal_entries" not in detail_payload
    assert detail_payload["parameter_version"] == "default"
    assert detail_payload["parameter_source"] == "default"
    assert detail_payload["model_snapshot"]["training_sample_count"] == 0
    assert detail_payload["model_snapshot"]["validation_health"] == "insufficient"
    assert detail_payload["model_snapshot"]["calibration_bucket"] == "rules-default"

    report_payload = client.get("/reports/daily").json()
    assert report_payload["report_date"] == terminal["latest_trade_date"]
    assert report_payload["capital_allocation_hint"]
    assert report_payload["action_checklist"]
    assert "strategy_id" not in report_payload
    assert "candidate_tiers" not in report_payload
    assert report_payload["parameter_version"] == "default"
    assert report_payload["parameter_source"] == "default"

    watchlist_response = client.put(
        "/watchlists/core",
        json={
            "items": [
                {
                    "code": top_code,
                    "note": "观察突破回踩机会",
                    "target_entry": detail_payload["current_price"] * 1.02,
                    "stop_loss": detail_payload["current_price"] * 0.97,
                    "take_profit": detail_payload["current_price"] * 1.08,
                }
            ]
        },
    )
    assert watchlist_response.status_code == 200
    assert watchlist_response.json()["items"][0]["code"] == top_code
    assert "tags" not in watchlist_response.json()["items"][0]


def test_sync_marks_partial_and_skips_warmup_when_financials_fail(tmp_path):
    settings = make_settings(tmp_path)
    client = make_client(tmp_path, provider=BrokenFinancialProvider(), settings=settings)

    sync_response = client.post("/sync/eod")
    terminal = wait_for_sync_completion(client, sync_response.json()["run_id"])

    assert terminal["status"] == "partial"
    assert "已跳过候选预热和日报生成" in terminal["message"]
    assert terminal["latest_trade_date"]

    with sqlite3.connect(settings.db_path) as connection:
        financial_count = connection.execute("SELECT COUNT(*) FROM financial_snapshot").fetchone()[0]
        validation_count = connection.execute("SELECT COUNT(*) FROM validation_cache").fetchone()[0]
        report_count = connection.execute("SELECT COUNT(*) FROM daily_report").fetchone()[0]

    assert financial_count == 0
    assert validation_count == 0
    assert report_count == 0


def test_removed_research_routes_return_404(tmp_path):
    client = make_client(tmp_path)

    removed_paths = [
        ("get", "/strategies"),
        ("put", "/strategies/balanced"),
        ("post", "/backtests"),
        ("get", "/analytics/validation"),
        ("get", "/analytics/paper-performance"),
        ("get", "/signals/2026-03-09"),
        ("post", "/journal/entries"),
    ]

    for method, path in removed_paths:
        response = getattr(client, method)(path)
        assert response.status_code == 404, path


def test_research_diagnostics_route_and_status_fields_work(tmp_path):
    client = make_client(tmp_path, research_provider=StubResearchProvider())

    sync_response = client.post("/sync/eod")
    terminal = wait_for_sync_completion(client, sync_response.json()["run_id"])
    assert terminal["status"] in {"success", "partial"}

    deadline = time.time() + 30
    diagnostics_payload = None
    while time.time() < deadline:
        diagnostics_payload = client.get("/analytics/research-diagnostics").json()
        if diagnostics_payload["status"] in {"ready", "limited"}:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("research diagnostics did not become available in time")

    status_payload = client.get("/data/status").json()
    assert diagnostics_payload is not None
    assert diagnostics_payload["sample_count"] > 0
    assert "source_quality_summary" in diagnostics_payload
    assert status_payload["research_status"] == diagnostics_payload["status"]
    assert status_payload["research_sample_count"] >= diagnostics_payload["sample_count"]


def test_explicit_parameter_version_keeps_default_until_user_applies_research(tmp_path):
    client = make_client(tmp_path, research_provider=StubResearchProvider())

    sync_response = client.post("/sync/eod")
    terminal = wait_for_sync_completion(client, sync_response.json()["run_id"])
    assert terminal["status"] in {"success", "partial"}

    deadline = time.time() + 30
    status_payload = None
    while time.time() < deadline:
        status_payload = client.get("/data/status").json()
        if status_payload["research_status"] == "ready" and status_payload["parameter_version"]:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("approved research parameter did not become available in time")

    assert status_payload is not None
    research_version = status_payload["parameter_version"]

    default_rankings = client.get("/rankings", params={"limit": 12, "parameter_version": "default"})
    assert default_rankings.status_code == 200
    default_rankings_payload = default_rankings.json()
    assert default_rankings_payload["parameter_version"] == "default"
    assert default_rankings_payload["parameter_source"] == "default"

    research_rankings = client.get("/rankings", params={"limit": 12, "parameter_version": research_version})
    assert research_rankings.status_code == 200
    research_rankings_payload = research_rankings.json()
    assert research_rankings_payload["parameter_version"] == research_version
    assert research_rankings_payload["parameter_source"] == "research"
    assert research_rankings_payload["items"]

    top_code = default_rankings_payload["items"][0]["code"]
    default_detail = client.get(f"/stocks/{top_code}", params={"parameter_version": "default"})
    assert default_detail.status_code == 200
    assert default_detail.json()["parameter_source"] == "default"

    research_detail = client.get(f"/stocks/{top_code}", params={"parameter_version": research_version})
    assert research_detail.status_code == 200
    assert research_detail.json()["parameter_version"] == research_version
    assert research_detail.json()["parameter_source"] == "research"

    default_report = client.get("/reports/daily", params={"parameter_version": "default"})
    assert default_report.status_code == 200
    assert default_report.json()["parameter_source"] == "default"

    research_report = client.get("/reports/daily", params={"parameter_version": research_version})
    assert research_report.status_code == 200
    assert research_report.json()["parameter_version"] == research_version
    assert research_report.json()["parameter_source"] == "research"

    missing = client.get("/rankings", params={"parameter_version": "missing-version"})
    assert missing.status_code == 404


def test_sync_progress_ratio_is_monotonic_and_no_strategy_warm_runs_exist(tmp_path):
    settings = make_settings(tmp_path)
    client = make_client(tmp_path, settings=settings)

    run_id = client.post("/sync/eod").json()["run_id"]
    seen_progress: list[float] = []
    deadline = time.time() + 60
    while time.time() < deadline:
        payload = client.get(f"/sync/runs/{run_id}").json()
        seen_progress.append(payload["progress_ratio"])
        if payload["status"] != "running":
            break
        time.sleep(0.05)
    else:
        raise AssertionError("sync did not complete in time")

    assert seen_progress
    assert all(left <= right for left, right in zip(seen_progress, seen_progress[1:], strict=False))
    assert seen_progress[-1] == 1

    with sqlite3.connect(settings.db_path) as connection:
        row = connection.execute(
            "SELECT COUNT(*) FROM strategy_run WHERE run_type = 'strategy_warm'"
        ).fetchone()
    assert row is not None
    assert row[0] == 0


def test_init_db_migrates_old_multistrategy_schema_to_single_strategy(tmp_path):
    settings = make_settings(tmp_path)
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(settings.db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE daily_report (
              report_date TEXT PRIMARY KEY,
              strategy_id TEXT NOT NULL,
              summary_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE validation_cache (
              strategy_id TEXT NOT NULL,
              as_of_date TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (strategy_id, as_of_date)
            );
            CREATE TABLE eligibility_snapshot (
              strategy_id TEXT NOT NULL,
              as_of_date TEXT NOT NULL,
              code TEXT NOT NULL,
              actionable INTEGER NOT NULL DEFAULT 0,
              tier TEXT NOT NULL DEFAULT '观察',
              total_score REAL NOT NULL DEFAULT 0,
              confidence_score REAL NOT NULL DEFAULT 0,
              block_reasons_json TEXT NOT NULL DEFAULT '[]',
              soft_penalties_json TEXT NOT NULL DEFAULT '[]',
              payload_json TEXT NOT NULL DEFAULT '{}',
              updated_at TEXT NOT NULL,
              PRIMARY KEY (strategy_id, as_of_date, code)
            );
            CREATE TABLE model_health_snapshot (
              strategy_id TEXT NOT NULL,
              as_of_date TEXT NOT NULL,
              status TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (strategy_id, as_of_date)
            );
            CREATE TABLE strategy_config (
              id TEXT PRIMARY KEY,
              is_preset INTEGER NOT NULL DEFAULT 0,
              config_json TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE paper_cohort (
              cohort_id TEXT PRIMARY KEY,
              strategy_id TEXT NOT NULL,
              signal_date TEXT NOT NULL,
              source_type TEXT NOT NULL DEFAULT 'historical_replay',
              trust_level TEXT NOT NULL,
              actionable INTEGER NOT NULL DEFAULT 0,
              block_reasons_json TEXT NOT NULL DEFAULT '[]',
              settled_at TEXT,
              payload_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL
            );
            CREATE TABLE paper_trade (
              cohort_id TEXT NOT NULL,
              code TEXT NOT NULL,
              name TEXT NOT NULL,
              board TEXT NOT NULL DEFAULT 'main_board',
              tier TEXT NOT NULL DEFAULT '观察',
              entry_trade_date TEXT,
              entry_price REAL,
              exit_trade_date TEXT,
              exit_price REAL,
              excess_return REAL NOT NULL DEFAULT 0,
              max_drawdown REAL NOT NULL DEFAULT 0,
              outcome TEXT NOT NULL DEFAULT 'flat',
              payload_json TEXT NOT NULL DEFAULT '{}',
              PRIMARY KEY (cohort_id, code)
            );
            CREATE TABLE signal_event (
              event_id TEXT PRIMARY KEY,
              signal_date TEXT NOT NULL,
              code TEXT NOT NULL,
              name TEXT NOT NULL,
              signal_type TEXT NOT NULL,
              severity TEXT NOT NULL,
              title TEXT NOT NULL,
              message TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE journal_entry (
              entry_id TEXT PRIMARY KEY,
              code TEXT NOT NULL,
              entry_date TEXT NOT NULL,
              stage TEXT NOT NULL,
              side TEXT NOT NULL,
              note TEXT NOT NULL,
              tags_json TEXT NOT NULL DEFAULT '[]',
              planned_entry REAL,
              actual_entry REAL,
              risk_note TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL
            );
            CREATE TABLE strategy_run (
              run_id TEXT PRIMARY KEY,
              run_type TEXT NOT NULL,
              status TEXT NOT NULL,
              provider TEXT NOT NULL,
              as_of_date TEXT,
              started_at TEXT NOT NULL,
              finished_at TEXT,
              message TEXT,
              payload_json TEXT
            );
            CREATE TABLE watchlist_item (
              watchlist_id TEXT NOT NULL,
              code TEXT NOT NULL,
              note TEXT NOT NULL DEFAULT '',
              tags_json TEXT NOT NULL DEFAULT '[]',
              target_entry REAL,
              stop_loss REAL,
              take_profit REAL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (watchlist_id, code)
            );
            """
        )
        now = datetime.now(timezone.utc).isoformat()
        connection.execute(
            "INSERT INTO daily_report(report_date, strategy_id, summary_json, created_at) VALUES (?, ?, ?, ?)",
            ("2026-03-09", "balanced", '{"report_date":"2026-03-09","summary":"ok","market_regime":"neutral","candidates":[]}', now),
        )
        connection.execute(
            "INSERT INTO daily_report(report_date, strategy_id, summary_json, created_at) VALUES (?, ?, ?, ?)",
            ("2026-03-08", "custom", '{"report_date":"2026-03-08","summary":"drop","market_regime":"neutral","candidates":[]}', now),
        )
        connection.execute(
            "INSERT INTO validation_cache(strategy_id, as_of_date, payload_json, updated_at) VALUES (?, ?, ?, ?)",
            ("balanced", "2026-03-09", '{"config_key":"balanced","walk_forward_windows":[]}', now),
        )
        connection.execute(
            """
            INSERT INTO eligibility_snapshot(
              strategy_id, as_of_date, code, actionable, tier, total_score, confidence_score,
              block_reasons_json, soft_penalties_json, payload_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("balanced", "2026-03-09", "600519", 1, "A", 88.2, 78.4, "[]", "[]", '{"code":"600519"}', now),
        )
        connection.execute(
            "INSERT INTO model_health_snapshot(strategy_id, as_of_date, status, payload_json, updated_at) VALUES (?, ?, ?, ?, ?)",
            ("balanced", "2026-03-09", "healthy", '{"config_key":"balanced"}', now),
        )
        connection.execute(
            "INSERT INTO strategy_run(run_id, run_type, status, provider, started_at, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
            ("warm-run", "strategy_warm", "success", "akshare", now, "{}"),
        )
        connection.execute(
            "INSERT INTO strategy_run(run_id, run_type, status, provider, started_at, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
            ("sync-run", "sync", "success", "akshare", now, "{}"),
        )
        connection.execute(
            "INSERT INTO watchlist_item(watchlist_id, code, note, tags_json, target_entry, stop_loss, take_profit, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("core", "600519", "保留观察", "[]", 1700.0, 1600.0, 1900.0, now),
        )

    repository = Repository(settings.db_path)
    repository.init_db()

    with sqlite3.connect(settings.db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert "strategy_config" not in tables
        assert "paper_cohort" not in tables
        assert "paper_trade" not in tables
        assert "signal_event" not in tables
        assert "journal_entry" not in tables

        warm_count = connection.execute(
            "SELECT COUNT(*) FROM strategy_run WHERE run_type = 'strategy_warm'"
        ).fetchone()[0]
        sync_count = connection.execute(
            "SELECT COUNT(*) FROM strategy_run WHERE run_type = 'sync'"
        ).fetchone()[0]
        assert warm_count == 0
        assert sync_count == 1

    assert repository.get_daily_report("2026-03-09") is not None
    assert "600519" in repository.get_eligibility_snapshot("2026-03-09")
    assert repository.get_validation_cache("2026-03-09") is not None
    assert repository.get_latest_model_health() is not None
    assert repository.get_watchlist("core")[0]["note"] == "保留观察"
