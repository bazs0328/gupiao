from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import sys
from typing import Literal

ROOT_DIR = Path(__file__).resolve().parents[2]
PYDEPS_DIR = ROOT_DIR / ".pydeps"
if PYDEPS_DIR.exists() and str(PYDEPS_DIR) not in sys.path:
    sys.path.insert(0, str(PYDEPS_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
import uvicorn

from .config import AppSettings
from .db import Repository
from .models import (
    DataStatusResponse,
    DailyReportResponse,
    HealthResponse,
    ResearchDiagnosticsResponse,
    RankingsResponse,
    StockDetailResponse,
    SyncResponse,
    WatchlistResponse,
    WatchlistUpdate,
)
from .research_db import ResearchRepository
from .services.analytics_service import AnalyticsService
from .services.ranking_service import RankingService
from .services.research_service import ResearchService
from .services.strategy_service import StrategyService
from .services.sync_service import SyncService
from .services.workspace_service import WorkspaceService


class SPAStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        try:
            return await super().get_response(path, scope)
        except (FileNotFoundError, StarletteHTTPException) as exc:
            status_code = getattr(exc, "status_code", None)
            if status_code not in (None, 404):
                raise
        return await super().get_response("index.html", scope)


def _resolve_frontend_dir(frontend_dir: Path | None = None) -> Path | None:
    candidate = frontend_dir or (Path(os.environ["GUPIAO_FRONTEND_DIR"]) if os.getenv("GUPIAO_FRONTEND_DIR") else None)
    if not candidate:
        return None
    resolved = candidate.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Frontend bundle directory does not exist: {resolved}")
    return resolved


def create_app(
    settings: AppSettings | None = None,
    frontend_dir: Path | None = None,
    provider=None,
    research_provider=None,
) -> FastAPI:
    settings = settings or AppSettings.from_env()
    repository = Repository(settings.db_path)
    repository.init_db()
    research_repository = ResearchRepository(settings.research_db_path)
    research_repository.init_db()
    abandoned_sync_runs = repository.fail_running_runs("sync", "上次同步在应用关闭或重启前未完成，已终止。")
    if abandoned_sync_runs:
        logging.warning(
            "Marked stale background runs as failed on startup: sync=%s",
            abandoned_sync_runs,
        )
    abandoned_research_runs = research_repository.fail_running_runs("上次研究刷新在应用关闭或重启前未完成，已终止。")
    if abandoned_research_runs:
        logging.warning("Marked stale research runs as failed on startup: %s", abandoned_research_runs)
    research_service = ResearchService(
        research_repository,
        repository,
        settings.benchmark_code,
        provider=research_provider,
    )
    strategy_service = StrategyService(research_service=research_service)
    analytics_service = AnalyticsService(repository, settings.benchmark_code)
    ranking_service = RankingService(repository, strategy_service, analytics_service)
    workspace_service = WorkspaceService(
        repository,
        strategy_service,
        ranking_service,
        analytics_service,
        settings.benchmark_code,
        research_service=research_service,
    )
    sync_service = SyncService(
        repository,
        settings,
        provider=provider,
        analytics_service=analytics_service,
        strategy_service=strategy_service,
        workspace_service=workspace_service,
        research_service=research_service,
    )

    app = FastAPI(title="Gupiao Desktop API", version="0.2.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        latest_sync = repository.get_latest_run("sync")
        return HealthResponse(
            status="ok",
            provider="akshare",
            latest_sync=latest_sync["finished_at"] if latest_sync else None,
        )

    @app.get("/data/status", response_model=DataStatusResponse)
    def get_data_status() -> DataStatusResponse:
        return workspace_service.get_data_status()

    @app.get("/analytics/research-diagnostics", response_model=ResearchDiagnosticsResponse)
    def get_research_diagnostics() -> ResearchDiagnosticsResponse:
        return research_service.get_diagnostics()

    @app.post("/sync/eod", response_model=SyncResponse)
    def sync_eod(mode: Literal["auto", "full"] = "auto") -> SyncResponse:
        return sync_service.start_or_resume_sync(mode=mode)

    @app.get("/sync/runs/latest", response_model=SyncResponse)
    def get_latest_sync_run() -> SyncResponse:
        response = sync_service.get_latest_run()
        if not response:
            return SyncResponse(
                run_id="",
                status="failed",
                provider="akshare",
                stage="idle",
                message="No sync run has been started yet.",
            )
        return response

    @app.get("/sync/runs/{run_id}", response_model=SyncResponse)
    def get_sync_run(run_id: str) -> SyncResponse:
        response = sync_service.get_run(run_id)
        if not response:
            raise StarletteHTTPException(status_code=404, detail="Sync run not found.")
        return response

    @app.get("/rankings", response_model=RankingsResponse)
    def get_rankings(limit: int = 30, parameter_version: str | None = None) -> RankingsResponse:
        return ranking_service.get_rankings(limit=limit, parameter_version=parameter_version)

    @app.get("/stocks/{code}", response_model=StockDetailResponse)
    def get_stock_detail(code: str, parameter_version: str | None = None) -> StockDetailResponse:
        return ranking_service.get_stock_detail(code=code, parameter_version=parameter_version)

    @app.get("/reports/daily", response_model=DailyReportResponse)
    def get_daily_report(report_date: str | None = None, parameter_version: str | None = None) -> DailyReportResponse:
        return workspace_service.get_daily_report(report_date=report_date, parameter_version=parameter_version)

    @app.get("/watchlists/{watchlist_id}", response_model=WatchlistResponse)
    def get_watchlist(watchlist_id: str) -> WatchlistResponse:
        return workspace_service.get_watchlist(watchlist_id)

    @app.put("/watchlists/{watchlist_id}", response_model=WatchlistResponse)
    def update_watchlist(watchlist_id: str, payload: WatchlistUpdate) -> WatchlistResponse:
        return workspace_service.update_watchlist(watchlist_id, payload)

    resolved_frontend_dir = _resolve_frontend_dir(frontend_dir)
    if resolved_frontend_dir:
        app.mount("/", SPAStaticFiles(directory=str(resolved_frontend_dir), html=True), name="frontend")

    return app


app = create_app()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local FastAPI backend for Gupiao Lab.")
    parser.add_argument("--host", default=AppSettings.from_env().host)
    parser.add_argument("--port", type=int, default=AppSettings.from_env().port)
    args = parser.parse_args()
    uvicorn.run("backend.app.main:create_app", factory=True, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
