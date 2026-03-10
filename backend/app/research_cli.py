from __future__ import annotations

import argparse

from .config import AppSettings
from .db import Repository
from .research_db import ResearchRepository
from .services.research_service import ResearchService


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh the local offline research calibration database.")
    parser.add_argument("--mode", choices=["incremental", "rebuild"], default="incremental")
    args = parser.parse_args()

    settings = AppSettings.from_env()
    business_repository = Repository(settings.db_path)
    business_repository.init_db()
    research_repository = ResearchRepository(settings.research_db_path)
    research_repository.init_db()

    service = ResearchService(
        research_repository,
        business_repository,
        settings.benchmark_code,
    )
    result = service.refresh_blocking(mode=args.mode)
    print(result.get("status", "unknown"))


if __name__ == "__main__":
    main()
