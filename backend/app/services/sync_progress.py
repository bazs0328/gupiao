from __future__ import annotations

from typing import Final


STAGE_BOUNDS: Final[dict[str, tuple[float, float]]] = {
    "queued": (0.0, 0.0),
    "loading_universe": (0.0, 0.05),
    "syncing_prices": (0.05, 0.70),
    "syncing_financials": (0.70, 0.90),
    "computing_factors": (0.90, 0.95),
    "warming_validation": (0.95, 0.98),
    "publishing": (0.98, 1.0),
    "completed": (1.0, 1.0),
    "failed": (0.0, 0.0),
    "idle": (0.0, 0.0),
}


def compute_progress_ratio(
    stage: str,
    processed_symbols: int,
    total_symbols: int,
    status: str | None = None,
    queued_symbols: int = 0,
) -> float:
    if status in {"success", "partial"} or stage == "completed":
        return 1.0

    start, end = STAGE_BOUNDS.get(stage, (0.0, 0.0))
    if end <= start:
        return start
    denominator = queued_symbols if queued_symbols > 0 else total_symbols
    if denominator <= 0:
        return start
    progress = max(0.0, min(processed_symbols / max(denominator, 1), 1.0))
    return round(start + (end - start) * progress, 4)
