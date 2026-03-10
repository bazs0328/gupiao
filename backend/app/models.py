from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


MarketRegime = Literal["bullish", "neutral", "cautious"]
RecommendationStatus = Literal["ready", "paused"]
ValidationHealth = Literal["healthy", "degraded", "insufficient"]
SyncStatus = Literal["running", "success", "partial", "failed"]
SyncMode = Literal["auto", "full", "initial_build", "daily_fast"]
BoardType = Literal["main_board", "chi_next", "index"]
ResearchStatus = Literal["idle", "running", "limited", "ready", "failed"]
ParameterSource = Literal["default", "research"]


class StockPoolRules(BaseModel):
    min_listing_days: int = 120
    min_avg_turnover_20d: float = 200_000_000
    min_price: float = 5.0
    max_pe_percentile: float = 0.9
    max_pb_percentile: float = 0.9
    exclude_st: bool = True
    exclude_suspended: bool = True
    exclude_negative_revenue_yoy: bool = False
    exclude_negative_profit_yoy: bool = True


class RiskGuardrails(BaseModel):
    industry_cap: int = 2
    max_volatility_20d: float = 0.055
    max_position_weight: float = 0.14
    max_drawdown_alert: float = 0.08
    avoid_earnings_window_days: int = 5


class RebalanceSettings(BaseModel):
    frequency: Literal["weekly"] = "weekly"
    top_n: int = 8
    holding_period_days: int = 10


class ExecutionAssumptions(BaseModel):
    commission_bps: float = 5
    slippage_bps: float = 8
    stamp_duty_bps: float = 10
    limit_move_pct: float = 0.095


class NotificationThresholds(BaseModel):
    break_trend_pct: float = 0.02
    volume_surge_score: float = 80
    watchlist_entry_buffer_pct: float = 0.02


class StrategyConfig(BaseModel):
    name: str
    description: str = ""
    weights: dict[str, float] = Field(default_factory=dict)
    confidence_floors: dict[str, float] = Field(default_factory=dict)
    stock_pool: StockPoolRules = Field(default_factory=StockPoolRules)
    risk: RiskGuardrails = Field(default_factory=RiskGuardrails)
    rebalance: RebalanceSettings = Field(default_factory=RebalanceSettings)
    execution: ExecutionAssumptions = Field(default_factory=ExecutionAssumptions)
    notifications: NotificationThresholds = Field(default_factory=NotificationThresholds)

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        legacy_stock_pool_keys = {
            "min_listing_days",
            "min_avg_turnover_20d",
            "min_price",
            "max_pe_percentile",
            "max_pb_percentile",
            "exclude_st",
            "exclude_suspended",
            "exclude_negative_revenue_yoy",
            "exclude_negative_profit_yoy",
        }
        legacy_rebalance_keys = {"top_n", "rebalance_frequency", "holding_period_days"}
        if not (legacy_stock_pool_keys & value.keys() or legacy_rebalance_keys & value.keys()):
            return value

        migrated = dict(value)
        stock_pool = dict(migrated.get("stock_pool", {}))
        rebalance = dict(migrated.get("rebalance", {}))

        for key in legacy_stock_pool_keys:
            if key in migrated:
                stock_pool.setdefault(key, migrated.pop(key))

        if "top_n" in migrated:
            rebalance.setdefault("top_n", migrated.pop("top_n"))
        if "rebalance_frequency" in migrated:
            rebalance.setdefault("frequency", migrated.pop("rebalance_frequency"))
        if "holding_period_days" in migrated:
            rebalance.setdefault("holding_period_days", migrated.pop("holding_period_days"))

        migrated["stock_pool"] = stock_pool
        migrated["rebalance"] = rebalance
        return migrated


class SyncResponse(BaseModel):
    run_id: str
    status: SyncStatus
    provider: str
    stage: str
    sync_mode: SyncMode = "auto"
    started_at: str | None = None
    finished_at: str | None = None
    message: str = ""
    processed_symbols: int = 0
    total_symbols: int = 0
    queued_symbols: int = 0
    skipped_symbols: int = 0
    stocks_synced: int = 0
    price_rows_synced: int = 0
    latest_trade_date: str | None = None
    warnings: list[str] = Field(default_factory=list)
    failure_ratio: float = 0.0
    progress_ratio: float = 0.0


class DataStatusResponse(BaseModel):
    status: Literal["ok", "warning", "error"]
    provider: str
    provider_mode: str
    sync_mode: SyncMode = "auto"
    latest_sync: str | None = None
    latest_trade_date: str | None = None
    benchmark_code: str
    equity_count: int
    watchlist_count: int
    warning_count: int = 0
    warnings: list[str] = Field(default_factory=list)
    data_quality_score: float = 0.0
    coverage_ratio: float = 0.0
    financial_lag_days: int = 0
    provider_warnings: list[str] = Field(default_factory=list)
    recommendation_status: RecommendationStatus = "paused"
    sync_stage: str = "idle"
    sync_progress: float = 0.0
    failure_ratio: float = 0.0
    block_reasons: list[str] = Field(default_factory=list)
    research_status: ResearchStatus = "idle"
    research_as_of_date: str | None = None
    parameter_version: str | None = None
    research_sample_count: int = 0
    research_refresh_message: str = ""


class PositionPlan(BaseModel):
    code: str
    name: str
    suggested_entry: float
    stop_loss: float
    take_profit: float
    suggested_weight: float


class SoftPenaltyItem(BaseModel):
    label: str
    points: float


class ContributionItem(BaseModel):
    factor: str
    label: str
    group: str
    raw_value: float
    score: float
    contribution: float
    direction: Literal["positive", "negative"]


class RankingRow(BaseModel):
    code: str
    name: str
    industry: str
    board: BoardType = "main_board"
    current_price: float
    total_score: float
    signal_date: str
    explanation_summary: str
    factor_scores: dict[str, float]
    section_scores: dict[str, float]
    avg_turnover_20d: float
    pe_percentile: float
    pb_percentile: float
    confidence_score: float
    tier: Literal["A", "B", "观察"]
    risk_flags: list[str] = Field(default_factory=list)
    peer_rank_in_industry: int = 1
    expected_holding_window: str = "10个交易日"
    position_plan: PositionPlan | None = None
    actionable: bool = False
    block_reasons: list[str] = Field(default_factory=list)
    soft_penalties: list[SoftPenaltyItem] = Field(default_factory=list)
    agreement_count: int = 0


class RankingsResponse(BaseModel):
    as_of_date: str
    latest_sync: str | None = None
    items: list[RankingRow]
    actionable: bool = False
    block_reasons: list[str] = Field(default_factory=list)
    parameter_version: str = "default"
    parameter_source: ParameterSource = "default"


class PricePoint(BaseModel):
    date: str
    open: float
    close: float
    volume: float


class FactorDetail(BaseModel):
    factor: str
    label: str
    group: str
    raw_value: float
    score: float
    contribution: float = 0.0


class FinancialView(BaseModel):
    report_date: str
    publish_date: str
    roe: float
    revenue_yoy: float
    profit_yoy: float
    cashflow_quality: float
    pe_ttm: float
    pb: float
    debt_ratio: float


class EventCalendarItem(BaseModel):
    event_date: str
    event_type: str
    title: str
    severity: Literal["info", "warning", "critical"] = "info"


class SimilarHistorySample(BaseModel):
    signal_date: str
    code: str
    name: str
    industry: str
    forward_excess_return: float
    summary: str


class ModelSnapshotView(BaseModel):
    alpha_score: float
    quality_score: float
    risk_score: float
    alpha_prediction: float
    downside_probability: float
    confidence_score: float
    training_window_start: str | None = None
    training_window_end: str | None = None
    training_sample_count: int = 0
    validation_health: ValidationHealth = "insufficient"
    precalibrated_total_score: float = 0.0
    calibration_bucket: str = "unknown"
    bucket_sample_count: int = 0
    expected_excess_return: float = 0.0
    agreement_count: int = 0


class EvidenceSnapshot(BaseModel):
    board: BoardType
    latest_price_date: str | None = None
    financial_report_date: str | None = None
    financial_publish_date: str | None = None
    financial_staleness_days: int = 0
    factor_coverage_pct: float = 0.0
    data_completeness_score: float = 0.0
    validation_health: ValidationHealth = "insufficient"
    calibration_bucket: str = "unknown"
    bucket_sample_count: int = 0
    expected_bucket_excess_return: float = 0.0
    agreement_count: int = 0
    recommendation_blocked: bool = True
    block_reasons: list[str] = Field(default_factory=list)
    soft_penalties: list[SoftPenaltyItem] = Field(default_factory=list)
    sync_run_id: str | None = None


class StockDetailResponse(BaseModel):
    code: str
    name: str
    industry: str
    signal_date: str
    current_price: float
    total_score: float
    explanation_summary: str
    plus_factors: list[str]
    minus_factors: list[str]
    ineligible_reasons: list[str] = Field(default_factory=list)
    factor_scores: list[FactorDetail]
    contribution_breakdown: list[ContributionItem] = Field(default_factory=list)
    peer_percentiles: dict[str, float] = Field(default_factory=dict)
    event_calendar: list[EventCalendarItem] = Field(default_factory=list)
    model_snapshot: ModelSnapshotView | None = None
    reason_not_to_buy_now: list[str] = Field(default_factory=list)
    similar_history_samples: list[SimilarHistorySample] = Field(default_factory=list)
    price_history: list[PricePoint]
    financial_snapshot: FinancialView | None = None
    position_plan: PositionPlan | None = None
    evidence_snapshot: EvidenceSnapshot | None = None
    parameter_version: str = "default"
    parameter_source: ParameterSource = "default"


class WatchlistItemInput(BaseModel):
    code: str
    note: str = ""
    target_entry: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None


class WatchlistItem(WatchlistItemInput):
    name: str = ""
    industry: str = ""
    last_price: float | None = None
    updated_at: str


class WatchlistUpdate(BaseModel):
    items: list[WatchlistItemInput] = Field(default_factory=list)


class WatchlistResponse(BaseModel):
    watchlist_id: str
    items: list[WatchlistItem]


class DailyCandidate(BaseModel):
    code: str
    name: str
    industry: str
    current_price: float | None = None
    total_score: float
    explanation_summary: str
    confidence_score: float
    tier: Literal["A", "B", "观察"]
    risk_flags: list[str] = Field(default_factory=list)
    position_plan: PositionPlan | None = None


class DailyReportResponse(BaseModel):
    report_date: str
    market_regime: MarketRegime
    summary: str
    candidates: list[DailyCandidate]
    capital_allocation_hint: str = ""
    action_checklist: list[str] = Field(default_factory=list)
    do_not_chase_flags: list[str] = Field(default_factory=list)
    position_plans: list[PositionPlan] = Field(default_factory=list)
    watchlist_hits: list[WatchlistItem] = Field(default_factory=list)
    generated_at: str
    actionable: bool = False
    block_reasons: list[str] = Field(default_factory=list)
    parameter_version: str = "default"
    parameter_source: ParameterSource = "default"


class ValidationWindow(BaseModel):
    signal_date: str
    train_start: str | None = None
    train_end: str | None = None
    sample_count: int
    regime: MarketRegime
    excess_return: float
    hit_rate: float
    max_drawdown: float
    ic: float


class ValidationBucket(BaseModel):
    label: str
    sample_count: int
    excess_return: float
    win_rate: float
    max_drawdown: float
    hit_rate: float
    ic: float


class ValidationSummary(BaseModel):
    as_of_date: str
    walk_forward_windows: list[ValidationWindow] = Field(default_factory=list)
    regime_breakdown: list[ValidationBucket] = Field(default_factory=list)
    industry_breakdown: list[ValidationBucket] = Field(default_factory=list)
    current_model_health: ValidationHealth = "insufficient"
    recent_six_month_excess_return: float = 0.0


class ResearchYearStat(BaseModel):
    label: str
    sample_count: int
    headline_sample_count: int
    excess_return: float
    hit_rate: float
    max_drawdown: float


class ResearchFactorDriftItem(BaseModel):
    factor: str
    recent_mean: float
    baseline_mean: float
    drift: float


class ResearchParameterView(BaseModel):
    version: str
    created_at: str
    approved_at: str | None = None
    stability_score: float = 0.0
    sample_count: int = 0
    headline_sample_count: int = 0
    config: StrategyConfig


class ResearchDiagnosticsResponse(BaseModel):
    status: ResearchStatus = "idle"
    as_of_date: str | None = None
    parameter_version: str | None = None
    sample_count: int = 0
    headline_sample_count: int = 0
    source_quality_summary: dict[str, int] = Field(default_factory=dict)
    year_breakdown: list[ResearchYearStat] = Field(default_factory=list)
    regime_breakdown: list[ResearchYearStat] = Field(default_factory=list)
    factor_drift: list[ResearchFactorDriftItem] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    parameter: ResearchParameterView | None = None


class HealthResponse(BaseModel):
    status: str
    provider: str
    latest_sync: str | None = None
