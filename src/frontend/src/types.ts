export type MarketRegime = "bullish" | "neutral" | "cautious";
export type RecommendationStatus = "ready" | "paused";
export type ValidationHealth = "healthy" | "degraded" | "insufficient";
export type SyncStatus = "running" | "success" | "partial" | "failed";
export type SyncMode = "auto" | "full" | "initial_build" | "daily_fast";
export type BoardType = "main_board" | "chi_next" | "index";
export type ResearchStatus = "idle" | "running" | "limited" | "ready" | "failed";
export type ParameterSource = "default" | "research";

export type SyncResponse = {
  run_id: string;
  status: SyncStatus;
  provider: string;
  stage: string;
  sync_mode: SyncMode;
  started_at: string | null;
  finished_at: string | null;
  message: string;
  processed_symbols: number;
  total_symbols: number;
  queued_symbols: number;
  skipped_symbols: number;
  stocks_synced: number;
  price_rows_synced: number;
  latest_trade_date: string | null;
  warnings: string[];
  failure_ratio: number;
  progress_ratio: number;
};

export type DataStatusResponse = {
  status: "ok" | "warning" | "error";
  provider: string;
  provider_mode: string;
  sync_mode: SyncMode;
  latest_sync: string | null;
  latest_trade_date: string | null;
  benchmark_code: string;
  equity_count: number;
  watchlist_count: number;
  warning_count: number;
  warnings: string[];
  data_quality_score: number;
  coverage_ratio: number;
  financial_lag_days: number;
  provider_warnings: string[];
  recommendation_status: RecommendationStatus;
  sync_stage: string;
  sync_progress: number;
  failure_ratio: number;
  block_reasons: string[];
  research_status: ResearchStatus;
  research_as_of_date: string | null;
  parameter_version: string | null;
  research_sample_count: number;
  research_refresh_message: string;
};

export type PositionPlan = {
  code: string;
  name: string;
  suggested_entry: number;
  stop_loss: number;
  take_profit: number;
  suggested_weight: number;
};

export type SoftPenaltyItem = {
  label: string;
  points: number;
};

export type ContributionItem = {
  factor: string;
  label: string;
  group: string;
  raw_value: number;
  score: number;
  contribution: number;
  direction: "positive" | "negative";
};

export type RankingRow = {
  code: string;
  name: string;
  industry: string;
  board: BoardType;
  current_price: number;
  total_score: number;
  signal_date: string;
  explanation_summary: string;
  factor_scores: Record<string, number>;
  section_scores: Record<string, number>;
  avg_turnover_20d: number;
  pe_percentile: number;
  pb_percentile: number;
  confidence_score: number;
  tier: "A" | "B" | "观察";
  risk_flags: string[];
  peer_rank_in_industry: number;
  expected_holding_window: string;
  position_plan: PositionPlan | null;
  actionable: boolean;
  block_reasons: string[];
  soft_penalties: SoftPenaltyItem[];
  agreement_count: number;
};

export type RankingsResponse = {
  as_of_date: string;
  latest_sync: string | null;
  items: RankingRow[];
  actionable: boolean;
  block_reasons: string[];
  parameter_version: string;
  parameter_source: ParameterSource;
};

export type FactorDetail = {
  factor: string;
  label: string;
  group: string;
  raw_value: number;
  score: number;
  contribution: number;
};

export type PricePoint = {
  date: string;
  open: number;
  close: number;
  volume: number;
};

export type FinancialView = {
  report_date: string;
  publish_date: string;
  roe: number;
  revenue_yoy: number;
  profit_yoy: number;
  cashflow_quality: number;
  pe_ttm: number;
  pb: number;
  debt_ratio: number;
};

export type EventCalendarItem = {
  event_date: string;
  event_type: string;
  title: string;
  severity: "info" | "warning" | "critical";
};

export type SimilarHistorySample = {
  signal_date: string;
  code: string;
  name: string;
  industry: string;
  forward_excess_return: number;
  summary: string;
};

export type ModelSnapshotView = {
  alpha_score: number;
  quality_score: number;
  risk_score: number;
  alpha_prediction: number;
  downside_probability: number;
  confidence_score: number;
  training_window_start: string | null;
  training_window_end: string | null;
  training_sample_count: number;
  validation_health: ValidationHealth;
  precalibrated_total_score: number;
  calibration_bucket: string;
  bucket_sample_count: number;
  expected_excess_return: number;
  agreement_count: number;
};

export type EvidenceSnapshot = {
  board: BoardType;
  latest_price_date: string | null;
  financial_report_date: string | null;
  financial_publish_date: string | null;
  financial_staleness_days: number;
  factor_coverage_pct: number;
  data_completeness_score: number;
  validation_health: ValidationHealth;
  calibration_bucket: string;
  bucket_sample_count: number;
  expected_bucket_excess_return: number;
  agreement_count: number;
  recommendation_blocked: boolean;
  block_reasons: string[];
  soft_penalties: SoftPenaltyItem[];
  sync_run_id: string | null;
};

export type StockDetailResponse = {
  code: string;
  name: string;
  industry: string;
  signal_date: string;
  current_price: number;
  total_score: number;
  explanation_summary: string;
  plus_factors: string[];
  minus_factors: string[];
  ineligible_reasons: string[];
  factor_scores: FactorDetail[];
  contribution_breakdown: ContributionItem[];
  peer_percentiles: Record<string, number>;
  event_calendar: EventCalendarItem[];
  model_snapshot: ModelSnapshotView | null;
  reason_not_to_buy_now: string[];
  similar_history_samples: SimilarHistorySample[];
  price_history: PricePoint[];
  financial_snapshot: FinancialView | null;
  position_plan: PositionPlan | null;
  evidence_snapshot: EvidenceSnapshot | null;
  parameter_version: string;
  parameter_source: ParameterSource;
};

export type WatchlistItemInput = {
  code: string;
  note: string;
  target_entry?: number | null;
  stop_loss?: number | null;
  take_profit?: number | null;
};

export type WatchlistItem = WatchlistItemInput & {
  name: string;
  industry: string;
  last_price: number | null;
  updated_at: string;
};

export type WatchlistUpdate = {
  items: WatchlistItemInput[];
};

export type WatchlistResponse = {
  watchlist_id: string;
  items: WatchlistItem[];
};

export type DailyCandidate = {
  code: string;
  name: string;
  industry: string;
  current_price: number | null;
  total_score: number;
  explanation_summary: string;
  confidence_score: number;
  tier: "A" | "B" | "观察";
  risk_flags: string[];
  position_plan: PositionPlan | null;
};

export type DailyReportResponse = {
  report_date: string;
  market_regime: MarketRegime;
  summary: string;
  candidates: DailyCandidate[];
  capital_allocation_hint: string;
  action_checklist: string[];
  do_not_chase_flags: string[];
  position_plans: PositionPlan[];
  watchlist_hits: WatchlistItem[];
  generated_at: string;
  actionable: boolean;
  block_reasons: string[];
  parameter_version: string;
  parameter_source: ParameterSource;
};

export type ResearchYearStat = {
  label: string;
  sample_count: number;
  headline_sample_count: number;
  excess_return: number;
  hit_rate: number;
  max_drawdown: number;
};

export type ResearchFactorDriftItem = {
  factor: string;
  recent_mean: number;
  baseline_mean: number;
  drift: number;
};

export type ResearchParameterView = {
  version: string;
  created_at: string;
  approved_at: string | null;
  stability_score: number;
  sample_count: number;
  headline_sample_count: number;
  config: {
    name: string;
    description: string;
    weights: Record<string, number>;
    confidence_floors: Record<string, number>;
  };
};

export type ResearchDiagnosticsResponse = {
  status: ResearchStatus;
  as_of_date: string | null;
  parameter_version: string | null;
  sample_count: number;
  headline_sample_count: number;
  source_quality_summary: Record<string, number>;
  year_breakdown: ResearchYearStat[];
  regime_breakdown: ResearchYearStat[];
  factor_drift: ResearchFactorDriftItem[];
  warnings: string[];
  parameter: ResearchParameterView | null;
};

export type HealthResponse = {
  status: string;
  provider: string;
  latest_sync: string | null;
};
