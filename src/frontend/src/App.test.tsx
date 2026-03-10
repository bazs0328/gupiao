import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { vi } from "vitest";

import App from "./App";

const mockApi = vi.hoisted(() => ({
  getHealth: vi.fn(),
  getLatestSyncRun: vi.fn(),
  getDataStatus: vi.fn(),
  getResearchDiagnostics: vi.fn(),
  getDailyReport: vi.fn(),
  getRankings: vi.fn(),
  getStockDetail: vi.fn(),
  getWatchlist: vi.fn(),
  syncEod: vi.fn(),
  getSyncRun: vi.fn(),
  updateWatchlist: vi.fn()
}));

vi.mock("./api", () => mockApi);

const rankingsPayload = {
  as_of_date: "2026-03-09",
  latest_sync: "2026-03-09T15:00:00",
  actionable: true,
  block_reasons: [],
  parameter_version: "default",
  parameter_source: "default" as const,
  items: [
    {
      code: "600519",
      name: "贵州茅台",
      industry: "消费白马",
      board: "main_board" as const,
      current_price: 1735,
      total_score: 88.2,
      signal_date: "2026-03-09",
      explanation_summary: "走势和基本面都比较靠前，适合先放到第一屏。",
      factor_scores: {},
      section_scores: { technical: 82.1, fundamental: 91.2, risk: 74.5 },
      avg_turnover_20d: 1600000000,
      pe_percentile: 0.42,
      pb_percentile: 0.68,
      confidence_score: 78.4,
      tier: "A" as const,
      risk_flags: ["不建议追高"],
      peer_rank_in_industry: 1,
      expected_holding_window: "10个交易日",
      position_plan: {
        code: "600519",
        name: "贵州茅台",
        suggested_entry: 1718,
        stop_loss: 1596.2,
        take_profit: 1984.8,
        suggested_weight: 0.12
      },
      actionable: true,
      block_reasons: [],
      soft_penalties: [],
      agreement_count: 3
    }
  ]
};

const stockDetailPayload = {
  code: "600519",
  name: "贵州茅台",
  industry: "消费白马",
  signal_date: "2026-03-09",
  current_price: 1735,
  total_score: 88.2,
  explanation_summary: "走势和基本面都比较靠前，当前先等回到计划价位附近。",
  plus_factors: ["ROE", "20/60日均线趋势"],
  minus_factors: ["不建议追高"],
  ineligible_reasons: [],
  factor_scores: [],
  contribution_breakdown: [],
  peer_percentiles: {
    industry_total_score: 100,
    industry_alpha_score: 89,
    industry_quality_score: 91,
    industry_risk_score: 75
  },
  event_calendar: [
    { event_date: "2026-03-09", event_type: "signal", title: "当日收盘信号", severity: "info" as const }
  ],
  model_snapshot: {
    alpha_score: 89,
    quality_score: 91,
    risk_score: 75,
    alpha_prediction: 0.04,
    downside_probability: 0.12,
    confidence_score: 78.4,
    training_window_start: "2025-01-03",
    training_window_end: "2026-03-06",
    training_sample_count: 120,
    validation_health: "healthy" as const,
    precalibrated_total_score: 88.2,
    calibration_bucket: "80-100",
    bucket_sample_count: 96,
    expected_excess_return: 0.032,
    agreement_count: 3
  },
  reason_not_to_buy_now: ["不建议追高"],
  similar_history_samples: [],
  price_history: [
    { date: "2026-03-06", open: 1700, close: 1712, volume: 3200000 },
    { date: "2026-03-09", open: 1712, close: 1735, volume: 3320000 }
  ],
  financial_snapshot: {
    report_date: "2025-12-31",
    publish_date: "2026-03-01",
    roe: 31.8,
    revenue_yoy: 15.2,
    profit_yoy: 14.8,
    cashflow_quality: 1.15,
    pe_ttm: 27,
    pb: 8.2,
    debt_ratio: 21
  },
  position_plan: {
    code: "600519",
    name: "贵州茅台",
    suggested_entry: 1718,
    stop_loss: 1596.2,
    take_profit: 1984.8,
    suggested_weight: 0.12
  },
  evidence_snapshot: {
    board: "main_board" as const,
    latest_price_date: "2026-03-09",
    financial_report_date: "2025-12-31",
    financial_publish_date: "2026-03-01",
    financial_staleness_days: 8,
    factor_coverage_pct: 100,
    data_completeness_score: 100,
    validation_health: "healthy" as const,
    calibration_bucket: "80-100",
    bucket_sample_count: 96,
    expected_bucket_excess_return: 0.032,
    agreement_count: 3,
    recommendation_blocked: false,
    block_reasons: [],
    soft_penalties: [],
    sync_run_id: "run-1"
  },
  parameter_version: "default",
  parameter_source: "default" as const
};

function primeCommonMocks() {
  mockApi.getHealth.mockResolvedValue({ status: "ok", provider: "akshare", latest_sync: "2026-03-09T15:00:00" });
  mockApi.getLatestSyncRun.mockResolvedValue({
    run_id: "",
    status: "failed",
    provider: "akshare",
    stage: "idle",
    sync_mode: "auto",
    started_at: null,
    finished_at: null,
    message: "",
    processed_symbols: 0,
    total_symbols: 0,
    queued_symbols: 0,
    skipped_symbols: 0,
    stocks_synced: 0,
    price_rows_synced: 0,
    latest_trade_date: null,
    warnings: [],
    failure_ratio: 0,
    progress_ratio: 0
  });
  mockApi.getDataStatus.mockResolvedValue({
    status: "ok",
    provider: "akshare",
    provider_mode: "real-a-share",
    sync_mode: "daily_fast",
    latest_sync: "2026-03-09T15:00:00",
    latest_trade_date: "2026-03-09",
    benchmark_code: "000300.SH",
    equity_count: 12,
    watchlist_count: 1,
    warning_count: 0,
    warnings: [],
    data_quality_score: 96,
    coverage_ratio: 0.99,
    financial_lag_days: 42,
    provider_warnings: [],
    recommendation_status: "ready",
    sync_stage: "completed",
    sync_progress: 1,
    failure_ratio: 0,
    block_reasons: [],
    research_status: "limited",
    research_as_of_date: "2026-03-09",
    parameter_version: null,
    research_sample_count: 126,
    research_refresh_message: "研究样本仍受限，当前继续沿用默认参数。"
  });
  mockApi.getResearchDiagnostics.mockResolvedValue({
    status: "limited",
    as_of_date: "2026-03-09",
    parameter_version: null,
    sample_count: 126,
    headline_sample_count: 0,
    source_quality_summary: { estimated: 126 },
    year_breakdown: [],
    regime_breakdown: [],
    factor_drift: [],
    warnings: ["高质量 PIT 样本不足。"],
    parameter: null
  });
  mockApi.getRankings.mockResolvedValue(rankingsPayload);
  mockApi.getDailyReport.mockResolvedValue({
    report_date: "2026-03-09",
    market_regime: "neutral",
    summary: "今天先看 1 只股票，优先从贵州茅台开始，市场状态 neutral。",
    candidates: [
      {
        code: "600519",
        name: "贵州茅台",
        industry: "消费白马",
        current_price: 1735,
        total_score: 88.2,
        explanation_summary: "走势和基本面都比较靠前，适合先放到第一屏。",
        confidence_score: 78.4,
        tier: "A" as const,
        risk_flags: ["不建议追高"],
        position_plan: rankingsPayload.items[0].position_plan
      }
    ],
    capital_allocation_hint: "先小仓试，优先等回踩或放量确认，不要一口气铺太多票。",
    action_checklist: ["先从贵州茅台开始，确认现价是否接近建议介入位。"],
    do_not_chase_flags: ["不建议追高"],
    position_plans: [rankingsPayload.items[0].position_plan],
    watchlist_hits: [],
    generated_at: "2026-03-09T15:01:00",
    actionable: true,
    block_reasons: [],
    parameter_version: "default",
    parameter_source: "default" as const
  });
  mockApi.getWatchlist.mockResolvedValue({
    watchlist_id: "core",
    items: []
  });
  mockApi.getStockDetail.mockResolvedValue(stockDetailPayload);
  mockApi.updateWatchlist.mockResolvedValue({ watchlist_id: "core", items: [] });
}

beforeEach(() => {
  vi.clearAllMocks();
  primeCommonMocks();
});

afterEach(() => {
  vi.useRealTimers();
});

test("renders the simplified workspace and only uses the reduced API surface", async () => {
  render(<App />);

  expect(await screen.findByText("今日候选工作台")).toBeInTheDocument();
  expect(await screen.findByRole("tab", { name: "今日候选" })).toHaveAttribute("aria-selected", "true");
  expect(screen.getByRole("tab", { name: "观察池" })).toBeInTheDocument();
  expect(screen.queryByText("策略实验室")).not.toBeInTheDocument();
  expect(await screen.findByText("观察池计划")).toBeInTheDocument();
  expect(await screen.findByRole("progressbar", { name: "同步进度" })).toHaveAttribute("aria-valuenow", "100");

  expect(mockApi.getRankings).toHaveBeenCalledWith(30, "default");
  expect(mockApi.getDailyReport).toHaveBeenCalledWith("default");
  expect(mockApi.getStockDetail).toHaveBeenCalledWith("600519", "default");
  expect(mockApi.getResearchDiagnostics).not.toHaveBeenCalled();
});

test("switches between the two simplified views", async () => {
  mockApi.getWatchlist.mockResolvedValue({
    watchlist_id: "core",
    items: [
      {
        code: "600519",
        name: "贵州茅台",
        industry: "消费白马",
        note: "等回踩到计划价位附近。",
        target_entry: 1718,
        stop_loss: 1596.2,
        take_profit: 1984.8,
        last_price: 1735,
        updated_at: "2026-03-09T15:01:00"
      }
    ]
  });

  render(<App />);

  const user = userEvent.setup();
  await user.click(await screen.findByRole("tab", { name: "观察池" }));

  expect(await screen.findByRole("tab", { name: "观察池" })).toHaveAttribute("aria-selected", "true");
  expect(screen.getByText("跟踪中的股票")).toBeInTheDocument();
  expect(screen.getAllByText("等回踩到计划价位附近。").length).toBeGreaterThan(0);
});

test("loads hidden research diagnostics only when advanced analysis is opened", async () => {
  render(<App />);

  const user = userEvent.setup();
  await user.click(await screen.findByText("展开高级分析"));

  await waitFor(() => expect(mockApi.getResearchDiagnostics).toHaveBeenCalledTimes(1));
  expect(await screen.findByText("研究诊断")).toBeInTheDocument();
});

test("polls sync runs until completion after clicking sync", async () => {
  mockApi.syncEod.mockResolvedValue({
    run_id: "run-2",
    status: "running",
    provider: "akshare",
    stage: "syncing_prices",
    sync_mode: "daily_fast",
    started_at: "2026-03-09T15:10:00",
    finished_at: null,
    message: "running",
    processed_symbols: 100,
    total_symbols: 4000,
    queued_symbols: 4000,
    skipped_symbols: 0,
    stocks_synced: 80,
    price_rows_synced: 12000,
    latest_trade_date: null,
    warnings: [],
    failure_ratio: 0,
    progress_ratio: 0.08
  });
  mockApi.getSyncRun
    .mockResolvedValueOnce({
      run_id: "run-2",
      status: "running",
      provider: "akshare",
      stage: "syncing_financials",
      sync_mode: "daily_fast",
      started_at: "2026-03-09T15:10:00",
      finished_at: null,
      message: "running",
      processed_symbols: 1200,
      total_symbols: 4000,
      queued_symbols: 1500,
      skipped_symbols: 2500,
      stocks_synced: 1100,
      price_rows_synced: 120000,
      latest_trade_date: "2026-03-09",
      warnings: [],
      failure_ratio: 0.01,
      progress_ratio: 0.76
    })
    .mockResolvedValueOnce({
      run_id: "run-2",
      status: "success",
      provider: "akshare",
      stage: "completed",
      sync_mode: "daily_fast",
      started_at: "2026-03-09T15:10:00",
      finished_at: "2026-03-09T15:12:00",
      message: "done",
      processed_symbols: 4000,
      total_symbols: 4000,
      queued_symbols: 4000,
      skipped_symbols: 0,
      stocks_synced: 4001,
      price_rows_synced: 800000,
      latest_trade_date: "2026-03-09",
      warnings: [],
      failure_ratio: 0.01,
      progress_ratio: 1
    });

  render(<App />);

  const user = userEvent.setup();
  await user.click(await screen.findByRole("button", { name: "同步收盘数据" }));

  await waitFor(() => expect(mockApi.getSyncRun).toHaveBeenCalled());
  await waitFor(() => expect(mockApi.syncEod).toHaveBeenCalledWith("auto"));
  await waitFor(() =>
    expect(screen.getByRole("progressbar", { name: "同步进度" })).toHaveAttribute("aria-valuenow", "100")
  );
  expect(screen.getAllByText(/日常增量/).length).toBeGreaterThan(0);
});

test("shows apply button when research becomes ready and only switches after click", async () => {
  const intervalSpy = vi.spyOn(window, "setInterval");
  mockApi.getDataStatus
    .mockResolvedValueOnce({
      status: "ok",
      provider: "akshare",
      provider_mode: "real-a-share",
      sync_mode: "daily_fast",
      latest_sync: "2026-03-09T15:00:00",
      latest_trade_date: "2026-03-09",
      benchmark_code: "000300.SH",
      equity_count: 12,
      watchlist_count: 0,
      warning_count: 0,
      warnings: [],
      data_quality_score: 96,
      coverage_ratio: 0.99,
      financial_lag_days: 42,
      provider_warnings: [],
      recommendation_status: "ready",
      sync_stage: "completed",
      sync_progress: 1,
      failure_ratio: 0,
      block_reasons: [],
      research_status: "running",
      research_as_of_date: "2026-03-09",
      parameter_version: null,
      research_sample_count: 126,
      research_refresh_message: "研究刷新中，准备好后可手动应用。"
    })
    .mockResolvedValueOnce({
      status: "ok",
      provider: "akshare",
      provider_mode: "real-a-share",
      sync_mode: "daily_fast",
      latest_sync: "2026-03-09T15:00:00",
      latest_trade_date: "2026-03-09",
      benchmark_code: "000300.SH",
      equity_count: 12,
      watchlist_count: 0,
      warning_count: 0,
      warnings: [],
      data_quality_score: 96,
      coverage_ratio: 0.99,
      financial_lag_days: 42,
      provider_warnings: [],
      recommendation_status: "ready",
      sync_stage: "completed",
      sync_progress: 1,
      failure_ratio: 0,
      block_reasons: [],
      research_status: "ready",
      research_as_of_date: "2026-03-09",
      parameter_version: "2026-03-09::balanced_quality_tilt",
      research_sample_count: 168,
      research_refresh_message: "研究结果已就绪，可手动刷新候选。"
    })
    .mockResolvedValue({
      status: "ok",
      provider: "akshare",
      provider_mode: "real-a-share",
      sync_mode: "daily_fast",
      latest_sync: "2026-03-09T15:00:00",
      latest_trade_date: "2026-03-09",
      benchmark_code: "000300.SH",
      equity_count: 12,
      watchlist_count: 0,
      warning_count: 0,
      warnings: [],
      data_quality_score: 96,
      coverage_ratio: 0.99,
      financial_lag_days: 42,
      provider_warnings: [],
      recommendation_status: "ready",
      sync_stage: "completed",
      sync_progress: 1,
      failure_ratio: 0,
      block_reasons: [],
      research_status: "ready",
      research_as_of_date: "2026-03-09",
      parameter_version: "2026-03-09::balanced_quality_tilt",
      research_sample_count: 168,
      research_refresh_message: "研究结果已就绪，可手动刷新候选。"
    });
  mockApi.getRankings
    .mockResolvedValueOnce(rankingsPayload)
    .mockResolvedValueOnce({
      ...rankingsPayload,
      parameter_version: "2026-03-09::balanced_quality_tilt",
      parameter_source: "research" as const,
      items: rankingsPayload.items.map((item) => ({
        ...item,
        total_score: 90.4,
        explanation_summary: "研究参数更偏向质量因子，这只票仍然留在第一位。"
      }))
    });
  mockApi.getDailyReport
    .mockResolvedValueOnce({
      report_date: "2026-03-09",
      market_regime: "neutral",
      summary: "今天先看 1 只股票，优先从贵州茅台开始，市场状态 neutral。",
      candidates: [
        {
          code: "600519",
          name: "贵州茅台",
          industry: "消费白马",
          current_price: 1735,
          total_score: 88.2,
          explanation_summary: "走势和基本面都比较靠前，适合先放到第一屏。",
          confidence_score: 78.4,
          tier: "A" as const,
          risk_flags: ["不建议追高"],
          position_plan: rankingsPayload.items[0].position_plan
        }
      ],
      capital_allocation_hint: "先小仓试，优先等回踩或放量确认，不要一口气铺太多票。",
      action_checklist: ["先从贵州茅台开始，确认现价是否接近建议介入位。"],
      do_not_chase_flags: ["不建议追高"],
      position_plans: [rankingsPayload.items[0].position_plan],
      watchlist_hits: [],
      generated_at: "2026-03-09T15:01:00",
      actionable: true,
      block_reasons: [],
      parameter_version: "default",
      parameter_source: "default" as const
    })
    .mockResolvedValueOnce({
      report_date: "2026-03-09",
      market_regime: "neutral",
      summary: "研究参数已就绪，候选已经切到新的口径。",
      candidates: [
        {
          code: "600519",
          name: "贵州茅台",
          industry: "消费白马",
          current_price: 1735,
          total_score: 90.4,
          explanation_summary: "研究参数更偏向质量因子，这只票仍然留在第一位。",
          confidence_score: 81.2,
          tier: "A" as const,
          risk_flags: ["不建议追高"],
          position_plan: rankingsPayload.items[0].position_plan
        }
      ],
      capital_allocation_hint: "研究参数已应用。",
      action_checklist: ["候选已切换到研究参数。"],
      do_not_chase_flags: ["不建议追高"],
      position_plans: [rankingsPayload.items[0].position_plan],
      watchlist_hits: [],
      generated_at: "2026-03-09T15:05:00",
      actionable: true,
      block_reasons: [],
      parameter_version: "2026-03-09::balanced_quality_tilt",
      parameter_source: "research" as const
    });
  mockApi.getStockDetail
    .mockResolvedValueOnce(stockDetailPayload)
    .mockResolvedValueOnce({
      ...stockDetailPayload,
      total_score: 90.4,
      explanation_summary: "研究参数已应用，质量因子权重更高。",
      parameter_version: "2026-03-09::balanced_quality_tilt",
      parameter_source: "research" as const
    });

  render(<App />);

  expect(await screen.findByText("研究刷新中，当前候选基于默认参数 / 默认参数 / 覆盖至 2026-03-09 / 样本 126")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "应用研究结果" })).not.toBeInTheDocument();

  await act(async () => {
    const intervalCallback = intervalSpy.mock.calls.at(-1)?.[0];
    if (typeof intervalCallback === "function") {
      await intervalCallback();
    }
  });

  await waitFor(() => expect(mockApi.getDataStatus).toHaveBeenCalledTimes(2));
  expect(await screen.findByRole("button", { name: "应用研究结果" })).toBeInTheDocument();
  expect(mockApi.getRankings).toHaveBeenCalledWith(30, "default");
  expect(mockApi.getDailyReport).toHaveBeenCalledWith("default");
  expect(mockApi.getStockDetail).toHaveBeenCalledWith("600519", "default");

  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "应用研究结果" }));

  await waitFor(() => expect(mockApi.getRankings).toHaveBeenLastCalledWith(30, "2026-03-09::balanced_quality_tilt"));
  expect(mockApi.getDailyReport).toHaveBeenLastCalledWith("2026-03-09::balanced_quality_tilt");
  expect(mockApi.getStockDetail).toHaveBeenLastCalledWith("600519", "2026-03-09::balanced_quality_tilt");
  expect(await screen.findByText(/当前候选已应用研究参数/)).toBeInTheDocument();
  intervalSpy.mockRestore();
});
