import type { DataStatusResponse, HealthResponse, ParameterSource, SyncResponse } from "../types";

type StatusStripProps = {
  health: HealthResponse | null;
  dataStatus: DataStatusResponse | null;
  syncInfo: SyncResponse | null;
  isSyncing: boolean;
  candidateCount: number;
  currentParameterVersion: string;
  currentParameterSource: ParameterSource;
  canApplyResearchResults: boolean;
  isApplyingResearchResults: boolean;
  onApplyResearchResults: () => Promise<void>;
  onSync: () => Promise<void>;
};

function formatTime(value: string | null | undefined) {
  if (!value) {
    return "未同步";
  }
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

function getStatusCopy(status: DataStatusResponse["status"] | undefined) {
  switch (status) {
    case "warning":
      return "需关注";
    case "error":
      return "异常";
    default:
      return "正常";
  }
}

function getSyncStageCopy(stage: string | undefined) {
  switch (stage) {
    case "loading_universe":
      return "加载股票池";
    case "syncing_prices":
      return "抓取日线";
    case "syncing_financials":
      return "抓取财务";
    case "computing_factors":
      return "计算候选";
    case "warming_validation":
      return "收尾检查";
    case "publishing":
      return "生成结果";
    case "completed":
      return "已完成";
    case "failed":
      return "失败";
    case "queued":
      return "排队中";
    default:
      return "等待同步";
  }
}

function getSyncModeCopy(mode: SyncResponse["sync_mode"] | DataStatusResponse["sync_mode"] | undefined) {
  switch (mode) {
    case "initial_build":
      return "首次建库";
    case "daily_fast":
      return "日常增量";
    case "full":
      return "全量重刷";
    default:
      return "自动";
  }
}

function getResearchCopy(status: DataStatusResponse["research_status"] | undefined) {
  switch (status) {
    case "running":
      return "研究刷新中";
    case "ready":
      return "多年统计就绪";
    case "limited":
      return "研究受限";
    case "failed":
      return "研究异常";
    default:
      return "未就绪";
  }
}

function buildResearchCopy(
  dataStatus: DataStatusResponse | null,
  currentParameterVersion: string,
  currentParameterSource: ParameterSource,
  canApplyResearchResults: boolean
) {
  if (!dataStatus) {
    return "多年统计未就绪";
  }

  let baseCopy = dataStatus.research_refresh_message || `${getResearchCopy(dataStatus.research_status)}。`;
  if (currentParameterSource === "research") {
    baseCopy = "当前候选已应用研究参数";
  } else if (canApplyResearchResults) {
    baseCopy = "研究结果已就绪，可刷新候选";
  } else if (dataStatus.research_status === "running") {
    baseCopy = "研究刷新中，当前候选基于默认参数";
  } else if (dataStatus.research_status === "limited") {
    baseCopy = "研究受限，当前候选基于默认参数";
  } else if (dataStatus.research_status === "failed") {
    baseCopy = "研究异常，当前候选基于默认参数";
  } else if (currentParameterSource === "default") {
    baseCopy = "多年研究结果尚未准备好，当前候选基于默认参数";
  }

  const metaParts = [
    currentParameterSource === "research"
      ? currentParameterVersion
      : dataStatus.parameter_version
        ? `最新 ${dataStatus.parameter_version}`
        : "默认参数",
    dataStatus.research_as_of_date ? `覆盖至 ${dataStatus.research_as_of_date}` : null,
    dataStatus.research_sample_count ? `样本 ${dataStatus.research_sample_count}` : null
  ].filter(Boolean);

  return metaParts.length > 0 ? `${baseCopy} / ${metaParts.join(" / ")}` : baseCopy;
}

export function StatusStrip({
  health,
  dataStatus,
  syncInfo,
  isSyncing,
  candidateCount,
  currentParameterVersion,
  currentParameterSource,
  canApplyResearchResults,
  isApplyingResearchResults,
  onApplyResearchResults,
  onSync
}: StatusStripProps) {
  const latestSync = dataStatus?.latest_sync ?? health?.latest_sync;
  const latestTradeDate = syncInfo?.latest_trade_date ?? dataStatus?.latest_trade_date ?? "等待同步";
  const warnings = [...(syncInfo?.warnings ?? []), ...(dataStatus?.warnings ?? []), ...(dataStatus?.provider_warnings ?? [])];
  const progressRatio = syncInfo?.progress_ratio ?? dataStatus?.sync_progress ?? 0;
  const progressPct = Math.round(progressRatio * 100);
  const syncStage = syncInfo?.stage ?? dataStatus?.sync_stage;
  const syncMode = syncInfo?.sync_mode ?? dataStatus?.sync_mode;
  const syncCounts = syncInfo
    ? (() => {
        const queued = syncInfo.queued_symbols > 0 ? syncInfo.queued_symbols : syncInfo.total_symbols;
        if (syncInfo.stage === "syncing_prices" && queued > 0) {
          return `价格 ${syncInfo.processed_symbols}/${queued}${syncInfo.skipped_symbols ? ` / 跳过 ${syncInfo.skipped_symbols}` : ""}`;
        }
        if (syncInfo.stage === "syncing_financials") {
          return `财务 ${syncInfo.processed_symbols}/${syncInfo.queued_symbols}${syncInfo.skipped_symbols ? ` / 跳过 ${syncInfo.skipped_symbols}` : ""}`;
        }
        if (queued > 0) {
          return `${syncInfo.processed_symbols}/${queued}`;
        }
        return null;
      })()
    : null;
  const syncMessage =
    syncInfo?.message ??
    (dataStatus ? `覆盖率 ${(dataStatus.coverage_ratio * 100).toFixed(0)}% / 财报滞后 ${dataStatus.financial_lag_days} 天` : "等待同步");
  const researchCopy = buildResearchCopy(
    dataStatus,
    currentParameterVersion,
    currentParameterSource,
    canApplyResearchResults
  );

  return (
    <section className="status-strip panel">
      <div>
        <p className="eyebrow">收盘后辅助选股</p>
        <h1>今日候选工作台</h1>
        <p className="hero-copy">同步数据后先看今天该看谁，再把真正想跟的股票放进观察池。</p>
      </div>

      <div className="status-grid">
        <div>
          <span className="status-label">最近同步</span>
          <strong>{formatTime(latestSync)}</strong>
          <em>{latestTradeDate}</em>
        </div>
        <div>
          <span className="status-label">同步状态</span>
          <strong>{syncInfo?.status === "running" ? `${progressPct}%` : getSyncStageCopy(syncStage)}</strong>
          <em>
            {getSyncStageCopy(syncStage)} / {getSyncModeCopy(syncMode)}
          </em>
        </div>
        <div>
          <span className="status-label">今日候选</span>
          <strong>{candidateCount}</strong>
          <em>{latestTradeDate === "等待同步" ? "同步后显示" : `${latestTradeDate} 收盘后结果`}</em>
        </div>
        <div>
          <span className="status-label">数据状态</span>
          <strong>{getStatusCopy(dataStatus?.status)}</strong>
          <em>
            {dataStatus
              ? `覆盖率 ${(dataStatus.coverage_ratio * 100).toFixed(0)}% / 财报滞后 ${dataStatus.financial_lag_days} 天`
              : health?.status ?? "等待同步"}
          </em>
        </div>
      </div>

      <div className="status-actions">
        <button className="primary-button" onClick={() => void onSync()} disabled={isSyncing}>
          {isSyncing ? "同步中..." : "同步收盘数据"}
        </button>
        {canApplyResearchResults ? (
          <button
            className="secondary-button"
            onClick={() => void onApplyResearchResults()}
            disabled={isApplyingResearchResults}
          >
            {isApplyingResearchResults ? "应用中..." : "应用研究结果"}
          </button>
        ) : null}
        <div className="sync-progress-card">
          <div className="sync-progress-head">
            <span>{isSyncing ? "同步进度" : "最近一次进度"}</span>
            <strong>{progressPct}%</strong>
          </div>
          <div className="sync-progress-meta">
            <em>
              {getSyncStageCopy(syncStage)} / {getSyncModeCopy(syncMode)}
            </em>
            {syncCounts ? <span>{syncCounts}</span> : null}
          </div>
          <div
            aria-label="同步进度"
            aria-valuemax={100}
            aria-valuemin={0}
            aria-valuenow={progressPct}
            className="sync-progress-track"
            role="progressbar"
          >
            <div className="sync-progress-fill" style={{ width: `${progressPct}%` }} />
          </div>
          <p className="sync-progress-copy">{syncMessage}</p>
        </div>
        {warnings.length > 0 ? (
          <div className="status-warning">{warnings[0]}</div>
        ) : dataStatus?.block_reasons.length ? (
          <div className="status-warning">{dataStatus.block_reasons[0]}</div>
        ) : null}
        <div className="status-warning">{researchCopy}</div>
      </div>
    </section>
  );
}
