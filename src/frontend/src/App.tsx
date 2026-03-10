import { useEffect, useState } from "react";
import {
  getDataStatus,
  getDailyReport,
  getHealth,
  getLatestSyncRun,
  getRankings,
  getResearchDiagnostics,
  getSyncRun,
  getStockDetail,
  getWatchlist,
  syncEod,
  updateWatchlist
} from "./api";
import { DailyReportPanel } from "./components/DailyReportPanel";
import { RankingTable } from "./components/RankingTable";
import { StatusStrip } from "./components/StatusStrip";
import { StockDetailCard } from "./components/StockDetailCard";
import { WatchlistPanel } from "./components/WatchlistPanel";
import type {
  DataStatusResponse,
  DailyReportResponse,
  HealthResponse,
  ParameterSource,
  RankingsResponse,
  ResearchDiagnosticsResponse,
  StockDetailResponse,
  SyncResponse,
  WatchlistItem,
  WatchlistItemInput
} from "./types";

const WATCHLIST_ID = "core";
const WORKSPACE_VIEWS = [
  { id: "decision", label: "今日候选" },
  { id: "watchlist", label: "观察池" }
] as const;

type WorkspaceView = (typeof WORKSPACE_VIEWS)[number]["id"];

function resolveSessionParameterVersion(status: DataStatusResponse | null) {
  if (status?.research_status === "ready" && status.parameter_version) {
    return status.parameter_version;
  }
  return "default";
}

function toWatchlistInput(item: WatchlistItem): WatchlistItemInput {
  return {
    code: item.code,
    note: item.note,
    target_entry: item.target_entry,
    stop_loss: item.stop_loss,
    take_profit: item.take_profit
  };
}

function App() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [dataStatus, setDataStatus] = useState<DataStatusResponse | null>(null);
  const [syncInfo, setSyncInfo] = useState<SyncResponse | null>(null);
  const [rankings, setRankings] = useState<RankingsResponse | null>(null);
  const [dailyReport, setDailyReport] = useState<DailyReportResponse | null>(null);
  const [watchlist, setWatchlist] = useState<WatchlistItem[]>([]);
  const [selectedCode, setSelectedCode] = useState<string | null>(null);
  const [stockDetail, setStockDetail] = useState<StockDetailResponse | null>(null);
  const [researchDiagnostics, setResearchDiagnostics] = useState<ResearchDiagnosticsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isSyncing, setIsSyncing] = useState(false);
  const [isApplyingResearch, setIsApplyingResearch] = useState(false);
  const [isSavingWatchlist, setIsSavingWatchlist] = useState(false);
  const [activeView, setActiveView] = useState<WorkspaceView>("decision");
  const [sessionParameterVersion, setSessionParameterVersion] = useState<string | null>(null);

  async function loadStockDetail(code: string, parameterVersion: string) {
    const detail = await getStockDetail(code, parameterVersion);
    setStockDetail(detail);
  }

  async function loadWorkspace(
    parameterVersion: string,
    preferredCode?: string | null,
    nextStatus?: DataStatusResponse,
    preserveRankedSelectionOnly = false
  ) {
    const [resolvedStatus, nextRankings, nextReport, nextWatchlist] = await Promise.all([
      nextStatus ? Promise.resolve(nextStatus) : getDataStatus(),
      getRankings(30, parameterVersion),
      getDailyReport(parameterVersion),
      getWatchlist(WATCHLIST_ID)
    ]);

    setDataStatus(resolvedStatus);
    setRankings(nextRankings);
    setDailyReport(nextReport);
    setWatchlist(nextWatchlist.items);

    const rankedCodes = new Set(nextRankings.items.map((item) => item.code));
    const watchlistCodes = new Set(nextWatchlist.items.map((item) => item.code));
    const canKeepPreferredCode = preferredCode
      ? preserveRankedSelectionOnly
        ? rankedCodes.has(preferredCode)
        : rankedCodes.has(preferredCode) || watchlistCodes.has(preferredCode)
      : false;
    const effectiveCode =
      (canKeepPreferredCode ? preferredCode : null) ??
      nextRankings.items[0]?.code ??
      nextWatchlist.items[0]?.code ??
      null;
    setSelectedCode(effectiveCode);

    if (!effectiveCode) {
      setStockDetail(null);
      return;
    }
    setStockDetail(await getStockDetail(effectiveCode, parameterVersion));
  }

  async function bootstrap() {
    setError(null);
    try {
      setHealth(await getHealth());
      const latestRun = await getLatestSyncRun().catch(() => null);
      if (latestRun?.run_id && latestRun.status === "running") {
        setSyncInfo(latestRun);
        await waitForSyncCompletion(latestRun.run_id);
      }
      try {
        const nextStatus = await getDataStatus();
        const initialParameterVersion = resolveSessionParameterVersion(nextStatus);
        setSessionParameterVersion(initialParameterVersion);
        await loadWorkspace(initialParameterVersion, null, nextStatus);
      } catch (innerError) {
        if (innerError instanceof Error && innerError.message.includes("No market data synced yet")) {
          await handleSync();
          return;
        }
        throw innerError;
      }
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "初始化失败。");
    }
  }

  async function waitForSyncCompletion(runId: string) {
    while (true) {
      const nextRun = await getSyncRun(runId);
      setSyncInfo(nextRun);
      if (nextRun.status !== "running") {
        return nextRun;
      }
      await new Promise((resolve) => window.setTimeout(resolve, 1000));
    }
  }

  useEffect(() => {
    void bootstrap();
  }, []);

  async function handleSync() {
    setIsSyncing(true);
    setError(null);
    try {
      const response = await syncEod("auto");
      setSyncInfo(response);
      const terminal = response.status === "running" ? await waitForSyncCompletion(response.run_id) : response;
      setSyncInfo(terminal);
      setHealth(await getHealth());
      setResearchDiagnostics(null);
      const nextStatus = await getDataStatus();
      const nextParameterVersion = sessionParameterVersion ?? "default";
      setSessionParameterVersion(nextParameterVersion);
      await loadWorkspace(nextParameterVersion, selectedCode, nextStatus);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "同步失败。");
    } finally {
      setIsSyncing(false);
    }
  }

  async function handleSelectCode(code: string) {
    setSelectedCode(code);
    try {
      await loadStockDetail(code, sessionParameterVersion ?? "default");
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "加载单股详情失败。");
    }
  }

  async function handleLoadResearchDiagnostics() {
    if (researchDiagnostics) {
      return;
    }
    try {
      setResearchDiagnostics(await getResearchDiagnostics());
    } catch {
      setResearchDiagnostics(null);
    }
  }

  async function handleSaveWatchlist(payload: WatchlistItemInput) {
    setIsSavingWatchlist(true);
    setError(null);
    try {
      const parameterVersion = sessionParameterVersion ?? "default";
      const otherItems = watchlist.filter((item) => item.code !== payload.code).map(toWatchlistInput);
      const nextWatchlist = await updateWatchlist(WATCHLIST_ID, {
        items: [payload, ...otherItems]
      });
      const [nextStatus, nextReport] = await Promise.all([getDataStatus(), getDailyReport(parameterVersion)]);
      setWatchlist(nextWatchlist.items);
      setDataStatus(nextStatus);
      setDailyReport(nextReport);
      await loadStockDetail(payload.code, parameterVersion);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "保存观察池失败。");
    } finally {
      setIsSavingWatchlist(false);
    }
  }

  async function handleRemoveWatchlist(code: string) {
    setIsSavingWatchlist(true);
    setError(null);
    try {
      const parameterVersion = sessionParameterVersion ?? "default";
      const nextWatchlist = await updateWatchlist(WATCHLIST_ID, {
        items: watchlist.filter((item) => item.code !== code).map(toWatchlistInput)
      });
      const [nextStatus, nextReport] = await Promise.all([getDataStatus(), getDailyReport(parameterVersion)]);
      setWatchlist(nextWatchlist.items);
      setDataStatus(nextStatus);
      setDailyReport(nextReport);
      if (selectedCode === code) {
        await loadStockDetail(code, parameterVersion);
      }
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "移除观察池失败。");
    } finally {
      setIsSavingWatchlist(false);
    }
  }

  async function handleApplyResearchResults() {
    const nextParameterVersion = dataStatus?.research_status === "ready" ? dataStatus.parameter_version : null;
    if (!nextParameterVersion) {
      return;
    }
    setIsApplyingResearch(true);
    setError(null);
    try {
      setResearchDiagnostics(null);
      await loadWorkspace(nextParameterVersion, selectedCode, undefined, true);
      setSessionParameterVersion(nextParameterVersion);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "应用研究结果失败。");
    } finally {
      setIsApplyingResearch(false);
    }
  }

  const syncInFlight = isSyncing || syncInfo?.status === "running";

  useEffect(() => {
    if (!dataStatus?.latest_trade_date || syncInFlight || (sessionParameterVersion ?? "default") !== "default") {
      return;
    }
    if (dataStatus.research_status === "ready" && dataStatus.parameter_version) {
      return;
    }
    let cancelled = false;
    const timer = window.setInterval(() => {
      void (async () => {
        try {
          const nextStatus = await getDataStatus();
          if (!cancelled) {
            setDataStatus(nextStatus);
          }
        } catch {
          // Keep the last known status; the next poll can recover.
        }
      })();
    }, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [
    dataStatus?.latest_trade_date,
    dataStatus?.parameter_version,
    dataStatus?.research_status,
    sessionParameterVersion,
    syncInFlight
  ]);

  const selectedWatchlistItem = watchlist.find((item) => item.code === selectedCode) ?? null;
  const currentParameterVersion =
    rankings?.parameter_version ??
    dailyReport?.parameter_version ??
    stockDetail?.parameter_version ??
    sessionParameterVersion ??
    "default";
  const currentParameterSource: ParameterSource =
    rankings?.parameter_source ??
    dailyReport?.parameter_source ??
    stockDetail?.parameter_source ??
    (currentParameterVersion === "default" ? "default" : "research");
  const canApplyResearchResults =
    dataStatus?.research_status === "ready" &&
    !!dataStatus.parameter_version &&
    dataStatus.parameter_version !== currentParameterVersion;
  const focusStockName =
    stockDetail?.name ??
    selectedWatchlistItem?.name ??
    rankings?.items.find((item) => item.code === selectedCode)?.name ??
    rankings?.items[0]?.name ??
    null;
  const dailyCandidateCount = dailyReport?.candidates.length ?? rankings?.items.filter((item) => item.actionable).length ?? 0;
  const viewDescription =
    activeView === "decision"
      ? "先看今天先看哪几只，再决定要不要放进观察池。"
      : "把已经盯住的股票集中在一起，只跟目标位、止损、止盈和备注。";

  return (
    <main className="app-shell">
      <div className="ambient ambient-a" />
      <div className="ambient ambient-b" />
      <StatusStrip
        health={health}
        dataStatus={dataStatus}
        syncInfo={syncInfo}
        isSyncing={syncInFlight}
        candidateCount={dailyCandidateCount}
        currentParameterVersion={currentParameterVersion}
        currentParameterSource={currentParameterSource}
        canApplyResearchResults={canApplyResearchResults}
        isApplyingResearchResults={isApplyingResearch}
        onApplyResearchResults={handleApplyResearchResults}
        onSync={handleSync}
      />

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="panel workspace-switcher">
        <div className="workspace-switcher-copy">
          <div>
            <p className="eyebrow">默认只做两件事</p>
            <h2>先看今天该看谁，再跟踪已经选中的股票</h2>
          </div>
          <p className="hero-copy">{viewDescription}</p>
        </div>
        <div className="workspace-tabs" role="tablist" aria-label="工作区切换">
          {WORKSPACE_VIEWS.map((view) => (
            <button
              key={view.id}
              type="button"
              role="tab"
              aria-selected={activeView === view.id}
              className={`workspace-tab ${activeView === view.id ? "active" : ""}`}
              onClick={() => setActiveView(view.id)}
            >
              {view.label}
            </button>
          ))}
        </div>
        <div className="workspace-metrics">
          <div>
            <span>今日候选</span>
            <strong>{dailyCandidateCount}</strong>
          </div>
          <div>
            <span>观察池</span>
            <strong>{watchlist.length}</strong>
          </div>
          <div>
            <span>当前焦点</span>
            <strong>{focusStockName ?? "等待数据"}</strong>
          </div>
        </div>
      </section>

      {activeView === "decision" ? (
        <section className="workspace-grid workspace-grid-primary">
          <DailyReportPanel report={dailyReport} onSelectCode={handleSelectCode} />
          <StockDetailCard
            stock={stockDetail}
            watchlistItem={selectedWatchlistItem}
            isSavingWatchlist={isSavingWatchlist}
            researchDiagnostics={researchDiagnostics}
            onLoadResearchDiagnostics={handleLoadResearchDiagnostics}
            onSaveWatchlist={handleSaveWatchlist}
            onRemoveWatchlist={handleRemoveWatchlist}
          />
          <RankingTable rows={rankings?.items ?? []} selectedCode={selectedCode} onSelect={handleSelectCode} />
        </section>
      ) : null}

      {activeView === "watchlist" ? (
        <section className="workspace-grid workspace-grid-watchlist">
          <WatchlistPanel
            items={watchlist}
            selectedCode={selectedCode}
            isUpdating={isSavingWatchlist}
            onSelect={handleSelectCode}
            onRemove={handleRemoveWatchlist}
          />
          <StockDetailCard
            stock={stockDetail}
            watchlistItem={selectedWatchlistItem}
            isSavingWatchlist={isSavingWatchlist}
            researchDiagnostics={researchDiagnostics}
            onLoadResearchDiagnostics={handleLoadResearchDiagnostics}
            onSaveWatchlist={handleSaveWatchlist}
            onRemoveWatchlist={handleRemoveWatchlist}
          />
        </section>
      ) : null}
    </main>
  );
}

export default App;
