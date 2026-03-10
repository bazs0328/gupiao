import type {
  DataStatusResponse,
  DailyReportResponse,
  HealthResponse,
  RankingsResponse,
  ResearchDiagnosticsResponse,
  StockDetailResponse,
  SyncResponse,
  WatchlistResponse,
  WatchlistUpdate
} from "./types";

let cachedBaseUrl: string | null = null;

async function getBaseUrl() {
  if (cachedBaseUrl) {
    return cachedBaseUrl;
  }
  const runtime = await window.desktop?.getRuntimeConfig();
  const sameOriginBaseUrl = typeof window !== "undefined" ? window.location.origin : null;
  cachedBaseUrl =
    runtime?.backendBaseUrl ??
    import.meta.env.VITE_BACKEND_BASE_URL ??
    sameOriginBaseUrl ??
    "http://127.0.0.1:8000";
  return cachedBaseUrl;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const baseUrl = await getBaseUrl();
  const response = await fetch(`${baseUrl}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {})
    }
  });
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
    throw new Error(payload?.detail ?? `Request failed with status ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export function getHealth() {
  return request<HealthResponse>("/health");
}

export function syncEod(mode: "auto" | "full" = "auto") {
  return request<SyncResponse>(`/sync/eod?mode=${encodeURIComponent(mode)}`, { method: "POST" });
}

export function getLatestSyncRun() {
  return request<SyncResponse>("/sync/runs/latest");
}

export function getSyncRun(runId: string) {
  return request<SyncResponse>(`/sync/runs/${encodeURIComponent(runId)}`);
}

export function getDataStatus() {
  return request<DataStatusResponse>("/data/status");
}

export function getRankings(limit = 30, parameterVersion?: string) {
  const params = new URLSearchParams({ limit: String(limit) });
  if (parameterVersion) {
    params.set("parameter_version", parameterVersion);
  }
  return request<RankingsResponse>(`/rankings?${params.toString()}`);
}

export function getStockDetail(code: string, parameterVersion?: string) {
  const params = new URLSearchParams();
  if (parameterVersion) {
    params.set("parameter_version", parameterVersion);
  }
  const suffix = params.size ? `?${params.toString()}` : "";
  return request<StockDetailResponse>(`/stocks/${encodeURIComponent(code)}${suffix}`);
}

export function getDailyReport(parameterVersion?: string) {
  const params = new URLSearchParams();
  if (parameterVersion) {
    params.set("parameter_version", parameterVersion);
  }
  const suffix = params.size ? `?${params.toString()}` : "";
  return request<DailyReportResponse>(`/reports/daily${suffix}`);
}

export function getResearchDiagnostics() {
  return request<ResearchDiagnosticsResponse>("/analytics/research-diagnostics");
}

export function getWatchlist(watchlistId: string) {
  return request<WatchlistResponse>(`/watchlists/${encodeURIComponent(watchlistId)}`);
}

export function updateWatchlist(watchlistId: string, payload: WatchlistUpdate) {
  return request<WatchlistResponse>(`/watchlists/${encodeURIComponent(watchlistId)}`, {
    method: "PUT",
    body: JSON.stringify(payload)
  });
}
