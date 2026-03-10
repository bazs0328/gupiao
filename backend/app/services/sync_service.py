from __future__ import annotations

from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import date, timedelta
import json
import os
import threading
import time

from .akshare_provider import AkshareProvider
from .factor_engine import compute_snapshot
from ..models import SyncResponse
from .sync_progress import compute_progress_ratio


DEFAULT_PRICE_WORKERS = 8
DEFAULT_FINANCIAL_WORKERS = 4
PRICE_FETCH_ATTEMPTS = 3
PRICE_FETCH_BACKOFF_SECONDS = (1.0, 3.0)


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _target_financial_report_date(as_of_date: date) -> date:
    if as_of_date.month <= 4:
        return date(as_of_date.year - 1, 9, 30)
    if as_of_date.month <= 8:
        return date(as_of_date.year, 3, 31)
    if as_of_date.month <= 10:
        return date(as_of_date.year, 6, 30)
    return date(as_of_date.year, 9, 30)


class SyncService:
    _run_lock = threading.Lock()

    def __init__(
        self,
        repository,
        settings,
        *,
        provider=None,
        analytics_service=None,
        strategy_service=None,
        workspace_service=None,
        research_service=None,
    ):
        self.repository = repository
        self.settings = settings
        self.provider = provider or AkshareProvider(benchmark_code=settings.benchmark_code)
        self.analytics_service = analytics_service
        self.strategy_service = strategy_service
        self.workspace_service = workspace_service
        self.research_service = research_service

    def _serialize_run(self, run: dict) -> SyncResponse:
        payload = json.loads(run.get("payload_json") or "{}")
        processed_symbols = int(payload.get("processed_symbols", 0) or 0)
        total_symbols = int(payload.get("total_symbols", 0) or 0)
        queued_symbols = int(payload.get("queued_symbols", 0) or 0)
        return SyncResponse(
            run_id=run["run_id"],
            status=run["status"],
            provider=run["provider"],
            stage=payload.get("stage", "queued"),
            sync_mode=payload.get("sync_mode", "auto"),
            started_at=run.get("started_at"),
            finished_at=run.get("finished_at"),
            message=run.get("message") or "",
            processed_symbols=processed_symbols,
            total_symbols=total_symbols,
            queued_symbols=queued_symbols,
            skipped_symbols=int(payload.get("skipped_symbols", 0) or 0),
            stocks_synced=int(payload.get("stocks_synced", 0) or 0),
            price_rows_synced=int(payload.get("price_rows_synced", 0) or 0),
            latest_trade_date=payload.get("latest_trade_date") or run.get("as_of_date"),
            warnings=list(payload.get("warnings", [])),
            failure_ratio=float(payload.get("failure_ratio", 0.0) or 0.0),
            progress_ratio=compute_progress_ratio(
                payload.get("stage", "queued"),
                processed_symbols,
                total_symbols,
                run.get("status"),
                queued_symbols=queued_symbols,
            ),
        )

    def _resolve_sync_mode(self, requested_mode: str) -> str:
        if requested_mode == "full":
            return "full"
        return "daily_fast" if self.repository.get_latest_trade_date() else "initial_build"

    def start_or_resume_sync(self, mode: str = "auto") -> SyncResponse:
        with self._run_lock:
            running = self.repository.get_running_run("sync")
            if running:
                return self._serialize_run(running)

            sync_mode = self._resolve_sync_mode(mode)
            run_id = self.repository.start_run(
                "sync",
                self.provider.provider_name,
                payload={"sync_mode": sync_mode},
            )
            worker = threading.Thread(target=self._run_sync_task, args=(run_id, sync_mode), daemon=True)
            worker.start()
            return self._serialize_run(self.repository.get_run(run_id))

    def get_latest_run(self) -> SyncResponse | None:
        run = self.repository.get_latest_run("sync")
        return self._serialize_run(run) if run else None

    def get_run(self, run_id: str) -> SyncResponse | None:
        run = self.repository.get_run(run_id)
        return self._serialize_run(run) if run else None

    def _update_progress(self, run_id: str, stage: str, message: str, payload: dict | None = None, *, as_of_date: str | None = None) -> None:
        self.repository.update_run_progress(
            run_id,
            stage=stage,
            message=message,
            payload=payload,
            as_of_date=as_of_date,
        )

    def _audit_price_series(self, code: str, bars: list[object]) -> list[dict]:
        issues: list[dict] = []
        for bar in bars:
            if bar.close <= 0 or bar.open <= 0:
                issues.append(
                    {
                        "code": code,
                        "trade_date": bar.trade_date,
                        "severity": "critical",
                        "issue_type": "non_positive_price",
                        "message": "存在非正价格数据。",
                    }
                )
                break
            if bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close):
                issues.append(
                    {
                        "code": code,
                        "trade_date": bar.trade_date,
                        "severity": "critical",
                        "issue_type": "ohlc_invalid",
                        "message": "OHLC 关系异常。",
                    }
                )
                break
            if bar.turnover < 0 or bar.volume < 0:
                issues.append(
                    {
                        "code": code,
                        "trade_date": bar.trade_date,
                        "severity": "critical",
                        "issue_type": "negative_turnover",
                        "message": "成交量或成交额出现负值。",
                    }
                )
                break
        if bars and len(bars) < 120:
            issues.append(
                {
                    "code": code,
                    "trade_date": bars[-1].trade_date,
                    "severity": "warning",
                    "issue_type": "short_history",
                    "message": "历史窗口较短，上市天数过滤会更严格。",
                }
            )
        return issues

    def _flush_price_batches(self, stocks: list[object], price_bars: list[object]) -> tuple[int, int]:
        stock_count = self.repository.upsert_stocks(stocks) if stocks else 0
        price_count = self.repository.upsert_price_bars(price_bars) if price_bars else 0
        return stock_count, price_count

    def _fetch_price_task(self, stock, start_date: date, end_date: date) -> tuple[object, list[object], Exception | None]:
        last_error: Exception | None = None
        for attempt in range(PRICE_FETCH_ATTEMPTS):
            try:
                bars = self.provider.fetch_price_bars(stock.code, start_date, end_date)
                if bars:
                    return stock, bars, None
            except Exception as exc:  # pragma: no cover - integration path
                last_error = exc
            if attempt < PRICE_FETCH_ATTEMPTS - 1:
                delay = PRICE_FETCH_BACKOFF_SECONDS[min(attempt, len(PRICE_FETCH_BACKOFF_SECONDS) - 1)]
                time.sleep(delay)
        return stock, [], last_error

    def _fetch_financial_task(self, stock) -> tuple[object, list[object], Exception | None]:
        try:
            return stock, self.provider.fetch_financial_series(stock.code), None
        except Exception as exc:  # pragma: no cover - integration path
            return stock, [], exc

    def _should_refresh_financials(self, stock, latest_financial_dates: dict[str, dict[str, str | None]], target_report_date: str) -> bool:
        snapshot = latest_financial_dates.get(stock.code)
        if not snapshot:
            return True
        report_date = snapshot.get("report_date")
        return not report_date or report_date < target_report_date

    @staticmethod
    def _latest_close_on_or_before(price_history: list[tuple[str, float]], target_date: str) -> float | None:
        if not price_history:
            return None
        trading_dates = [trade_date for trade_date, _close in price_history]
        index = bisect_right(trading_dates, target_date) - 1
        if index < 0:
            return None
        return price_history[index][1]

    def _derive_financial_valuations(self, financial_rows: list[object], price_history: list[tuple[str, float]]) -> list[object]:
        derived_rows: list[object] = []
        for row in financial_rows:
            close = self._latest_close_on_or_before(price_history, row.publish_date)
            pe_ttm = round(close / row.basic_eps, 4) if close and row.basic_eps > 0 else 10000.0
            pb = round(close / row.per_net_assets, 4) if close and row.per_net_assets > 0 else 10000.0
            derived_rows.append(replace(row, pe_ttm=pe_ttm, pb=pb))
        return derived_rows

    def _run_sync_task(self, run_id: str, sync_mode: str) -> None:
        end_date = date.today()
        start_date = end_date - timedelta(days=self.provider.lookback_days)
        warnings: list[str] = []
        audits: list[dict] = []
        processed_symbols = 0
        successful_symbols: list[object] = []
        stocks_synced = 0
        price_rows_synced = 0
        price_failures = 0
        price_history_by_code: dict[str, list[tuple[str, float]]] = {}

        try:
            self._update_progress(
                run_id,
                "loading_universe",
                "加载沪深主板与创业板股票池。",
                payload={"sync_mode": sync_mode},
            )
            universe = self.provider.fetch_universe()
            total_symbols = len(universe)

            self._update_progress(
                run_id,
                "syncing_prices",
                "开始并发抓取全市场日线。",
                payload={
                    "sync_mode": sync_mode,
                    "processed_symbols": 0,
                    "total_symbols": total_symbols,
                    "queued_symbols": total_symbols,
                    "skipped_symbols": 0,
                },
            )

            batched_stocks: list[object] = []
            batched_bars: list[object] = []
            price_workers = _env_int("GUPIAO_SYNC_PRICE_WORKERS", DEFAULT_PRICE_WORKERS)
            with ThreadPoolExecutor(max_workers=price_workers) as executor:
                futures = [executor.submit(self._fetch_price_task, stock, start_date, end_date) for stock in universe]
                for future in as_completed(futures):
                    stock, bars, last_error = future.result()
                    processed_symbols += 1
                    if not bars:
                        price_failures += 1
                        message = f"{stock.code} 缺少日线历史。" if last_error is None else f"{stock.code}: {last_error}"
                        warnings.append(message)
                    else:
                        stock_issues = self._audit_price_series(stock.code, bars)
                        audits.extend(stock_issues)
                        if any(item["severity"] == "critical" for item in stock_issues):
                            price_failures += 1
                            warnings.append(f"{stock.code} 因数据审计异常被隔离。")
                        else:
                            latest_bar = bars[-1]
                            synced_stock = replace(
                                stock,
                                listing_date=bars[0].trade_date,
                                is_suspended=latest_bar.is_suspended,
                            )
                            successful_symbols.append(synced_stock)
                            price_history_by_code[stock.code] = [(bar.trade_date, bar.close) for bar in bars]
                            batched_stocks.append(synced_stock)
                            batched_bars.extend(bars)
                            if len(batched_stocks) >= 60 or len(batched_bars) >= 12_000:
                                batch_stock_count, batch_price_count = self._flush_price_batches(batched_stocks, batched_bars)
                                stocks_synced += batch_stock_count
                                price_rows_synced += batch_price_count
                                batched_stocks = []
                                batched_bars = []
                    if processed_symbols % 20 == 0 or processed_symbols == total_symbols:
                        self._update_progress(
                            run_id,
                            "syncing_prices",
                            f"价格 {processed_symbols}/{total_symbols}，跳过 {price_failures}。",
                            payload={
                                "sync_mode": sync_mode,
                                "processed_symbols": processed_symbols,
                                "total_symbols": total_symbols,
                                "queued_symbols": total_symbols,
                                "skipped_symbols": price_failures,
                                "stocks_synced": stocks_synced,
                                "price_rows_synced": price_rows_synced,
                                "warning_count": len(warnings),
                                "warnings": warnings[:20],
                                "failure_ratio": round(price_failures / max(processed_symbols, 1), 4),
                            },
                        )

            if batched_stocks or batched_bars:
                batch_stock_count, batch_price_count = self._flush_price_batches(batched_stocks, batched_bars)
                stocks_synced += batch_stock_count
                price_rows_synced += batch_price_count

            benchmark_stock, benchmark_bars = self.provider.fetch_benchmark_series(start_date, end_date)
            stocks_synced += self.repository.upsert_stocks([benchmark_stock])
            price_rows_synced += self.repository.upsert_price_bars(benchmark_bars)

            latest_financial_dates = self.repository.get_latest_financial_dates([stock.code for stock in successful_symbols])
            target_report_date = _target_financial_report_date(end_date).isoformat()
            refresh_all_financials = sync_mode in {"full", "initial_build"}
            financial_queue = [
                stock
                for stock in successful_symbols
                if refresh_all_financials or self._should_refresh_financials(stock, latest_financial_dates, target_report_date)
            ]
            skipped_financials = max(len(successful_symbols) - len(financial_queue), 0)

            self._update_progress(
                run_id,
                "syncing_financials",
                "开始抓取财务指标。",
                payload={
                    "sync_mode": sync_mode,
                    "processed_symbols": 0,
                    "total_symbols": len(successful_symbols),
                    "queued_symbols": len(financial_queue),
                    "skipped_symbols": skipped_financials,
                    "stocks_synced": stocks_synced,
                    "price_rows_synced": price_rows_synced,
                },
            )

            financial_failures = 0
            financial_buffer: list[object] = []
            financial_workers = _env_int("GUPIAO_SYNC_FINANCIAL_WORKERS", DEFAULT_FINANCIAL_WORKERS)
            if financial_queue:
                with ThreadPoolExecutor(max_workers=financial_workers) as executor:
                    futures = [executor.submit(self._fetch_financial_task, stock) for stock in financial_queue]
                    processed_financials = 0
                    for future in as_completed(futures):
                        stock, financial_rows, error = future.result()
                        processed_financials += 1
                        if error:
                            financial_failures += 1
                            warnings.append(f"{stock.code}: 财务抓取失败 {error}")
                        elif not financial_rows:
                            financial_failures += 1
                            warnings.append(f"{stock.code}: 财务抓取失败 财务接口返回空结果。")
                        else:
                            financial_buffer.extend(
                                self._derive_financial_valuations(
                                    financial_rows,
                                    price_history_by_code.get(stock.code, []),
                                )
                            )
                            if len(financial_buffer) >= 500:
                                self.repository.upsert_financials(financial_buffer)
                                financial_buffer = []
                        if processed_financials % 20 == 0 or processed_financials == len(financial_queue):
                            self._update_progress(
                                run_id,
                                "syncing_financials",
                                f"财务 {processed_financials}/{len(financial_queue)}，跳过 {skipped_financials}。",
                                payload={
                                    "sync_mode": sync_mode,
                                    "processed_symbols": processed_financials,
                                    "total_symbols": len(successful_symbols),
                                    "queued_symbols": len(financial_queue),
                                    "skipped_symbols": skipped_financials,
                                    "stocks_synced": stocks_synced,
                                    "price_rows_synced": price_rows_synced,
                                    "warning_count": len(warnings),
                                    "warnings": warnings[:20],
                                    "failure_ratio": round((price_failures + financial_failures) / max(len(universe), 1), 4),
                                },
                            )
            else:
                self._update_progress(
                    run_id,
                    "syncing_financials",
                    f"财务 0/0，跳过 {skipped_financials}。",
                    payload={
                        "sync_mode": sync_mode,
                        "processed_symbols": 0,
                        "total_symbols": len(successful_symbols),
                        "queued_symbols": 0,
                        "skipped_symbols": skipped_financials,
                        "stocks_synced": stocks_synced,
                        "price_rows_synced": price_rows_synced,
                        "warning_count": len(warnings),
                        "warnings": warnings[:20],
                        "failure_ratio": round(price_failures / max(len(universe), 1), 4),
                    },
                )
            if financial_buffer:
                self.repository.upsert_financials(financial_buffer)

            self.repository.save_data_audits(run_id, audits)

            latest_date = self.repository.get_latest_trade_date()
            if not latest_date:
                raise RuntimeError("同步未产生任何可用交易日。")

            audit_summary = self.repository.get_audit_summary(run_id)
            if audit_summary["critical_count"] > 0:
                warnings.append(f"存在 {audit_summary['critical_count']} 条关键数据异常。")

            failure_ratio = round((price_failures + financial_failures) / max(len(universe), 1), 4)
            visible_financials = self.repository.get_visible_financials(as_of_date=latest_date)
            financial_coverage = len(visible_financials) / max(len(successful_symbols), 1)
            if financial_coverage < 0.90:
                warnings.append(f"最新可见财务覆盖率仅 {financial_coverage * 100:.2f}%。")
                self.repository.finish_run(
                    run_id,
                    "partial",
                    "财务抓取异常，已跳过候选预热和日报生成。",
                    {
                        "sync_mode": sync_mode,
                        "processed_symbols": total_symbols,
                        "total_symbols": total_symbols,
                        "queued_symbols": total_symbols,
                        "skipped_symbols": price_failures + financial_failures,
                        "stocks_synced": stocks_synced,
                        "price_rows_synced": price_rows_synced,
                        "warning_count": len(warnings),
                        "warnings": warnings[:20],
                        "latest_trade_date": latest_date,
                        "failure_ratio": failure_ratio,
                        "financial_coverage": round(financial_coverage, 4),
                    },
                    as_of_date=latest_date,
                )
                return

            self._update_progress(
                run_id,
                "computing_factors",
                "计算最新因子快照。",
                payload={
                    "sync_mode": sync_mode,
                    "latest_trade_date": latest_date,
                    "failure_ratio": failure_ratio,
                },
                as_of_date=latest_date,
            )
            computed = compute_snapshot(
                self.repository.get_stock_meta(),
                self.repository.get_recent_price_map(latest_date, days=130),
                self.repository.get_visible_financials(as_of_date=latest_date),
                latest_date,
            )
            serializable_snapshot = {
                code: {
                    "raw_factors": entry.raw_factors,
                    "factor_scores": entry.factor_scores,
                    "factor_groups": entry.factor_groups,
                }
                for code, entry in computed.items()
                if entry.factor_scores
            }
            self.repository.save_factor_snapshot(latest_date, serializable_snapshot)

            self._update_progress(
                run_id,
                "warming_validation",
                "预热默认候选缓存。",
                payload={"sync_mode": sync_mode, "latest_trade_date": latest_date, "failure_ratio": failure_ratio},
                as_of_date=latest_date,
            )
            if self.analytics_service and self.strategy_service:
                self.analytics_service.warm_default_candidates(
                    self.strategy_service.get_strategy("default"),
                    as_of_date=latest_date,
                )

            self._update_progress(
                run_id,
                "publishing",
                "生成默认日报。",
                payload={"sync_mode": sync_mode, "latest_trade_date": latest_date},
                as_of_date=latest_date,
            )
            if self.workspace_service:
                self.workspace_service.generate_daily_report(parameter_version="default", cache_result=True)

            status = "partial" if warnings else "success"
            self.repository.finish_run(
                run_id,
                status,
                "sync completed",
                {
                    "sync_mode": sync_mode,
                    "processed_symbols": total_symbols,
                    "total_symbols": total_symbols,
                    "queued_symbols": total_symbols,
                    "skipped_symbols": price_failures + financial_failures,
                    "stocks_synced": stocks_synced,
                    "price_rows_synced": price_rows_synced,
                    "warning_count": len(warnings),
                    "warnings": warnings[:20],
                    "latest_trade_date": latest_date,
                    "failure_ratio": failure_ratio,
                },
                as_of_date=latest_date,
            )
            if self.research_service:
                self.research_service.start_background_refresh(mode="incremental")
        except Exception as exc:  # pragma: no cover - integration path
            self.repository.finish_run(
                run_id,
                "failed",
                str(exc),
                {
                    "sync_mode": sync_mode,
                    "processed_symbols": processed_symbols,
                    "total_symbols": 0,
                    "queued_symbols": 0,
                    "skipped_symbols": 0,
                    "stocks_synced": stocks_synced,
                    "price_rows_synced": price_rows_synced,
                    "warning_count": len(warnings) + 1,
                    "warnings": [*warnings[:19], str(exc)],
                    "failure_ratio": 1.0,
                },
            )
