from __future__ import annotations

from datetime import date
import json

from ..models import (
    DataStatusResponse,
    DailyCandidate,
    DailyReportResponse,
    WatchlistItem,
    WatchlistResponse,
    WatchlistUpdate,
)
from .sync_progress import compute_progress_ratio


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


class WorkspaceService:
    def __init__(self, repository, strategy_service, ranking_service, analytics_service, benchmark_code: str, research_service=None):
        self.repository = repository
        self.strategy_service = strategy_service
        self.ranking_service = ranking_service
        self.analytics_service = analytics_service
        self.benchmark_code = benchmark_code
        self.research_service = research_service

    @staticmethod
    def _days_between(first: str, second: str) -> int:
        return abs((date.fromisoformat(first) - date.fromisoformat(second)).days)

    @staticmethod
    def _research_refresh_message(research_status: str, parameter_version: str | None) -> str:
        if research_status == "running":
            return "研究刷新中，准备好后可手动应用。"
        if research_status == "ready" and parameter_version:
            return "研究结果已就绪，可手动刷新候选。"
        if research_status == "limited":
            return "研究样本仍受限，当前继续沿用默认参数。"
        if research_status == "failed":
            return "研究刷新异常，当前继续沿用默认参数。"
        return "多年研究结果尚未准备好。"

    def get_data_status(self) -> DataStatusResponse:
        latest_sync = self.repository.get_latest_run("sync")
        provider = latest_sync["provider"] if latest_sync else "akshare"
        warnings: list[str] = []
        provider_warnings: list[str] = []
        status = "ok"
        latest_trade_date = self.repository.get_latest_trade_date()
        payload: dict[str, object] = {}
        if latest_sync and latest_sync.get("payload_json"):
            payload = json.loads(latest_sync["payload_json"])
            warnings.extend(payload.get("warnings", []))
            provider_warnings.extend(payload.get("warnings", []))

        if latest_sync and latest_sync["status"] == "failed":
            status = "error"
            if latest_sync.get("message"):
                warnings.append(latest_sync["message"])

        equity_count = self.repository.count_equities()
        latest_prices = self.repository.get_latest_price_lookup(as_of_date=latest_trade_date) if latest_trade_date else {}
        visible_financials = self.repository.get_visible_financials(as_of_date=latest_trade_date) if latest_trade_date else {}
        price_coverage = len([row for row in latest_prices.values() if row and row.get("code") != self.benchmark_code]) / max(equity_count, 1)
        financial_coverage = len(visible_financials) / max(equity_count, 1)
        coverage_ratio = round(min(price_coverage, financial_coverage), 4)

        lag_days = [
            self._days_between(latest_trade_date, row["publish_date"])
            for row in visible_financials.values()
            if latest_trade_date and row.get("publish_date")
        ]
        financial_lag_days = round(sum(lag_days) / len(lag_days)) if lag_days else 999
        warning_penalty = min(len(warnings), 4) * 5
        lag_penalty = 0 if financial_lag_days <= 90 else min((financial_lag_days - 90) / 2, 24)
        failure_penalty = float(payload.get("failure_ratio", 0.0) or 0.0) * 100
        data_quality_score = round(_clamp(coverage_ratio * 100 - warning_penalty - lag_penalty - failure_penalty, 0.0, 100.0), 2)

        block_reasons: list[str] = []
        if not latest_trade_date:
            block_reasons.append("尚未完成首次同步")
        if latest_sync and latest_sync["status"] == "failed":
            block_reasons.append("最近一次同步失败")
        if coverage_ratio < 0.98:
            block_reasons.append("价格或财务覆盖率未达标")
        if float(payload.get("failure_ratio", 0.0) or 0.0) > 0.02:
            block_reasons.append("同步失败率过高")
        audit_summary = self.repository.get_audit_summary(latest_sync["run_id"] if latest_sync else None)
        if audit_summary["critical_count"] > 0:
            block_reasons.append("存在关键数据异常")

        recommendation_status = "ready" if latest_trade_date and not block_reasons else "paused"
        if recommendation_status == "paused" and status == "ok":
            status = "warning"

        sync_stage = payload.get("stage", "idle")
        total_symbols = int(payload.get("total_symbols", 0) or 0)
        processed_symbols = int(payload.get("processed_symbols", 0) or 0)
        progress = compute_progress_ratio(
            sync_stage,
            processed_symbols,
            total_symbols,
            latest_sync["status"] if latest_sync else None,
            queued_symbols=int(payload.get("queued_symbols", 0) or 0),
        )
        research_status = self.research_service.get_status() if self.research_service else {}

        return DataStatusResponse(
            status=status,
            provider=provider,
            provider_mode="real-a-share",
            sync_mode=payload.get("sync_mode", "auto"),
            latest_sync=latest_sync["finished_at"] if latest_sync else None,
            latest_trade_date=latest_trade_date,
            benchmark_code=self.benchmark_code,
            equity_count=equity_count,
            watchlist_count=self.repository.count_watchlist_items(),
            warning_count=len(warnings),
            warnings=warnings,
            data_quality_score=data_quality_score,
            coverage_ratio=coverage_ratio,
            financial_lag_days=financial_lag_days,
            provider_warnings=provider_warnings,
            recommendation_status=recommendation_status,
            sync_stage=sync_stage,
            sync_progress=progress,
            failure_ratio=float(payload.get("failure_ratio", 0.0) or 0.0),
            block_reasons=list(dict.fromkeys(block_reasons)),
            research_status=research_status.get("research_status", "idle"),
            research_as_of_date=research_status.get("research_as_of_date"),
            parameter_version=research_status.get("parameter_version"),
            research_sample_count=int(research_status.get("research_sample_count", 0) or 0),
            research_refresh_message=self._research_refresh_message(
                research_status.get("research_status", "idle"),
                research_status.get("parameter_version"),
            ),
        )

    def generate_daily_report(self, *, parameter_version: str | None = None, cache_result: bool | None = None) -> DailyReportResponse:
        config, resolved_parameter_version, parameter_source = self.strategy_service.resolve_strategy(parameter_version)
        as_of_date, shortlist, payload = self.analytics_service.shortlist(config)
        watchlist = [WatchlistItem.model_validate(item) for item in self.repository.get_watchlist("core")]
        market_regime = self.analytics_service.get_market_regime(as_of_date)

        candidates = [DailyCandidate.model_validate(row) for row in shortlist]
        position_plans = [row["position_plan"] for row in shortlist if row.get("position_plan")]
        watchlist_hits = [item for item in watchlist if item.code in {row["code"] for row in shortlist}]
        lead_candidate = candidates[0] if candidates else None

        if payload["actionable"] and candidates:
            summary = f"今天先看 {len(candidates)} 只股票，优先从 {lead_candidate.name} 开始，市场状态 {market_regime}。"
            allocation = {
                "bullish": "市场偏强，可以分批试仓，但先从最强的 1 到 2 只开始。",
                "neutral": "先小仓试，优先等回踩或放量确认，不要一口气铺太多票。",
                "cautious": "今天更适合慢一点，优先观察是否回到计划价位附近。",
            }[market_regime]
        elif payload["actionable"]:
            summary = "今天没有特别突出的新候选，更适合先复核观察池里的老票。"
            allocation = "没有明确机会时，仓位以等待为主，不用硬找出手点。"
        else:
            summary = "今天先别急着找新票，先等同步和数据状态恢复。"
            allocation = "当前以等待和复核为主，不急着扩大新仓位。"

        action_checklist: list[str] = []
        if candidates:
            action_checklist.append(f"先从 {lead_candidate.name} 开始，确认现价是否接近建议介入位。")
        if len(candidates) > 1:
            next_names = "、".join(candidate.name for candidate in candidates[1:3])
            action_checklist.append(f"次优先关注 {next_names}，只保留最顺手的 1 到 2 只。")
        if position_plans:
            action_checklist.append("先写好介入位、止损位和止盈位，再决定要不要加入观察池。")
        if watchlist_hits:
            action_checklist.append("观察池里命中的股票先复核原计划，不要临盘随意改价位。")
        if not payload["actionable"]:
            action_checklist = [*payload["block_reasons"], "先检查同步状态和数据覆盖率，再决定是否恢复筛选。"]
        if not action_checklist:
            action_checklist = ["今天没有必须出手的票，先观察已有计划是否继续成立。"]

        do_not_chase_flags = list(
            dict.fromkeys(
                flag
                for row in shortlist
                for flag in row.get("risk_flags", [])
                if flag in {"不建议追高", "估值偏高", "临近财报窗口"}
            )
        )[:4]

        report = DailyReportResponse(
            report_date=as_of_date,
            market_regime=market_regime,
            summary=summary,
            candidates=candidates if payload["actionable"] else [],
            capital_allocation_hint=allocation,
            action_checklist=action_checklist,
            do_not_chase_flags=do_not_chase_flags,
            position_plans=position_plans if payload["actionable"] else [],
            watchlist_hits=watchlist_hits,
            generated_at=self.repository._utc_now_iso(),
            actionable=payload["actionable"],
            block_reasons=payload["block_reasons"],
            parameter_version=resolved_parameter_version,
            parameter_source=parameter_source,
        )
        should_cache = cache_result if cache_result is not None else parameter_source == "default"
        if should_cache:
            self.repository.upsert_daily_report(report.report_date, report.model_dump(mode="json"))
        return report

    def get_daily_report(self, report_date: str | None = None, parameter_version: str | None = None) -> DailyReportResponse:
        if parameter_version == "default":
            payload = self.repository.get_daily_report(report_date)
            if payload:
                report = DailyReportResponse.model_validate(payload)
                if report.parameter_source == "default":
                    return report
            return self.generate_daily_report(parameter_version="default", cache_result=True)

        if parameter_version:
            return self.generate_daily_report(parameter_version=parameter_version, cache_result=False)

        _, resolved_parameter_version, parameter_source = self.strategy_service.resolve_strategy()
        if parameter_source == "default":
            payload = self.repository.get_daily_report(report_date)
            if payload:
                report = DailyReportResponse.model_validate(payload)
                if report.parameter_source == "default":
                    return report
            return self.generate_daily_report(parameter_version="default", cache_result=True)
        return self.generate_daily_report(parameter_version=resolved_parameter_version, cache_result=False)

    def get_watchlist(self, watchlist_id: str) -> WatchlistResponse:
        return WatchlistResponse(
            watchlist_id=watchlist_id,
            items=[WatchlistItem.model_validate(item) for item in self.repository.get_watchlist(watchlist_id)],
        )

    def update_watchlist(self, watchlist_id: str, payload: WatchlistUpdate) -> WatchlistResponse:
        self.repository.replace_watchlist(watchlist_id, [item.model_dump(mode="json") for item in payload.items])
        return self.get_watchlist(watchlist_id)
