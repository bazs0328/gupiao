from __future__ import annotations

from fastapi import HTTPException

from ..models import (
    ContributionItem,
    EventCalendarItem,
    FactorDetail,
    FinancialView,
    ModelSnapshotView,
    PositionPlan,
    PricePoint,
    RankingRow,
    RankingsResponse,
    SimilarHistorySample,
    StockDetailResponse,
)


class RankingService:
    def __init__(self, repository, strategy_service, analytics_service):
        self.repository = repository
        self.strategy_service = strategy_service
        self.analytics_service = analytics_service

    def get_rankings(self, limit: int = 30, parameter_version: str | None = None) -> RankingsResponse:
        config, resolved_parameter_version, parameter_source = self.strategy_service.resolve_strategy(parameter_version)
        latest_date, rows, payload = self.analytics_service.get_ranked_rows(config, limit=limit)
        latest_sync = self.repository.get_latest_run("sync")
        return RankingsResponse(
            as_of_date=latest_date,
            latest_sync=latest_sync["finished_at"] if latest_sync else None,
            items=[RankingRow.model_validate(row) for row in rows],
            actionable=payload["actionable"],
            block_reasons=payload["block_reasons"],
            parameter_version=resolved_parameter_version,
            parameter_source=parameter_source,
        )

    def get_stock_detail(self, code: str, parameter_version: str | None = None) -> StockDetailResponse:
        config, resolved_parameter_version, parameter_source = self.strategy_service.resolve_strategy(parameter_version)
        latest_date, entry, analysis, payload = self.analytics_service.get_analysis_for_code(code, config)
        if not entry or not analysis:
            raise HTTPException(status_code=404, detail="Stock not found.")

        financial_raw = self.repository.get_visible_financials(as_of_date=latest_date).get(code)
        financial_snapshot = None
        if financial_raw:
            cashflow_quality = (
                financial_raw["operating_cashflow"] / abs(financial_raw["net_profit"])
                if financial_raw["net_profit"]
                else 0.0
            )
            financial_snapshot = FinancialView(
                report_date=financial_raw["report_date"],
                publish_date=financial_raw["publish_date"],
                roe=financial_raw["roe"],
                revenue_yoy=financial_raw["revenue_yoy"],
                profit_yoy=financial_raw["profit_yoy"],
                cashflow_quality=round(cashflow_quality, 2),
                pe_ttm=financial_raw["pe_ttm"],
                pb=financial_raw["pb"],
                debt_ratio=financial_raw["debt_ratio"],
            )

        return StockDetailResponse(
            code=entry.code,
            name=entry.name,
            industry=entry.industry,
            signal_date=latest_date,
            current_price=round(entry.current_price, 2),
            total_score=analysis["total_score"],
            explanation_summary=analysis["explanation_summary"],
            plus_factors=analysis["plus_factors"],
            minus_factors=analysis["minus_factors"],
            ineligible_reasons=analysis["ineligible_reasons"],
            factor_scores=[FactorDetail.model_validate(item) for item in analysis["factor_details"]],
            contribution_breakdown=[ContributionItem.model_validate(item) for item in analysis["contribution_breakdown"][:8]],
            peer_percentiles=analysis["peer_percentiles"],
            event_calendar=[EventCalendarItem.model_validate(item) for item in analysis["event_calendar"]],
            model_snapshot=ModelSnapshotView.model_validate(analysis["model_snapshot"]),
            reason_not_to_buy_now=analysis["reason_not_to_buy_now"],
            similar_history_samples=[
                SimilarHistorySample.model_validate(item) for item in analysis["similar_history_samples"]
            ],
            price_history=[
                PricePoint(
                    date=item["trade_date"],
                    open=item["open"],
                    close=item["close"],
                    volume=item["volume"],
                )
                for item in self.repository.get_price_history(code, limit=90)
            ],
            financial_snapshot=financial_snapshot,
            position_plan=PositionPlan.model_validate(analysis["position_plan"]),
            evidence_snapshot={
                "board": entry.board,
                "latest_price_date": latest_date,
                "financial_report_date": entry.financial_report_date,
                "financial_publish_date": entry.financial_publish_date,
                "financial_staleness_days": entry.financial_staleness_days,
                "factor_coverage_pct": round(entry.data_completeness * 100, 2),
                "data_completeness_score": round(entry.data_completeness * 100, 2),
                "validation_health": payload["validation_summary"].current_model_health,
                "calibration_bucket": analysis["model_snapshot"]["calibration_bucket"],
                "bucket_sample_count": analysis["model_snapshot"]["bucket_sample_count"],
                "expected_bucket_excess_return": analysis["model_snapshot"]["expected_excess_return"],
                "agreement_count": analysis["model_snapshot"]["agreement_count"],
                "recommendation_blocked": not analysis["actionable"],
                "block_reasons": analysis["block_reasons"],
                "soft_penalties": analysis["soft_penalties"],
                "sync_run_id": self.repository.get_latest_run("sync")["run_id"] if self.repository.get_latest_run("sync") else None,
            },
            parameter_version=resolved_parameter_version,
            parameter_source=parameter_source,
        )
