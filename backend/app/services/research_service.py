from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from datetime import date, datetime
import json
import math
import threading
from statistics import fmean

from ..models import (
    ResearchDiagnosticsResponse,
    ResearchFactorDriftItem,
    ResearchParameterView,
    ResearchYearStat,
    StrategyConfig,
)
from ..research_db import ResearchRepository
from .factor_engine import (
    FACTOR_METADATA,
    SnapshotEntry,
    build_position_plan,
    compute_snapshot,
    evaluate_hard_blocks,
    evaluate_soft_penalties,
    score_entry,
)
from .research_execution import simulate_trade
from .research_provider import BaseResearchProvider, RepositoryResearchProvider
from .strategy_service import default_strategy_config


def _mean(values: list[float]) -> float:
    return fmean(values) if values else 0.0


def _regime_from_closes(closes: list[float]) -> str:
    if len(closes) < 60:
        return "neutral"
    ma20 = _mean(closes[-20:])
    ma60 = _mean(closes[-60:])
    latest_close = closes[-1]
    if latest_close > ma20 > ma60:
        return "bullish"
    if latest_close < ma20 < ma60:
        return "cautious"
    return "neutral"


def _week_end_dates(trading_dates: list[str]) -> list[str]:
    result: list[str] = []
    for index in range(len(trading_dates) - 1):
        current = date.fromisoformat(trading_dates[index]).isocalendar()[:2]
        next_value = date.fromisoformat(trading_dates[index + 1]).isocalendar()[:2]
        if current != next_value:
            result.append(trading_dates[index])
    return result


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


class ResearchService:
    _run_lock = threading.Lock()

    def __init__(
        self,
        repository: ResearchRepository,
        business_repository,
        benchmark_code: str,
        *,
        provider: BaseResearchProvider | None = None,
    ):
        self.repository = repository
        self.business_repository = business_repository
        self.benchmark_code = benchmark_code
        self.provider = provider or RepositoryResearchProvider(business_repository)

    def start_background_refresh(self, mode: str = "incremental") -> dict:
        with self._run_lock:
            running = self.repository.get_running_run()
            if running:
                return running
            run_id = self.repository.start_run(mode, self.provider.provider_name, payload={"stage": "queued"})
            worker = threading.Thread(target=self._run_refresh, args=(run_id, mode), daemon=True)
            worker.start()
            return self.repository.get_run(run_id) or {"run_id": run_id}

    def refresh_blocking(self, mode: str = "rebuild") -> dict:
        with self._run_lock:
            running = self.repository.get_running_run()
            if running:
                return running
            run_id = self.repository.start_run(mode, self.provider.provider_name, payload={"stage": "queued"})
            self._run_refresh(run_id, mode)
            return self.repository.get_run(run_id) or {"run_id": run_id}

    def get_active_strategy_config(self) -> StrategyConfig | None:
        latest = self.get_active_parameter()
        if not latest:
            return None
        return StrategyConfig.model_validate(latest["config_json"])

    def get_active_parameter(self) -> dict | None:
        return self.repository.get_latest_parameter(approved_only=True)

    def get_strategy_config_for_version(self, version_id: str) -> StrategyConfig | None:
        parameter = self.repository.get_parameter_by_version(version_id, approved_only=True)
        if not parameter:
            return None
        return StrategyConfig.model_validate(parameter["config_json"])

    def get_status(self) -> dict[str, object]:
        running = self.repository.get_running_run()
        if running:
            payload = json.loads(running.get("payload_json") or "{}")
            return {
                "research_status": "running",
                "research_as_of_date": payload.get("as_of_date"),
                "parameter_version": None,
                "research_sample_count": int(payload.get("sample_count", self.repository.count_samples()) or 0),
            }

        diagnostic = self.repository.get_latest_diagnostic()
        if diagnostic:
            return {
                "research_status": diagnostic["status"],
                "research_as_of_date": diagnostic["as_of_date"],
                "parameter_version": diagnostic.get("parameter_version"),
                "research_sample_count": int(diagnostic.get("sample_count", 0) or 0),
            }

        latest_run = self.repository.get_latest_run()
        if latest_run and latest_run["status"] == "failed":
            return {
                "research_status": "failed",
                "research_as_of_date": None,
                "parameter_version": None,
                "research_sample_count": self.repository.count_samples(),
            }
        return {
            "research_status": "idle",
            "research_as_of_date": None,
            "parameter_version": None,
            "research_sample_count": self.repository.count_samples(),
        }

    def get_diagnostics(self) -> ResearchDiagnosticsResponse:
        diagnostic = self.repository.get_latest_diagnostic()
        if not diagnostic:
            running = self.repository.get_running_run()
            if running:
                payload = json.loads(running.get("payload_json") or "{}")
                return ResearchDiagnosticsResponse(
                    status="running",
                    as_of_date=payload.get("as_of_date"),
                    sample_count=int(payload.get("sample_count", 0) or 0),
                    warnings=list(payload.get("warnings", [])),
                )
            latest_run = self.repository.get_latest_run()
            if latest_run and latest_run["status"] == "failed":
                return ResearchDiagnosticsResponse(status="failed", warnings=[latest_run.get("message") or "研究刷新失败。"])
            return ResearchDiagnosticsResponse(status="idle")

        payload = diagnostic["payload_json"]
        parameter_view = None
        parameter_payload = payload.get("parameter")
        if parameter_payload:
            parameter_view = ResearchParameterView.model_validate(parameter_payload)
        return ResearchDiagnosticsResponse(
            status=diagnostic["status"],
            as_of_date=diagnostic["as_of_date"],
            parameter_version=diagnostic.get("parameter_version"),
            sample_count=int(diagnostic.get("sample_count", 0) or 0),
            headline_sample_count=int(diagnostic.get("headline_sample_count", 0) or 0),
            source_quality_summary=payload.get("source_quality_summary", {}),
            year_breakdown=[ResearchYearStat.model_validate(item) for item in payload.get("year_breakdown", [])],
            regime_breakdown=[ResearchYearStat.model_validate(item) for item in payload.get("regime_breakdown", [])],
            factor_drift=[ResearchFactorDriftItem.model_validate(item) for item in payload.get("factor_drift", [])],
            warnings=payload.get("warnings", []),
            parameter=parameter_view,
        )

    def _candidate_configs(self) -> list[tuple[str, StrategyConfig]]:
        baseline = default_strategy_config()

        tech_tilt = deepcopy(baseline)
        tech_tilt.name = "Balanced Technical Tilt"
        tech_tilt.weights.update(
            {
                "ma_trend": 0.14,
                "relative_strength_60d": 0.13,
                "volume_surge_20d": 0.08,
                "pe_score": 0.05,
                "pb_score": 0.05,
            }
        )
        tech_tilt.confidence_floors = {"bullish": 58.0, "neutral": 64.0, "cautious": 72.0}

        quality_tilt = deepcopy(baseline)
        quality_tilt.name = "Balanced Quality Tilt"
        quality_tilt.weights.update(
            {
                "roe": 0.1,
                "revenue_yoy": 0.09,
                "profit_yoy": 0.11,
                "cashflow_quality": 0.09,
                "ma_trend": 0.09,
                "relative_strength_60d": 0.09,
            }
        )
        quality_tilt.stock_pool.exclude_negative_profit_yoy = True

        defensive = deepcopy(baseline)
        defensive.name = "Balanced Defensive"
        defensive.risk.max_volatility_20d = 0.05
        defensive.risk.industry_cap = 1
        defensive.stock_pool.min_avg_turnover_20d = 260_000_000
        defensive.confidence_floors = {"bullish": 62.0, "neutral": 68.0, "cautious": 76.0}

        strict_growth = deepcopy(baseline)
        strict_growth.name = "Balanced Strict Growth"
        strict_growth.stock_pool.exclude_negative_profit_yoy = True
        strict_growth.stock_pool.exclude_negative_revenue_yoy = True
        strict_growth.stock_pool.min_price = 6.0
        strict_growth.weights.update({"revenue_yoy": 0.1, "profit_yoy": 0.11, "pe_score": 0.06, "pb_score": 0.05})

        return [
            ("balanced_baseline", baseline),
            ("balanced_tech_tilt", tech_tilt),
            ("balanced_quality_tilt", quality_tilt),
            ("balanced_defensive", defensive),
            ("balanced_strict_growth", strict_growth),
        ]

    def _import_research_data(self, run_id: str, mode: str) -> None:
        latest_price = None if mode == "rebuild" else self.repository.get_latest_import_date("research_price_bar", "trade_date")
        latest_financial = None if mode == "rebuild" else self.repository.get_latest_import_date("research_financial_record", "report_date")
        latest_event = None if mode == "rebuild" else self.repository.get_latest_import_date("research_security_state_event", "event_date")

        self.repository.update_run_progress(run_id, stage="importing_states", message="导入研究证券状态。")
        self.repository.upsert_security_state_events(
            self.provider.fetch_security_state_events(since_date=date.fromisoformat(latest_event) if latest_event else None)
        )
        self.repository.update_run_progress(run_id, stage="importing_prices", message="导入研究价格数据。")
        self.repository.upsert_price_bars(
            self.provider.fetch_price_bars(since_date=date.fromisoformat(latest_price) if latest_price else None)
        )
        self.repository.update_run_progress(run_id, stage="importing_financials", message="导入研究财务数据。")
        self.repository.upsert_financial_records(
            self.provider.fetch_financial_records(since_date=date.fromisoformat(latest_financial) if latest_financial else None)
        )
        self.repository.upsert_corporate_actions(self.provider.fetch_corporate_actions())

    def _freeze_samples(self, run_id: str, mode: str) -> str | None:
        trading_dates = self.repository.get_trading_dates(price_basis="adjusted")
        if not trading_dates:
            return None

        signal_dates = _week_end_dates(trading_dates)
        if mode != "rebuild":
            last_sample_date = self.repository.get_last_sample_date()
            if last_sample_date:
                signal_dates = [signal_date for signal_date in signal_dates if signal_date > last_sample_date]

        latest_signal_date = self.repository.get_last_sample_date()
        for index, signal_date in enumerate(signal_dates, start=1):
            meta_map = self.repository.get_meta_snapshot(signal_date)
            price_map = self.repository.get_price_map(as_of_date=signal_date, price_basis="adjusted")
            financial_map = self.repository.get_visible_financials(as_of_date=signal_date)
            snapshot = compute_snapshot(meta_map, price_map, financial_map, signal_date)
            regime = self._market_regime(signal_date)
            rows: list[dict] = []
            for entry in snapshot.values():
                if not entry.factor_scores:
                    continue
                visible_financial = financial_map.get(entry.code, {})
                headline_eligible = (
                    visible_financial.get("publish_date_quality") == "actual"
                    and visible_financial.get("source_quality") == "actual"
                    and self.repository.has_high_quality_execution_window(
                        entry.code,
                        signal_date,
                        default_strategy_config().rebalance.holding_period_days,
                    )
                )
                rows.append(
                    {
                        "code": entry.code,
                        "name": entry.name,
                        "industry": entry.industry,
                        "board": entry.board,
                        "regime": regime,
                        "headline_eligible": headline_eligible,
                        "source_quality": "actual" if headline_eligible else visible_financial.get("source_quality", "estimated"),
                        "snapshot": asdict(entry),
                        "factor_scores": entry.factor_scores,
                        "section_scores": entry.section_scores,
                    }
                )
            self.repository.replace_samples_for_signal_date(signal_date, rows)
            latest_signal_date = signal_date
            if index % 8 == 0 or index == len(signal_dates):
                self.repository.update_run_progress(
                    run_id,
                    stage="freezing_samples",
                    message=f"冻结周频研究样本 {index}/{len(signal_dates)}。",
                    payload={"as_of_date": signal_date, "sample_count": self.repository.count_samples()},
                )
        return latest_signal_date or (signal_dates[-1] if signal_dates else self.repository.get_last_sample_date())

    def _market_regime(self, as_of_date: str) -> str:
        price_map = self.repository.get_price_map(as_of_date=as_of_date, price_basis="adjusted")
        benchmark_bars = price_map.get(self.benchmark_code) or price_map.get("000300.SH") or []
        closes = [bar["close"] for bar in benchmark_bars if bar["trade_date"] <= as_of_date]
        return _regime_from_closes(closes)

    def _walk_forward_windows(self, signal_dates: list[str]) -> list[dict[str, list[str]]]:
        train_size = 52
        validation_size = 13
        windows: list[dict[str, list[str]]] = []
        for end_index in range(train_size, len(signal_dates), validation_size):
            train_dates = signal_dates[max(0, end_index - train_size) : end_index]
            validation_dates = signal_dates[end_index : end_index + validation_size]
            if len(train_dates) < 20 or len(validation_dates) < 4:
                continue
            windows.append({"train": train_dates, "validation": validation_dates})
        return windows

    def _build_bar_lookup(self, *, price_basis: str) -> dict[str, dict[str, dict]]:
        price_map = self.repository.get_price_map(price_basis=price_basis)
        return {code: {bar["trade_date"]: bar for bar in bars} for code, bars in price_map.items()}

    def _evaluate_dates(
        self,
        config: StrategyConfig,
        signal_dates: list[str],
        bar_lookup: dict[str, dict[str, dict]],
        trading_dates: list[str],
        date_index: dict[str, int],
        benchmark_code: str,
    ) -> list[dict]:
        records: list[dict] = []
        for signal_date in signal_dates:
            sample_rows = self.repository.get_samples_by_signal_date(signal_date)
            if not sample_rows:
                continue
            ranked: list[dict] = []
            for sample in sample_rows:
                entry = SnapshotEntry(**sample["snapshot_json"])
                hard_blocks = evaluate_hard_blocks(entry, config)
                if hard_blocks:
                    continue
                penalties = evaluate_soft_penalties(entry, config)
                penalty_points = min(sum(item["points"] for item in penalties), 25.0)
                weighted_score = score_entry(entry, config)["total_score"] - penalty_points
                ranked.append(
                    {
                        "entry": entry,
                        "weighted_score": round(weighted_score, 4),
                        "headline_eligible": bool(sample["headline_eligible"]),
                    }
                )

            shortlisted: list[dict] = []
            industry_counter: dict[str, int] = {}
            for row in sorted(ranked, key=lambda item: item["weighted_score"], reverse=True):
                industry = row["entry"].industry
                if industry_counter.get(industry, 0) >= config.risk.industry_cap:
                    continue
                industry_counter[industry] = industry_counter.get(industry, 0) + 1
                shortlisted.append(row)
                if len(shortlisted) >= config.rebalance.top_n:
                    break
            if not shortlisted:
                continue

            trades = []
            headline_trades = 0
            for row in shortlisted:
                entry = row["entry"]
                position_plan = build_position_plan(entry, config)
                executed = simulate_trade(
                    code=entry.code,
                    signal_date=signal_date,
                    trading_dates=trading_dates,
                    date_index=date_index,
                    bar_lookup=bar_lookup,
                    benchmark_code=benchmark_code,
                    holding_period_days=config.rebalance.holding_period_days,
                    stop_loss=position_plan.stop_loss,
                    take_profit=position_plan.take_profit,
                    commission_bps=config.execution.commission_bps,
                    slippage_bps=config.execution.slippage_bps,
                    stamp_duty_bps=config.execution.stamp_duty_bps,
                )
                if executed is None or executed.blocked:
                    continue
                trades.append(executed)
                if row["headline_eligible"]:
                    headline_trades += 1
            if not trades:
                continue

            records.append(
                {
                    "signal_date": signal_date,
                    "regime": sample_rows[0]["regime"],
                    "sample_count": len(trades),
                    "headline_sample_count": headline_trades,
                    "excess_return": round(_mean([trade.excess_return for trade in trades]), 6),
                    "hit_rate": round(_mean([1.0 if trade.excess_return > 0 else 0.0 for trade in trades]), 6),
                    "max_drawdown": round(min(trade.max_drawdown for trade in trades), 6),
                }
            )
        return records

    def _stability_score(self, validation_records: list[dict]) -> float:
        if not validation_records:
            return -999.0
        excess = [item["excess_return"] for item in validation_records]
        hit = [item["hit_rate"] for item in validation_records]
        drawdowns = [item["max_drawdown"] for item in validation_records]
        avg_excess = _mean(excess)
        avg_hit = _mean(hit)
        avg_drawdown = abs(_mean(drawdowns))
        dispersion = 0.0
        if len(excess) > 1:
            dispersion = math.sqrt(_mean([(value - avg_excess) ** 2 for value in excess]))
        return round(avg_excess * 120 + avg_hit * 18 - avg_drawdown * 30 - dispersion * 55, 4)

    def _factor_drift(self, signal_dates: list[str]) -> list[dict]:
        if not signal_dates:
            return []
        recent_dates = signal_dates[-26:]
        baseline_dates = signal_dates[-78:-26] if len(signal_dates) > 26 else signal_dates[:-26]
        if not baseline_dates:
            baseline_dates = signal_dates[:-len(recent_dates)] or signal_dates

        def collect_means(target_dates: list[str]) -> dict[str, float]:
            values: dict[str, list[float]] = {factor: [] for factor in FACTOR_METADATA}
            for signal_date in target_dates:
                for row in self.repository.get_samples_by_signal_date(signal_date):
                    for factor, score in row["factor_scores_json"].items():
                        values.setdefault(factor, []).append(float(score))
            return {factor: round(_mean(scores), 4) for factor, scores in values.items() if scores}

        recent = collect_means(recent_dates)
        baseline = collect_means(baseline_dates)
        drift_rows = []
        for factor in FACTOR_METADATA:
            recent_mean = float(recent.get(factor, 0.0))
            baseline_mean = float(baseline.get(factor, recent_mean))
            drift_rows.append(
                {
                    "factor": factor,
                    "recent_mean": round(recent_mean, 4),
                    "baseline_mean": round(baseline_mean, 4),
                    "drift": round(recent_mean - baseline_mean, 4),
                }
            )
        return sorted(drift_rows, key=lambda item: abs(item["drift"]), reverse=True)[:6]

    def _group_records(self, records: list[dict], key_name: str) -> list[dict]:
        grouped: dict[str, list[dict]] = {}
        for item in records:
            grouped.setdefault(item[key_name], []).append(item)
        result = []
        for label, items in sorted(grouped.items()):
            result.append(
                {
                    "label": label,
                    "sample_count": sum(item["sample_count"] for item in items),
                    "headline_sample_count": sum(item["headline_sample_count"] for item in items),
                    "excess_return": round(_mean([item["excess_return"] for item in items]), 6),
                    "hit_rate": round(_mean([item["hit_rate"] for item in items]), 6),
                    "max_drawdown": round(min(item["max_drawdown"] for item in items), 6),
                }
            )
        return result

    def _calibrate(self, as_of_date: str | None) -> None:
        signal_dates = self.repository.get_sample_signal_dates()
        if not signal_dates:
            return

        trading_dates = self.repository.get_trading_dates(price_basis="raw")
        bar_lookup = self._build_bar_lookup(price_basis="raw")
        benchmark_code = self.benchmark_code if self.benchmark_code in bar_lookup else "000300.SH"
        date_index = {trade_date: index for index, trade_date in enumerate(trading_dates)}
        windows = self._walk_forward_windows(signal_dates)
        candidates = self._candidate_configs()

        candidate_payloads: list[dict] = []
        for candidate_name, config in candidates:
            validation_records: list[dict] = []
            for window in windows or [{"train": [], "validation": signal_dates[-26:]}]:
                validation_records.extend(
                    self._evaluate_dates(
                        config,
                        window["validation"],
                        bar_lookup,
                        trading_dates,
                        date_index,
                        benchmark_code,
                    )
                )
            stability_score = self._stability_score(validation_records)
            candidate_payloads.append(
                {
                    "candidate_name": candidate_name,
                    "config": config,
                    "validation_records": validation_records,
                    "stability_score": stability_score,
                    "sample_count": sum(item["sample_count"] for item in validation_records),
                    "headline_sample_count": sum(item["headline_sample_count"] for item in validation_records),
                }
            )

        if not candidate_payloads:
            return

        best = max(
            candidate_payloads,
            key=lambda item: (
                item["headline_sample_count"] > 0,
                item["stability_score"],
                item["sample_count"],
            ),
        )
        version_id = f"{as_of_date or signal_dates[-1]}::{best['candidate_name']}"
        approved = len(windows) >= 2 and best["headline_sample_count"] >= 40
        status = "approved" if approved else "candidate"
        summary_payload = {
            "candidate_name": best["candidate_name"],
            "window_count": len(windows),
            "validation_record_count": len(best["validation_records"]),
            "mean_excess_return": round(_mean([item["excess_return"] for item in best["validation_records"]]), 6),
            "mean_hit_rate": round(_mean([item["hit_rate"] for item in best["validation_records"]]), 6),
        }
        self.repository.upsert_parameter_version(
            version_id=version_id,
            status=status,
            as_of_date=as_of_date or signal_dates[-1],
            sample_count=int(best["sample_count"]),
            headline_sample_count=int(best["headline_sample_count"]),
            stability_score=float(best["stability_score"]),
            config_payload=best["config"].model_dump(mode="json"),
            summary_payload=summary_payload,
            approve=approved,
        )

        decorated_records = [
            {**item, "label": item["signal_date"][:4]}
            for item in best["validation_records"]
        ]
        diagnostic_status = "ready" if approved else "limited"
        warnings = []
        if best["headline_sample_count"] < 40:
            warnings.append("高质量 PIT 样本不足，当前版本不会接管线上参数。")
        if self.repository.source_quality_summary().get("proxy", 0) > 0:
            warnings.append("当前研究库仍包含 proxy/raw 价格口径，headline 统计只计入 actual 样本。")

        parameter_row = self.repository.get_latest_parameter(approved_only=approved) or self.repository.get_latest_parameter()
        parameter_payload = None
        if parameter_row:
            parameter_payload = ResearchParameterView(
                version=parameter_row["version_id"],
                created_at=parameter_row["created_at"],
                approved_at=parameter_row.get("approved_at"),
                stability_score=parameter_row["stability_score"],
                sample_count=parameter_row["sample_count"],
                headline_sample_count=parameter_row["headline_sample_count"],
                config=StrategyConfig.model_validate(parameter_row["config_json"]),
            ).model_dump(mode="json")

        payload = {
            "source_quality_summary": self.repository.source_quality_summary(),
            "year_breakdown": self._group_records(decorated_records, "label"),
            "regime_breakdown": self._group_records(best["validation_records"], "regime"),
            "factor_drift": self._factor_drift(signal_dates),
            "warnings": warnings,
            "parameter": parameter_payload,
        }
        self.repository.upsert_diagnostic_snapshot(
            as_of_date=as_of_date or signal_dates[-1],
            status=diagnostic_status,
            parameter_version=version_id,
            sample_count=self.repository.count_samples(),
            headline_sample_count=self.repository.count_headline_samples(),
            payload=payload,
        )

    def _run_refresh(self, run_id: str, mode: str) -> None:
        try:
            self._import_research_data(run_id, mode)
            latest_signal_date = self._freeze_samples(run_id, mode)
            self.repository.update_run_progress(
                run_id,
                stage="calibrating",
                message="运行滚动研究校准。",
                payload={"as_of_date": latest_signal_date, "sample_count": self.repository.count_samples()},
            )
            if latest_signal_date:
                self._calibrate(latest_signal_date)
            self.repository.finish_run(
                run_id,
                "success",
                "research refresh completed",
                {
                    "as_of_date": latest_signal_date,
                    "sample_count": self.repository.count_samples(),
                    "headline_sample_count": self.repository.count_headline_samples(),
                },
            )
        except Exception as exc:  # pragma: no cover - integration path
            self.repository.finish_run(
                run_id,
                "failed",
                str(exc),
                {"warnings": [str(exc)], "sample_count": self.repository.count_samples()},
            )
