from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import math
from statistics import fmean

from ..models import (
    SoftPenaltyItem,
    ValidationBucket,
    ValidationSummary,
    ValidationWindow,
)
from .factor_engine import (
    SnapshotEntry,
    build_position_plan,
    compute_snapshot,
    evaluate_hard_blocks,
    evaluate_soft_penalties,
)
from .strategy_service import FACTOR_METADATA, default_strategy_config, normalize_weights


FEATURE_NAMES = tuple(FACTOR_METADATA.keys())
QUALITY_FACTORS = ("roe", "revenue_yoy", "profit_yoy", "cashflow_quality", "debt_ratio")
SECTION_FACTORS = {
    "technical": tuple(factor for factor, meta in FACTOR_METADATA.items() if meta["group"] == "technical"),
    "fundamental": tuple(factor for factor, meta in FACTOR_METADATA.items() if meta["group"] == "fundamental"),
    "risk": tuple(factor for factor, meta in FACTOR_METADATA.items() if meta["group"] == "risk"),
}
VIEW_FACTORS = {
    "momentum": ("ma_trend", "relative_strength_60d", "volume_surge_20d", "liquidity"),
    "quality": ("roe", "revenue_yoy", "profit_yoy", "cashflow_quality"),
    "defensive": ("debt_ratio", "volatility_20d", "liquidity", "roe"),
}
MODEL_WINDOW_DAYS = 756
FORWARD_WINDOW_DAYS = 10
REGIME_WEIGHTS = {
    "bullish": (0.60, 0.20, 0.20),
    "neutral": (0.50, 0.25, 0.25),
    "cautious": (0.35, 0.25, 0.40),
}
DEFAULT_REGIME_CONFIDENCE_FLOOR = {"bullish": 60.0, "neutral": 66.0, "cautious": 74.0}


@dataclass(slots=True)
class TrainingSample:
    signal_date: str
    code: str
    name: str
    industry: str
    board: str
    features: list[float]
    excess_return: float
    asset_return: float
    benchmark_return: float
    max_drawdown: float
    downside_hit: int


@dataclass(slots=True)
class ModelFit:
    alpha_weights: list[float]
    risk_weights: list[float]
    train_start: str | None
    train_end: str | None
    sample_count: int


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _mean(values: list[float]) -> float:
    return fmean(values) if values else 0.0


def _weights(config) -> dict[str, float]:
    return normalize_weights(config.weights)


def _subset_weights(config, factors: tuple[str, ...]) -> dict[str, float]:
    weights = _weights(config)
    available = {factor: weights.get(factor, 0.0) for factor in factors}
    total = sum(available.values())
    if total <= 0:
        fallback = 1 / max(len(factors), 1)
        return {factor: fallback for factor in factors}
    return {factor: value / total for factor, value in available.items()}


def _weighted_factor_score(entry: SnapshotEntry, factor_weights: dict[str, float]) -> float:
    available = {factor: weight for factor, weight in factor_weights.items() if factor in entry.factor_scores}
    total = sum(available.values())
    if total <= 0:
        scores = [entry.factor_scores[factor] for factor in factor_weights if factor in entry.factor_scores]
        return round(_mean(scores), 2)
    return round(
        sum(entry.factor_scores[factor] * (weight / total) for factor, weight in available.items()),
        2,
    )


def _section_scores(entry: SnapshotEntry, config) -> dict[str, float]:
    return {
        group: _weighted_factor_score(entry, _subset_weights(config, factors))
        for group, factors in SECTION_FACTORS.items()
    }


def _factor_contributions(entry: SnapshotEntry, config) -> list[dict]:
    weights = _weights(config)
    details: list[dict] = []
    for factor, score in entry.factor_scores.items():
        details.append(
            {
                "factor": factor,
                "label": FACTOR_METADATA[factor]["label"],
                "group": FACTOR_METADATA[factor]["group"],
                "raw_value": round(entry.raw_factors.get(factor, 0.0), 4),
                "score": round(score, 2),
                "contribution": round((score - 50) * weights.get(factor, 0.0), 2),
                "direction": "positive" if score >= 50 else "negative",
            }
        )
    return sorted(details, key=lambda item: item["contribution"], reverse=True)


def _feature_vector(entry: SnapshotEntry, config) -> list[float]:
    weights = _weights(config)
    return [(((entry.factor_scores.get(name, 50.0) / 100) - 0.5) * 2) * weights.get(name, 0.0) for name in FEATURE_NAMES]


def _dot(weights: list[float], features: list[float]) -> float:
    return weights[0] + sum(weight * feature for weight, feature in zip(weights[1:], features, strict=False))


def _sigmoid(value: float) -> float:
    bounded = _clamp(value, -24, 24)
    return 1 / (1 + math.exp(-bounded))


def _safe_corr(left: list[float], right: list[float]) -> float:
    if len(left) != len(right) or len(left) < 2:
        return 0.0
    left_mean = _mean(left)
    right_mean = _mean(right)
    left_dev = [value - left_mean for value in left]
    right_dev = [value - right_mean for value in right]
    denominator = math.sqrt(sum(value * value for value in left_dev) * sum(value * value for value in right_dev))
    if denominator <= 0:
        return 0.0
    return sum(left_value * right_value for left_value, right_value in zip(left_dev, right_dev, strict=False)) / denominator


def _solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    augmented = [row[:] + [vector[index]] for index, row in enumerate(matrix)]
    for pivot in range(size):
        pivot_row = max(range(pivot, size), key=lambda row_index: abs(augmented[row_index][pivot]))
        if abs(augmented[pivot_row][pivot]) < 1e-9:
            continue
        if pivot_row != pivot:
            augmented[pivot], augmented[pivot_row] = augmented[pivot_row], augmented[pivot]
        pivot_value = augmented[pivot][pivot]
        augmented[pivot] = [value / pivot_value for value in augmented[pivot]]
        for row_index in range(size):
            if row_index == pivot:
                continue
            factor = augmented[row_index][pivot]
            augmented[row_index] = [
                value - factor * pivot_value
                for value, pivot_value in zip(augmented[row_index], augmented[pivot], strict=False)
            ]
    return [row[-1] for row in augmented]


def _fit_ridge(samples: list[TrainingSample], alpha: float = 1.6) -> list[float]:
    feature_count = len(FEATURE_NAMES) + 1
    xtx = [[0.0 for _ in range(feature_count)] for _ in range(feature_count)]
    xty = [0.0 for _ in range(feature_count)]
    for sample in samples:
        row = [1.0, *sample.features]
        for row_index in range(feature_count):
            xty[row_index] += row[row_index] * sample.excess_return
            for col_index in range(feature_count):
                xtx[row_index][col_index] += row[row_index] * row[col_index]
    for index in range(1, feature_count):
        xtx[index][index] += alpha
    return _solve_linear_system(xtx, xty)


def _fit_logistic(samples: list[TrainingSample], learning_rate: float = 0.12, iterations: int = 240, penalty: float = 0.4) -> list[float]:
    feature_count = len(FEATURE_NAMES) + 1
    weights = [0.0 for _ in range(feature_count)]
    if not samples:
        return weights
    for _ in range(iterations):
        gradients = [0.0 for _ in range(feature_count)]
        for sample in samples:
            row = [1.0, *sample.features]
            prediction = _sigmoid(_dot(weights, sample.features))
            error = prediction - sample.downside_hit
            for index, value in enumerate(row):
                gradients[index] += error * value
        sample_count = max(len(samples), 1)
        for index in range(feature_count):
            gradients[index] /= sample_count
            if index > 0:
                gradients[index] += penalty * weights[index] / sample_count
            weights[index] -= learning_rate * gradients[index]
    return weights


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


def _bucket_label(score: float) -> str:
    if score >= 80:
        return "80-100"
    if score >= 60:
        return "60-80"
    if score >= 40:
        return "40-60"
    if score >= 20:
        return "20-40"
    return "0-20"


def _confidence_floor(config, regime: str) -> float:
    return float(config.confidence_floors.get(regime, DEFAULT_REGIME_CONFIDENCE_FLOOR[regime]))


class AnalyticsService:
    def __init__(self, repository, benchmark_code: str):
        self.repository = repository
        self.benchmark_code = benchmark_code
        self._snapshot_cache: dict[str, dict[str, SnapshotEntry]] = {}
        self._analysis_cache: dict[tuple[str, str], dict] = {}
        self._validation_cache: dict[tuple[str, str], dict] = {}
        self._default_config_key = self._config_key(default_strategy_config())

    @staticmethod
    def _config_key(config) -> str:
        return json.dumps(config.model_dump(mode="json"), ensure_ascii=False, sort_keys=True)

    def _get_latest_date(self, as_of_date: str | None = None) -> str:
        latest_date = as_of_date or self.repository.get_latest_trade_date()
        if not latest_date:
            raise ValueError("No market data synced yet.")
        return latest_date

    def _get_snapshot(self, as_of_date: str) -> dict[str, SnapshotEntry]:
        cached = self._snapshot_cache.get(as_of_date)
        if cached is not None:
            return cached
        snapshot = compute_snapshot(
            self.repository.get_stock_meta(),
            self.repository.get_price_map(as_of_date=as_of_date),
            self.repository.get_visible_financials(as_of_date=as_of_date),
            as_of_date,
        )
        self._snapshot_cache[as_of_date] = snapshot
        return snapshot

    def _get_bar_lookup(self) -> dict[str, dict[str, dict]]:
        price_map = self.repository.get_price_map()
        return {code: {bar["trade_date"]: bar for bar in bars} for code, bars in price_map.items()}

    def _resolve_benchmark_code(self, bar_lookup: dict[str, dict[str, dict]]) -> str:
        if self.benchmark_code in bar_lookup:
            return self.benchmark_code
        return "000300.SH"

    def _get_week_end_dates(self, trading_dates: list[str]) -> list[str]:
        week_end_dates: list[str] = []
        for index in range(len(trading_dates) - FORWARD_WINDOW_DAYS):
            current = date.fromisoformat(trading_dates[index]).isocalendar()[:2]
            next_value = date.fromisoformat(trading_dates[index + 1]).isocalendar()[:2]
            if current != next_value:
                week_end_dates.append(trading_dates[index])
        return week_end_dates

    def _forward_metrics(
        self,
        code: str,
        signal_date: str,
        trading_dates: list[str],
        date_index: dict[str, int],
        bar_lookup: dict[str, dict[str, dict]],
        benchmark_code: str,
    ) -> dict | None:
        signal_index = date_index.get(signal_date)
        if signal_index is None or signal_index + FORWARD_WINDOW_DAYS >= len(trading_dates):
            return None
        end_date = trading_dates[signal_index + FORWARD_WINDOW_DAYS]
        asset_signal = bar_lookup.get(code, {}).get(signal_date)
        asset_end = bar_lookup.get(code, {}).get(end_date)
        benchmark_signal = bar_lookup.get(benchmark_code, {}).get(signal_date)
        benchmark_end = bar_lookup.get(benchmark_code, {}).get(end_date)
        if not asset_signal or not asset_end or not benchmark_signal or not benchmark_end:
            return None

        asset_return = asset_end["close"] / asset_signal["close"] - 1 if asset_signal["close"] else 0.0
        benchmark_return = benchmark_end["close"] / benchmark_signal["close"] - 1 if benchmark_signal["close"] else 0.0
        future_dates = trading_dates[signal_index + 1 : signal_index + FORWARD_WINDOW_DAYS + 1]
        drawdown_points = []
        for future_date in future_dates:
            future_bar = bar_lookup.get(code, {}).get(future_date)
            if not future_bar or not asset_signal["close"]:
                continue
            drawdown_points.append(future_bar["close"] / asset_signal["close"] - 1)
        max_drawdown = min(drawdown_points) if drawdown_points else 0.0
        return {
            "trade_date": trading_dates[signal_index + 1],
            "end_date": end_date,
            "asset_return": asset_return,
            "benchmark_return": benchmark_return,
            "excess_return": asset_return - benchmark_return,
            "max_drawdown": max_drawdown,
            "downside_hit": 1 if max_drawdown <= -0.08 else 0,
        }

    def _paper_trade_metrics(
        self,
        code: str,
        signal_date: str,
        trading_dates: list[str],
        date_index: dict[str, int],
        bar_lookup: dict[str, dict[str, dict]],
        benchmark_code: str,
    ) -> dict | None:
        signal_index = date_index.get(signal_date)
        if signal_index is None or signal_index + FORWARD_WINDOW_DAYS >= len(trading_dates):
            return None
        trade_date = trading_dates[signal_index + 1]
        exit_date = trading_dates[signal_index + FORWARD_WINDOW_DAYS]
        entry_bar = bar_lookup.get(code, {}).get(trade_date)
        exit_bar = bar_lookup.get(code, {}).get(exit_date)
        benchmark_entry = bar_lookup.get(benchmark_code, {}).get(trade_date)
        benchmark_exit = bar_lookup.get(benchmark_code, {}).get(exit_date)
        if not entry_bar or not exit_bar or not benchmark_entry or not benchmark_exit or not entry_bar["open"]:
            return None
        entry_price = entry_bar["open"]
        exit_price = exit_bar["close"]
        asset_return = exit_price / entry_price - 1 if entry_price else 0.0
        benchmark_return = benchmark_exit["close"] / benchmark_entry["open"] - 1 if benchmark_entry["open"] else 0.0
        future_dates = trading_dates[signal_index + 1 : signal_index + FORWARD_WINDOW_DAYS + 1]
        drawdowns = []
        for future_date in future_dates:
            future_bar = bar_lookup.get(code, {}).get(future_date)
            if not future_bar or not entry_price:
                continue
            drawdowns.append(future_bar["close"] / entry_price - 1)
        max_drawdown = min(drawdowns) if drawdowns else 0.0
        return {
            "entry_trade_date": trade_date,
            "entry_price": round(entry_price, 4),
            "exit_trade_date": exit_date,
            "exit_price": round(exit_price, 4),
            "asset_return": asset_return,
            "benchmark_return": benchmark_return,
            "excess_return": asset_return - benchmark_return,
            "max_drawdown": max_drawdown,
            "outcome": "win" if asset_return - benchmark_return > 0 else "loss",
        }

    def _training_samples(self, config, as_of_date: str | None = None) -> list[TrainingSample]:
        trading_dates = self.repository.get_trading_dates(security_type="equity")
        if not trading_dates:
            return []
        bar_lookup = self._get_bar_lookup()
        benchmark_code = self._resolve_benchmark_code(bar_lookup)
        date_index = {trade_date: index for index, trade_date in enumerate(trading_dates)}
        cutoff = as_of_date or trading_dates[-1]
        samples: list[TrainingSample] = []
        for signal_date in self._get_week_end_dates(trading_dates):
            if signal_date >= cutoff:
                continue
            snapshot = self._get_snapshot(signal_date)
            for entry in snapshot.values():
                if not entry.factor_scores or evaluate_hard_blocks(entry, config):
                    continue
                forward = self._forward_metrics(
                    entry.code,
                    signal_date,
                    trading_dates,
                    date_index,
                    bar_lookup,
                    benchmark_code,
                )
                if not forward:
                    continue
                samples.append(
                    TrainingSample(
                        signal_date=signal_date,
                        code=entry.code,
                        name=entry.name,
                        industry=entry.industry,
                        board=entry.board,
                        features=_feature_vector(entry, config),
                        excess_return=forward["excess_return"],
                        asset_return=forward["asset_return"],
                        benchmark_return=forward["benchmark_return"],
                        max_drawdown=forward["max_drawdown"],
                        downside_hit=forward["downside_hit"],
                    )
                )
        return samples

    def _train_models(self, samples: list[TrainingSample], as_of_date: str) -> ModelFit:
        if not samples:
            zeros = [0.0] * (len(FEATURE_NAMES) + 1)
            return ModelFit(zeros, zeros, None, None, 0)
        available_samples = sorted(samples, key=lambda item: item.signal_date)
        train_end_date = date.fromisoformat(as_of_date)
        trailing_samples = [
            sample
            for sample in available_samples
            if (train_end_date - date.fromisoformat(sample.signal_date)).days <= MODEL_WINDOW_DAYS
        ]
        if len(trailing_samples) < 40:
            trailing_samples = available_samples

        return ModelFit(
            alpha_weights=_fit_ridge(trailing_samples),
            risk_weights=_fit_logistic(trailing_samples),
            train_start=trailing_samples[0].signal_date,
            train_end=trailing_samples[-1].signal_date,
            sample_count=len(trailing_samples),
        )

    def _alpha_percentiles(self, predictions: dict[str, float]) -> dict[str, float]:
        ordered = sorted(predictions.items(), key=lambda item: item[1])
        if not ordered:
            return {}
        if len(ordered) == 1:
            return {ordered[0][0]: 100.0}
        result: dict[str, float] = {}
        for index, (code, _value) in enumerate(ordered):
            result[code] = round(index / (len(ordered) - 1) * 100, 2)
        return result

    def _industry_relative_alpha(self, snapshot: dict[str, SnapshotEntry], predictions: dict[str, float]) -> dict[str, float]:
        global_scores = self._alpha_percentiles(predictions)
        by_industry: dict[str, dict[str, float]] = {}
        for code, entry in snapshot.items():
            if code not in predictions:
                continue
            by_industry.setdefault(entry.industry, {})[code] = predictions[code]
        result: dict[str, float] = {}
        for industry, values in by_industry.items():
            industry_scores = self._alpha_percentiles(values)
            for code in values:
                if len(values) < 8:
                    result[code] = global_scores.get(code, 50.0)
                else:
                    result[code] = round(industry_scores.get(code, 50.0) * 0.7 + global_scores.get(code, 50.0) * 0.3, 2)
        return result

    def _quality_score(self, entry: SnapshotEntry, config) -> float:
        weights = _subset_weights(config, QUALITY_FACTORS)
        return _weighted_factor_score(entry, weights)

    def _risk_score(self, entry: SnapshotEntry, config, downside_probability: float, soft_penalties: list[dict]) -> float:
        base_risk_score = _section_scores(entry, config)["risk"]
        event_penalty = 25.0 if any(item["label"] == "处于财报事件窗口" for item in soft_penalties) else 100.0
        values = [
            100 - downside_probability * 100,
            base_risk_score,
            event_penalty,
        ]
        return round(_mean(values), 2)

    def _view_agreement(self, rows: list[dict], config) -> dict[str, int]:
        scores_by_view: dict[str, dict[str, float]] = {view: {} for view in VIEW_FACTORS}
        industries: dict[str, dict[str, list[tuple[str, float]]]] = {}
        for row in rows:
            entry = row["entry"]
            for view, factors in VIEW_FACTORS.items():
                score = _weighted_factor_score(entry, _subset_weights(config, factors))
                scores_by_view[view][entry.code] = score
                industries.setdefault(view, {}).setdefault(entry.industry, []).append((entry.code, score))

        top_flags: dict[str, int] = {}
        for view, industry_groups in industries.items():
            for industry, values in industry_groups.items():
                ordered = [code for code, _score in sorted(values, key=lambda item: item[1], reverse=True)]
                cutoff = max(1, math.ceil(len(ordered) * 0.2))
                for code in ordered[:cutoff]:
                    top_flags[code] = top_flags.get(code, 0) + 1
        return top_flags

    def _risk_flags(self, entry: SnapshotEntry, confidence_score: float, soft_penalties: list[dict], position_plan) -> list[str]:
        flags: list[str] = []
        for penalty in soft_penalties:
            if penalty["label"] in {"估值超阈值", "估值极端"}:
                flags.append("估值偏高")
            if penalty["label"] == "20日波动率过高":
                flags.append("波动偏高")
            if penalty["label"] == "流动性偏弱":
                flags.append("流动性不足")
            if penalty["label"] == "处于财报事件窗口":
                flags.append("临近财报窗口")
        if position_plan and entry.current_price >= position_plan.suggested_entry * 1.03:
            flags.append("不建议追高")
        if confidence_score < 60:
            flags.append("模型置信不足")
        return list(dict.fromkeys(flags))[:3]

    def _similar_history(self, entry: SnapshotEntry, samples: list[TrainingSample], config) -> list[dict]:
        if not samples:
            return []
        current_features = _feature_vector(entry, config)
        peer_samples = [sample for sample in samples if sample.industry == entry.industry] or samples
        ranked = sorted(
            peer_samples,
            key=lambda sample: sum((left - right) ** 2 for left, right in zip(current_features, sample.features, strict=False)),
        )
        result: list[dict] = []
        for sample in ranked[:3]:
            result.append(
                {
                    "signal_date": sample.signal_date,
                    "code": sample.code,
                    "name": sample.name,
                    "industry": sample.industry,
                    "forward_excess_return": round(sample.excess_return, 4),
                    "summary": f"未来10日超额 {sample.excess_return * 100:.1f}% / 最大回撤 {sample.max_drawdown * 100:.1f}%",
                }
            )
        return result

    def _event_calendar(self, entry: SnapshotEntry, as_of_date: str, trading_dates: list[str]) -> list[dict]:
        items: list[dict] = [
            {
                "event_date": as_of_date,
                "event_type": "signal",
                "title": "当日收盘信号",
                "severity": "info",
            }
        ]
        if entry.financial_publish_date:
            items.append(
                {
                    "event_date": entry.financial_publish_date,
                    "event_type": "earnings",
                    "title": "最近可见财报披露日",
                    "severity": "warning" if entry.financial_staleness_days <= 5 else "info",
                }
            )
        if as_of_date in trading_dates:
            review_index = min(trading_dates.index(as_of_date) + FORWARD_WINDOW_DAYS, len(trading_dates) - 1)
            items.append(
                {
                    "event_date": trading_dates[review_index],
                    "event_type": "review",
                    "title": "10日持有窗口复核",
                    "severity": "info",
                }
            )
        return items

    def get_market_regime(self, as_of_date: str) -> str:
        price_map = self.repository.get_price_map(as_of_date=as_of_date)
        benchmark_bars = price_map.get(self.benchmark_code) or price_map.get("000300.SH") or []
        closes = [bar["close"] for bar in benchmark_bars if bar["trade_date"] <= as_of_date]
        return _regime_from_closes(closes)

    def _sync_gate_reasons(self, as_of_date: str) -> list[str]:
        reasons: list[str] = []
        latest_run = self.repository.get_latest_run("sync")
        equity_count = self.repository.count_equities()
        latest_prices = self.repository.get_latest_price_lookup(as_of_date=as_of_date)
        visible_financials = self.repository.get_visible_financials(as_of_date=as_of_date)
        price_coverage = len([row for row in latest_prices.values() if row]) / max(equity_count, 1)
        financial_coverage = len(visible_financials) / max(equity_count, 1)
        if price_coverage < 0.98:
            reasons.append("价格覆盖率低于 98%")
        if financial_coverage < 0.90:
            reasons.append("财务覆盖率低于 90%")
        if latest_run:
            payload = json.loads(latest_run.get("payload_json") or "{}")
            if float(payload.get("failure_ratio", 0.0) or 0.0) > 0.02:
                reasons.append("同步失败率高于 2%")
        audit_summary = self.repository.get_audit_summary(latest_run["run_id"] if latest_run else None)
        if audit_summary["critical_count"] > max(1, math.floor(equity_count * 0.005)):
            reasons.append("关键数据异常率过高")
        return reasons

    def _validation_status_payload(
        self,
        windows: list[ValidationWindow],
    ) -> dict[str, object]:
        recent_windows = windows[-8:]
        recent_effective_windows = len(recent_windows)
        recent_excess = _mean([window.excess_return for window in recent_windows])
        recent_hit_rate = _mean([window.hit_rate for window in recent_windows])
        recent_ic = _mean([window.ic for window in recent_windows])
        if recent_effective_windows >= 8 and recent_excess > 0 and recent_hit_rate >= 0.55 and recent_ic >= 0.03:
            health = "healthy"
        elif recent_effective_windows >= 4:
            health = "degraded"
        else:
            health = "insufficient"

        block_reasons: list[str] = []
        if recent_effective_windows < 8:
            block_reasons.append("最近有效验证窗口不足 8 个")
        if recent_excess <= 0:
            block_reasons.append("最近 8 个窗口平均超额收益不为正")
        if recent_hit_rate < 0.55:
            block_reasons.append("最近 8 个窗口平均命中率低于 55%")
        if recent_ic < 0.03:
            block_reasons.append("最近 8 个窗口平均 IC 低于 0.03")
        return {
            "health": health,
            "block_reasons": block_reasons,
            "recent_six_month_excess_return": round(_mean([window.excess_return for window in windows[-26:]]), 4),
        }

    def _build_validation_artifacts(self, config, as_of_date: str) -> dict:
        config_key = self._config_key(config)
        cache_key = (config_key, as_of_date)
        cached = self._validation_cache.get(cache_key)
        if cached is not None:
            return cached

        repo_cached = self.repository.get_validation_cache(as_of_date)
        if repo_cached and repo_cached.get("config_key") == config_key:
            status_payload = self._validation_status_payload(
                [ValidationWindow.model_validate(item) for item in repo_cached.get("walk_forward_windows", [])],
            )
            summary = ValidationSummary.model_validate(
                {
                    **repo_cached,
                    "current_model_health": status_payload["health"],
                    "recent_six_month_excess_return": status_payload["recent_six_month_excess_return"],
                }
            )
            payload = {
                "summary": summary,
                "calibration": repo_cached.get("calibration", {}),
            }
            self._validation_cache[cache_key] = payload
            return payload

        trading_dates = self.repository.get_trading_dates(security_type="equity")
        if not trading_dates:
            summary = ValidationSummary(as_of_date=as_of_date)
            payload = {"summary": summary, "calibration": {}}
            self._validation_cache[cache_key] = payload
            return payload

        bar_lookup = self._get_bar_lookup()
        benchmark_code = self._resolve_benchmark_code(bar_lookup)
        date_index = {trade_date: index for index, trade_date in enumerate(trading_dates)}
        training_samples = self._training_samples(config, as_of_date=as_of_date)
        signal_dates = [value for value in self._get_week_end_dates(trading_dates) if value < as_of_date]

        windows: list[ValidationWindow] = []
        regime_records: dict[str, list[dict]] = {"bullish": [], "neutral": [], "cautious": []}
        industry_records: dict[str, list[dict]] = {}
        calibration: dict[str, dict[str, list[dict]]] = {"bullish": {}, "neutral": {}, "cautious": {}}

        for signal_date in signal_dates:
            prior_samples = [sample for sample in training_samples if sample.signal_date < signal_date]
            if len(prior_samples) < 40:
                continue
            model_fit = self._train_models(prior_samples, signal_date)
            snapshot = self._get_snapshot(signal_date)
            regime = self.get_market_regime(signal_date)
            viewable_rows: list[dict] = []
            predictions: dict[str, float] = {}
            downside_probabilities: dict[str, float] = {}
            actual_returns: list[float] = []
            predicted_returns: list[float] = []

            for entry in snapshot.values():
                hard_blocks = evaluate_hard_blocks(entry, config)
                if not entry.factor_scores or hard_blocks:
                    continue
                forward = self._forward_metrics(
                    entry.code,
                    signal_date,
                    trading_dates,
                    date_index,
                    bar_lookup,
                    benchmark_code,
                )
                if not forward:
                    continue
                features = _feature_vector(entry, config)
                prediction = _dot(model_fit.alpha_weights, features)
                downside = _sigmoid(_dot(model_fit.risk_weights, features))
                predictions[entry.code] = prediction
                downside_probabilities[entry.code] = downside
                predicted_returns.append(prediction)
                actual_returns.append(forward["excess_return"])
                viewable_rows.append({"entry": entry, "forward": forward})

            if not viewable_rows:
                continue

            alpha_scores = self._industry_relative_alpha(snapshot, predictions)
            agreement_map = self._view_agreement(viewable_rows, config)
            alpha_weight, quality_weight, risk_weight = REGIME_WEIGHTS[regime]
            ranked_candidates: list[dict] = []
            for item in viewable_rows:
                entry = item["entry"]
                soft_penalties = evaluate_soft_penalties(entry, config)
                penalty_points = min(sum(penalty["points"] for penalty in soft_penalties), 25.0)
                quality_score = self._quality_score(entry, config)
                risk_score = self._risk_score(entry, config, downside_probabilities.get(entry.code, 0.5), soft_penalties)
                pre_score = round(
                    alpha_scores.get(entry.code, 50.0) * alpha_weight
                    + quality_score * quality_weight
                    + risk_score * risk_weight
                    - penalty_points,
                    2,
                )
                ranked_candidates.append(
                    {
                        "entry": entry,
                        "forward": item["forward"],
                        "pre_score": pre_score,
                        "agreement_count": agreement_map.get(entry.code, 0),
                    }
                )
                calibration.setdefault(regime, {}).setdefault(_bucket_label(pre_score), []).append(
                    {
                        "hit_rate": 1.0 if item["forward"]["excess_return"] > 0 else 0.0,
                        "excess_return": item["forward"]["excess_return"],
                    }
                )

            shortlisted: list[dict] = []
            industry_counter: dict[str, int] = {}
            for row in sorted(ranked_candidates, key=lambda item: item["pre_score"], reverse=True):
                if row["agreement_count"] < 2:
                    continue
                if industry_counter.get(row["entry"].industry, 0) >= config.risk.industry_cap:
                    continue
                industry_counter[row["entry"].industry] = industry_counter.get(row["entry"].industry, 0) + 1
                shortlisted.append(row)
                if len(shortlisted) >= config.rebalance.top_n:
                    break

            if not shortlisted:
                continue

            period_excess = _mean([item["forward"]["excess_return"] for item in shortlisted])
            hit_rate = _mean([1.0 if item["forward"]["excess_return"] > 0 else 0.0 for item in shortlisted])
            max_drawdown = min(item["forward"]["max_drawdown"] for item in shortlisted)
            ic = _safe_corr(predicted_returns, actual_returns)
            window = ValidationWindow(
                signal_date=signal_date,
                train_start=model_fit.train_start,
                train_end=model_fit.train_end,
                sample_count=len(shortlisted),
                regime=regime,
                excess_return=round(period_excess, 4),
                hit_rate=round(hit_rate, 4),
                max_drawdown=round(max_drawdown, 4),
                ic=round(ic, 4),
            )
            windows.append(window)
            record = {
                "excess_return": period_excess,
                "portfolio_return": _mean([item["forward"]["asset_return"] for item in shortlisted]),
                "benchmark_return": _mean([item["forward"]["benchmark_return"] for item in shortlisted]),
                "max_drawdown": max_drawdown,
                "hit_rate": hit_rate,
                "ic": ic,
            }
            regime_records.setdefault(regime, []).append(record)
            for item in shortlisted:
                industry_records.setdefault(item["entry"].industry, []).append(record)
        status_payload = self._validation_status_payload(windows)

        summary = ValidationSummary(
            as_of_date=as_of_date,
            walk_forward_windows=windows,
            regime_breakdown=[self._aggregate_bucket(label, items) for label, items in regime_records.items()],
            industry_breakdown=[
                self._aggregate_bucket(label, items)
                for label, items in sorted(industry_records.items(), key=lambda item: len(item[1]), reverse=True)[:8]
            ],
            current_model_health=status_payload["health"],
            recent_six_month_excess_return=status_payload["recent_six_month_excess_return"],
        )
        serialized = summary.model_dump(mode="json")
        serialized["calibration"] = {
            regime: {
                bucket: {
                    "sample_count": len(values),
                    "hit_rate": round(_mean([item["hit_rate"] for item in values]), 4),
                    "expected_excess_return": round(_mean([item["excess_return"] for item in values]), 4),
                }
                for bucket, values in buckets.items()
            }
            for regime, buckets in calibration.items()
        }
        serialized["config_key"] = config_key
        if config_key == self._default_config_key:
            self.repository.upsert_validation_cache(as_of_date, serialized)
            self.repository.upsert_model_health_snapshot(
                as_of_date,
                summary.current_model_health,
                {"block_reasons": status_payload["block_reasons"], "config_key": config_key},
            )
        payload = {
            "summary": summary,
            "calibration": serialized["calibration"],
        }
        self._validation_cache[cache_key] = payload
        return payload

    def _aggregate_bucket(self, label: str, items: list[dict]) -> ValidationBucket:
        if not items:
            return ValidationBucket(label=label, sample_count=0, excess_return=0.0, win_rate=0.0, max_drawdown=0.0, hit_rate=0.0, ic=0.0)
        cumulative = 1.0
        benchmark = 1.0
        max_drawdown = 0.0
        peak = 1.0
        for item in items:
            cumulative *= 1 + item.get("portfolio_return", item.get("excess_return", 0.0))
            benchmark *= 1 + item.get("benchmark_return", 0.0)
            peak = max(peak, cumulative)
            max_drawdown = min(max_drawdown, cumulative / peak - 1)
        return ValidationBucket(
            label=label,
            sample_count=len(items),
            excess_return=round(_mean([item["excess_return"] for item in items]), 4),
            win_rate=round(_mean([1.0 if item.get("portfolio_return", item["excess_return"]) > 0 else 0.0 for item in items]), 4),
            max_drawdown=round(max_drawdown, 4),
            hit_rate=round(_mean([item["hit_rate"] for item in items]), 4),
            ic=round(_mean([item["ic"] for item in items]), 4),
        )

    def _calibrate_confidence(self, regime: str, pre_score: float, calibration: dict) -> tuple[float, str, int, float]:
        bucket = _bucket_label(pre_score)
        regime_buckets = calibration.get(regime, {})
        bucket_payload = regime_buckets.get(bucket, {})
        sample_count = int(bucket_payload.get("sample_count", 0) or 0)
        expected_excess = float(bucket_payload.get("expected_excess_return", 0.0) or 0.0)
        if sample_count >= 40:
            hit_rate = float(bucket_payload.get("hit_rate", 0.0) or 0.0)
            return round(hit_rate * 100, 2), bucket, sample_count, expected_excess
        aggregated = [value for value in regime_buckets.values()]
        total_samples = sum(int(item.get("sample_count", 0) or 0) for item in aggregated)
        if total_samples >= 40:
            hit_rate = _mean([float(item.get("hit_rate", 0.0) or 0.0) for item in aggregated])
            expected = _mean([float(item.get("expected_excess_return", 0.0) or 0.0) for item in aggregated])
            return round(hit_rate * 100, 2), "regime-fallback", total_samples, expected
        return 0.0, "insufficient", total_samples, 0.0

    def _analysis_payload(self, config, as_of_date: str) -> dict:
        config_key = self._config_key(config)
        cache_key = (config_key, as_of_date)
        cached = self._analysis_cache.get(cache_key)
        if cached is not None:
            return cached

        snapshot = self._get_snapshot(as_of_date)
        training_samples = self._training_samples(config, as_of_date=as_of_date)
        model_fit = self._train_models(training_samples, as_of_date)
        validation_artifacts = self._build_validation_artifacts(config, as_of_date)
        validation_summary: ValidationSummary = validation_artifacts["summary"]
        calibration = validation_artifacts["calibration"]
        market_regime = self.get_market_regime(as_of_date)
        alpha_weight, quality_weight, risk_weight = REGIME_WEIGHTS[market_regime]

        predictions: dict[str, float] = {}
        downside_probabilities: dict[str, float] = {}
        analyses_by_code: dict[str, dict] = {}
        trading_dates = self.repository.get_trading_dates(security_type="equity")

        candidates_for_views: list[dict] = []
        for entry in snapshot.values():
            if not entry.factor_scores:
                continue
            features = _feature_vector(entry, config)
            predictions[entry.code] = _dot(model_fit.alpha_weights, features)
            downside_probabilities[entry.code] = _sigmoid(_dot(model_fit.risk_weights, features))
            candidates_for_views.append({"entry": entry})

        alpha_scores = self._industry_relative_alpha(snapshot, predictions)
        agreement_map = self._view_agreement(candidates_for_views, config)
        sync_gate_reasons = self._sync_gate_reasons(as_of_date)

        raw_rows: list[dict] = []
        industry_peers: dict[str, list[tuple[str, float]]] = {}
        for entry in snapshot.values():
            if not entry.factor_scores:
                continue
            hard_blocks = evaluate_hard_blocks(entry, config)
            soft_penalties = evaluate_soft_penalties(entry, config)
            penalty_points = min(sum(item["points"] for item in soft_penalties), 25.0)
            alpha_prediction = predictions.get(entry.code, 0.0)
            downside_probability = downside_probabilities.get(entry.code, 0.5)
            section_scores = _section_scores(entry, config)
            factor_details = _factor_contributions(entry, config)
            quality_score = self._quality_score(entry, config)
            risk_score = self._risk_score(entry, config, downside_probability, soft_penalties)
            alpha_score = alpha_scores.get(entry.code, 50.0)
            pre_total_score = round(
                alpha_score * alpha_weight + quality_score * quality_weight + risk_score * risk_weight - penalty_points,
                2,
            )
            confidence_score, calibration_bucket, bucket_sample_count, expected_excess_return = self._calibrate_confidence(
                market_regime,
                pre_total_score,
                calibration,
            )
            agreement_count = agreement_map.get(entry.code, 0)
            position_plan = build_position_plan(entry, config)
            row_block_reasons = list(hard_blocks)
            if agreement_count < 2:
                row_block_reasons.append("三视角一致性不足")
            if bucket_sample_count < 40:
                row_block_reasons.append("校准样本不足")
            risk_flags = self._risk_flags(entry, confidence_score, soft_penalties, position_plan)
            plus_factors = [item["label"] for item in factor_details if item["contribution"] > 0][:3]
            minus_factors = [penalty["label"] for penalty in soft_penalties][:3] or [
                item["label"] for item in reversed(factor_details) if item["contribution"] < 0
            ][:3]
            summary_parts = [
                f"Alpha {alpha_score:.0f}",
                f"质量 {quality_score:.0f}",
                f"风险 {risk_score:.0f}",
                f"校准胜率 {confidence_score:.0f}",
            ]
            if plus_factors:
                summary_parts.append("强项：" + "、".join(plus_factors))
            if risk_flags:
                summary_parts.append("留意：" + "、".join(risk_flags))
            row = {
                "code": entry.code,
                "name": entry.name,
                "industry": entry.industry,
                "board": entry.board,
                "current_price": round(entry.current_price, 2),
                "total_score": round(pre_total_score, 2),
                "signal_date": entry.signal_date,
                "explanation_summary": "；".join(summary_parts),
                "factor_scores": entry.factor_scores,
                "section_scores": section_scores,
                "avg_turnover_20d": round(entry.avg_turnover_20d, 2),
                "pe_percentile": round(entry.pe_percentile, 4),
                "pb_percentile": round(entry.pb_percentile, 4),
                "confidence_score": confidence_score,
                "tier": "观察",
                "risk_flags": risk_flags,
                "peer_rank_in_industry": 1,
                "expected_holding_window": f"{max(5, config.rebalance.holding_period_days)}-{max(10, config.rebalance.holding_period_days)}个交易日",
                "position_plan": position_plan.model_dump(mode="json"),
                "plus_factors": plus_factors or ["暂无显著优势"],
                "minus_factors": minus_factors or ["暂无显著拖累"],
                "factor_details": factor_details,
                "contribution_breakdown": factor_details,
                "reason_not_to_buy_now": list(dict.fromkeys(risk_flags + row_block_reasons))[:5],
                "model_snapshot": {
                    "alpha_score": round(alpha_score, 2),
                    "quality_score": round(quality_score, 2),
                    "risk_score": round(risk_score, 2),
                    "alpha_prediction": round(alpha_prediction, 4),
                    "downside_probability": round(downside_probability, 4),
                    "confidence_score": confidence_score,
                    "training_window_start": model_fit.train_start,
                    "training_window_end": model_fit.train_end,
                    "training_sample_count": model_fit.sample_count,
                    "validation_health": validation_summary.current_model_health,
                    "precalibrated_total_score": pre_total_score,
                    "calibration_bucket": calibration_bucket,
                    "bucket_sample_count": bucket_sample_count,
                    "expected_excess_return": round(expected_excess_return, 4),
                    "agreement_count": agreement_count,
                },
                "peer_percentiles": {},
                "event_calendar": self._event_calendar(entry, as_of_date, trading_dates),
                "similar_history_samples": self._similar_history(entry, training_samples, config),
                "ineligible_reasons": hard_blocks,
                "actionable": False,
                "block_reasons": row_block_reasons,
                "soft_penalties": soft_penalties,
                "agreement_count": agreement_count,
            }
            analyses_by_code[entry.code] = row
            raw_rows.append(row)
            industry_peers.setdefault(entry.industry, []).append((entry.code, pre_total_score))

        for industry, peers in industry_peers.items():
            ordered = [code for code, _score in sorted(peers, key=lambda item: item[1], reverse=True)]
            for code in ordered:
                row = analyses_by_code[code]
                row["peer_rank_in_industry"] = ordered.index(code) + 1
                peer_count = max(len(ordered) - 1, 1)
                row["peer_percentiles"] = {
                    "industry_total_score": round((1 - (ordered.index(code) / peer_count)) * 100, 2),
                    "industry_alpha_score": row["model_snapshot"]["alpha_score"],
                    "industry_quality_score": row["model_snapshot"]["quality_score"],
                    "industry_risk_score": row["model_snapshot"]["risk_score"],
                }

        ranked_rows = sorted(raw_rows, key=lambda item: (item["total_score"], item["confidence_score"]), reverse=True)
        industry_counter: dict[str, int] = {}
        shortlist: list[dict] = []
        for row in ranked_rows:
            if row["ineligible_reasons"] or row["agreement_count"] < 2:
                continue
            if industry_counter.get(row["industry"], 0) >= config.risk.industry_cap:
                continue
            if row["confidence_score"] < _confidence_floor(config, market_regime):
                row["block_reasons"] = list(dict.fromkeys(row["block_reasons"] + ["低于当前市场状态置信阈值"]))
                continue
            industry_counter[row["industry"]] = industry_counter.get(row["industry"], 0) + 1
            shortlist.append(row)
            if len(shortlist) >= config.rebalance.top_n:
                break

        global_block_reasons = list(dict.fromkeys(sync_gate_reasons))
        actionable = not global_block_reasons
        if actionable:
            a_codes = {
                row["code"]
                for row in shortlist[:3]
                if row["confidence_score"] >= _confidence_floor(config, market_regime) + 5
            }
            shortlist_codes = {row["code"] for row in shortlist}
        else:
            a_codes = set()
            shortlist_codes = set()

        final_rows: list[dict] = []
        for row in ranked_rows:
            base_block_reasons = list(dict.fromkeys(global_block_reasons + row["block_reasons"]))
            if actionable and row["code"] in shortlist_codes:
                row["actionable"] = True
                row["block_reasons"] = [reason for reason in base_block_reasons if reason not in global_block_reasons]
                row["tier"] = "A" if row["code"] in a_codes else "B"
            else:
                row["actionable"] = False
                row["tier"] = "观察"
                row["block_reasons"] = base_block_reasons
                row["explanation_summary"] = f"{row['explanation_summary']}；当前仅列入观察名单。"
            analyses_by_code[row["code"]] = row
            final_rows.append(row)

        if config_key == self._default_config_key:
            self.repository.replace_eligibility_snapshot(as_of_date, final_rows)
        payload = {
            "snapshot": snapshot,
            "ranked_rows": final_rows,
            "analyses_by_code": analyses_by_code,
            "validation_summary": validation_summary,
            "actionable": actionable,
            "block_reasons": global_block_reasons,
        }
        self._analysis_cache[cache_key] = payload
        return payload

    def get_ranked_rows(self, config, as_of_date: str | None = None, limit: int = 30) -> tuple[str, list[dict], dict]:
        effective_date = self._get_latest_date(as_of_date)
        payload = self._analysis_payload(config, effective_date)
        return effective_date, payload["ranked_rows"][:limit], payload

    def get_analysis_for_code(self, code: str, config, as_of_date: str | None = None) -> tuple[str, SnapshotEntry | None, dict | None, dict]:
        effective_date = self._get_latest_date(as_of_date)
        payload = self._analysis_payload(config, effective_date)
        snapshot = payload["snapshot"]
        return effective_date, snapshot.get(code), payload["analyses_by_code"].get(code), payload

    def shortlist(self, config, as_of_date: str | None = None) -> tuple[str, list[dict], dict]:
        effective_date = self._get_latest_date(as_of_date)
        payload = self._analysis_payload(config, effective_date)
        shortlist = [row for row in payload["ranked_rows"] if row["actionable"]][: config.rebalance.top_n]
        return effective_date, shortlist, payload

    def backtest_shortlist(self, config, as_of_date: str) -> tuple[list[dict], str]:
        snapshot = self._get_snapshot(as_of_date)
        training_samples = self._training_samples(config, as_of_date=as_of_date)
        model_fit = self._train_models(training_samples, as_of_date)
        market_regime = self.get_market_regime(as_of_date)
        alpha_weight, quality_weight, risk_weight = REGIME_WEIGHTS[market_regime]

        predictions: dict[str, float] = {}
        downside_probabilities: dict[str, float] = {}
        view_rows: list[dict] = []
        for entry in snapshot.values():
            if not entry.factor_scores:
                continue
            features = _feature_vector(entry, config)
            predictions[entry.code] = _dot(model_fit.alpha_weights, features)
            downside_probabilities[entry.code] = _sigmoid(_dot(model_fit.risk_weights, features))
            view_rows.append({"entry": entry})

        alpha_scores = self._industry_relative_alpha(snapshot, predictions)
        agreement_map = self._view_agreement(view_rows, config)

        ranked_rows: list[dict] = []
        for entry in snapshot.values():
            if not entry.factor_scores or evaluate_hard_blocks(entry, config):
                continue
            soft_penalties = evaluate_soft_penalties(entry, config)
            penalty_points = min(sum(item["points"] for item in soft_penalties), 25.0)
            quality_score = self._quality_score(entry, config)
            risk_score = self._risk_score(entry, config, downside_probabilities.get(entry.code, 0.5), soft_penalties)
            pre_total_score = round(
                alpha_scores.get(entry.code, 50.0) * alpha_weight
                + quality_score * quality_weight
                + risk_score * risk_weight
                - penalty_points,
                2,
            )
            confidence_score = pre_total_score
            ranked_rows.append(
                {
                    "code": entry.code,
                    "industry": entry.industry,
                    "total_score": pre_total_score,
                    "confidence_score": confidence_score,
                    "agreement_count": agreement_map.get(entry.code, 0),
                }
            )

        shortlist: list[dict] = []
        industry_counter: dict[str, int] = {}
        for row in sorted(ranked_rows, key=lambda item: (item["total_score"], item["confidence_score"]), reverse=True):
            if row["agreement_count"] < 2:
                continue
            if row["confidence_score"] < _confidence_floor(config, market_regime):
                continue
            if industry_counter.get(row["industry"], 0) >= config.risk.industry_cap:
                continue
            industry_counter[row["industry"]] = industry_counter.get(row["industry"], 0) + 1
            shortlist.append(row)
            if len(shortlist) >= config.rebalance.top_n:
                break
        return shortlist, market_regime
