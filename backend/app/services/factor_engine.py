from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from statistics import fmean, pstdev

from ..models import PositionPlan, StrategyConfig
from .strategy_service import FACTOR_METADATA, normalize_weights


POSITIVE_FACTORS = {
    "ma_trend",
    "relative_strength_60d",
    "volume_surge_20d",
    "roe",
    "revenue_yoy",
    "profit_yoy",
    "cashflow_quality",
    "liquidity",
}

SUPPORTED_BOARDS = {"main_board", "chi_next"}
INDUSTRY_PRIORITY_FACTORS = {
    "roe",
    "revenue_yoy",
    "profit_yoy",
    "cashflow_quality",
    "pe_score",
    "pb_score",
    "debt_ratio",
}


@dataclass(slots=True)
class SnapshotEntry:
    code: str
    name: str
    industry: str
    board: str
    security_type: str
    signal_date: str
    listing_days: int
    is_st: bool
    is_suspended: bool
    current_price: float
    previous_close: float
    ma20: float
    ma60: float
    avg_turnover_20d: float
    pe_percentile: float
    pb_percentile: float
    financial_report_date: str | None
    financial_publish_date: str | None
    financial_staleness_days: int
    data_completeness: float
    raw_factors: dict[str, float]
    factor_scores: dict[str, float]
    factor_groups: dict[str, str]
    section_scores: dict[str, float]
    basic_reasons: list[str]


def _mean(values: list[float]) -> float:
    return fmean(values) if values else 0.0


def _returns(close_prices: list[float]) -> list[float]:
    result: list[float] = []
    for index in range(1, len(close_prices)):
        prev = close_prices[index - 1]
        current = close_prices[index]
        result.append(current / prev - 1 if prev else 0.0)
    return result


def _percentile_scores(values_by_code: dict[str, float], invert: bool = False) -> tuple[dict[str, float], dict[str, float]]:
    if not values_by_code:
        return {}, {}
    ordered = sorted(values_by_code.items(), key=lambda item: item[1])
    if len(ordered) == 1:
        code, _value = ordered[0]
        return {code: 100.0}, {code: 1.0}
    scores: dict[str, float] = {}
    percentiles: dict[str, float] = {}
    for index, (code, _value) in enumerate(ordered):
        percentile = index / (len(ordered) - 1)
        percentiles[code] = percentile
        score = (1 - percentile) if invert else percentile
        scores[code] = round(score * 100, 2)
    return scores, percentiles


def _industry_aware_percentiles(
    values_by_code: dict[str, float],
    industry_by_code: dict[str, str],
    *,
    invert: bool = False,
    min_group_size: int = 4,
) -> tuple[dict[str, float], dict[str, float]]:
    global_scores, global_percentiles = _percentile_scores(values_by_code, invert=invert)
    if not values_by_code:
        return global_scores, global_percentiles

    grouped: dict[str, dict[str, float]] = {}
    for code, value in values_by_code.items():
        grouped.setdefault(industry_by_code.get(code, "未知"), {})[code] = value

    scores = dict(global_scores)
    percentiles = dict(global_percentiles)
    for _industry, group_values in grouped.items():
        if len(group_values) < min_group_size:
            continue
        group_scores, group_percentiles = _percentile_scores(group_values, invert=invert)
        for code in group_values:
            scores[code] = round(group_scores.get(code, global_scores.get(code, 0.0)) * 0.7 + global_scores.get(code, 0.0) * 0.3, 2)
            percentiles[code] = round(
                group_percentiles.get(code, global_percentiles.get(code, 0.0)) * 0.7 + global_percentiles.get(code, 0.0) * 0.3,
                4,
            )
    return scores, percentiles


def _days_between(first: str, second: str) -> int:
    return abs((date.fromisoformat(first) - date.fromisoformat(second)).days)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def build_position_plan(entry: SnapshotEntry, config: StrategyConfig) -> PositionPlan:
    current_price = entry.current_price or 0.0
    buffer_pct = config.notifications.watchlist_entry_buffer_pct
    volatility_20d = float(entry.raw_factors.get("volatility_20d", 0.0) or 0.0)
    adaptive_stop_pct = _clamp(volatility_20d * 3.0, 0.05, 0.12)
    base_weight = 1 / max(config.rebalance.top_n, 1)
    suggested_weight = min(config.risk.max_position_weight, round(base_weight * 0.08 / adaptive_stop_pct, 4))
    return PositionPlan(
        code=entry.code,
        name=entry.name,
        suggested_entry=round(current_price * (1 - buffer_pct / 2), 2),
        stop_loss=round(current_price * (1 - adaptive_stop_pct), 2),
        take_profit=round(current_price * (1 + adaptive_stop_pct * 1.8), 2),
        suggested_weight=round(suggested_weight, 4),
    )


def compute_snapshot(
    meta_map: dict[str, dict],
    price_map: dict[str, list[dict]],
    financial_map: dict[str, dict],
    as_of_date: str,
) -> dict[str, SnapshotEntry]:
    raw_records: dict[str, dict] = {}
    percentile_inputs: dict[str, dict[str, float]] = {factor: {} for factor in FACTOR_METADATA}
    industry_by_code: dict[str, str] = {}

    for code, meta in meta_map.items():
        if meta["security_type"] != "equity":
            continue
        bars = [bar for bar in price_map.get(code, []) if bar["trade_date"] <= as_of_date]
        listing_days = len(bars)
        basic_reasons: list[str] = []
        board = meta.get("board", "main_board")
        if board not in SUPPORTED_BOARDS:
            basic_reasons.append("不在主板/创业板范围内")
        if meta["is_st"]:
            basic_reasons.append("ST 风险")
        if listing_days < 120:
            basic_reasons.append("上市未满 120 个交易日")
        if not bars:
            basic_reasons.append("缺少行情数据")
        latest_bar = bars[-1] if bars else None
        prev_bar = bars[-2] if len(bars) >= 2 else latest_bar
        if latest_bar and latest_bar["is_suspended"]:
            basic_reasons.append("最新交易日停牌")
        avg_turnover_20d = _mean([bar["turnover"] for bar in bars[-20:]]) if len(bars) >= 20 else 0.0
        financial = financial_map.get(code)
        if not financial:
            basic_reasons.append("缺少财务快照")

        raw_factors: dict[str, float] = {}
        ma20 = _mean([bar["close"] for bar in bars[-20:]]) if len(bars) >= 20 else 0.0
        ma60 = _mean([bar["close"] for bar in bars[-60:]]) if len(bars) >= 60 else 0.0
        current_price = latest_bar["close"] if latest_bar else 0.0
        previous_close = prev_bar["close"] if prev_bar else current_price
        publish_date = financial.get("publish_date") if financial else None
        report_date = financial.get("report_date") if financial else None
        financial_staleness_days = _days_between(as_of_date, publish_date) if publish_date else 999

        if bars and len(bars) >= 60 and financial:
            closes = [bar["close"] for bar in bars]
            volumes = [bar["volume"] for bar in bars]
            volume20 = _mean(volumes[-20:])
            volume5 = _mean(volumes[-5:])
            close_now = closes[-1]
            close_60 = closes[-60]
            trailing_returns = _returns(closes[-21:])
            cashflow_quality = (
                financial["operating_cashflow"] / abs(financial["net_profit"])
                if financial["net_profit"]
                else 0.0
            )
            raw_factors = {
                "ma_trend": (close_now / ma20 - 1) + (ma20 / ma60 - 1) if ma20 and ma60 else 0.0,
                "relative_strength_60d": close_now / close_60 - 1 if close_60 else 0.0,
                "volume_surge_20d": (volume5 / volume20 - 1) if volume20 else 0.0,
                "roe": financial["roe"],
                "revenue_yoy": financial["revenue_yoy"],
                "profit_yoy": financial["profit_yoy"],
                "cashflow_quality": cashflow_quality,
                "pe_score": financial["pe_ttm"],
                "pb_score": financial["pb"],
                "debt_ratio": financial["debt_ratio"],
                "volatility_20d": pstdev(trailing_returns) if len(trailing_returns) > 1 else 0.0,
                "liquidity": avg_turnover_20d,
            }
            for factor_name, value in raw_factors.items():
                percentile_inputs[factor_name][code] = value

        raw_records[code] = {
            "code": code,
            "name": meta["name"],
            "industry": meta["industry"],
            "board": board,
            "security_type": meta["security_type"],
            "signal_date": bars[-1]["trade_date"] if bars else as_of_date,
            "listing_days": listing_days,
            "is_st": bool(meta["is_st"]),
            "is_suspended": bool(latest_bar["is_suspended"]) if latest_bar else False,
            "current_price": current_price,
            "previous_close": previous_close,
            "ma20": ma20,
            "ma60": ma60,
            "avg_turnover_20d": avg_turnover_20d,
            "financial_report_date": report_date,
            "financial_publish_date": publish_date,
            "financial_staleness_days": financial_staleness_days,
            "basic_reasons": basic_reasons,
            "raw_factors": raw_factors,
        }
        industry_by_code[code] = meta["industry"]

    factor_scores_by_factor: dict[str, dict[str, float]] = {}
    factor_percentiles: dict[str, dict[str, float]] = {}
    for factor_name, values_by_code in percentile_inputs.items():
        invert = factor_name not in POSITIVE_FACTORS
        if factor_name in INDUSTRY_PRIORITY_FACTORS:
            scores, percentiles = _industry_aware_percentiles(values_by_code, industry_by_code, invert=invert)
        else:
            scores, percentiles = _percentile_scores(values_by_code, invert=invert)
        factor_scores_by_factor[factor_name] = scores
        factor_percentiles[factor_name] = percentiles

    snapshot: dict[str, SnapshotEntry] = {}
    for code, item in raw_records.items():
        raw_factors = item["raw_factors"]
        factor_scores = {factor: factor_scores_by_factor.get(factor, {}).get(code, 0.0) for factor in raw_factors}
        grouped_scores: dict[str, list[float]] = {"technical": [], "fundamental": [], "risk": []}
        for factor, score in factor_scores.items():
            grouped_scores[FACTOR_METADATA[factor]["group"]].append(score)
        snapshot[code] = SnapshotEntry(
            code=code,
            name=item["name"],
            industry=item["industry"],
            board=item["board"],
            security_type=item["security_type"],
            signal_date=item["signal_date"],
            listing_days=item["listing_days"],
            is_st=item["is_st"],
            is_suspended=item["is_suspended"],
            current_price=item["current_price"],
            previous_close=item["previous_close"],
            ma20=item["ma20"],
            ma60=item["ma60"],
            avg_turnover_20d=item["avg_turnover_20d"],
            pe_percentile=factor_percentiles.get("pe_score", {}).get(code, 1.0),
            pb_percentile=factor_percentiles.get("pb_score", {}).get(code, 1.0),
            financial_report_date=item["financial_report_date"],
            financial_publish_date=item["financial_publish_date"],
            financial_staleness_days=item["financial_staleness_days"],
            data_completeness=round(len(raw_factors) / max(len(FACTOR_METADATA), 1), 4),
            raw_factors=raw_factors,
            factor_scores=factor_scores,
            factor_groups={name: FACTOR_METADATA[name]["group"] for name in raw_factors},
            section_scores={group: round(_mean(scores), 2) for group, scores in grouped_scores.items()},
            basic_reasons=item["basic_reasons"],
        )
    return snapshot


def evaluate_hard_blocks(entry: SnapshotEntry, config: StrategyConfig) -> list[str]:
    rules = config.stock_pool
    reasons = list(entry.basic_reasons)
    if rules.exclude_st and entry.is_st and "ST 风险" not in reasons:
        reasons.append("ST 风险")
    if rules.exclude_suspended and entry.is_suspended and "最新交易日停牌" not in reasons:
        reasons.append("最新交易日停牌")
    if entry.listing_days < rules.min_listing_days and not any(reason.startswith("上市未满") for reason in reasons):
        reasons.append(f"上市未满 {rules.min_listing_days} 个交易日")
    if entry.current_price < rules.min_price:
        reasons.append(f"股价低于 {rules.min_price:.0f} 元")
    if entry.avg_turnover_20d < rules.min_avg_turnover_20d:
        reasons.append("近20日日均成交额不足")
    if entry.financial_staleness_days > 180:
        reasons.append("财务快照超过 180 天")
    if rules.exclude_negative_revenue_yoy and entry.raw_factors.get("revenue_yoy", 0.0) < 0:
        reasons.append("营收同比为负")
    if rules.exclude_negative_profit_yoy and entry.raw_factors.get("profit_yoy", 0.0) < 0:
        reasons.append("利润同比为负")
    if entry.board not in SUPPORTED_BOARDS:
        reasons.append("不在主板/创业板范围内")
    return list(dict.fromkeys(reasons))


def evaluate_soft_penalties(entry: SnapshotEntry, config: StrategyConfig) -> list[dict]:
    penalties: list[dict] = []
    pe_or_pb_over = entry.pe_percentile > config.stock_pool.max_pe_percentile or entry.pb_percentile > config.stock_pool.max_pb_percentile
    pe_or_pb_extreme = entry.pe_percentile >= 0.98 or entry.pb_percentile >= 0.98
    if pe_or_pb_extreme:
        penalties.append({"label": "估值极端", "points": 15.0})
    elif pe_or_pb_over:
        penalties.append({"label": "估值超阈值", "points": 8.0})
    if entry.raw_factors.get("revenue_yoy", 0.0) < 0:
        penalties.append({"label": "营收同比为负", "points": 8.0})
    if entry.raw_factors.get("profit_yoy", 0.0) < 0:
        penalties.append({"label": "利润同比为负", "points": 10.0})
    if entry.raw_factors.get("volatility_20d", 0.0) > config.risk.max_volatility_20d:
        penalties.append({"label": "20日波动率过高", "points": 8.0})
    if entry.avg_turnover_20d < max(config.stock_pool.min_avg_turnover_20d * 1.5, 300_000_000):
        penalties.append({"label": "流动性偏弱", "points": 6.0})
    if entry.financial_publish_date and _days_between(entry.signal_date, entry.financial_publish_date) <= config.risk.avoid_earnings_window_days:
        penalties.append({"label": "处于财报事件窗口", "points": 10.0})
    return penalties


def evaluate_filters(entry: SnapshotEntry, config: StrategyConfig) -> list[str]:
    soft_labels = [item["label"] for item in evaluate_soft_penalties(entry, config)]
    return list(dict.fromkeys(evaluate_hard_blocks(entry, config) + soft_labels))


def score_entry(entry: SnapshotEntry, config: StrategyConfig) -> dict:
    weights = normalize_weights(config.weights)
    weighted = [entry.factor_scores[factor] * weight for factor, weight in weights.items() if factor in entry.factor_scores]
    total_score = round(sum(weighted), 2)
    ranked_contributions = sorted(
        (
            {
                "factor": factor,
                "label": FACTOR_METADATA[factor]["label"],
                "group": FACTOR_METADATA[factor]["group"],
                "score": entry.factor_scores.get(factor, 0.0),
                "raw_value": entry.raw_factors.get(factor, 0.0),
                "contribution": (entry.factor_scores.get(factor, 0.0) - 50) * weights.get(factor, 0.0),
            }
            for factor in weights
            if factor in entry.factor_scores
        ),
        key=lambda item: item["contribution"],
        reverse=True,
    )
    plus_factors = [item["label"] for item in ranked_contributions[:3] if item["score"] >= 55]
    minus_factors = [item["label"] for item in ranked_contributions[-2:] if item["score"] <= 45]
    summary_parts: list[str] = []
    if plus_factors:
        summary_parts.append("优势：" + "、".join(plus_factors))
    if minus_factors:
        summary_parts.append("风险：" + "、".join(minus_factors))
    if not summary_parts:
        summary_parts.append("信号中性，等待更多趋势确认。")
    return {
        "total_score": total_score,
        "factor_details": ranked_contributions,
        "plus_factors": plus_factors,
        "minus_factors": minus_factors,
        "summary": "；".join(summary_parts),
    }


def rank_snapshot(snapshot: dict[str, SnapshotEntry], config: StrategyConfig) -> list[dict]:
    rows: list[dict] = []
    industry_counter: dict[str, int] = {}
    for entry in sorted(snapshot.values(), key=lambda item: score_entry(item, config)["total_score"], reverse=True):
        if not entry.factor_scores:
            continue
        if evaluate_hard_blocks(entry, config):
            continue
        if industry_counter.get(entry.industry, 0) >= config.risk.industry_cap:
            continue
        industry_counter[entry.industry] = industry_counter.get(entry.industry, 0) + 1
        scored = score_entry(entry, config)
        rows.append(
            {
                "code": entry.code,
                "name": entry.name,
                "industry": entry.industry,
                "board": entry.board,
                "current_price": round(entry.current_price, 2),
                "signal_date": entry.signal_date,
                "total_score": scored["total_score"],
                "explanation_summary": scored["summary"],
                "factor_scores": entry.factor_scores,
                "section_scores": entry.section_scores,
                "avg_turnover_20d": round(entry.avg_turnover_20d, 2),
                "pe_percentile": round(entry.pe_percentile, 4),
                "pb_percentile": round(entry.pb_percentile, 4),
                "position_plan": build_position_plan(entry, config).model_dump(),
            }
        )
    return rows
