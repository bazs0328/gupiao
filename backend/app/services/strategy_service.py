from __future__ import annotations

from copy import deepcopy
from typing import Literal

from fastapi import HTTPException

from ..models import (
    ExecutionAssumptions,
    NotificationThresholds,
    RebalanceSettings,
    RiskGuardrails,
    StockPoolRules,
    StrategyConfig,
)


DEFAULT_STRATEGY_ID = "balanced"

FACTOR_METADATA: dict[str, dict[str, str]] = {
    "ma_trend": {"label": "20/60日均线趋势", "group": "technical"},
    "relative_strength_60d": {"label": "60日相对强度", "group": "technical"},
    "volume_surge_20d": {"label": "20日量能放大", "group": "technical"},
    "roe": {"label": "ROE", "group": "fundamental"},
    "revenue_yoy": {"label": "营收同比", "group": "fundamental"},
    "profit_yoy": {"label": "净利润同比", "group": "fundamental"},
    "cashflow_quality": {"label": "现金流质量", "group": "fundamental"},
    "pe_score": {"label": "PE 分位", "group": "fundamental"},
    "pb_score": {"label": "PB 分位", "group": "fundamental"},
    "debt_ratio": {"label": "资产负债率", "group": "risk"},
    "volatility_20d": {"label": "20日波动率", "group": "risk"},
    "liquidity": {"label": "流动性惩罚", "group": "risk"},
}

DEFAULT_STRATEGY_CONFIG = StrategyConfig(
    name="Balanced",
    description="均衡配置技术面、基本面与风险约束，适合收盘后默认候选视图。",
    weights={
        "ma_trend": 0.14,
        "relative_strength_60d": 0.13,
        "volume_surge_20d": 0.08,
        "roe": 0.076,
        "revenue_yoy": 0.076,
        "profit_yoy": 0.084,
        "cashflow_quality": 0.076,
        "pe_score": 0.06,
        "pb_score": 0.05,
        "debt_ratio": 0.076,
        "volatility_20d": 0.076,
        "liquidity": 0.076,
    },
    confidence_floors={"bullish": 60.0, "neutral": 66.0, "cautious": 74.0},
    stock_pool=StockPoolRules(
        min_listing_days=120,
        min_avg_turnover_20d=200_000_000,
        min_price=5.0,
        max_pe_percentile=0.9,
        max_pb_percentile=0.9,
        exclude_negative_profit_yoy=True,
    ),
    risk=RiskGuardrails(industry_cap=2, max_volatility_20d=0.055, max_position_weight=0.14),
    rebalance=RebalanceSettings(frequency="weekly", top_n=8, holding_period_days=10),
    execution=ExecutionAssumptions(commission_bps=5, slippage_bps=8, stamp_duty_bps=10),
    notifications=NotificationThresholds(break_trend_pct=0.02, volume_surge_score=80),
)


def normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    merged = {key: max(0.0, float(weights.get(key, 0.0))) for key in FACTOR_METADATA}
    total = sum(merged.values())
    if total <= 0:
        equal = 1 / len(FACTOR_METADATA)
        return {key: equal for key in FACTOR_METADATA}
    return {key: value / total for key, value in merged.items()}


def normalize_strategy(config: StrategyConfig) -> StrategyConfig:
    normalized = config.model_copy(deep=True)
    normalized.weights = normalize_weights(normalized.weights)
    merged_floors = {"bullish": 60.0, "neutral": 66.0, "cautious": 74.0, **normalized.confidence_floors}
    normalized.confidence_floors = {
        regime: round(max(45.0, min(95.0, float(value))), 2) for regime, value in merged_floors.items()
    }
    normalized.rebalance.top_n = max(5, normalized.rebalance.top_n)
    normalized.risk.industry_cap = max(1, normalized.risk.industry_cap)
    normalized.stock_pool.min_listing_days = max(60, normalized.stock_pool.min_listing_days)
    normalized.stock_pool.min_avg_turnover_20d = max(50_000_000, normalized.stock_pool.min_avg_turnover_20d)
    normalized.stock_pool.min_price = max(1.0, normalized.stock_pool.min_price)
    normalized.execution.limit_move_pct = max(0.05, normalized.execution.limit_move_pct)
    return normalized


def default_strategy_config() -> StrategyConfig:
    return normalize_strategy(deepcopy(DEFAULT_STRATEGY_CONFIG))


class StrategyService:
    def __init__(self, research_service=None):
        self.research_service = research_service

    def resolve_strategy(self, parameter_version: str | None = None) -> tuple[StrategyConfig, str, Literal["default", "research"]]:
        if parameter_version == "default":
            return default_strategy_config(), "default", "default"

        if parameter_version:
            if not self.research_service:
                raise HTTPException(status_code=404, detail="Research parameter version not found.")
            calibrated = self.research_service.get_strategy_config_for_version(parameter_version)
            if not calibrated:
                raise HTTPException(status_code=404, detail="Research parameter version not found.")
            return normalize_strategy(calibrated), parameter_version, "research"

        if self.research_service:
            active_parameter = self.research_service.get_active_parameter()
            if active_parameter:
                calibrated = StrategyConfig.model_validate(active_parameter["config_json"])
                return normalize_strategy(calibrated), active_parameter["version_id"], "research"

        return default_strategy_config(), "default", "default"

    def get_strategy(self, parameter_version: str | None = None) -> StrategyConfig:
        config, _, _ = self.resolve_strategy(parameter_version)
        return config

    def is_default_config(self, config: StrategyConfig) -> bool:
        return normalize_strategy(config).model_dump(mode="json") == default_strategy_config().model_dump(mode="json")
