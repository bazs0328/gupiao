from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date


@dataclass(slots=True)
class ProviderStock:
    code: str
    name: str
    industry: str
    board: str
    security_type: str
    listing_date: str
    is_st: bool = False
    is_suspended: bool = False


@dataclass(slots=True)
class ProviderPriceBar:
    code: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
    is_suspended: bool = False


@dataclass(slots=True)
class ProviderFinancial:
    code: str
    report_date: str
    publish_date: str
    roe: float
    revenue_yoy: float
    profit_yoy: float
    operating_cashflow: float
    net_profit: float
    pe_ttm: float
    pb: float
    debt_ratio: float
    basic_eps: float = 0.0
    per_net_assets: float = 0.0


@dataclass(slots=True)
class DataAuditIssue:
    code: str
    issue_type: str
    message: str
    severity: str = "warning"
    trade_date: str | None = None


class BaseProvider(ABC):
    provider_name: str
    provider_mode: str

    @abstractmethod
    def fetch_universe(self) -> list[ProviderStock]:
        raise NotImplementedError

    @abstractmethod
    def fetch_price_bars(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[ProviderPriceBar]:
        raise NotImplementedError

    @abstractmethod
    def fetch_financial_series(self, symbol: str) -> list[ProviderFinancial]:
        raise NotImplementedError

    @abstractmethod
    def fetch_benchmark_series(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[ProviderStock, list[ProviderPriceBar]]:
        raise NotImplementedError
