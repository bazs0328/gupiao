from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date


SourceQuality = str
PublishDateQuality = str


@dataclass(slots=True)
class ResearchSecurityStateEvent:
    code: str
    event_date: str
    event_type: str
    name: str
    industry: str
    board: str
    security_type: str
    listing_date: str
    delisting_date: str | None = None
    is_st: bool = False
    tradable: bool = True
    source_quality: SourceQuality = "estimated"


@dataclass(slots=True)
class ResearchPriceBar:
    code: str
    trade_date: str
    price_basis: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
    is_suspended: bool = False
    source_quality: SourceQuality = "estimated"


@dataclass(slots=True)
class ResearchFinancialRecord:
    code: str
    report_date: str
    publish_date: str
    publish_date_quality: PublishDateQuality
    source_quality: SourceQuality
    roe: float
    revenue_yoy: float
    profit_yoy: float
    operating_cashflow: float
    net_profit: float
    pe_ttm: float
    pb: float
    debt_ratio: float


@dataclass(slots=True)
class ResearchCorporateAction:
    code: str
    action_date: str
    action_type: str
    payload_json: str
    source_quality: SourceQuality = "estimated"


class BaseResearchProvider(ABC):
    provider_name: str
    provider_mode: str

    @abstractmethod
    def fetch_security_state_events(
        self,
        *,
        since_date: date | None = None,
    ) -> list[ResearchSecurityStateEvent]:
        raise NotImplementedError

    @abstractmethod
    def fetch_price_bars(
        self,
        *,
        since_date: date | None = None,
        end_date: date | None = None,
    ) -> list[ResearchPriceBar]:
        raise NotImplementedError

    @abstractmethod
    def fetch_financial_records(
        self,
        *,
        since_date: date | None = None,
    ) -> list[ResearchFinancialRecord]:
        raise NotImplementedError

    @abstractmethod
    def fetch_corporate_actions(
        self,
        *,
        since_date: date | None = None,
    ) -> list[ResearchCorporateAction]:
        raise NotImplementedError


class RepositoryResearchProvider(BaseResearchProvider):
    provider_name = "repository_import"
    provider_mode = "free-public-proxy"

    def __init__(self, repository):
        self.repository = repository

    def fetch_security_state_events(
        self,
        *,
        since_date: date | None = None,
    ) -> list[ResearchSecurityStateEvent]:
        cutoff = since_date.isoformat() if since_date else None
        latest_trade_date = self.repository.get_latest_trade_date()
        events: list[ResearchSecurityStateEvent] = []
        for meta in self.repository.get_stock_meta().values():
            if meta["security_type"] != "equity":
                continue
            listing_date = meta["listing_date"]
            if cutoff and listing_date < cutoff and not latest_trade_date:
                continue
            events.append(
                ResearchSecurityStateEvent(
                    code=meta["code"],
                    event_date=listing_date,
                    event_type="listed",
                    name=meta["name"],
                    industry=meta["industry"],
                    board=meta.get("board", "main_board"),
                    security_type=meta["security_type"],
                    listing_date=listing_date,
                    is_st=bool(meta["is_st"]),
                    tradable=not bool(meta["is_suspended"]),
                    source_quality="estimated",
                )
            )
            if latest_trade_date and (not cutoff or latest_trade_date >= cutoff):
                events.append(
                    ResearchSecurityStateEvent(
                        code=meta["code"],
                        event_date=latest_trade_date,
                        event_type="status_snapshot",
                        name=meta["name"],
                        industry=meta["industry"],
                        board=meta.get("board", "main_board"),
                        security_type=meta["security_type"],
                        listing_date=listing_date,
                        is_st=bool(meta["is_st"]),
                        tradable=not bool(meta["is_suspended"]),
                        source_quality="estimated",
                    )
                )
        return events

    def fetch_price_bars(
        self,
        *,
        since_date: date | None = None,
        end_date: date | None = None,
    ) -> list[ResearchPriceBar]:
        lower = since_date.isoformat() if since_date else None
        upper = end_date.isoformat() if end_date else None
        bars: list[ResearchPriceBar] = []
        for code, items in self.repository.get_price_map().items():
            for row in items:
                if lower and row["trade_date"] < lower:
                    continue
                if upper and row["trade_date"] > upper:
                    continue
                adjusted = ResearchPriceBar(
                    code=code,
                    trade_date=row["trade_date"],
                    price_basis="adjusted",
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                    turnover=row["turnover"],
                    is_suspended=bool(row["is_suspended"]),
                    source_quality="estimated",
                )
                raw = ResearchPriceBar(
                    code=code,
                    trade_date=row["trade_date"],
                    price_basis="raw",
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                    turnover=row["turnover"],
                    is_suspended=bool(row["is_suspended"]),
                    source_quality="proxy",
                )
                bars.extend((adjusted, raw))
        return bars

    def fetch_financial_records(
        self,
        *,
        since_date: date | None = None,
    ) -> list[ResearchFinancialRecord]:
        cutoff = since_date.isoformat() if since_date else None
        with self.repository.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM financial_snapshot
                ORDER BY code, report_date
                """
            ).fetchall()
        result: list[ResearchFinancialRecord] = []
        for row in rows:
            if cutoff and row["report_date"] < cutoff and row["publish_date"] < cutoff:
                continue
            result.append(
                ResearchFinancialRecord(
                    code=row["code"],
                    report_date=row["report_date"],
                    publish_date=row["publish_date"],
                    publish_date_quality="estimated",
                    source_quality="estimated",
                    roe=row["roe"],
                    revenue_yoy=row["revenue_yoy"],
                    profit_yoy=row["profit_yoy"],
                    operating_cashflow=row["operating_cashflow"],
                    net_profit=row["net_profit"],
                    pe_ttm=row["pe_ttm"],
                    pb=row["pb"],
                    debt_ratio=row["debt_ratio"],
                )
            )
        return result

    def fetch_corporate_actions(
        self,
        *,
        since_date: date | None = None,
    ) -> list[ResearchCorporateAction]:
        return []
