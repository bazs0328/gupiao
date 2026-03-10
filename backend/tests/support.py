from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import math
import threading
import time

from backend.app.services.provider import BaseProvider, ProviderFinancial, ProviderPriceBar, ProviderStock
from backend.app.services.research_provider import (
    BaseResearchProvider,
    ResearchCorporateAction,
    ResearchFinancialRecord,
    ResearchPriceBar,
    ResearchSecurityStateEvent,
)


@dataclass(slots=True)
class Seed:
    code: str
    name: str
    industry: str
    board: str
    listing_date: str
    base_price: float
    drift: float
    volatility: float
    volume_base: float
    roe: float
    revenue_yoy: float
    profit_yoy: float
    operating_cashflow: float
    net_profit: float
    pe_ttm: float
    pb: float
    debt_ratio: float
    is_st: bool = False
    suspended_tail: int = 0
    listing_offset_days: int = 0


def business_days(start: date, end: date) -> list[date]:
    current = start
    dates: list[date] = []
    while current <= end:
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def infer_publish_date(report_date: date) -> date:
    if report_date.month == 3:
        return report_date + timedelta(days=30)
    if report_date.month == 6:
        return report_date + timedelta(days=45)
    if report_date.month == 9:
        return report_date + timedelta(days=30)
    return report_date + timedelta(days=90)


class StubAkshareProvider(BaseProvider):
    provider_name = "akshare"
    provider_mode = "real-a-share"

    def __init__(
        self,
        *,
        price_delay_seconds: float = 0.0,
        financial_delay_seconds: float = 0.0,
        universe_limit: int | None = None,
    ):
        self.lookback_days = 1100
        self.price_delay_seconds = price_delay_seconds
        self.financial_delay_seconds = financial_delay_seconds
        self._call_lock = threading.Lock()
        self.price_fetch_calls = 0
        self.financial_fetch_calls = 0
        self.price_fetch_ranges: list[tuple[str, str, str]] = []
        self.financial_symbols_fetched: list[str] = []
        end_day = date.today()
        start_day = end_day - timedelta(days=self.lookback_days)
        self._calendar = business_days(start_day, end_day)
        self._bars_by_code: dict[str, list[ProviderPriceBar]] = {}
        self._financials_by_code: dict[str, list[ProviderFinancial]] = {}
        self._seed_list = self._seeds()[:universe_limit] if universe_limit else self._seeds()
        self._universe = self._build_universe()
        for seed in self._seed_list:
            self._bars_by_code[seed.code] = self._generate_equity_series(seed)
            self._financials_by_code[seed.code] = self._generate_financial_series(seed)
        self._benchmark_stock, self._benchmark_bars = self._generate_benchmark_series()

    def _seeds(self) -> list[Seed]:
        return [
            Seed("600519", "贵州茅台", "消费白马", "main_board", "2001-08-27", 1550.0, 0.0028, 0.013, 3_600_000, 31.8, 15.2, 14.8, 78_500_000_000, 68_200_000_000, 27.0, 8.2, 21.0),
            Seed("000858", "五粮液", "消费白马", "main_board", "1998-04-27", 132.0, 0.0024, 0.014, 9_200_000, 25.1, 13.0, 12.1, 41_000_000_000, 34_200_000_000, 22.5, 5.6, 28.4),
            Seed("300750", "宁德时代", "新能源", "chi_next", "2018-06-11", 215.0, 0.0036, 0.020, 14_500_000, 23.6, 28.0, 25.1, 75_000_000_000, 62_000_000_000, 29.8, 6.1, 43.2),
            Seed("002594", "比亚迪", "新能源车", "main_board", "2011-06-30", 208.0, 0.0032, 0.022, 16_000_000, 21.4, 24.5, 21.2, 48_500_000_000, 36_400_000_000, 24.4, 4.7, 52.1),
            Seed("600036", "招商银行", "银行", "main_board", "2002-04-09", 34.0, 0.0014, 0.011, 18_000_000, 15.2, 8.6, 7.1, 210_000_000_000, 138_000_000_000, 6.8, 0.92, 88.0),
            Seed("600276", "恒瑞医药", "创新药", "main_board", "2000-10-18", 42.0, 0.0021, 0.015, 13_000_000, 17.3, 16.9, 13.2, 9_800_000_000, 7_600_000_000, 31.0, 6.8, 24.8),
            Seed("601318", "中国平安", "保险", "main_board", "2007-03-01", 46.0, 0.0016, 0.013, 20_000_000, 12.1, 6.4, 5.2, 129_000_000_000, 98_000_000_000, 7.2, 0.81, 89.5),
            Seed("000333", "美的集团", "家电", "main_board", "2013-09-18", 58.0, 0.0022, 0.013, 12_500_000, 24.8, 10.6, 11.5, 43_800_000_000, 35_200_000_000, 13.7, 3.3, 56.2),
            Seed("300124", "汇川技术", "自动化", "chi_next", "2010-09-28", 52.0, 0.0030, 0.016, 9_800_000, 27.6, 21.8, 20.1, 6_500_000_000, 4_700_000_000, 25.6, 4.9, 36.5),
            Seed("603259", "药明康德", "CXO", "main_board", "2018-05-08", 54.0, 0.0021, 0.018, 8_800_000, 18.6, 17.8, 16.1, 8_200_000_000, 6_300_000_000, 22.4, 4.1, 28.0),
            Seed("600001", "*ST样本", "风险股", "main_board", "1997-01-30", 6.4, -0.0008, 0.026, 4_000_000, 2.1, -18.4, -22.6, -210_000_000, -380_000_000, 80.0, 2.2, 91.0, is_st=True),
            Seed("301501", "新股样本", "半导体", "chi_next", self._calendar[-40].isoformat(), 68.0, 0.0042, 0.024, 3_500_000, 12.0, 35.0, 28.0, 550_000_000, 420_000_000, 56.0, 7.1, 33.0, listing_offset_days=max(len(self._calendar) - 40, 0)),
            Seed("000725", "停牌样本", "面板", "main_board", "2001-01-12", 5.2, 0.0007, 0.012, 18_000_000, 5.1, 4.3, 3.2, 8_000_000_000, 4_800_000_000, 18.0, 1.4, 62.0, suspended_tail=12),
        ]

    def _build_universe(self) -> list[ProviderStock]:
        return [
            ProviderStock(
                code=seed.code,
                name=seed.name,
                industry=seed.industry,
                board=seed.board,
                security_type="equity",
                listing_date=seed.listing_date,
                is_st=seed.is_st,
                is_suspended=seed.suspended_tail > 0,
            )
            for seed in self._seed_list
        ]

    def fetch_universe(self) -> list[ProviderStock]:
        return [
            ProviderStock(
                code=stock.code,
                name=stock.name,
                industry=stock.industry,
                board=stock.board,
                security_type=stock.security_type,
                listing_date=stock.listing_date,
                is_st=stock.is_st,
                is_suspended=stock.is_suspended,
            )
            for stock in self._universe
        ]

    def fetch_price_bars(self, symbol: str, start_date: date, end_date: date) -> list[ProviderPriceBar]:
        with self._call_lock:
            self.price_fetch_calls += 1
            self.price_fetch_ranges.append((symbol, start_date.isoformat(), end_date.isoformat()))
        if self.price_delay_seconds > 0:
            time.sleep(self.price_delay_seconds)
        return [
            ProviderPriceBar(
                code=bar.code,
                trade_date=bar.trade_date,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                turnover=bar.turnover,
                is_suspended=bar.is_suspended,
            )
            for bar in self._bars_by_code.get(symbol, [])
            if start_date.isoformat() <= bar.trade_date <= end_date.isoformat()
        ]

    def fetch_financial_series(self, symbol: str) -> list[ProviderFinancial]:
        with self._call_lock:
            self.financial_fetch_calls += 1
            self.financial_symbols_fetched.append(symbol)
        if self.financial_delay_seconds > 0:
            time.sleep(self.financial_delay_seconds)
        return [
            ProviderFinancial(
                code=item.code,
                report_date=item.report_date,
                publish_date=item.publish_date,
                roe=item.roe,
                revenue_yoy=item.revenue_yoy,
                profit_yoy=item.profit_yoy,
                operating_cashflow=item.operating_cashflow,
                net_profit=item.net_profit,
                pe_ttm=item.pe_ttm,
                pb=item.pb,
                debt_ratio=item.debt_ratio,
                basic_eps=item.basic_eps,
                per_net_assets=item.per_net_assets,
            )
            for item in self._financials_by_code.get(symbol, [])
        ]

    def fetch_benchmark_series(self, start_date: date, end_date: date) -> tuple[ProviderStock, list[ProviderPriceBar]]:
        bars = [
            ProviderPriceBar(
                code=bar.code,
                trade_date=bar.trade_date,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                turnover=bar.turnover,
                is_suspended=bar.is_suspended,
            )
            for bar in self._benchmark_bars
            if start_date.isoformat() <= bar.trade_date <= end_date.isoformat()
        ]
        return (
            ProviderStock(
                code=self._benchmark_stock.code,
                name=self._benchmark_stock.name,
                industry=self._benchmark_stock.industry,
                board=self._benchmark_stock.board,
                security_type=self._benchmark_stock.security_type,
                listing_date=self._benchmark_stock.listing_date,
                is_st=self._benchmark_stock.is_st,
                is_suspended=self._benchmark_stock.is_suspended,
            ),
            bars,
        )

    def _generate_equity_series(self, seed: Seed) -> list[ProviderPriceBar]:
        bars: list[ProviderPriceBar] = []
        active_calendar = self._calendar[seed.listing_offset_days :]
        previous_close = seed.base_price
        phase = (sum(ord(char) for char in seed.code) % 17) / 4
        total_days = len(active_calendar)

        for index, trade_day in enumerate(active_calendar):
            cyclical = math.sin((index + phase) / 6.4) * seed.volatility * 0.65
            swing = math.cos((index + phase) / 11.0) * seed.volatility * 0.38
            daily_return = seed.drift + cyclical + swing
            open_price = max(1.0, previous_close * (1 + daily_return * 0.35))
            close_price = max(1.0, previous_close * (1 + daily_return))
            high_price = max(open_price, close_price) * (1 + seed.volatility * 0.82)
            low_price = min(open_price, close_price) * (1 - seed.volatility * 0.82)
            volume_wave = 1 + math.sin((index + phase) / 5.3) * 0.22 + math.cos((index + phase) / 8.1) * 0.14
            volume = max(250_000, seed.volume_base * volume_wave)
            is_suspended = False

            if seed.suspended_tail and index >= total_days - seed.suspended_tail:
                open_price = previous_close
                close_price = previous_close
                high_price = previous_close
                low_price = previous_close
                volume = 0
                is_suspended = True

            turnover = close_price * volume
            previous_close = close_price
            bars.append(
                ProviderPriceBar(
                    code=seed.code,
                    trade_date=trade_day.isoformat(),
                    open=round(open_price, 2),
                    high=round(high_price, 2),
                    low=round(low_price, 2),
                    close=round(close_price, 2),
                    volume=round(volume, 2),
                    turnover=round(turnover, 2),
                    is_suspended=is_suspended,
                )
            )
        return bars

    def _generate_financial_series(self, seed: Seed) -> list[ProviderFinancial]:
        current_day = self._calendar[-1]
        candidate_dates: list[date] = []
        for year in range(current_day.year - 2, current_day.year + 1):
            candidate_dates.extend(
                [
                    date(year, 3, 31),
                    date(year, 6, 30),
                    date(year, 9, 30),
                    date(year, 12, 31),
                ]
            )
        report_dates = [value for value in sorted(candidate_dates) if value <= current_day][-6:]
        series: list[ProviderFinancial] = []
        for index, report_date in enumerate(report_dates):
            freshness = 0.92 + index * 0.03
            growth_wave = 1 + math.sin(index / 2 + len(seed.code)) * 0.08
            revenue_yoy = round(seed.revenue_yoy * growth_wave * freshness, 2)
            profit_yoy = round(seed.profit_yoy * (1 + math.cos(index / 2.3) * 0.07) * freshness, 2)
            roe = round(seed.roe * (0.88 + index * 0.025), 2)
            operating_cashflow = round(seed.operating_cashflow * (0.9 + index * 0.03), 2)
            net_profit = round(seed.net_profit * (0.9 + index * 0.028), 2)
            pe_ttm = round(max(2.0, seed.pe_ttm * (1 + math.sin(index / 2.4) * 0.05)), 2)
            pb = round(max(0.3, seed.pb * (1 + math.cos(index / 3.0) * 0.05)), 2)
            debt_ratio = round(max(5.0, seed.debt_ratio * (1 + math.sin(index / 3.5) * 0.04)), 2)
            basic_eps = round(
                (-1 if net_profit < 0 else 1) * max(seed.base_price / max(pe_ttm, 1.0), 0.01),
                4,
            )
            per_net_assets = round(max(seed.base_price / max(pb, 0.01), 0.01), 4)
            series.append(
                ProviderFinancial(
                    code=seed.code,
                    report_date=report_date.isoformat(),
                    publish_date=infer_publish_date(report_date).isoformat(),
                    roe=roe,
                    revenue_yoy=revenue_yoy,
                    profit_yoy=profit_yoy,
                    operating_cashflow=operating_cashflow,
                    net_profit=net_profit,
                    pe_ttm=pe_ttm,
                    pb=pb,
                    debt_ratio=debt_ratio,
                    basic_eps=basic_eps,
                    per_net_assets=per_net_assets,
                )
            )
        return series

    def _generate_benchmark_series(self) -> tuple[ProviderStock, list[ProviderPriceBar]]:
        bars: list[ProviderPriceBar] = []
        previous_close = 3650.0
        for index, trade_day in enumerate(self._calendar):
            daily_return = 0.0012 + math.sin(index / 8.2) * 0.005 + math.cos(index / 13.5) * 0.002
            open_price = previous_close * (1 + daily_return * 0.4)
            close_price = previous_close * (1 + daily_return)
            high_price = max(open_price, close_price) * 1.004
            low_price = min(open_price, close_price) * 0.996
            volume = 78_000_000 + math.sin(index / 9.0) * 8_000_000
            turnover = close_price * volume
            previous_close = close_price
            bars.append(
                ProviderPriceBar(
                    code="000300.SH",
                    trade_date=trade_day.isoformat(),
                    open=round(open_price, 2),
                    high=round(high_price, 2),
                    low=round(low_price, 2),
                    close=round(close_price, 2),
                    volume=round(volume, 2),
                    turnover=round(turnover, 2),
                )
            )
        stock = ProviderStock(
            code="000300.SH",
            name="沪深300",
            industry="指数",
            board="index",
            security_type="index",
            listing_date=bars[0].trade_date,
        )
        return stock, bars


def wait_for_sync_completion(client, run_id: str, timeout_seconds: float = 60.0):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        payload = client.get(f"/sync/runs/{run_id}").json()
        if payload["status"] != "running":
            return payload
        time.sleep(0.05)
    raise AssertionError("sync did not complete in time")


class StubResearchProvider(BaseResearchProvider):
    provider_name = "stub-research"
    provider_mode = "pit-actual"

    def __init__(self):
        self._sync_provider = StubAkshareProvider(universe_limit=8)

    def fetch_security_state_events(
        self,
        *,
        since_date: date | None = None,
    ) -> list[ResearchSecurityStateEvent]:
        cutoff = since_date.isoformat() if since_date else None
        events = [
            ResearchSecurityStateEvent(
                code=stock.code,
                event_date=stock.listing_date,
                event_type="listed",
                name=stock.name,
                industry=stock.industry,
                board=stock.board,
                security_type=stock.security_type,
                listing_date=stock.listing_date,
                is_st=stock.is_st,
                tradable=not stock.is_suspended,
                source_quality="actual",
            )
            for stock in self._sync_provider.fetch_universe()
        ]
        events.append(
            ResearchSecurityStateEvent(
                code="600001",
                event_date=(date.today() - timedelta(days=120)).isoformat(),
                event_type="st_start",
                name="*ST样本",
                industry="风险股",
                board="main_board",
                security_type="equity",
                listing_date="1997-01-30",
                is_st=True,
                tradable=True,
                source_quality="actual",
            )
        )
        events.append(
            ResearchSecurityStateEvent(
                code="688001",
                event_date=(date.today() - timedelta(days=40)).isoformat(),
                event_type="delisted",
                name="退市样本",
                industry="测试",
                board="main_board",
                security_type="equity",
                listing_date=(date.today() - timedelta(days=400)).isoformat(),
                delisting_date=(date.today() - timedelta(days=20)).isoformat(),
                is_st=False,
                tradable=False,
                source_quality="actual",
            )
        )
        return [item for item in events if not cutoff or item.event_date >= cutoff]

    def fetch_price_bars(
        self,
        *,
        since_date: date | None = None,
        end_date: date | None = None,
    ) -> list[ResearchPriceBar]:
        lower = since_date.isoformat() if since_date else None
        upper = end_date.isoformat() if end_date else None
        start_day = date.today() - timedelta(days=self._sync_provider.lookback_days)
        end_day = date.today()
        benchmark_stock, benchmark_bars = self._sync_provider.fetch_benchmark_series(start_day, end_day)
        rows: list[ResearchPriceBar] = []
        symbols = {stock.code for stock in self._sync_provider.fetch_universe()}
        symbols.add(benchmark_stock.code)
        for code in symbols:
            bars = benchmark_bars if code == benchmark_stock.code else self._sync_provider.fetch_price_bars(code, start_day, end_day)
            for bar in bars:
                if lower and bar.trade_date < lower:
                    continue
                if upper and bar.trade_date > upper:
                    continue
                rows.append(
                    ResearchPriceBar(
                        code=code,
                        trade_date=bar.trade_date,
                        price_basis="adjusted",
                        open=bar.open,
                        high=bar.high,
                        low=bar.low,
                        close=bar.close,
                        volume=bar.volume,
                        turnover=bar.turnover,
                        is_suspended=bar.is_suspended,
                        source_quality="actual",
                    )
                )
                rows.append(
                    ResearchPriceBar(
                        code=code,
                        trade_date=bar.trade_date,
                        price_basis="raw",
                        open=round(bar.open * 0.998, 4),
                        high=round(bar.high * 0.998, 4),
                        low=round(bar.low * 0.998, 4),
                        close=round(bar.close * 0.998, 4),
                        volume=bar.volume,
                        turnover=bar.turnover,
                        is_suspended=bar.is_suspended,
                        source_quality="actual",
                    )
                )
        return rows

    def fetch_financial_records(
        self,
        *,
        since_date: date | None = None,
    ) -> list[ResearchFinancialRecord]:
        cutoff = since_date.isoformat() if since_date else None
        rows: list[ResearchFinancialRecord] = []
        for stock in self._sync_provider.fetch_universe():
            for item in self._sync_provider.fetch_financial_series(stock.code):
                if cutoff and item.report_date < cutoff and item.publish_date < cutoff:
                    continue
                publish_quality = "estimated" if stock.code in {"600001", "000333"} else "actual"
                rows.append(
                    ResearchFinancialRecord(
                        code=item.code,
                        report_date=item.report_date,
                        publish_date=item.publish_date,
                        publish_date_quality=publish_quality,
                        source_quality="actual" if publish_quality == "actual" else "estimated",
                        roe=item.roe,
                        revenue_yoy=item.revenue_yoy,
                        profit_yoy=item.profit_yoy,
                        operating_cashflow=item.operating_cashflow,
                        net_profit=item.net_profit,
                        pe_ttm=item.pe_ttm,
                        pb=item.pb,
                        debt_ratio=item.debt_ratio,
                    )
                )
        return rows

    def fetch_corporate_actions(
        self,
        *,
        since_date: date | None = None,
    ) -> list[ResearchCorporateAction]:
        return []
