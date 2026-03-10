from __future__ import annotations

from datetime import date, timedelta
import importlib
from typing import Any

from .provider import BaseProvider, ProviderFinancial, ProviderPriceBar, ProviderStock


ALLOWED_PREFIXES = ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605")
REQUIRED_FINANCIAL_METRICS = {
    "basic_eps": "basic_eps",
    "debt_ratio": "assets_debt_ratio",
    "net_profit": "parent_holder_net_profit",
    "operating_cash_per_share": "index_per_operating_cash_flow_net",
    "per_net_assets": "calc_per_net_assets",
    "profit_yoy": "calculate_parent_holder_net_profit_yoy_growth_ratio",
    "revenue_yoy": "calculate_operating_income_total_yoy_growth_ratio",
}
ROE_METRIC_CANDIDATES = ("index_weighted_avg_roe", "index_full_diluted_roe")


def _pick_column(columns: list[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        for column in columns:
            if candidate in column:
                return column
    return None


def _to_float(value: Any) -> float:
    if value in (None, "", "-", "--"):
        return 0.0
    text = str(value).strip().lower()
    if text in {"", "-", "--", "false", "nan", "none"}:
        return 0.0
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _is_blank_metric(value: Any) -> bool:
    if value in (None, "", "-", "--"):
        return True
    return str(value).strip().lower() in {"", "-", "--", "false", "nan", "none"}


def infer_publish_date(report_date: date) -> date:
    if report_date.month == 3:
        return report_date + timedelta(days=30)
    if report_date.month == 6:
        return report_date + timedelta(days=45)
    if report_date.month == 9:
        return report_date + timedelta(days=30)
    return report_date + timedelta(days=90)


def _board_for_symbol(symbol: str) -> str | None:
    if symbol.startswith(("300", "301")):
        return "chi_next"
    if symbol.startswith(ALLOWED_PREFIXES):
        return "main_board"
    return None


class AkshareProvider(BaseProvider):
    provider_name = "akshare"
    provider_mode = "real-a-share"

    def __init__(
        self,
        benchmark_code: str = "000300.SH",
        lookback_days: int = 1100,
        akshare_module: Any | None = None,
    ):
        self.benchmark_code = benchmark_code
        self.lookback_days = lookback_days
        self._akshare = akshare_module

    @property
    def akshare(self):
        if self._akshare is None:
            self._akshare = importlib.import_module("akshare")
        return self._akshare

    def fetch_universe(self) -> list[ProviderStock]:
        spot_df = self.akshare.stock_zh_a_spot_em()
        if spot_df is None or spot_df.empty:
            raise RuntimeError("Akshare spot snapshot returned no rows.")

        columns = list(spot_df.columns)
        code_column = _pick_column(columns, ["代码"])
        name_column = _pick_column(columns, ["名称"])
        industry_column = _pick_column(columns, ["所处行业", "行业"])
        if not code_column or not name_column:
            raise RuntimeError("Akshare spot snapshot schema is missing code/name columns.")

        st_codes: set[str] = set()
        try:
            st_df = self.akshare.stock_zh_a_st_em()
            if st_df is not None and not st_df.empty:
                st_code_column = _pick_column(list(st_df.columns), ["代码"])
                if st_code_column:
                    st_codes = {str(value).zfill(6) for value in st_df[st_code_column].astype(str)}
        except Exception:
            st_codes = set()

        stocks: list[ProviderStock] = []
        for row in spot_df.to_dict("records"):
            symbol = str(row[code_column]).zfill(6)
            if symbol.startswith(("688", "689", "4", "8")):
                continue
            board = _board_for_symbol(symbol)
            if not board:
                continue
            stocks.append(
                ProviderStock(
                    code=symbol,
                    name=str(row[name_column]),
                    industry=str(row.get(industry_column) or "未知行业") if industry_column else "未知行业",
                    board=board,
                    security_type="equity",
                    listing_date=date.today().isoformat(),
                    is_st=symbol in st_codes,
                    is_suspended=False,
                )
            )
        return stocks

    def fetch_price_bars(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[ProviderPriceBar]:
        hist_df = self.akshare.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            adjust="qfq",
        )
        if hist_df is None or hist_df.empty:
            return []

        columns = list(hist_df.columns)
        date_column = _pick_column(columns, ["日期"])
        open_column = _pick_column(columns, ["开盘"])
        close_column = _pick_column(columns, ["收盘"])
        high_column = _pick_column(columns, ["最高"])
        low_column = _pick_column(columns, ["最低"])
        volume_column = _pick_column(columns, ["成交量"])
        turnover_column = _pick_column(columns, ["成交额"])
        if not all([date_column, open_column, close_column, high_column, low_column, volume_column, turnover_column]):
            return []

        return [
            ProviderPriceBar(
                code=symbol,
                trade_date=str(row[date_column])[:10],
                open=_to_float(row[open_column]),
                high=_to_float(row[high_column]),
                low=_to_float(row[low_column]),
                close=_to_float(row[close_column]),
                volume=_to_float(row[volume_column]),
                turnover=_to_float(row[turnover_column]),
                is_suspended=_to_float(row[volume_column]) <= 0,
            )
            for row in hist_df.to_dict("records")
        ]

    def fetch_financial_series(self, symbol: str) -> list[ProviderFinancial]:
        try:
            financial_df = self.akshare.stock_financial_abstract_new_ths(symbol=symbol, indicator="按报告期")
        except Exception as exc:
            raise RuntimeError(f"{symbol}: 财务接口请求失败 {exc}") from exc
        if financial_df is None or financial_df.empty:
            raise RuntimeError(f"{symbol}: 财务接口返回空数据。")

        columns = {str(column) for column in financial_df.columns}
        missing_columns = sorted({"report_date", "metric_name", "value"} - columns)
        if missing_columns:
            raise RuntimeError(f"{symbol}: 财务接口返回缺少字段 {', '.join(missing_columns)}。")

        available_metrics = {str(value) for value in financial_df["metric_name"].dropna().astype(str)}
        missing_metrics = sorted(
            metric
            for metric in {value for value in REQUIRED_FINANCIAL_METRICS.values()} | set(ROE_METRIC_CANDIDATES)
            if metric not in available_metrics
        )
        if all(metric not in available_metrics for metric in ROE_METRIC_CANDIDATES):
            raise RuntimeError(f"{symbol}: 财务接口缺少关键指标 {', '.join(ROE_METRIC_CANDIDATES)}。")
        missing_metrics = [metric for metric in missing_metrics if metric not in ROE_METRIC_CANDIDATES]
        if missing_metrics:
            raise RuntimeError(f"{symbol}: 财务接口缺少关键指标 {', '.join(missing_metrics)}。")

        metrics_by_report_date: dict[str, dict[str, Any]] = {}
        for row in financial_df.to_dict("records"):
            raw_date = str(row.get("report_date") or "")[:10]
            metric_name = str(row.get("metric_name") or "")
            if not raw_date or not metric_name:
                continue
            metrics_by_report_date.setdefault(raw_date, {})[metric_name] = row.get("value")

        series: list[ProviderFinancial] = []
        for raw_date, metrics in sorted(metrics_by_report_date.items()):
            try:
                report_date = date.fromisoformat(raw_date)
            except ValueError:
                continue

            roe_raw = next((metrics.get(metric) for metric in ROE_METRIC_CANDIDATES if not _is_blank_metric(metrics.get(metric))), None)
            required_values = {
                "roe": roe_raw,
                **{name: metrics.get(metric_name) for name, metric_name in REQUIRED_FINANCIAL_METRICS.items()},
            }
            if any(_is_blank_metric(value) for value in required_values.values()):
                continue

            basic_eps = _to_float(required_values["basic_eps"])
            net_profit = _to_float(required_values["net_profit"])
            operating_cash_per_share = _to_float(required_values["operating_cash_per_share"])
            operating_cashflow = (
                operating_cash_per_share * abs(net_profit) / abs(basic_eps)
                if abs(basic_eps) > 1e-9
                else 0.0
            )
            series.append(
                ProviderFinancial(
                    code=symbol,
                    report_date=raw_date,
                    publish_date=infer_publish_date(report_date).isoformat(),
                    roe=_to_float(required_values["roe"]),
                    revenue_yoy=_to_float(required_values["revenue_yoy"]),
                    profit_yoy=_to_float(required_values["profit_yoy"]),
                    operating_cashflow=round(operating_cashflow, 4),
                    net_profit=net_profit,
                    pe_ttm=0.0,
                    pb=0.0,
                    debt_ratio=_to_float(required_values["debt_ratio"]),
                    basic_eps=basic_eps,
                    per_net_assets=_to_float(required_values["per_net_assets"]),
                )
            )
        if not series:
            raise RuntimeError(f"{symbol}: 财务接口未返回可用报告期。")
        return series

    def fetch_benchmark_series(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[ProviderStock, list[ProviderPriceBar]]:
        symbol = self.benchmark_code.replace(".SH", "").replace(".SZ", "")
        benchmark_df = self.akshare.index_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
        )
        if benchmark_df is None or benchmark_df.empty:
            raise RuntimeError("Benchmark price series is unavailable from Akshare.")
        columns = list(benchmark_df.columns)
        date_column = _pick_column(columns, ["日期"])
        open_column = _pick_column(columns, ["开盘"])
        close_column = _pick_column(columns, ["收盘"])
        high_column = _pick_column(columns, ["最高"])
        low_column = _pick_column(columns, ["最低"])
        volume_column = _pick_column(columns, ["成交量"])
        turnover_column = _pick_column(columns, ["成交额"])
        if not all([date_column, open_column, close_column, high_column, low_column, volume_column, turnover_column]):
            raise RuntimeError("Benchmark price series schema is incomplete.")

        benchmark_code = f"{symbol}.SH" if "." not in symbol else symbol
        bars = [
            ProviderPriceBar(
                code=benchmark_code,
                trade_date=str(row[date_column])[:10],
                open=_to_float(row[open_column]),
                high=_to_float(row[high_column]),
                low=_to_float(row[low_column]),
                close=_to_float(row[close_column]),
                volume=_to_float(row[volume_column]),
                turnover=_to_float(row[turnover_column]),
                is_suspended=False,
            )
            for row in benchmark_df.to_dict("records")
        ]
        stock = ProviderStock(
            code=benchmark_code,
            name="沪深300",
            industry="指数",
            board="index",
            security_type="index",
            listing_date=bars[0].trade_date if bars else "2005-04-08",
            is_st=False,
            is_suspended=False,
        )
        return stock, bars
