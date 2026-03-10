from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ExecutionResult:
    code: str
    signal_date: str
    entry_trade_date: str | None
    exit_trade_date: str | None
    entry_price: float | None
    exit_price: float | None
    exit_reason: str
    asset_return: float
    benchmark_return: float
    excess_return: float
    max_drawdown: float
    blocked: bool = False


def _net_return(
    entry_price: float,
    exit_price: float,
    *,
    commission_bps: float,
    slippage_bps: float,
    stamp_duty_bps: float,
) -> float:
    buy_cost = (commission_bps + slippage_bps) / 10_000
    sell_cost = (commission_bps + slippage_bps + stamp_duty_bps) / 10_000
    gross_entry = entry_price * (1 + buy_cost)
    gross_exit = exit_price * (1 - sell_cost)
    return gross_exit / gross_entry - 1 if gross_entry else 0.0


def simulate_trade(
    *,
    code: str,
    signal_date: str,
    trading_dates: list[str],
    date_index: dict[str, int],
    bar_lookup: dict[str, dict[str, dict]],
    benchmark_code: str,
    holding_period_days: int,
    stop_loss: float | None,
    take_profit: float | None,
    commission_bps: float,
    slippage_bps: float,
    stamp_duty_bps: float,
) -> ExecutionResult | None:
    signal_index = date_index.get(signal_date)
    if signal_index is None or signal_index + 1 >= len(trading_dates):
        return None

    entry_trade_date = trading_dates[signal_index + 1]
    entry_bar = bar_lookup.get(code, {}).get(entry_trade_date)
    benchmark_entry = bar_lookup.get(benchmark_code, {}).get(entry_trade_date)
    if not entry_bar or not benchmark_entry or entry_bar["is_suspended"] or not entry_bar["open"]:
        return ExecutionResult(
            code=code,
            signal_date=signal_date,
            entry_trade_date=entry_trade_date,
            exit_trade_date=None,
            entry_price=None,
            exit_price=None,
            exit_reason="blocked_entry",
            asset_return=0.0,
            benchmark_return=0.0,
            excess_return=0.0,
            max_drawdown=0.0,
            blocked=True,
        )

    entry_price = float(entry_bar["open"])
    last_exit_date = entry_trade_date
    last_exit_price = float(entry_bar["close"])
    exit_reason = "time_exit"
    max_drawdown = 0.0

    max_offset = min(signal_index + holding_period_days, len(trading_dates) - 1)
    for index in range(signal_index + 1, max_offset + 1):
        trade_date = trading_dates[index]
        bar = bar_lookup.get(code, {}).get(trade_date)
        if not bar:
            continue
        if bar.get("is_suspended"):
            exit_reason = "blocked_exit"
            continue

        low_return = bar["low"] / entry_price - 1 if entry_price else 0.0
        max_drawdown = min(max_drawdown, low_return)

        if stop_loss is not None and bar["open"] <= stop_loss:
            last_exit_date = trade_date
            last_exit_price = float(bar["open"])
            exit_reason = "stop_loss_gap"
            break
        if take_profit is not None and bar["open"] >= take_profit:
            last_exit_date = trade_date
            last_exit_price = float(bar["open"])
            exit_reason = "take_profit_gap"
            break

        hit_stop = stop_loss is not None and bar["low"] <= stop_loss
        hit_take = take_profit is not None and bar["high"] >= take_profit
        if hit_stop and hit_take:
            last_exit_date = trade_date
            last_exit_price = float(stop_loss)
            exit_reason = "stop_loss"
            break
        if hit_stop:
            last_exit_date = trade_date
            last_exit_price = float(stop_loss)
            exit_reason = "stop_loss"
            break
        if hit_take:
            last_exit_date = trade_date
            last_exit_price = float(take_profit)
            exit_reason = "take_profit"
            break

        last_exit_date = trade_date
        last_exit_price = float(bar["close"])

    benchmark_exit_bar = bar_lookup.get(benchmark_code, {}).get(last_exit_date)
    benchmark_return = 0.0
    if benchmark_exit_bar and benchmark_entry["open"]:
        benchmark_return = benchmark_exit_bar["close"] / benchmark_entry["open"] - 1

    asset_return = _net_return(
        entry_price,
        last_exit_price,
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
        stamp_duty_bps=stamp_duty_bps,
    )
    return ExecutionResult(
        code=code,
        signal_date=signal_date,
        entry_trade_date=entry_trade_date,
        exit_trade_date=last_exit_date,
        entry_price=round(entry_price, 4),
        exit_price=round(last_exit_price, 4),
        exit_reason=exit_reason,
        asset_return=round(asset_return, 6),
        benchmark_return=round(benchmark_return, 6),
        excess_return=round(asset_return - benchmark_return, 6),
        max_drawdown=round(max_drawdown, 6),
        blocked=exit_reason in {"blocked_entry", "blocked_exit"},
    )
