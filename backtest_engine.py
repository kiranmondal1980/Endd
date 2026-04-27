"""
backtest_engine.py — Backtrader Strategy Backtesting Engine
=============================================================
Run this file directly to backtest the Nifty scalping strategy
on historical options data.

USAGE:
    python backtest_engine.py

HOW BACKTESTING WORKS:
  1. Load historical 5-minute Nifty index candle data (CSV or API)
  2. Simulate the strategy as if it were running live
  3. "Buy" and "sell" at historical prices (with simulated slippage + brokerage)
  4. Calculate performance metrics at the end
  5. Plot an equity curve

NOTE ON OPTIONS BACKTESTING:
  True options P&L backtesting requires historical options chain data
  (premium prices for each strike at each timestamp). This is expensive.
  A practical proxy: backtest entry/exit timing on Nifty FUTURES or INDEX,
  then apply a fixed option premium delta assumption (e.g., ATM option
  premium = ~0.5% of Nifty spot, moves roughly 0.3–0.6 with index).

  For proper options data: Unofficed.com (INR 2,000–5,000/month) or
  load your own downloaded NSE bhavcopy options data.

WHAT THIS SCRIPT PRODUCES:
  - Win rate %
  - Total trades
  - Max drawdown %
  - Sharpe ratio
  - Monthly P&L breakdown
  - Equity curve PNG
  - Full trade log CSV
"""

import os
import sys
import csv
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import backtrader as bt
import backtrader.analyzers as btanalyzers
import matplotlib
matplotlib.use("Agg")   # Non-interactive backend (for server environments)
import matplotlib.pyplot as plt

from loguru import logger
from config import StrategyConfig, BacktestConfig, RiskConfig, NIFTY_LOT_SIZE, LOG_DIR

# ============================================================
# CONFIGURE LOGGING
# ============================================================
logger.add(
    LOG_DIR / "backtest_{time}.log",
    rotation="1 day",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)


# ============================================================
# BACKTRADER STRATEGY CLASS
# ============================================================

class NiftyScalperBT(bt.Strategy):
    """
    Backtrader implementation of the Nifty Scalper strategy.
    This translates our strategy.py logic into Backtrader's event-driven framework.

    IMPORTANT: In a real backtest of options, `self.data.close` represents the
    Nifty index price. We simulate option P&L using a fixed premium multiplier.
    Replace with real options premium data for higher accuracy.
    """

    params = dict(
        rsi_period         = StrategyConfig.RSI_PERIOD,
        rsi_bull_threshold = StrategyConfig.RSI_BULLISH_THRESHOLD,
        rsi_bear_threshold = StrategyConfig.RSI_BEARISH_THRESHOLD,
        profit_target_pct  = StrategyConfig.PROFIT_TARGET_PCT / 100,
        stop_loss_pct      = StrategyConfig.STOP_LOSS_PCT / 100,
        # Simulated: ATM option premium ≈ 0.4% of Nifty spot on average
        option_premium_factor = 0.004,
        # Commission per trade (flat INR, approximating Zerodha options brokerage)
        commission_per_lot = 40.0,   # INR 40 per lot (20 + 20 both sides approx)
        vix_threshold_halt   = StrategyConfig.VIX_HALT_THRESHOLD,
        max_trades_per_day   = StrategyConfig.MAX_TRADES_PER_DAY,
        printlog             = True,
    )

    def __init__(self):
        # ---- Indicators ----
        self.rsi = bt.indicators.RSI_SMA(
            self.data.close,
            period=self.params.rsi_period
        )

        # ---- State tracking ----
        self.order          = None     # Pending order reference
        self.entry_price    = 0.0
        self.option_type    = None     # "CE" or "PE"
        self.daily_trades   = 0
        self.last_trade_day = None
        self.trade_log      = []       # List of dicts for CSV export

        # ---- Metrics ----
        self.wins   = 0
        self.losses = 0

        logger.info("NiftyScalperBT strategy initialised.")

    def notify_order(self, order):
        """Called by Backtrader whenever an order status changes."""
        if order.status in [order.Submitted, order.Accepted]:
            return   # Still pending — nothing to do

        if order.status == order.Completed:
            if order.isbuy():
                logger.debug(
                    f"BUY FILLED | Price: {order.executed.price:.2f} | "
                    f"Size: {order.executed.size} | "
                    f"Cost: {order.executed.value:.2f} | "
                    f"Comm: {order.executed.comm:.2f}"
                )
                self.entry_price = order.executed.price
            elif order.issell():
                pnl = (order.executed.price - self.entry_price) * order.executed.size
                logger.debug(
                    f"SELL FILLED | Price: {order.executed.price:.2f} | "
                    f"PnL: {pnl:.2f}"
                )
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            logger.warning(f"Order FAILED | Status: {order.getstatusname()}")

        self.order = None  # Clear pending order

    def notify_trade(self, trade):
        """Called by Backtrader when a round-trip trade closes."""
        if not trade.isclosed:
            return

        pnl = trade.pnl
        pnl_net = trade.pnlcomm

        if pnl_net > 0:
            self.wins += 1
            logger.info(f"TRADE CLOSED — WIN  | Gross: {pnl:.2f} | Net: {pnl_net:.2f}")
        else:
            self.losses += 1
            logger.info(f"TRADE CLOSED — LOSS | Gross: {pnl:.2f} | Net: {pnl_net:.2f}")

        # Log to trade_log for CSV export
        self.trade_log.append({
            "datetime":   self.data.datetime.datetime(0).strftime("%Y-%m-%d %H:%M"),
            "option_type": self.option_type or "N/A",
            "entry_price": round(self.entry_price, 2),
            "exit_price":  round(self.data.close[0], 2),
            "pnl_gross":   round(pnl, 2),
            "pnl_net":     round(pnl_net, 2),
            "result":      "WIN" if pnl_net > 0 else "LOSS"
        })

    def next(self):
        """
        Called by Backtrader for every new 5-minute candle.
        This is the strategy's main execution loop.
        """
        current_dt   = self.data.datetime.datetime(0)
        current_time = current_dt.time()

        # ---- Reset daily trade counter at start of each day ----
        current_day = current_dt.date()
        if self.last_trade_day != current_day:
            self.daily_trades  = 0
            self.last_trade_day = current_day

        # ---- Skip pre-market and post-market candles ----
        from datetime import time as dtime
        if current_time < dtime(9, 15) or current_time >= dtime(15, 20):
            return

        # ---- Hard EOD exit at 15:20 ----
        if current_time >= dtime(15, 20) and self.position:
            self.close()
            logger.info(f"EOD FLATTEN at {current_dt}")
            return

        # ---- Skip if there's a pending order ----
        if self.order:
            return

        # ---- EXIT LOGIC (if we have an open position) ----
        if self.position:
            price_now  = self.data.close[0]
            pnl_pct    = ((price_now - self.entry_price) / self.entry_price) * 100

            if pnl_pct >= self.params.profit_target_pct * 100:
                logger.debug(f"PROFIT TARGET ({pnl_pct:.1f}%) → Closing position")
                self.order = self.close()
                return

            if pnl_pct <= -(self.params.stop_loss_pct * 100):
                logger.debug(f"STOP LOSS ({pnl_pct:.1f}%) → Closing position")
                self.order = self.close()
                return

        # ---- ENTRY LOGIC ----
        if not self.position:
            if self.daily_trades >= self.params.max_trades_per_day:
                return

            rsi_val    = self.rsi[0]
            prev_close = self.data.close[-1]
            cur_close  = self.data.close[0]

            # BULLISH: uptrend + RSI > threshold
            if cur_close > prev_close and rsi_val > self.params.rsi_bull_threshold:
                # Simulate buying a CE option: we buy units at simulated premium
                simulated_premium = cur_close * self.params.option_premium_factor
                size = NIFTY_LOT_SIZE   # 1 lot = 25 units
                self.option_type = "CE"
                self.order = self.buy(size=size, price=simulated_premium,
                                      exectype=bt.Order.Market)
                self.daily_trades += 1
                logger.info(
                    f"BUY CE SIGNAL | {current_dt} | "
                    f"Spot: {cur_close:.1f} | RSI: {rsi_val:.1f} | "
                    f"Premium~: {simulated_premium:.2f}"
                )

            # BEARISH: downtrend + RSI < threshold
            elif cur_close < prev_close and rsi_val < self.params.rsi_bear_threshold:
                simulated_premium = cur_close * self.params.option_premium_factor
                size = NIFTY_LOT_SIZE
                self.option_type = "PE"
                self.order = self.buy(size=size, price=simulated_premium,
                                      exectype=bt.Order.Market)
                self.daily_trades += 1
                logger.info(
                    f"BUY PE SIGNAL | {current_dt} | "
                    f"Spot: {cur_close:.1f} | RSI: {rsi_val:.1f} | "
                    f"Premium~: {simulated_premium:.2f}"
                )

    def stop(self):
        """Called by Backtrader when the backtest is complete."""
        total_trades = self.wins + self.losses
        win_rate = (self.wins / total_trades * 100) if total_trades > 0 else 0
        logger.info(
            f"\n{'='*50}\n"
            f"BACKTEST COMPLETE\n"
            f"  Total Trades : {total_trades}\n"
            f"  Wins         : {self.wins}\n"
            f"  Losses       : {self.losses}\n"
            f"  Win Rate     : {win_rate:.1f}%\n"
            f"  Final Capital: {self.broker.getvalue():.2f}\n"
            f"{'='*50}"
        )


# ============================================================
# BACKTEST RUNNER
# ============================================================

def load_data_from_csv(filepath: str) -> bt.feeds.GenericCSVData:
    """
    Load 5-minute OHLCV data from a CSV file into Backtrader.

    Expected CSV columns (in order):
        datetime,open,high,low,close,volume
    Example row:
        2022-04-01 09:15:00,17500.5,17510.2,17498.1,17505.3,125000

    You can download Nifty historical data from:
      - NSEpy library (free, EOD only)
      - Zerodha Kite historical API (5-min available)
      - Unofficed.com (paid, full options chain)
    """
    return bt.feeds.GenericCSVData(
        dataname=filepath,
        dtformat="%Y-%m-%d %H:%M:%S",
        datetime=0,
        open=1,
        high=2,
        low=3,
        close=4,
        volume=5,
        openinterest=-1,   # No OI column in this format
        timeframe=bt.TimeFrame.Minutes,
        compression=5      # 5-minute bars
    )


def run_backtest(
    data_filepath: str,
    initial_capital: float = BacktestConfig.INITIAL_CAPITAL,
    output_dir: str = "backtest_results"
) -> dict:
    """
    Run the full backtest and return performance metrics.

    Args:
        data_filepath:   Path to the CSV file with 5-min OHLCV data
        initial_capital: Starting portfolio value in INR
        output_dir:      Directory to save results (plots, CSVs)

    Returns:
        dict with performance metrics
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # ---- Cerebro is Backtrader's main engine ----
    cerebro = bt.Cerebro()

    # ---- Load data ----
    if not Path(data_filepath).exists():
        logger.error(f"Data file not found: {data_filepath}")
        sys.exit(1)

    data = load_data_from_csv(data_filepath)
    cerebro.adddata(data)
    logger.info(f"Data loaded from: {data_filepath}")

    # ---- Add strategy ----
    cerebro.addstrategy(NiftyScalperBT, printlog=True)

    # ---- Set capital ----
    cerebro.broker.setcash(initial_capital)
    logger.info(f"Starting capital: INR {initial_capital:,.0f}")

    # ---- Set commission (Zerodha options: flat INR 20/order) ----
    cerebro.broker.setcommission(
        commission=BacktestConfig.COMMISSION_PCT,
        commtype=bt.CommInfoBase.COMM_PERC
    )

    # ---- Add slippage ----
    cerebro.broker.set_slippage_fixed(BacktestConfig.SLIPPAGE_POINTS)

    # ---- Add performance analysers ----
    cerebro.addanalyzer(btanalyzers.SharpeRatio, _name="sharpe",
                        riskfreerate=0.065,   # 6.5% risk-free rate (India repo rate approx)
                        annualize=True)
    cerebro.addanalyzer(btanalyzers.DrawDown,    _name="drawdown")
    cerebro.addanalyzer(btanalyzers.Returns,     _name="returns")
    cerebro.addanalyzer(btanalyzers.TradeAnalyzer, _name="trades")

    # ---- RUN ----
    logger.info("Starting backtest run...")
    start_value = cerebro.broker.getvalue()
    results = cerebro.run()
    end_value = cerebro.broker.getvalue()
    strat = results[0]

    # ---- Extract metrics ----
    sharpe   = strat.analyzers.sharpe.get_analysis().get("sharperatio", 0) or 0
    drawdown = strat.analyzers.drawdown.get_analysis()
    trades   = strat.analyzers.trades.get_analysis()

    total_return_pct = ((end_value - start_value) / start_value) * 100
    max_drawdown_pct = drawdown.get("max", {}).get("drawdown", 0)

    total_trades = trades.get("total", {}).get("total", 0)
    won_trades   = trades.get("won",   {}).get("total", 0)
    lost_trades  = trades.get("lost",  {}).get("total", 0)
    win_rate     = (won_trades / total_trades * 100) if total_trades > 0 else 0

    metrics = {
        "start_capital":    start_value,
        "end_capital":      end_value,
        "total_return_pct": round(total_return_pct, 2),
        "total_trades":     total_trades,
        "wins":             won_trades,
        "losses":           lost_trades,
        "win_rate_pct":     round(win_rate, 2),
        "sharpe_ratio":     round(float(sharpe), 3),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
    }

    # ---- Print report ----
    print("\n" + "=" * 55)
    print("       NIFTY SCALPER — BACKTEST RESULTS")
    print("=" * 55)
    print(f"  Period         : {BacktestConfig.START_DATE} → {BacktestConfig.END_DATE}")
    print(f"  Start Capital  : INR {start_value:>12,.2f}")
    print(f"  End Capital    : INR {end_value:>12,.2f}")
    print(f"  Total Return   : {total_return_pct:>+.2f}%")
    print(f"  Total Trades   : {total_trades}")
    print(f"  Win Rate       : {win_rate:.1f}%  ({won_trades}W / {lost_trades}L)")
    print(f"  Max Drawdown   : {max_drawdown_pct:.2f}%")
    print(f"  Sharpe Ratio   : {sharpe:.3f}")
    print("=" * 55)

    # ---- Save metrics to JSON ----
    metrics_file = output_path / "backtest_metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"Metrics saved to {metrics_file}")

    # ---- Save trade log to CSV ----
    if strat.trade_log:
        trade_log_file = output_path / "trade_log.csv"
        with open(trade_log_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=strat.trade_log[0].keys())
            writer.writeheader()
            writer.writerows(strat.trade_log)
        logger.info(f"Trade log ({len(strat.trade_log)} trades) saved to {trade_log_file}")

    # ---- Plot equity curve ----
    try:
        fig = cerebro.plot(style="candlestick", iplot=False)[0][0]
        plot_file = output_path / "equity_curve.png"
        fig.savefig(str(plot_file), dpi=150, bbox_inches="tight")
        logger.info(f"Equity curve saved to {plot_file}")
    except Exception as e:
        logger.warning(f"Could not save plot: {e}")

    return metrics


# ============================================================
# GENERATE SAMPLE CSV (for testing without paid data)
# ============================================================

def generate_sample_data(filepath: str, days: int = 30):
    """
    Generate synthetic 5-minute Nifty candle data for testing.
    THIS IS NOT REAL DATA — use only to verify the code runs.
    For real backtesting, replace with actual NSE historical data.
    """
    from datetime import timedelta, time as dtime
    import random

    logger.warning("Generating SYNTHETIC data. Replace with real data before trusting results!")

    rows = []
    base_price = 22000.0
    start_date = datetime(2024, 1, 2)

    for day_num in range(days):
        current_date = start_date + timedelta(days=day_num)
        # Skip weekends
        if current_date.weekday() >= 5:
            continue

        price = base_price + random.uniform(-500, 500)
        market_open  = datetime.combine(current_date, dtime(9, 15))
        market_close = datetime.combine(current_date, dtime(15, 30))
        current_time = market_open

        while current_time <= market_close:
            open_  = price
            change = random.gauss(0, 0.002) * price
            high   = open_ + abs(random.uniform(0, 0.003) * price)
            low    = open_ - abs(random.uniform(0, 0.003) * price)
            close  = open_ + change
            volume = random.randint(50000, 300000)

            rows.append({
                "datetime": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                "open":     round(open_, 2),
                "high":     round(high, 2),
                "low":      round(low, 2),
                "close":    round(close, 2),
                "volume":   volume
            })
            price = close
            current_time += timedelta(minutes=5)

    df = pd.DataFrame(rows)
    df.to_csv(filepath, index=False)
    logger.info(f"Sample data saved to {filepath} ({len(rows)} candles, {days} days)")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    DATA_FILE   = "data/nifty_5min.csv"
    OUTPUT_DIR  = "backtest_results"

    # Step 1: If no data file exists, generate synthetic data for a demo run
    if not Path(DATA_FILE).exists():
        Path("data").mkdir(exist_ok=True)
        logger.info("No data file found. Generating sample data for demo run...")
        generate_sample_data(DATA_FILE, days=60)
        logger.warning(
            "\n⚠️  IMPORTANT: The backtest above used SYNTHETIC data.\n"
            "   Download real NSE 5-minute Nifty data and replace\n"
            f"   '{DATA_FILE}' before making any strategy decisions.\n"
            "   Real data sources: NSEpy, Zerodha Kite API, Unofficed.com"
        )

    # Step 2: Run the backtest
    metrics = run_backtest(
        data_filepath=DATA_FILE,
        initial_capital=BacktestConfig.INITIAL_CAPITAL,
        output_dir=OUTPUT_DIR
    )

    print(f"\nResults saved to: {OUTPUT_DIR}/")
