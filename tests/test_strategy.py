"""
tests/test_strategy.py — Unit Tests for Strategy Logic
========================================================
Run with:  python -m pytest tests/ -v
"""
import pytest
import pandas as pd
import numpy as np
from strategy import (
    NiftyScalperStrategy, SignalType, PositionState,
    ExitReason
)
from config import StrategyConfig


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture
def strategy():
    return NiftyScalperStrategy()


def make_candles(n: int = 30, trend: str = "up", base: float = 22000.0) -> pd.DataFrame:
    """Generate synthetic candle data for testing."""
    closes = []
    price  = base
    for i in range(n):
        if trend == "up":
            price += np.random.uniform(5, 30)
        elif trend == "down":
            price -= np.random.uniform(5, 30)
        else:
            price += np.random.uniform(-15, 15)
        closes.append(price)

    data = []
    for c in closes:
        o = c - np.random.uniform(0, 10)
        data.append({
            "open":   o,
            "high":   c + np.random.uniform(0, 10),
            "low":    o - np.random.uniform(0, 5),
            "close":  c,
            "volume": 100000
        })
    return pd.DataFrame(data)


# ============================================================
# TESTS: SIGNAL GENERATION
# ============================================================

class TestSignalGeneration:

    def test_no_signal_with_insufficient_candles(self, strategy):
        short_df = make_candles(n=10)
        pos = PositionState(is_open=False)
        signal = strategy.evaluate(short_df, vix=12.0, nifty_spot=22000.0,
                                   position_state=pos, daily_trade_count=0,
                                   circuit_breaker_active=False)
        assert signal.signal_type == SignalType.NO_TRADE

    def test_trading_halted_at_high_vix(self, strategy):
        df = make_candles(n=30, trend="up")
        pos = PositionState(is_open=False)
        signal = strategy.evaluate(df, vix=26.0, nifty_spot=22000.0,
                                   position_state=pos, daily_trade_count=0,
                                   circuit_breaker_active=False)
        assert signal.signal_type == SignalType.NO_TRADE
        assert "VIX" in signal.reason or "halt" in signal.reason.lower()

    def test_no_signal_with_circuit_breaker(self, strategy):
        df = make_candles(n=30, trend="up")
        pos = PositionState(is_open=False)
        signal = strategy.evaluate(df, vix=12.0, nifty_spot=22000.0,
                                   position_state=pos, daily_trade_count=0,
                                   circuit_breaker_active=True)
        assert signal.signal_type == SignalType.NO_TRADE

    def test_no_entry_when_max_trades_reached(self, strategy):
        df = make_candles(n=30, trend="up")
        pos = PositionState(is_open=False)
        signal = strategy.evaluate(
            df, vix=12.0, nifty_spot=22000.0, position_state=pos,
            daily_trade_count=StrategyConfig.MAX_TRADES_PER_DAY,
            circuit_breaker_active=False
        )
        assert signal.signal_type == SignalType.NO_TRADE


class TestExitConditions:

    def test_profit_target_exit(self, strategy):
        df = make_candles(n=30)
        # Create position at +16% P&L (above 15% target)
        pos = PositionState(
            is_open=True, option_type="CE",
            entry_price=100.0,
            current_price=116.0,  # +16%
            quantity=25
        )
        signal = strategy.evaluate(df, vix=12.0, nifty_spot=22000.0,
                                   position_state=pos, daily_trade_count=1,
                                   circuit_breaker_active=False)
        assert signal.signal_type == SignalType.EXIT
        assert signal.exit_reason == ExitReason.PROFIT_TARGET

    def test_stop_loss_exit(self, strategy):
        df = make_candles(n=30)
        pos = PositionState(
            is_open=True, option_type="CE",
            entry_price=100.0,
            current_price=89.0,   # -11%
            quantity=25
        )
        signal = strategy.evaluate(df, vix=12.0, nifty_spot=22000.0,
                                   position_state=pos, daily_trade_count=1,
                                   circuit_breaker_active=False)
        assert signal.signal_type == SignalType.EXIT
        assert signal.exit_reason == ExitReason.STOP_LOSS

    def test_vix_spike_exits_open_position(self, strategy):
        df = make_candles(n=30)
        pos = PositionState(
            is_open=True, option_type="CE",
            entry_price=100.0, current_price=102.0, quantity=25
        )
        signal = strategy.evaluate(df, vix=26.0, nifty_spot=22000.0,
                                   position_state=pos, daily_trade_count=1,
                                   circuit_breaker_active=False)
        assert signal.signal_type == SignalType.EXIT
        assert signal.exit_reason == ExitReason.VIX_SPIKE

    def test_circuit_breaker_exits_open_position(self, strategy):
        df = make_candles(n=30)
        pos = PositionState(
            is_open=True, option_type="CE",
            entry_price=100.0, current_price=95.0, quantity=25
        )
        signal = strategy.evaluate(df, vix=12.0, nifty_spot=22000.0,
                                   position_state=pos, daily_trade_count=1,
                                   circuit_breaker_active=True)
        assert signal.signal_type == SignalType.EXIT
        assert signal.exit_reason == ExitReason.CIRCUIT_BREAKER


class TestPositionSizing:

    def test_lot_multiplier_full_at_low_vix(self):
        from config import RiskConfig
        assert RiskConfig.get_lot_multiplier(12.0) == 1.0

    def test_lot_multiplier_half_at_medium_vix(self):
        from config import RiskConfig
        assert RiskConfig.get_lot_multiplier(17.0) == 0.5

    def test_lot_multiplier_zero_at_high_vix(self):
        from config import RiskConfig
        assert RiskConfig.get_lot_multiplier(26.0) == 0.0


class TestCandleDataProcessing:

    def test_build_candle_df_from_list(self):
        raw = [
            {"date": "2024-01-01", "open": 100, "high": 110, "low": 95, "close": 105, "volume": 1000},
            {"date": "2024-01-01", "open": 105, "high": 115, "low": 100, "close": 112, "volume": 1200},
        ]
        df = NiftyScalperStrategy.build_candle_df(raw)
        assert len(df) == 2
        assert "close" in df.columns
        assert df["close"].iloc[1] == 112.0

    def test_build_candle_df_empty_input(self):
        df = NiftyScalperStrategy.build_candle_df([])
        assert df.empty
