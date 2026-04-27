"""
strategy.py — Core Nifty50 Options Scalping Strategy
======================================================
This module contains the signal generation engine.
It is PURE LOGIC — no broker calls, no order placement.
It takes market data as input and returns a trade signal as output.

STRATEGY LOGIC (from Blueprint Section 9):
  BULLISH ENTRY:
    - 5-min candle closes ABOVE previous 5-min candle close (uptrend)
    - RSI(14) on 5-min chart > 55 (bullish momentum)
    - India VIX < 15 (low volatility — full size) or < 20 (reduced size)
    → BUY ATM Call Option (CE)

  BEARISH ENTRY:
    - 5-min candle closes BELOW previous 5-min candle close (downtrend)
    - RSI(14) on 5-min chart < 45 (bearish momentum)
    - India VIX < 15 or < 20
    → BUY ATM Put Option (PE)

  EXIT CONDITIONS (whichever hits first):
    - +15% profit on premium paid (PROFIT TARGET)
    - -10% loss on premium paid (STOP LOSS)
    - 15:20 IST (EOD hard exit, handled by live_main.py)

  FILTERS (trade is BLOCKED if):
    - VIX >= 25 (too volatile — halt trading entirely)
    - Already 1 open position for this account (one trade at a time)
    - Daily trade count >= 20 (overtrading filter)
    - Circuit breaker active (daily drawdown limit breached)
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, List
import pandas as pd
import pandas_ta as ta
from loguru import logger

from config import StrategyConfig, RiskConfig


# ============================================================
# SIGNAL TYPES
# ============================================================

class SignalType(Enum):
    BUY_CE  = "BUY_CE"    # Buy Call Option (bullish)
    BUY_PE  = "BUY_PE"    # Buy Put Option  (bearish)
    EXIT    = "EXIT"       # Exit current position
    NO_TRADE = "NO_TRADE"  # Conditions not met — do nothing


class ExitReason(Enum):
    PROFIT_TARGET     = "PROFIT_TARGET"      # +15% reached
    STOP_LOSS         = "STOP_LOSS"          # -10% reached
    EOD_FLATTEN       = "EOD_FLATTEN"        # 15:20 hard exit
    VIX_SPIKE         = "VIX_SPIKE"          # VIX jumped above halt threshold
    CIRCUIT_BREAKER   = "CIRCUIT_BREAKER"    # Daily drawdown limit hit
    MANUAL_OVERRIDE   = "MANUAL_OVERRIDE"    # Admin manually closed


@dataclass
class TradeSignal:
    """
    The output of the strategy engine — tells live_main.py exactly
    what action to take for a subscriber's account.
    """
    signal_type: SignalType
    option_type: Optional[str]   = None   # "CE" or "PE"
    confidence: float            = 0.0    # 0.0 to 1.0 (for logging only)
    rsi_value: float             = 0.0
    vix_value: float             = 0.0
    nifty_spot: float            = 0.0
    lot_multiplier: float        = 1.0
    reason: str                  = ""
    exit_reason: Optional[ExitReason] = None

    def __str__(self):
        if self.signal_type in (SignalType.BUY_CE, SignalType.BUY_PE):
            return (f"SIGNAL: {self.signal_type.value} | "
                    f"RSI={self.rsi_value:.1f} | VIX={self.vix_value:.1f} | "
                    f"Spot={self.nifty_spot:.1f} | LotMult={self.lot_multiplier:.2f}")
        elif self.signal_type == SignalType.EXIT:
            return f"EXIT SIGNAL | Reason: {self.exit_reason.value if self.exit_reason else 'N/A'}"
        else:
            return f"NO_TRADE | {self.reason}"


@dataclass
class PositionState:
    """
    Tracks the current state of an open trade for a subscriber.
    Passed into the strategy to check exit conditions.
    """
    is_open: bool                = False
    option_type: Optional[str]   = None   # "CE" or "PE"
    entry_price: float           = 0.0
    current_price: float         = 0.0
    quantity: int                = 0

    @property
    def pnl_pct(self) -> float:
        """Current P&L as a percentage of entry price."""
        if self.entry_price <= 0:
            return 0.0
        return ((self.current_price - self.entry_price) / self.entry_price) * 100


# ============================================================
# STRATEGY ENGINE
# ============================================================

class NiftyScalperStrategy:
    """
    The core signal generator.
    Feed it market data → get a TradeSignal back.

    This class is STATELESS per call — it doesn't remember previous
    signals. State (open positions, daily P&L, circuit breaker status)
    is managed by live_main.py in the SubscriberProfile.
    """

    def __init__(self):
        self._log = logger.bind(component="Strategy")

    # ----------------------------------------------------------
    # PRIMARY ENTRY POINT
    # ----------------------------------------------------------

    def evaluate(
        self,
        candles: pd.DataFrame,
        vix: float,
        nifty_spot: float,
        position_state: PositionState,
        daily_trade_count: int,
        circuit_breaker_active: bool
    ) -> TradeSignal:
        """
        Main strategy evaluation method.
        Called once per 5-minute candle close during market hours.

        Args:
            candles:               DataFrame with columns: open, high, low, close, volume
                                   Must contain at least 20 rows (for RSI calculation).
            vix:                   Current India VIX value
            nifty_spot:            Current Nifty 50 index spot price
            position_state:        Current open trade state for this account
            daily_trade_count:     How many trades placed today (overtrading filter)
            circuit_breaker_active: True = halt all trading for this account

        Returns:
            TradeSignal with action to take
        """

        # ---- GUARD: Exit conditions always checked first ----
        if position_state.is_open:
            exit_signal = self._check_exit_conditions(
                position_state, vix, circuit_breaker_active
            )
            if exit_signal.signal_type == SignalType.EXIT:
                return exit_signal

        # ---- GUARD: Pre-entry filters ----
        block_signal = self._check_entry_filters(
            vix, daily_trade_count, circuit_breaker_active, position_state
        )
        if block_signal:
            return block_signal

        # ---- MAIN: Calculate indicators ----
        if len(candles) < 20:
            return TradeSignal(
                signal_type=SignalType.NO_TRADE,
                reason=f"Insufficient candle data ({len(candles)} rows, need 20+)"
            )

        rsi = self._calculate_rsi(candles)
        if rsi is None:
            return TradeSignal(signal_type=SignalType.NO_TRADE, reason="RSI calculation failed")

        trend = self._get_trend(candles)
        lot_multiplier = RiskConfig.get_lot_multiplier(vix)

        self._log.debug(
            f"Indicators | RSI={rsi:.1f} | Trend={trend} | "
            f"VIX={vix:.1f} (×{lot_multiplier:.2f}) | Spot={nifty_spot:.1f}"
        )

        # ---- BULLISH SIGNAL ----
        if (trend == "UP"
                and rsi > StrategyConfig.RSI_BULLISH_THRESHOLD
                and lot_multiplier > 0):
            return TradeSignal(
                signal_type=SignalType.BUY_CE,
                option_type="CE",
                confidence=self._confidence_score(rsi, trend, vix),
                rsi_value=rsi,
                vix_value=vix,
                nifty_spot=nifty_spot,
                lot_multiplier=lot_multiplier,
                reason=f"Bullish: RSI={rsi:.1f}>{StrategyConfig.RSI_BULLISH_THRESHOLD}, Trend=UP"
            )

        # ---- BEARISH SIGNAL ----
        if (trend == "DOWN"
                and rsi < StrategyConfig.RSI_BEARISH_THRESHOLD
                and lot_multiplier > 0):
            return TradeSignal(
                signal_type=SignalType.BUY_PE,
                option_type="PE",
                confidence=self._confidence_score(rsi, trend, vix),
                rsi_value=rsi,
                vix_value=vix,
                nifty_spot=nifty_spot,
                lot_multiplier=lot_multiplier,
                reason=f"Bearish: RSI={rsi:.1f}<{StrategyConfig.RSI_BEARISH_THRESHOLD}, Trend=DOWN"
            )

        # ---- NO SIGNAL ----
        return TradeSignal(
            signal_type=SignalType.NO_TRADE,
            rsi_value=rsi,
            vix_value=vix,
            nifty_spot=nifty_spot,
            reason=f"No signal: RSI={rsi:.1f}, Trend={trend}"
        )

    # ----------------------------------------------------------
    # EXIT CONDITIONS
    # ----------------------------------------------------------

    def _check_exit_conditions(
        self,
        pos: PositionState,
        vix: float,
        circuit_breaker_active: bool
    ) -> TradeSignal:
        """
        Check if the open position should be closed.
        Checked every candle tick when a position is open.
        """

        # Circuit breaker takes priority
        if circuit_breaker_active:
            self._log.warning("Circuit breaker active — forcing position exit.")
            return TradeSignal(
                signal_type=SignalType.EXIT,
                exit_reason=ExitReason.CIRCUIT_BREAKER,
                reason="Daily drawdown limit breached. Closing position."
            )

        # VIX spike while in trade — exit for safety
        if vix >= StrategyConfig.VIX_HALT_THRESHOLD:
            self._log.warning(
                f"VIX spike detected ({vix:.1f} ≥ {StrategyConfig.VIX_HALT_THRESHOLD}). "
                "Exiting position."
            )
            return TradeSignal(
                signal_type=SignalType.EXIT,
                exit_reason=ExitReason.VIX_SPIKE,
                vix_value=vix,
                reason=f"VIX={vix:.1f} exceeded halt threshold"
            )

        # Profit target hit (+15%)
        if pos.pnl_pct >= StrategyConfig.PROFIT_TARGET_PCT:
            self._log.success(
                f"PROFIT TARGET HIT | P&L: +{pos.pnl_pct:.2f}% | "
                f"Entry: {pos.entry_price:.2f} → Current: {pos.current_price:.2f}"
            )
            return TradeSignal(
                signal_type=SignalType.EXIT,
                exit_reason=ExitReason.PROFIT_TARGET,
                reason=f"Profit target +{StrategyConfig.PROFIT_TARGET_PCT}% reached "
                       f"(+{pos.pnl_pct:.2f}%)"
            )

        # Stop-loss hit (-10%)
        if pos.pnl_pct <= -StrategyConfig.STOP_LOSS_PCT:
            self._log.warning(
                f"STOP LOSS HIT | P&L: {pos.pnl_pct:.2f}% | "
                f"Entry: {pos.entry_price:.2f} → Current: {pos.current_price:.2f}"
            )
            return TradeSignal(
                signal_type=SignalType.EXIT,
                exit_reason=ExitReason.STOP_LOSS,
                reason=f"Stop loss -{StrategyConfig.STOP_LOSS_PCT}% triggered "
                       f"({pos.pnl_pct:.2f}%)"
            )

        # Position is fine — no exit needed
        return TradeSignal(signal_type=SignalType.NO_TRADE, reason="Position within bounds")

    # ----------------------------------------------------------
    # PRE-ENTRY FILTERS
    # ----------------------------------------------------------

    def _check_entry_filters(
        self,
        vix: float,
        daily_trade_count: int,
        circuit_breaker_active: bool,
        position_state: PositionState
    ) -> Optional[TradeSignal]:
        """
        Returns a NO_TRADE signal with reason if any filter blocks entry.
        Returns None if all filters pass (entry is allowed).
        """

        if circuit_breaker_active:
            return TradeSignal(
                signal_type=SignalType.NO_TRADE,
                reason="Circuit breaker active — no new entries allowed today",
                vix_value=vix
            )

        if vix >= StrategyConfig.VIX_HALT_THRESHOLD:
            return TradeSignal(
                signal_type=SignalType.NO_TRADE,
                reason=f"VIX={vix:.1f} ≥ halt threshold ({StrategyConfig.VIX_HALT_THRESHOLD}). "
                       "No trading.",
                vix_value=vix
            )

        if position_state.is_open:
            return TradeSignal(
                signal_type=SignalType.NO_TRADE,
                reason="Position already open — waiting for exit before new entry"
            )

        if daily_trade_count >= StrategyConfig.MAX_TRADES_PER_DAY:
            return TradeSignal(
                signal_type=SignalType.NO_TRADE,
                reason=f"Max trades/day ({StrategyConfig.MAX_TRADES_PER_DAY}) reached. "
                       "No more entries today."
            )

        return None  # All filters passed

    # ----------------------------------------------------------
    # TECHNICAL INDICATORS
    # ----------------------------------------------------------

    def _calculate_rsi(self, candles: pd.DataFrame) -> Optional[float]:
        """
        Calculate RSI(14) on the 5-minute close prices.
        Returns the most recent RSI value, or None on failure.
        """
        try:
            closes = candles["close"].copy().reset_index(drop=True)
            rsi_series = ta.rsi(closes, length=StrategyConfig.RSI_PERIOD)
            if rsi_series is None or rsi_series.empty:
                return None
            # Return the latest RSI value (last row)
            latest_rsi = rsi_series.dropna().iloc[-1]
            return float(latest_rsi)
        except Exception as e:
            self._log.error(f"RSI calculation error: {e}")
            return None

    def _get_trend(self, candles: pd.DataFrame) -> str:
        """
        Simple trend detection: compare last close vs previous close.
        'UP'   if last_close > prev_close
        'DOWN' if last_close < prev_close
        'FLAT' if equal

        Blueprint logic: "5-min candle close > previous close = uptrend"
        """
        try:
            closes = candles["close"].values
            if len(closes) < 2:
                return "FLAT"
            last_close = closes[-1]
            prev_close = closes[-2]
            if last_close > prev_close:
                return "UP"
            elif last_close < prev_close:
                return "DOWN"
            else:
                return "FLAT"
        except Exception as e:
            self._log.error(f"Trend detection error: {e}")
            return "FLAT"

    def _confidence_score(self, rsi: float, trend: str, vix: float) -> float:
        """
        Optional: score how strong the signal is (0.0 to 1.0).
        Used only for logging and reporting — does NOT affect order placement.
        Higher confidence = stronger signal conditions.
        """
        score = 0.5  # Base score

        # RSI distance from threshold adds confidence
        if trend == "UP":
            rsi_distance = rsi - StrategyConfig.RSI_BULLISH_THRESHOLD
        else:
            rsi_distance = StrategyConfig.RSI_BEARISH_THRESHOLD - rsi

        # Cap RSI contribution at 0.3
        score += min(rsi_distance / 50, 0.3)

        # Low VIX = more confidence
        if vix < StrategyConfig.VIX_MAX_NORMAL:
            score += 0.2
        elif vix < StrategyConfig.VIX_MAX_REDUCED:
            score += 0.1

        return min(score, 1.0)

    # ----------------------------------------------------------
    # CANDLE UTILITIES (used by backtester and live feed)
    # ----------------------------------------------------------

    @staticmethod
    def build_candle_df(raw_candles: list) -> pd.DataFrame:
        """
        Convert raw OHLCV data (list of dicts from Kite/Angel) into
        a clean pandas DataFrame for indicator calculation.

        Expected input format (Kite):
            [{"date": datetime, "open": x, "high": x, "low": x, "close": x, "volume": x}, ...]
        """
        if not raw_candles:
            return pd.DataFrame()

        df = pd.DataFrame(raw_candles)

        # Normalise column names (different brokers use different field names)
        rename_map = {
            "date": "datetime",
            "Date": "datetime",
            "timestamp": "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        required_cols = ["open", "high", "low", "close"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column '{col}' in candle data")

        df[required_cols] = df[required_cols].astype(float)
        if "volume" in df.columns:
            df["volume"] = df["volume"].astype(float)

        return df.reset_index(drop=True)
