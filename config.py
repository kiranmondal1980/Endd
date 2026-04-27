"""
config.py — Global Configuration & Risk Parameters
====================================================
This is the single source of truth for all strategy parameters,
risk limits, market timings, and multi-tenant subscriber profiles.

IMPORTANT: Risk parameters are enforced at multiple levels:
  1. strategy.py  — signal generation (entry filters)
  2. live_main.py — order placement (position sizing)
  3. live_main.py — circuit breaker (daily drawdown halt)
"""

import os
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Optional
from dotenv import load_dotenv

# Load .env file (does nothing if already set by OS environment)
load_dotenv()

# ============================================================
# PATHS
# ============================================================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR  = Path(os.getenv("LOG_DIR", str(BASE_DIR / "logs")))
TRADE_DB = Path(os.getenv("TRADE_LOG_DB_PATH", str(DATA_DIR / "trade_log.db")))
SUBSCRIBER_CONFIG_PATH = Path(
    os.getenv("SUBSCRIBER_CONFIG_PATH", str(DATA_DIR / "subscribers.enc.json"))
)

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# ENVIRONMENT
# ============================================================
APP_ENV = os.getenv("APP_ENV", "development")
IS_PROD  = APP_ENV == "production"

# ============================================================
# MARKET TIMINGS  (All times in IST — Asia/Kolkata)
# ============================================================
TIMEZONE = "Asia/Kolkata"

MARKET_OPEN_TIME   = "09:15"   # NSE opens
MARKET_CLOSE_TIME  = "15:30"   # NSE closes

# Algorithm-specific schedule
ALGO_START_TIME         = "09:15"  # First signal scan
ALGO_FLATTEN_TIME       = "15:20"  # Hard close ALL positions (10 min before close)
ALGO_MONITOR_START_TIME = "07:00"  # VIX check, API warmup
DAILY_REPORT_TIME       = "15:50"  # Telegram P&L report
MORNING_BRIEF_TIME      = "07:50"  # Pre-market WhatsApp brief

# ============================================================
# NIFTY50 CONTRACT SPECIFICATIONS
# ============================================================
NIFTY_LOT_SIZE         = 25       # NSE mandated lot size for Nifty options
NIFTY_STRIKE_INTERVAL  = 50       # Strike price gap (e.g., 22100, 22150, 22200)
NIFTY_SYMBOL_NSE       = "NIFTY"  # NSE symbol
NIFTY_INDEX_TOKEN_KITE = "256265" # Zerodha instrument token for NIFTY 50 index

# ============================================================
# CORE STRATEGY PARAMETERS  (Section 9 of Blueprint)
# ============================================================
class StrategyConfig:
    # ---- Entry Filters ----
    RSI_PERIOD          = 14
    RSI_BULLISH_THRESHOLD = 55   # RSI > 55 → bullish bias → buy CE
    RSI_BEARISH_THRESHOLD = 45   # RSI < 45 → bearish bias → buy PE

    CANDLE_TIMEFRAME    = "5minute"   # 5-minute candles
    TREND_CONFIRMATION  = True        # Require close > prev close for bullish

    # ---- VIX Filters ----
    VIX_MAX_NORMAL       = 15.0   # VIX < 15  → full position size
    VIX_MAX_REDUCED      = 20.0   # 15 ≤ VIX < 20 → 50% position size
    VIX_HALT_THRESHOLD   = 25.0   # VIX ≥ 25  → NO trading at all

    # ---- Exit Rules ----
    PROFIT_TARGET_PCT    = 15.0   # Exit option at +15% of premium paid
    STOP_LOSS_PCT        = 10.0   # Exit option at -10% of premium paid
    EOD_EXIT_TIME        = "15:20" # Hard exit — no open positions after this

    # ---- Trade Frequency ----
    MIN_TRADES_PER_DAY   = 3      # Below this → algo may not be triggering (alert)
    MAX_TRADES_PER_DAY   = 20     # Above this → overtrading risk (alert)

    # ---- Option Selection ----
    OPTION_TYPE_BULLISH  = "CE"   # Call option when bullish
    OPTION_TYPE_BEARISH  = "PE"   # Put option when bearish
    STRIKE_SELECTION     = "ATM"  # At-the-money strike

    # ---- Expiry ----
    USE_WEEKLY_EXPIRY    = True   # Trade weekly expiry (Thursday)

# ============================================================
# RISK MANAGEMENT PARAMETERS  (Blueprint Section 9, 30, 38)
# ============================================================
class RiskConfig:
    # ---- Per-Account Daily Circuit Breaker ----
    MAX_DAILY_DRAWDOWN_PCT      = 3.0    # Pause account if daily P&L < -3%
    MAX_WEEKLY_DRAWDOWN_PCT     = 5.0    # Alert + review if weekly loss > 5%
    MAX_MONTHLY_DRAWDOWN_PCT    = 10.0   # Emergency review if monthly > -10%

    # ---- Capital Allocation ----
    MAX_CAPITAL_PER_TRADE_PCT   = 10.0   # Never use > 10% of account in one trade
    DEFAULT_LOTS_PER_TRADE      = 1      # Start with 1 lot (25 qty) per trade

    # ---- Slippage Monitoring ----
    SLIPPAGE_ALERT_THRESHOLD    = 1.0    # Alert if slippage > 1 point per trade
    SLIPPAGE_HALT_THRESHOLD     = 3.0    # Halt if consistent slippage > 3 points

    # ---- Consecutive Losses ----
    MAX_CONSECUTIVE_LOSSES      = 5      # Pause account after 5 consecutive losers

    # ---- VIX-based Position Sizing ----
    @staticmethod
    def get_lot_multiplier(vix: float) -> float:
        """
        Returns a multiplier for lot size based on VIX.
        Full size at low VIX, reduced at high VIX.
        """
        if vix >= StrategyConfig.VIX_HALT_THRESHOLD:
            return 0.0   # No trading
        elif vix >= StrategyConfig.VIX_MAX_REDUCED:
            return 0.5   # Half size
        elif vix >= StrategyConfig.VIX_MAX_NORMAL:
            return 0.75  # Three-quarter size
        else:
            return 1.0   # Full size

# ============================================================
# BROKER CONFIGURATION
# ============================================================
BROKER_ZERODHA    = "zerodha"
BROKER_ANGEL      = "angel"
SUPPORTED_BROKERS = [BROKER_ZERODHA, BROKER_ANGEL]

# Zerodha Admin (your own account used for live feed / VIX)
ZERODHA_ADMIN_API_KEY    = os.getenv("ZERODHA_ADMIN_API_KEY", "")
ZERODHA_ADMIN_API_SECRET = os.getenv("ZERODHA_ADMIN_API_SECRET", "")

# Angel One Admin
ANGEL_ADMIN_API_KEY    = os.getenv("ANGEL_ADMIN_API_KEY", "")
ANGEL_ADMIN_CLIENT_ID  = os.getenv("ANGEL_ADMIN_CLIENT_ID", "")
ANGEL_ADMIN_PASSWORD   = os.getenv("ANGEL_ADMIN_PASSWORD", "")
ANGEL_ADMIN_TOTP_SECRET = os.getenv("ANGEL_ADMIN_TOTP_SECRET", "")

# ============================================================
# TELEGRAM CONFIGURATION
# ============================================================
TELEGRAM_BOT_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ADMIN_CHAT_ID  = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
TELEGRAM_CHANNEL_ID     = os.getenv("TELEGRAM_CHANNEL_ID", "")

# ============================================================
# ENCRYPTION
# ============================================================
MASTER_ENCRYPTION_KEY = os.getenv("MASTER_ENCRYPTION_KEY", "").encode()

# ============================================================
# SUBSCRIBER PROFILE  (Loaded at runtime from encrypted JSON)
# ============================================================
@dataclass
class SubscriberProfile:
    """
    Represents one paying subscriber and their broker configuration.
    These are loaded from the encrypted subscribers.enc.json file.
    Sensitive fields (api_key, api_secret, etc.) are decrypted at runtime.
    NEVER store plain-text secrets in source code.
    """
    subscriber_id: str            # Unique ID e.g. "SUB_001"
    name: str                     # Subscriber full name
    broker: str                   # "zerodha" or "angel"
    active: bool                  # False = paused subscription

    # Zerodha fields (used if broker == "zerodha")
    zerodha_api_key: Optional[str]    = None
    zerodha_api_secret: Optional[str] = None
    zerodha_access_token: Optional[str] = None  # Refreshed daily

    # Angel One fields (used if broker == "angel")
    angel_api_key: Optional[str]     = None
    angel_client_id: Optional[str]   = None
    angel_password: Optional[str]    = None
    angel_totp_secret: Optional[str] = None

    # Risk overrides (per-subscriber, overrides global if set)
    max_lots: int                 = RiskConfig.DEFAULT_LOTS_PER_TRADE
    max_daily_drawdown_pct: float = RiskConfig.MAX_DAILY_DRAWDOWN_PCT
    circuit_breaker_active: bool  = False   # Set True when drawdown breached

    # Tracking
    telegram_chat_id: Optional[str] = None  # For individual P&L messages
    daily_pnl: float               = 0.0
    daily_trade_count: int         = 0
    consecutive_losses: int        = 0

    def to_safe_dict(self) -> dict:
        """Returns a dict with secrets masked — safe for logging."""
        return {
            "subscriber_id": self.subscriber_id,
            "name": self.name,
            "broker": self.broker,
            "active": self.active,
            "max_lots": self.max_lots,
            "circuit_breaker_active": self.circuit_breaker_active,
            "daily_pnl": self.daily_pnl,
            "daily_trade_count": self.daily_trade_count,
        }


# ============================================================
# LOGGING CONFIG  (used by loguru in each module)
# ============================================================
LOG_LEVEL       = "DEBUG" if not IS_PROD else "INFO"
LOG_ROTATION    = "1 day"
LOG_RETENTION   = "30 days"
LOG_FORMAT      = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)

# ============================================================
# BACKTEST CONFIGURATION
# ============================================================
class BacktestConfig:
    START_DATE         = "2022-04-01"
    END_DATE           = "2024-04-01"
    INITIAL_CAPITAL    = 100_000      # INR 1 lakh
    COMMISSION_PCT     = 0.0003       # ~0.03% per side (Zerodha options)
    SLIPPAGE_POINTS    = 0.5          # Assumed slippage in backtest
    DATA_TIMEFRAME     = "5min"
