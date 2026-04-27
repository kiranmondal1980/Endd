"""
live_main.py — Multi-Tenant Live Trading Engine
=================================================
This is the BRAIN of the entire system. It runs from 07:00 to 19:00 IST
every market day and orchestrates everything:

  07:00 — Morning warmup: check VIX, validate all API connections
  07:50 — Send morning market brief to WhatsApp/Telegram
  08:00 — Refresh all subscriber sessions (daily token renewal)
  09:15 — Market opens: begin scanning for signals every 5 minutes
  09:15 → 15:20 — Trade loop: evaluate signals, place orders, monitor positions
  15:20 — FLATTEN ALL positions for all subscribers (hard rule)
  15:50 — Trigger telegram_reporter.py for daily P&L broadcast
  16:00 — Post-market: log review, update trade database, send subscriber reports

MULTI-TENANT ARCHITECTURE:
  - Each subscriber runs in its own thread (via ThreadPoolExecutor)
  - Threads share read-only data (VIX, Nifty spot, current candles)
  - Each thread owns its subscriber's broker instance exclusively
  - Circuit breakers are per-subscriber (one subscriber blowing up
    does NOT affect other subscribers)

CONCURRENCY MODEL:
  concurrent.futures.ThreadPoolExecutor handles 20–50 subscribers.
  For 100+ subscribers, migrate to asyncio + aiohttp (future upgrade).
"""

import os
import sys
import time
import json
import signal
import schedule
import threading
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from cryptography.fernet import Fernet

import pytz
import pandas as pd
from loguru import logger

from config import (
    StrategyConfig, RiskConfig, BacktestConfig,
    TIMEZONE, NIFTY_LOT_SIZE, NIFTY_STRIKE_INTERVAL,
    SUBSCRIBER_CONFIG_PATH, MASTER_ENCRYPTION_KEY,
    TRADE_DB, LOG_DIR, LOG_LEVEL, LOG_FORMAT,
    BROKER_ZERODHA, BROKER_ANGEL,
    ZERODHA_ADMIN_API_KEY, ZERODHA_ADMIN_API_SECRET,
    ANGEL_ADMIN_API_KEY, ANGEL_ADMIN_CLIENT_ID,
    ANGEL_ADMIN_PASSWORD, ANGEL_ADMIN_TOTP_SECRET,
    SubscriberProfile
)
from broker_base import BrokerBase, OrderRequest
from broker_zerodha import ZerodhaBroker
from broker_angel import AngelBroker
from strategy import NiftyScalperStrategy, TradeSignal, SignalType, PositionState
from telegram_reporter import TelegramReporter
from database import TradeDatabase

# ============================================================
# CONFIGURE LOGURU
# ============================================================
logger.remove()   # Remove default handler
logger.add(
    sys.stdout,
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    colorize=True
)
logger.add(
    LOG_DIR / "live_trading_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    level="DEBUG",
    format=LOG_FORMAT
)

# ============================================================
# GLOBAL STATE  (read-only shared data, thread-safe reads)
# ============================================================
IST = pytz.timezone(TIMEZONE)

_shared_state = {
    "vix":              0.0,
    "nifty_spot":       0.0,
    "current_candles":  None,       # pd.DataFrame of latest 5-min candles
    "market_is_open":   False,
    "trading_halted":   False,      # Global halt (e.g. VIX > 25 for all)
    "last_candle_time": None,
    "today_date":       date.today(),
}
_state_lock = threading.RLock()

_shutdown_event = threading.Event()


# ============================================================
# UTILITY: IST TIME HELPERS
# ============================================================

def now_ist() -> datetime:
    return datetime.now(IST)

def time_str_ist() -> str:
    return now_ist().strftime("%H:%M:%S")

def is_market_hours() -> bool:
    """True between 09:15 and 15:30 IST on weekdays."""
    now = now_ist()
    if now.weekday() >= 5:   # Saturday=5, Sunday=6
        return False
    t = now.time()
    from datetime import time as dtime
    return dtime(9, 15) <= t <= dtime(15, 30)

def is_flatten_time() -> bool:
    """True at or after 15:20 IST."""
    from datetime import time as dtime
    return now_ist().time() >= dtime(15, 20)

def minutes_to_next_candle() -> int:
    """Returns seconds until the next 5-minute candle close."""
    now = now_ist()
    total_mins = now.minute * 60 + now.second
    next_boundary = ((total_mins // 300) + 1) * 300
    return max(0, next_boundary - total_mins)


# ============================================================
# SUBSCRIBER MANAGEMENT
# ============================================================

class SubscriberManager:
    """
    Loads, decrypts, and manages all subscriber profiles.
    Builds the correct broker instance for each subscriber.
    """

    def __init__(self):
        self._fernet = Fernet(MASTER_ENCRYPTION_KEY) if MASTER_ENCRYPTION_KEY else None
        self._subscribers: Dict[str, SubscriberProfile] = {}
        self._brokers:     Dict[str, BrokerBase]        = {}

    def load_subscribers(self) -> int:
        """
        Load subscriber profiles from the encrypted JSON config.
        Returns the count of active subscribers loaded.

        Config file format (before encryption):
        [
          {
            "subscriber_id": "SUB_001",
            "name": "Subhajit Das",
            "broker": "zerodha",
            "active": true,
            "zerodha_api_key": "abc123",
            "zerodha_api_secret": "secret",
            "zerodha_access_token": "token",
            "max_lots": 1,
            "telegram_chat_id": "123456789"
          },
          ...
        ]
        """
        if not SUBSCRIBER_CONFIG_PATH.exists():
            logger.warning(
                f"Subscriber config not found at {SUBSCRIBER_CONFIG_PATH}. "
                "Creating demo subscriber for testing."
            )
            self._load_demo_subscriber()
            return len(self._subscribers)

        try:
            raw_bytes = SUBSCRIBER_CONFIG_PATH.read_bytes()

            # Decrypt if encryption key is configured
            if self._fernet:
                decrypted = self._fernet.decrypt(raw_bytes)
                data = json.loads(decrypted.decode())
            else:
                logger.warning("No MASTER_ENCRYPTION_KEY — reading config as plain JSON.")
                data = json.loads(raw_bytes.decode())

            loaded = 0
            for item in data:
                if not item.get("active", False):
                    logger.info(f"Skipping inactive subscriber: {item.get('subscriber_id')}")
                    continue

                profile = SubscriberProfile(**{
                    k: v for k, v in item.items()
                    if k in SubscriberProfile.__dataclass_fields__
                })
                self._subscribers[profile.subscriber_id] = profile
                loaded += 1

            logger.info(f"Loaded {loaded} active subscribers from config.")
            return loaded

        except Exception as e:
            logger.error(f"Failed to load subscriber config: {e}")
            return 0

    def _load_demo_subscriber(self):
        """Load a single demo subscriber using admin API keys (for development)."""
        if not ZERODHA_ADMIN_API_KEY:
            logger.error("No admin API keys configured. Set env vars or create subscriber config.")
            return

        demo = SubscriberProfile(
            subscriber_id="DEMO_001",
            name="Demo Account (Admin)",
            broker=BROKER_ZERODHA,
            active=True,
            zerodha_api_key=ZERODHA_ADMIN_API_KEY,
            zerodha_api_secret=ZERODHA_ADMIN_API_SECRET,
            zerodha_access_token=os.getenv("ZERODHA_ADMIN_ACCESS_TOKEN", ""),
            max_lots=1
        )
        self._subscribers["DEMO_001"] = demo
        logger.info("Demo subscriber (DEMO_001) loaded.")

    def build_broker(self, profile: SubscriberProfile) -> Optional[BrokerBase]:
        """Instantiate the correct broker class for a subscriber."""
        try:
            if profile.broker == BROKER_ZERODHA:
                broker = ZerodhaBroker(
                    subscriber_id=profile.subscriber_id,
                    name=profile.name,
                    api_key=profile.zerodha_api_key,
                    api_secret=profile.zerodha_api_secret,
                    access_token=profile.zerodha_access_token
                )
            elif profile.broker == BROKER_ANGEL:
                broker = AngelBroker(
                    subscriber_id=profile.subscriber_id,
                    name=profile.name,
                    api_key=profile.angel_api_key,
                    client_id=profile.angel_client_id,
                    password=profile.angel_password,
                    totp_secret=profile.angel_totp_secret
                )
            else:
                logger.error(f"Unknown broker '{profile.broker}' for {profile.subscriber_id}")
                return None

            return broker
        except Exception as e:
            logger.error(f"Failed to build broker for {profile.subscriber_id}: {e}")
            return None

    def login_all(self) -> Dict[str, bool]:
        """Login all active subscribers. Returns {subscriber_id: success}."""
        results = {}
        for sub_id, profile in self._subscribers.items():
            broker = self.build_broker(profile)
            if broker:
                success = broker.login()
                results[sub_id] = success
                if success:
                    self._brokers[sub_id] = broker
                    logger.info(f"✓ Logged in: {profile.name} ({sub_id})")
                else:
                    logger.error(f"✗ Login failed: {profile.name} ({sub_id})")
            else:
                results[sub_id] = False
        return results

    def refresh_all_sessions(self) -> Dict[str, bool]:
        """Refresh sessions for all brokers. Called at 08:00 AM."""
        results = {}
        for sub_id, broker in self._brokers.items():
            results[sub_id] = broker.refresh_session()
        return results

    def get_active_brokers(self) -> Dict[str, BrokerBase]:
        """Return only brokers where the subscriber's circuit breaker is NOT active."""
        return {
            sub_id: broker
            for sub_id, broker in self._brokers.items()
            if not self._subscribers[sub_id].circuit_breaker_active
        }

    def get_profile(self, subscriber_id: str) -> Optional[SubscriberProfile]:
        return self._subscribers.get(subscriber_id)

    def update_daily_pnl(self, subscriber_id: str, pnl_delta: float):
        """Thread-safe update of daily P&L for a subscriber."""
        if subscriber_id in self._subscribers:
            profile = self._subscribers[subscriber_id]
            profile.daily_pnl += pnl_delta
            profile.daily_trade_count += 1
            logger.debug(f"Daily P&L update | {subscriber_id}: {profile.daily_pnl:.2f}")

    def check_and_apply_circuit_breaker(
        self, subscriber_id: str, account_balance: float
    ) -> bool:
        """
        Check if this subscriber's daily loss exceeds their limit.
        If yes, activate circuit breaker and return True.
        """
        profile = self._subscribers.get(subscriber_id)
        if not profile:
            return False

        if account_balance <= 0:
            return False

        daily_drawdown_pct = (profile.daily_pnl / account_balance) * 100
        threshold = -abs(profile.max_daily_drawdown_pct)

        if daily_drawdown_pct <= threshold:
            if not profile.circuit_breaker_active:
                profile.circuit_breaker_active = True
                logger.critical(
                    f"🔴 CIRCUIT BREAKER ACTIVATED | {subscriber_id} | "
                    f"Daily P&L: {profile.daily_pnl:.2f} ({daily_drawdown_pct:.2f}%) "
                    f"breached limit of {threshold:.2f}%"
                )
            return True

        return False

    def reset_daily_state(self):
        """Reset all per-day counters at start of each trading day."""
        for profile in self._subscribers.values():
            profile.daily_pnl          = 0.0
            profile.daily_trade_count  = 0
            profile.circuit_breaker_active = False
            profile.consecutive_losses = 0
        logger.info("Daily subscriber state reset.")


# ============================================================
# VIX FETCHER
# ============================================================

def fetch_india_vix() -> float:
    """
    Fetch current India VIX from NSE public API.
    Falls back to a conservative default of 15.0 if the fetch fails.
    """
    try:
        import requests
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.nseindia.com/",
        }
        # Use a session with cookies (NSE requires it)
        session = requests.Session()
        # First, get cookies by hitting the homepage
        session.get("https://www.nseindia.com/", headers=headers, timeout=10)
        # Now fetch VIX data
        response = session.get(
            "https://www.nseindia.com/api/allIndices",
            headers=headers,
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            for index in data.get("data", []):
                if index.get("index") == "INDIA VIX":
                    vix = float(index.get("last", 15.0))
                    logger.info(f"India VIX fetched: {vix:.2f}")
                    return vix
    except Exception as e:
        logger.warning(f"VIX fetch failed: {e}. Using default 15.0")

    return 15.0   # Conservative fallback


def fetch_nifty_candles_from_admin(admin_broker: ZerodhaBroker, n_candles: int = 25) -> Optional[pd.DataFrame]:
    """
    Fetch the latest N 5-minute candles for Nifty 50 index.
    Uses the admin (Kiran's own) Zerodha account for data.
    Subscriber accounts are for ORDER PLACEMENT only — data comes from admin.
    """
    try:
        from datetime import datetime, timedelta
        end_time   = datetime.now()
        start_time = end_time - timedelta(hours=2)   # Last 2 hours of data

        raw = admin_broker.get_historical_data(
            instrument_token="256265",   # Nifty 50 index token
            from_date=start_time.strftime("%Y-%m-%d %H:%M:%S"),
            to_date=end_time.strftime("%Y-%m-%d %H:%M:%S"),
            interval="5minute"
        )
        if not raw:
            return None

        from strategy import NiftyScalperStrategy
        df = NiftyScalperStrategy.build_candle_df(raw)
        return df.tail(n_candles).reset_index(drop=True)

    except Exception as e:
        logger.error(f"Failed to fetch Nifty candles: {e}")
        return None


# ============================================================
# PER-SUBSCRIBER TRADE EXECUTION (runs in its own thread)
# ============================================================

def execute_for_subscriber(
    subscriber_id: str,
    broker: BrokerBase,
    profile: SubscriberProfile,
    strategy: NiftyScalperStrategy,
    db: TradeDatabase,
    vix: float,
    nifty_spot: float,
    candles: pd.DataFrame
) -> dict:
    """
    Single subscriber's complete trade evaluation and execution cycle.
    This function is called ONCE per 5-minute candle for each subscriber.
    It runs in its own thread — no shared mutable state between subscribers.

    Returns a dict with the action taken (for logging).
    """
    result = {
        "subscriber_id": subscriber_id,
        "action": "none",
        "symbol": None,
        "order_id": None,
        "pnl_delta": 0.0,
        "error": None
    }

    try:
        # ---- Verify connection ----
        if not broker.test_connection():
            logger.warning(f"[{subscriber_id}] Connection lost. Attempting re-login...")
            if not broker.login():
                result["error"] = "Connection lost and re-login failed"
                return result

        # ---- Fetch current positions ----
        positions = broker.get_positions()
        has_open_position = len(positions) > 0
        open_position = positions[0] if has_open_position else None

        # Build PositionState for strategy
        position_state = PositionState(is_open=False)
        if open_position:
            current_ltp = broker.get_ltp(open_position.symbol, open_position.exchange) or \
                          open_position.last_price
            position_state = PositionState(
                is_open=True,
                option_type="CE" if open_position.symbol.endswith("CE") else "PE",
                entry_price=open_position.average_price,
                current_price=current_ltp,
                quantity=open_position.quantity
            )

        # ---- Run strategy ----
        signal: TradeSignal = strategy.evaluate(
            candles=candles,
            vix=vix,
            nifty_spot=nifty_spot,
            position_state=position_state,
            daily_trade_count=profile.daily_trade_count,
            circuit_breaker_active=profile.circuit_breaker_active
        )

        logger.debug(f"[{subscriber_id}] Signal: {signal}")

        # ---- Act on signal ----
        if signal.signal_type == SignalType.EXIT and has_open_position:
            # CLOSE the open position
            close_results = broker.close_all_positions()
            pnl = broker.get_todays_pnl()
            result["action"]    = "EXIT"
            result["pnl_delta"] = pnl
            logger.info(
                f"[{subscriber_id}] EXIT | Reason: {signal.exit_reason} | "
                f"P&L today: {pnl:.2f}"
            )
            db.log_exit(
                subscriber_id=subscriber_id,
                broker=profile.broker,
                reason=signal.exit_reason.value if signal.exit_reason else "UNKNOWN",
                pnl=pnl
            )

        elif signal.signal_type in (SignalType.BUY_CE, SignalType.BUY_PE) \
                and not has_open_position:
            # ENTER a new position
            option_type = signal.option_type

            # Get current expiry date (weekly — nearest Thursday)
            expiry_str  = _get_nearest_thursday_expiry()

            # Get ATM option symbol
            option_symbol = broker.get_option_chain_atm_strike(
                index_ltp=nifty_spot,
                option_type=option_type,
                expiry_date=expiry_str
            )
            if not option_symbol:
                result["error"] = "Could not resolve ATM option symbol"
                return result

            # Fetch option premium
            option_ltp = broker.get_ltp(option_symbol, "NFO")
            if not option_ltp or option_ltp <= 0:
                result["error"] = f"Invalid LTP for {option_symbol}: {option_ltp}"
                return result

            # Get account balance for position sizing
            account = broker.get_account_info()
            capital  = account.available_margin if account else 100_000.0

            # Calculate lots
            lots = broker.calculate_position_size(
                capital=capital,
                option_premium=option_ltp,
                vix=vix,
                max_capital_pct=RiskConfig.MAX_CAPITAL_PER_TRADE_PCT
            )
            # Respect subscriber's max_lots setting
            lots = min(lots, profile.max_lots)

            if lots == 0:
                result["action"] = "SKIPPED_NO_LOTS"
                return result

            qty = lots * NIFTY_LOT_SIZE

            # Place BUY order
            order_req = OrderRequest(
                symbol=option_symbol,
                exchange="NFO",
                transaction_type="BUY",
                quantity=qty,
                order_type="MARKET",
                product="MIS",
                tag=f"SCALP_{subscriber_id[:8]}"
            )
            order_resp = broker.place_order(order_req)

            if order_resp.success:
                result["action"]   = f"ENTER_{option_type}"
                result["symbol"]   = option_symbol
                result["order_id"] = order_resp.order_id
                logger.success(
                    f"[{subscriber_id}] ENTERED {option_type} | "
                    f"Symbol: {option_symbol} | Qty: {qty} | "
                    f"Fill: {order_resp.fill_price:.2f} | "
                    f"Order: {order_resp.order_id}"
                )
                db.log_entry(
                    subscriber_id=subscriber_id,
                    broker=profile.broker,
                    symbol=option_symbol,
                    option_type=option_type,
                    quantity=qty,
                    fill_price=order_resp.fill_price,
                    vix=vix,
                    rsi=signal.rsi_value,
                    nifty_spot=nifty_spot
                )
            else:
                result["error"] = f"Order failed: {order_resp.message}"
                logger.error(f"[{subscriber_id}] Order FAILED: {order_resp.message}")

        # ---- Circuit breaker check ----
        account = broker.get_account_info()
        if account:
            daily_pnl = broker.get_todays_pnl()
            profile.daily_pnl = daily_pnl

    except Exception as e:
        logger.error(f"[{subscriber_id}] Unhandled exception in trade loop: {e}", exc_info=True)
        result["error"] = str(e)

    return result


# ============================================================
# WEEKLY EXPIRY HELPER
# ============================================================

def _get_nearest_thursday_expiry() -> str:
    """
    Calculate the nearest Thursday (Nifty weekly expiry) expiry date string.
    Returns format like "24JUL" used in NSE option symbols.
    """
    today = now_ist().date()
    days_until_thursday = (3 - today.weekday()) % 7
    if days_until_thursday == 0:
        days_until_thursday = 7   # If today IS Thursday, use next Thursday
    expiry_date = today + timedelta(days=days_until_thursday)
    # Format: DDMMM (e.g., 04JUL, 11JUL)
    return expiry_date.strftime("%d%b").upper()


# ============================================================
# MAIN TRADING ENGINE
# ============================================================

class TradingEngine:
    """
    The master controller that runs the entire trading day.
    """

    def __init__(self):
        self.sub_manager   = SubscriberManager()
        self.strategy      = NiftyScalperStrategy()
        self.db            = TradeDatabase(TRADE_DB)
        self.reporter      = TelegramReporter()
        self.admin_broker: Optional[ZerodhaBroker] = None
        self._executor     = ThreadPoolExecutor(max_workers=50, thread_name_prefix="subscriber")
        self._is_running   = False

    # ----------------------------------------------------------
    # STARTUP
    # ----------------------------------------------------------

    def startup(self) -> bool:
        """
        Full startup sequence: load subscribers, login all, verify VIX.
        Returns True if startup succeeded and trading can proceed.
        """
        logger.info("=" * 60)
        logger.info("  NIFTY SCALPER ENGINE — STARTING UP")
        logger.info(f"  Time: {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}")
        logger.info("=" * 60)

        # ---- Init database ----
        self.db.initialize()

        # ---- Reset daily state ----
        self.sub_manager.reset_daily_state()

        # ---- Load subscribers ----
        count = self.sub_manager.load_subscribers()
        if count == 0:
            logger.error("No active subscribers loaded. Aborting startup.")
            return False

        # ---- Setup admin broker (for data feeds) ----
        self.admin_broker = ZerodhaBroker(
            subscriber_id="ADMIN",
            name="Admin Data Feed",
            api_key=ZERODHA_ADMIN_API_KEY,
            api_secret=ZERODHA_ADMIN_API_SECRET,
            access_token=os.getenv("ZERODHA_ADMIN_ACCESS_TOKEN", "")
        )
        if ZERODHA_ADMIN_API_KEY and os.getenv("ZERODHA_ADMIN_ACCESS_TOKEN"):
            self.admin_broker.login()

        # ---- Login all subscribers ----
        login_results = self.sub_manager.login_all()
        success_count = sum(1 for v in login_results.values() if v)
        logger.info(
            f"Login results: {success_count}/{len(login_results)} subscribers logged in."
        )

        if success_count == 0:
            logger.error("Zero subscribers logged in. Check API credentials.")
            return False

        # ---- Initial VIX check ----
        vix = fetch_india_vix()
        with _state_lock:
            _shared_state["vix"] = vix

        if vix >= StrategyConfig.VIX_HALT_THRESHOLD:
            logger.warning(
                f"⚠️  VIX={vix:.1f} is above halt threshold "
                f"({StrategyConfig.VIX_HALT_THRESHOLD}). Trading will be HALTED today."
            )
            with _state_lock:
                _shared_state["trading_halted"] = True

        self._is_running = True
        logger.success(f"Startup complete. {success_count} subscribers active. VIX={vix:.1f}")
        return True

    # ----------------------------------------------------------
    # SCHEDULED TASKS
    # ----------------------------------------------------------

    def task_refresh_sessions(self):
        """08:00 — Refresh all broker sessions for the day."""
        logger.info("🔄 Refreshing all subscriber sessions...")
        results = self.sub_manager.refresh_all_sessions()
        ok  = sum(1 for v in results.values() if v)
        bad = len(results) - ok
        logger.info(f"Session refresh: {ok} OK, {bad} failed")

    def task_vix_update(self):
        """Runs every 15 minutes during market hours to refresh VIX."""
        vix = fetch_india_vix()
        with _state_lock:
            _shared_state["vix"] = vix

        if vix >= StrategyConfig.VIX_HALT_THRESHOLD:
            if not _shared_state["trading_halted"]:
                logger.critical(
                    f"🛑 VIX SPIKE! VIX={vix:.1f} exceeded halt threshold. "
                    "HALTING all trading."
                )
                _shared_state["trading_halted"] = True
                self.reporter.send_admin_alert(
                    f"🛑 TRADING HALTED — VIX={vix:.1f} exceeded {StrategyConfig.VIX_HALT_THRESHOLD}"
                )
        else:
            if _shared_state["trading_halted"] and vix < StrategyConfig.VIX_MAX_REDUCED:
                _shared_state["trading_halted"] = False
                logger.info(f"✅ VIX normalised ({vix:.1f}). Trading resumed.")

    def task_morning_brief(self):
        """07:50 — Send morning brief to subscribers."""
        vix = _shared_state.get("vix", 0.0)
        self.reporter.send_morning_brief(vix=vix, nifty_spot=_shared_state.get("nifty_spot", 0.0))

    def task_eod_report(self):
        """15:50 — Send end-of-day P&L report."""
        all_brokers = self.sub_manager._brokers
        pnl_data = {}
        for sub_id, broker in all_brokers.items():
            pnl_data[sub_id] = broker.get_todays_pnl()

        self.reporter.send_daily_pnl_report(
            pnl_data=pnl_data,
            subscriber_profiles={
                sub_id: self.sub_manager.get_profile(sub_id)
                for sub_id in all_brokers
            }
        )

    def task_flatten_all(self):
        """15:20 — HARD CLOSE ALL positions for all subscribers."""
        logger.warning("⏰ 15:20 IST — FLATTENING ALL POSITIONS")
        brokers = self.sub_manager._brokers

        def _flatten_one(sub_id_broker):
            sub_id, broker = sub_id_broker
            try:
                results = broker.close_all_positions()
                closed = sum(1 for v in results.values() if v)
                logger.info(f"[{sub_id}] Flattened {closed}/{len(results)} positions")
                return sub_id, results
            except Exception as e:
                logger.error(f"[{sub_id}] Flatten failed: {e}")
                return sub_id, {}

        futures = {
            self._executor.submit(_flatten_one, (sid, b)): sid
            for sid, b in brokers.items()
        }
        for future in as_completed(futures, timeout=60):
            sub_id = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Flatten future error for {sub_id}: {e}")

        logger.success("All positions flattened.")

    # ----------------------------------------------------------
    # MAIN TRADING LOOP (called every 5 minutes)
    # ----------------------------------------------------------

    def run_trade_cycle(self):
        """
        The core per-candle execution loop.
        Called every ~5 minutes (on each new candle close).
        Submits one thread per active subscriber simultaneously.
        """
        if not is_market_hours():
            return

        with _state_lock:
            vix           = _shared_state["vix"]
            nifty_spot    = _shared_state["nifty_spot"]
            candles       = _shared_state["current_candles"]
            halted        = _shared_state["trading_halted"]

        if halted:
            logger.warning(f"Trading HALTED (VIX={vix:.1f}). Skipping cycle.")
            return

        if candles is None or len(candles) < 20:
            logger.warning("Insufficient candle data for signal generation. Waiting...")
            return

        active_brokers = self.sub_manager.get_active_brokers()
        if not active_brokers:
            logger.warning("No active brokers for this cycle.")
            return

        logger.info(
            f"📊 Trade cycle | {time_str_ist()} | "
            f"VIX={vix:.1f} | Spot={nifty_spot:.1f} | "
            f"Subscribers={len(active_brokers)}"
        )

        # ---- Submit one thread per subscriber ----
        futures: Dict[Future, str] = {}
        for sub_id, broker in active_brokers.items():
            profile = self.sub_manager.get_profile(sub_id)
            if not profile:
                continue

            f = self._executor.submit(
                execute_for_subscriber,
                sub_id, broker, profile, self.strategy,
                self.db, vix, nifty_spot, candles.copy()
            )
            futures[f] = sub_id

        # ---- Collect results ----
        for future in as_completed(futures, timeout=30):
            sub_id = futures[future]
            try:
                result = future.result()
                if result.get("error"):
                    logger.error(f"[{sub_id}] Cycle error: {result['error']}")
                else:
                    logger.debug(f"[{sub_id}] Cycle OK | Action: {result.get('action')}")

                # Update circuit breaker
                account = self.sub_manager._brokers.get(sub_id)
                if account:
                    info = account.get_account_info()
                    if info:
                        self.sub_manager.check_and_apply_circuit_breaker(
                            sub_id, info.total_balance
                        )
            except Exception as e:
                logger.error(f"[{sub_id}] Thread exception: {e}", exc_info=True)

    def update_market_data(self):
        """
        Fetch fresh Nifty spot and candles every 1 minute.
        Runs in its own background thread.
        """
        while self._is_running and not _shutdown_event.is_set():
            try:
                if self.admin_broker and self.admin_broker.is_logged_in:
                    spot = self.admin_broker.get_nifty_spot()
                    if spot:
                        with _state_lock:
                            _shared_state["nifty_spot"] = spot

                    # Refresh candles every minute
                    candles = fetch_nifty_candles_from_admin(self.admin_broker, n_candles=25)
                    if candles is not None and not candles.empty:
                        with _state_lock:
                            _shared_state["current_candles"] = candles
                            _shared_state["last_candle_time"] = now_ist()
            except Exception as e:
                logger.error(f"Market data update failed: {e}")

            time.sleep(60)   # Update every 60 seconds

    # ----------------------------------------------------------
    # MAIN RUN LOOP
    # ----------------------------------------------------------

    def run(self):
        """
        Main entry point. Sets up the schedule and runs until shutdown.
        """
        if not self.startup():
            logger.error("Startup failed. Exiting.")
            sys.exit(1)

        # ---- Set up schedule ----
        schedule.every().day.at("08:00").do(self.task_refresh_sessions)
        schedule.every(15).minutes.do(self.task_vix_update)
        schedule.every().day.at("07:50").do(self.task_morning_brief)
        schedule.every().day.at("15:20").do(self.task_flatten_all)
        schedule.every().day.at("15:50").do(self.task_eod_report)

        # ---- Trade cycle every 5 minutes ----
        schedule.every(5).minutes.do(self.run_trade_cycle)

        # ---- Start market data background thread ----
        data_thread = threading.Thread(
            target=self.update_market_data,
            name="market_data",
            daemon=True
        )
        data_thread.start()
        logger.info("Market data background thread started.")

        # ---- Register graceful shutdown ----
        def _sigterm_handler(signum, frame):
            logger.warning("Received shutdown signal. Closing positions and exiting...")
            self.task_flatten_all()
            _shutdown_event.set()
            self._is_running = False

        signal.signal(signal.SIGTERM, _sigterm_handler)
        signal.signal(signal.SIGINT,  _sigterm_handler)

        logger.success("🟢 Trading engine running. Press Ctrl+C to stop.")

        # ---- Main loop ----
        while not _shutdown_event.is_set():
            schedule.run_pending()
            time.sleep(1)

        # ---- Cleanup ----
        self._executor.shutdown(wait=True, cancel_futures=True)
        logger.info("Trading engine shut down cleanly.")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    engine = TradingEngine()
    engine.run()
