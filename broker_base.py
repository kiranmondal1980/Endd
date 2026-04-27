"""
broker_base.py — Abstract Broker Interface
============================================
This defines the CONTRACT that every broker implementation must follow.
Think of it as a blueprint for a blueprint (meta!).

WHY AN ABSTRACT BASE CLASS?
  - It forces every broker (Zerodha, Angel, future brokers) to implement
    the SAME set of methods with the SAME signatures.
  - live_main.py can call broker.place_order() without caring whether
    it's talking to Zerodha or Angel One underneath.
  - Adding a new broker in the future = create a new file, inherit this class,
    implement the methods. Zero changes to live_main.py.

This is called the "Strategy Pattern" in software engineering.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, Any
from loguru import logger


# ============================================================
# DATA CLASSES — Shared order/position structures
# ============================================================

@dataclass
class OrderRequest:
    """Standardised order request that any broker can understand."""
    symbol: str              # e.g. "NIFTY24JUL22100CE"
    exchange: str            # "NFO" for Nifty options
    transaction_type: str    # "BUY" or "SELL"
    quantity: int            # Number of units (lot_size * num_lots)
    order_type: str          # "MARKET" or "LIMIT"
    price: float = 0.0       # Required only for LIMIT orders
    product: str = "MIS"     # "MIS" = intraday margin (squared off auto at EOD)
    variety: str = "regular" # "regular", "amo" (after market), "co" (cover)
    tag: str = ""            # Optional label for tracking e.g. "SCALPER_001"

    def __str__(self):
        return (f"[{self.transaction_type}] {self.symbol} | "
                f"Qty: {self.quantity} | Type: {self.order_type} | "
                f"Price: {self.price:.2f}")


@dataclass
class OrderResponse:
    """Standardised response returned after placing an order."""
    success: bool
    order_id: Optional[str]     # Broker-assigned order ID
    symbol: str
    transaction_type: str
    quantity: int
    fill_price: float           # Actual execution price (0 if pending)
    status: str                 # "COMPLETE", "PENDING", "REJECTED", "ERROR"
    message: str                # Human-readable status or error
    raw_response: Optional[Dict] = None  # Full broker response for debugging

    def __str__(self):
        return (f"Order [{self.order_id}] | {self.transaction_type} {self.symbol} | "
                f"Status: {self.status} | Fill: {self.fill_price:.2f} | {self.message}")


@dataclass
class Position:
    """Represents an open position in a subscriber's account."""
    symbol: str
    exchange: str
    quantity: int            # Positive = long, Negative = short
    average_price: float     # Average buy/sell price
    last_price: float        # Current market price
    unrealised_pnl: float    # Mark-to-market P&L
    product: str             # "MIS" etc.


@dataclass
class AccountInfo:
    """Basic account information for a subscriber."""
    client_id: str
    broker: str
    available_margin: float  # Cash available to trade
    used_margin: float
    total_balance: float
    is_connected: bool


# ============================================================
# ABSTRACT BASE BROKER CLASS
# ============================================================

class BrokerBase(ABC):
    """
    Every broker implementation MUST inherit from this class and
    implement ALL abstract methods. If a method is not implemented,
    Python will raise a TypeError when you try to instantiate the class.
    """

    def __init__(self, subscriber_id: str, name: str):
        self.subscriber_id = subscriber_id
        self.name = name
        self.is_logged_in = False
        self._logger = logger.bind(
            subscriber=subscriber_id,
            broker=self.__class__.__name__
        )

    # ----------------------------------------------------------
    # AUTHENTICATION
    # ----------------------------------------------------------

    @abstractmethod
    def login(self) -> bool:
        """
        Authenticate with the broker and establish a session.
        Returns True if login succeeded, False otherwise.
        Must set self.is_logged_in = True on success.
        """
        ...

    @abstractmethod
    def refresh_session(self) -> bool:
        """
        Refresh an expired session token without full re-login.
        Called at 08:00 AM daily before market open.
        Returns True if refresh succeeded.
        """
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        """
        Verify the current session is active and valid.
        Does NOT re-authenticate — just pings the broker API.
        Returns True if connection is live.
        """
        ...

    # ----------------------------------------------------------
    # MARKET DATA
    # ----------------------------------------------------------

    @abstractmethod
    def get_ltp(self, symbol: str, exchange: str) -> Optional[float]:
        """
        Get the Last Traded Price (LTP) for a symbol.
        Args:
            symbol:   e.g. "NIFTY24JUL22100CE"
            exchange: e.g. "NFO" or "NSE"
        Returns:
            float price, or None if the call fails.
        """
        ...

    @abstractmethod
    def get_option_chain_atm_strike(
        self, 
        index_ltp: float, 
        option_type: str, 
        expiry_date: str
    ) -> Optional[str]:
        """
        Given the current Nifty index LTP, find the ATM (At-The-Money)
        option symbol for the given type (CE/PE) and expiry.
        Args:
            index_ltp:   Current Nifty 50 spot price
            option_type: "CE" or "PE"
            expiry_date: Expiry date string e.g. "24JUL"
        Returns:
            Full option symbol string e.g. "NIFTY24JUL22100CE"
        """
        ...

    # ----------------------------------------------------------
    # ORDER MANAGEMENT
    # ----------------------------------------------------------

    @abstractmethod
    def place_order(self, order: OrderRequest) -> OrderResponse:
        """
        Place an order in the subscriber's account.
        This is the most critical method — implement with full error handling.
        """
        ...

    @abstractmethod
    def cancel_order(self, order_id: str, variety: str = "regular") -> bool:
        """
        Cancel a pending order by its broker-assigned order ID.
        Returns True if cancellation succeeded.
        """
        ...

    @abstractmethod
    def get_order_status(self, order_id: str) -> Optional[OrderResponse]:
        """
        Check the current status of a placed order.
        Returns OrderResponse with current status, or None on API failure.
        """
        ...

    # ----------------------------------------------------------
    # POSITIONS & PORTFOLIO
    # ----------------------------------------------------------

    @abstractmethod
    def get_positions(self) -> list[Position]:
        """
        Fetch all open intraday (MIS) positions for this account.
        Returns a list of Position objects (empty list if no positions).
        """
        ...

    @abstractmethod
    def get_account_info(self) -> Optional[AccountInfo]:
        """
        Fetch account balance and margin information.
        Used for position sizing and daily P&L calculation.
        """
        ...

    @abstractmethod
    def close_all_positions(self) -> Dict[str, bool]:
        """
        EMERGENCY METHOD: Close ALL open MIS positions immediately
        at market price. Called at 15:20 IST by live_main.py.
        Returns a dict: {symbol: True/False} indicating success per position.
        """
        ...

    # ----------------------------------------------------------
    # DAILY P&L
    # ----------------------------------------------------------

    @abstractmethod
    def get_todays_pnl(self) -> float:
        """
        Fetch today's realised + unrealised P&L for this account.
        Returns INR P&L as a float (positive = profit, negative = loss).
        """
        ...

    # ----------------------------------------------------------
    # CONCRETE UTILITY METHODS (Same for all brokers)
    # ----------------------------------------------------------

    def calculate_position_size(
        self,
        capital: float,
        option_premium: float,
        vix: float,
        max_capital_pct: float = 10.0
    ) -> int:
        """
        Calculate how many lots to buy based on:
          - Available capital
          - Option premium (price per unit)
          - Current VIX (adjusts lot count via multiplier)
          - Maximum capital per trade (default 10% of account)

        Returns number of lots (minimum 1, 0 if capital insufficient).
        """
        from config import RiskConfig, NIFTY_LOT_SIZE

        vix_multiplier = RiskConfig.get_lot_multiplier(vix)
        if vix_multiplier == 0:
            self._logger.warning(f"VIX {vix:.1f} ≥ halt threshold. Position size = 0.")
            return 0

        # Max capital allowed per trade
        max_spend = capital * (max_capital_pct / 100)
        cost_per_lot = option_premium * NIFTY_LOT_SIZE

        if cost_per_lot <= 0:
            self._logger.error("Option premium ≤ 0. Cannot calculate position size.")
            return 0

        raw_lots = int(max_spend / cost_per_lot)
        adjusted_lots = max(1, int(raw_lots * vix_multiplier))

        self._logger.debug(
            f"Position sizing | Capital: {capital:.0f} | Premium: {option_premium:.2f} | "
            f"VIX: {vix:.1f} (×{vix_multiplier}) | Lots: {raw_lots} → {adjusted_lots}"
        )
        return adjusted_lots

    def log_order(self, request: OrderRequest, response: OrderResponse):
        """Structured log of every order attempt — success or failure."""
        level = "INFO" if response.success else "ERROR"
        self._logger.log(
            level,
            f"ORDER | {response} | Request was: {request}"
        )

    def __repr__(self):
        return (f"{self.__class__.__name__}("
                f"subscriber='{self.subscriber_id}', "
                f"name='{self.name}', "
                f"logged_in={self.is_logged_in})")
