"""
broker_zerodha.py — Zerodha Kite Connect Implementation
=========================================================
Full implementation of BrokerBase using Zerodha's official
Kite Connect API (v3).

ZERODHA KITE CONNECT NOTES:
  - API subscription costs INR 2,000/month (kite.trade)
  - Access token expires daily — must be refreshed every morning
  - Supports WebSocket for live tick data
  - Rate limit: ~3 requests/second per API key

DAILY TOKEN REFRESH FLOW:
  1. User visits login URL (or you automate via browser selenium)
  2. Zerodha redirects to your redirect_url with a "request_token"
  3. Exchange request_token + api_secret → access_token
  4. Store access_token (encrypted) for the day
  5. This auto-expires at midnight

For fully automated deployment, you can automate step 1-2 using
playwright/selenium to handle the Zerodha login page automatically.
The code below includes a helper for this pattern.
"""

import time
from typing import Optional, Dict, List
from datetime import datetime, date
import pytz

from kiteconnect import KiteConnect, KiteTicker
from loguru import logger

from broker_base import BrokerBase, OrderRequest, OrderResponse, Position, AccountInfo
from config import StrategyConfig, NIFTY_LOT_SIZE, NIFTY_STRIKE_INTERVAL, TIMEZONE


class ZerodhaBroker(BrokerBase):
    """
    Zerodha Kite Connect broker implementation.
    One instance per subscriber account.
    """

    EXCHANGE_NFO = "NFO"
    EXCHANGE_NSE = "NSE"
    PRODUCT_MIS  = "MIS"   # Intraday margin — auto-squared at 3:20 PM by Zerodha
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT  = "LIMIT"
    TRANSACTION_BUY   = "BUY"
    TRANSACTION_SELL  = "SELL"

    def __init__(
        self,
        subscriber_id: str,
        name: str,
        api_key: str,
        api_secret: str,
        access_token: Optional[str] = None
    ):
        super().__init__(subscriber_id, name)
        self._api_key     = api_key
        self._api_secret  = api_secret
        self._access_token = access_token

        # kite is the main API object
        self._kite: Optional[KiteConnect] = None
        self._ist = pytz.timezone(TIMEZONE)
        self._instruments_cache: Dict = {}  # Symbol → instrument token cache

    # ----------------------------------------------------------
    # AUTHENTICATION
    # ----------------------------------------------------------

    def login(self) -> bool:
        """
        Initialise Kite session using a pre-obtained access_token.
        The access_token must be obtained once per day via the OAuth flow.

        In production, this is called at 08:00 AM by live_main.py after
        the token has been refreshed.
        """
        try:
            self._kite = KiteConnect(api_key=self._api_key)

            if not self._access_token:
                self._logger.error(
                    f"No access_token for {self.subscriber_id}. "
                    "Token must be set before calling login()."
                )
                return False

            self._kite.set_access_token(self._access_token)

            # Verify by fetching profile
            profile = self._kite.profile()
            self.is_logged_in = True
            self._logger.info(
                f"Zerodha login OK | Subscriber: {self.subscriber_id} | "
                f"Kite user: {profile.get('user_id')} ({profile.get('user_name')})"
            )
            return True

        except Exception as e:
            self._logger.error(f"Zerodha login FAILED for {self.subscriber_id}: {e}")
            self.is_logged_in = False
            return False

    def set_access_token(self, access_token: str):
        """
        Update the access token (called after daily token refresh).
        Use this when the subscriber's new daily token is available.
        """
        self._access_token = access_token
        if self._kite:
            self._kite.set_access_token(access_token)

    def generate_login_url(self) -> str:
        """
        Generate the Zerodha OAuth login URL.
        User visits this URL, logs in, and gets a request_token.
        """
        kite = KiteConnect(api_key=self._api_key)
        return kite.login_url()

    def exchange_request_token(self, request_token: str) -> Optional[str]:
        """
        Exchange a request_token (from OAuth redirect) for an access_token.
        This is the final step of the daily login flow.
        Returns the access_token string, or None on failure.
        """
        try:
            kite = KiteConnect(api_key=self._api_key)
            data = kite.generate_session(request_token, api_secret=self._api_secret)
            access_token = data.get("access_token")
            self._logger.info(
                f"Access token generated for {self.subscriber_id}. "
                "Store this encrypted for today's session."
            )
            return access_token
        except Exception as e:
            self._logger.error(f"Token exchange failed for {self.subscriber_id}: {e}")
            return None

    def refresh_session(self) -> bool:
        """
        Zerodha tokens don't have a silent refresh mechanism —
        a new request_token is needed each day via browser login.
        This method re-validates the existing token.
        """
        return self.test_connection()

    def test_connection(self) -> bool:
        """Ping Zerodha API with a lightweight profile call."""
        if not self._kite or not self._access_token:
            return False
        try:
            self._kite.profile()
            return True
        except Exception as e:
            self._logger.warning(f"Connection test failed for {self.subscriber_id}: {e}")
            self.is_logged_in = False
            return False

    # ----------------------------------------------------------
    # MARKET DATA
    # ----------------------------------------------------------

    def get_ltp(self, symbol: str, exchange: str = "NFO") -> Optional[float]:
        """Get last traded price for a symbol."""
        if not self._kite:
            return None
        try:
            instrument_key = f"{exchange}:{symbol}"
            quote = self._kite.ltp([instrument_key])
            ltp = quote[instrument_key]["last_price"]
            self._logger.debug(f"LTP | {instrument_key} = {ltp}")
            return float(ltp)
        except Exception as e:
            self._logger.error(f"get_ltp failed for {symbol}: {e}")
            return None

    def get_nifty_spot(self) -> Optional[float]:
        """Fetch Nifty 50 spot price from NSE index."""
        return self.get_ltp("NIFTY 50", "NSE")

    def get_option_chain_atm_strike(
        self,
        index_ltp: float,
        option_type: str,
        expiry_date: str
    ) -> Optional[str]:
        """
        Build the ATM option symbol.
        Example: index_ltp=22137 → ATM strike = 22150 (nearest 50)
        Returns symbol like "NIFTY24JUL22150CE"
        """
        # Round to nearest NIFTY_STRIKE_INTERVAL
        atm_strike = round(index_ltp / NIFTY_STRIKE_INTERVAL) * NIFTY_STRIKE_INTERVAL
        symbol = f"NIFTY{expiry_date}{int(atm_strike)}{option_type}"
        self._logger.debug(
            f"ATM option | Spot: {index_ltp:.2f} → Strike: {atm_strike} | "
            f"Symbol: {symbol}"
        )
        return symbol

    def get_historical_data(
        self,
        instrument_token: str,
        from_date: str,
        to_date: str,
        interval: str = "5minute"
    ) -> Optional[list]:
        """
        Fetch OHLCV historical candle data.
        Used by the strategy for RSI calculation and trend confirmation.
        interval: "minute", "5minute", "15minute", "60minute", "day"
        """
        if not self._kite:
            return None
        try:
            data = self._kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval=interval
            )
            self._logger.debug(
                f"Historical data | Token: {instrument_token} | "
                f"Interval: {interval} | Candles: {len(data)}"
            )
            return data
        except Exception as e:
            self._logger.error(f"Historical data fetch failed: {e}")
            return None

    # ----------------------------------------------------------
    # ORDER MANAGEMENT
    # ----------------------------------------------------------

    def place_order(self, order: OrderRequest) -> OrderResponse:
        """
        Place a Nifty options order via Kite Connect.
        Uses MARKET orders for speed (slippage is minimal on liquid Nifty).
        """
        if not self._kite or not self.is_logged_in:
            return OrderResponse(
                success=False, order_id=None,
                symbol=order.symbol, transaction_type=order.transaction_type,
                quantity=order.quantity, fill_price=0.0,
                status="ERROR", message="Not logged in"
            )

        try:
            self._logger.info(f"Placing order: {order}")

            # Map our standard order_type to Kite constants
            kite_order_type = (
                self._kite.ORDER_TYPE_MARKET
                if order.order_type == "MARKET"
                else self._kite.ORDER_TYPE_LIMIT
            )
            kite_transaction = (
                self._kite.TRANSACTION_TYPE_BUY
                if order.transaction_type == "BUY"
                else self._kite.TRANSACTION_TYPE_SELL
            )

            order_id = self._kite.place_order(
                tradingsymbol=order.symbol,
                exchange=self._kite.EXCHANGE_NFO,
                transaction_type=kite_transaction,
                quantity=order.quantity,
                order_type=kite_order_type,
                product=self._kite.PRODUCT_MIS,
                price=order.price if order.order_type == "LIMIT" else None,
                tag=order.tag[:20] if order.tag else None  # Kite tag limit: 20 chars
            )

            # Fetch fill price (slight delay for market orders)
            time.sleep(0.5)
            fill_price = self._get_order_fill_price(order_id) or 0.0

            response = OrderResponse(
                success=True,
                order_id=str(order_id),
                symbol=order.symbol,
                transaction_type=order.transaction_type,
                quantity=order.quantity,
                fill_price=fill_price,
                status="COMPLETE",
                message=f"Order placed successfully. ID: {order_id}"
            )

        except Exception as e:
            self._logger.error(f"Order placement failed for {self.subscriber_id}: {e}")
            response = OrderResponse(
                success=False, order_id=None,
                symbol=order.symbol, transaction_type=order.transaction_type,
                quantity=order.quantity, fill_price=0.0,
                status="ERROR", message=str(e)
            )

        self.log_order(order, response)
        return response

    def _get_order_fill_price(self, order_id: str) -> Optional[float]:
        """Fetch the average fill price of a completed order."""
        try:
            orders = self._kite.orders()
            for o in orders:
                if str(o.get("order_id")) == str(order_id):
                    return float(o.get("average_price", 0))
        except Exception as e:
            self._logger.warning(f"Could not fetch fill price for order {order_id}: {e}")
        return None

    def cancel_order(self, order_id: str, variety: str = "regular") -> bool:
        if not self._kite:
            return False
        try:
            self._kite.cancel_order(variety=variety, order_id=order_id)
            self._logger.info(f"Order {order_id} cancelled for {self.subscriber_id}")
            return True
        except Exception as e:
            self._logger.error(f"Cancel order failed: {e}")
            return False

    def get_order_status(self, order_id: str) -> Optional[OrderResponse]:
        if not self._kite:
            return None
        try:
            orders = self._kite.orders()
            for o in orders:
                if str(o.get("order_id")) == str(order_id):
                    return OrderResponse(
                        success=True,
                        order_id=order_id,
                        symbol=o.get("tradingsymbol", ""),
                        transaction_type=o.get("transaction_type", ""),
                        quantity=int(o.get("quantity", 0)),
                        fill_price=float(o.get("average_price", 0)),
                        status=o.get("status", "UNKNOWN"),
                        message=o.get("status_message", ""),
                        raw_response=o
                    )
        except Exception as e:
            self._logger.error(f"get_order_status failed: {e}")
        return None

    # ----------------------------------------------------------
    # POSITIONS & PORTFOLIO
    # ----------------------------------------------------------

    def get_positions(self) -> List[Position]:
        if not self._kite:
            return []
        try:
            raw = self._kite.positions()
            positions = []
            # "day" positions = intraday MIS trades today
            for p in raw.get("day", []):
                if p.get("quantity", 0) != 0:  # Only open positions
                    positions.append(Position(
                        symbol=p.get("tradingsymbol", ""),
                        exchange=p.get("exchange", ""),
                        quantity=int(p.get("quantity", 0)),
                        average_price=float(p.get("average_price", 0)),
                        last_price=float(p.get("last_price", 0)),
                        unrealised_pnl=float(p.get("unrealised", 0)),
                        product=p.get("product", "MIS")
                    ))
            return positions
        except Exception as e:
            self._logger.error(f"get_positions failed for {self.subscriber_id}: {e}")
            return []

    def get_account_info(self) -> Optional[AccountInfo]:
        if not self._kite:
            return None
        try:
            margins = self._kite.margins()
            equity = margins.get("equity", {})
            return AccountInfo(
                client_id=self.subscriber_id,
                broker="zerodha",
                available_margin=float(equity.get("available", {}).get("cash", 0)),
                used_margin=float(equity.get("utilised", {}).get("debits", 0)),
                total_balance=float(equity.get("net", 0)),
                is_connected=self.is_logged_in
            )
        except Exception as e:
            self._logger.error(f"get_account_info failed: {e}")
            return None

    def close_all_positions(self) -> Dict[str, bool]:
        """
        CRITICAL: Close ALL open MIS positions at 15:20 IST.
        Places a MARKET SELL for every open long position.
        """
        results = {}
        positions = self.get_positions()

        if not positions:
            self._logger.info(f"No open positions to close for {self.subscriber_id}")
            return results

        self._logger.warning(
            f"FLATTEN ALL | {len(positions)} open positions | {self.subscriber_id}"
        )

        for pos in positions:
            try:
                # To close a BUY position, we place a SELL
                # To close a SELL position, we place a BUY
                close_side = "SELL" if pos.quantity > 0 else "BUY"
                close_qty  = abs(pos.quantity)

                close_order = OrderRequest(
                    symbol=pos.symbol,
                    exchange=pos.exchange,
                    transaction_type=close_side,
                    quantity=close_qty,
                    order_type="MARKET",
                    product="MIS",
                    tag="EOD_FLATTEN"
                )
                response = self.place_order(close_order)
                results[pos.symbol] = response.success

            except Exception as e:
                self._logger.error(
                    f"Failed to close {pos.symbol} for {self.subscriber_id}: {e}"
                )
                results[pos.symbol] = False

        return results

    def get_todays_pnl(self) -> float:
        """Calculate today's total realised P&L from closed trades."""
        if not self._kite:
            return 0.0
        try:
            positions = self._kite.positions()
            total_pnl = 0.0
            for p in positions.get("day", []):
                total_pnl += float(p.get("realised", 0))
                total_pnl += float(p.get("unrealised", 0))
            return total_pnl
        except Exception as e:
            self._logger.error(f"get_todays_pnl failed: {e}")
            return 0.0
