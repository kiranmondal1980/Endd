"""
broker_angel.py — Angel One SmartAPI Implementation
=====================================================
Full implementation of BrokerBase using Angel One's SmartAPI.

ANGEL ONE SMARTAPI NOTES:
  - Free API (no monthly subscription fee unlike Zerodha)
  - Requires TOTP (Time-based One-Time Password) for 2FA at login
  - pyotp library generates the TOTP from the base32 secret
  - Sessions last 24 hours and can be refreshed with a JWT token
  - Good rate limits for retail use

TOTP SETUP:
  1. Log into Angel One web/app → SmartAPI → Enable API
  2. It shows a QR code — scan it or extract the base32 secret string
  3. Store that base32 secret in .env as ANGEL_ADMIN_TOTP_SECRET
  4. pyotp.TOTP(secret).now() generates the current 6-digit OTP

This file mirrors broker_zerodha.py but uses SmartAPI calls.
"""

import time
import pyotp
from typing import Optional, Dict, List

from SmartApi import SmartConnect
from SmartApi.smartExceptions import DataException
from loguru import logger

from broker_base import BrokerBase, OrderRequest, OrderResponse, Position, AccountInfo
from config import StrategyConfig, NIFTY_LOT_SIZE, NIFTY_STRIKE_INTERVAL, TIMEZONE


class AngelBroker(BrokerBase):
    """
    Angel One SmartAPI broker implementation.
    One instance per subscriber account.
    """

    EXCHANGE_NFO = "NFO"
    EXCHANGE_NSE = "NSE"
    PRODUCT_MIS  = "INTRADAY"   # Angel uses "INTRADAY" not "MIS"
    ORDER_MARKET = "MARKET"
    ORDER_LIMIT  = "LIMIT"
    BUY          = "BUY"
    SELL         = "SELL"
    VARIETY_NORMAL = "NORMAL"

    def __init__(
        self,
        subscriber_id: str,
        name: str,
        api_key: str,
        client_id: str,
        password: str,    # MPIN for Angel One
        totp_secret: str
    ):
        super().__init__(subscriber_id, name)
        self._api_key     = api_key
        self._client_id   = client_id
        self._password    = password
        self._totp_secret = totp_secret

        self._smart: Optional[SmartConnect] = None
        self._auth_token: Optional[str]  = None
        self._refresh_token: Optional[str] = None
        self._feed_token: Optional[str]  = None

    # ----------------------------------------------------------
    # AUTHENTICATION
    # ----------------------------------------------------------

    def login(self) -> bool:
        """
        Login to Angel One SmartAPI using MPIN + TOTP.
        TOTP is generated fresh every call (it's time-based, changes every 30s).
        """
        try:
            self._smart = SmartConnect(api_key=self._api_key)

            # Generate current TOTP from secret
            totp = pyotp.TOTP(self._totp_secret).now()
            self._logger.debug(f"Generated TOTP for {self.subscriber_id}")

            data = self._smart.generateSession(
                clientCode=self._client_id,
                password=self._password,
                totp=totp
            )

            if data and data.get("status"):
                self._auth_token    = data["data"]["jwtToken"]
                self._refresh_token = data["data"]["refreshToken"]
                self._feed_token    = self._smart.getfeedToken()
                self.is_logged_in   = True

                self._logger.info(
                    f"Angel One login OK | Subscriber: {self.subscriber_id} | "
                    f"Client: {self._client_id}"
                )
                return True
            else:
                error_msg = data.get("message", "Unknown error") if data else "No response"
                self._logger.error(
                    f"Angel One login FAILED for {self.subscriber_id}: {error_msg}"
                )
                return False

        except Exception as e:
            self._logger.error(f"Angel One login exception for {self.subscriber_id}: {e}")
            self.is_logged_in = False
            return False

    def refresh_session(self) -> bool:
        """
        Refresh Angel One session using the refresh token (JWT-based, lasts 24h).
        Called at 08:00 AM daily.
        """
        if not self._smart or not self._refresh_token:
            self._logger.warning(
                f"No refresh token for {self.subscriber_id}. Doing full re-login."
            )
            return self.login()

        try:
            data = self._smart.generateToken(self._refresh_token)
            if data and data.get("status"):
                self._auth_token  = data["data"]["jwtToken"]
                self._refresh_token = data["data"]["refreshToken"]
                self.is_logged_in  = True
                self._logger.info(f"Angel One session refreshed for {self.subscriber_id}")
                return True
            else:
                self._logger.warning(
                    f"Refresh failed for {self.subscriber_id}. Attempting full re-login."
                )
                return self.login()
        except Exception as e:
            self._logger.error(f"Session refresh failed: {e}. Attempting re-login.")
            return self.login()

    def test_connection(self) -> bool:
        """Verify Angel One session by fetching profile."""
        if not self._smart:
            return False
        try:
            profile = self._smart.getProfile(self._refresh_token)
            return bool(profile and profile.get("status"))
        except Exception as e:
            self._logger.warning(f"Angel connection test failed: {e}")
            self.is_logged_in = False
            return False

    # ----------------------------------------------------------
    # MARKET DATA
    # ----------------------------------------------------------

    def get_ltp(self, symbol: str, exchange: str = "NFO") -> Optional[float]:
        """
        Get last traded price from Angel One.
        Angel uses numeric token IDs (not symbol strings) for LTP calls,
        so we need to look up the token. For simplicity we use the
        searchScrip method to resolve the symbol first.
        """
        if not self._smart:
            return None
        try:
            # Search for instrument to get token
            search_result = self._smart.searchScrip(exchange=exchange, searchscrip=symbol)
            if not search_result or not search_result.get("data"):
                self._logger.warning(f"Symbol not found: {symbol}")
                return None

            # Pick first result (most relevant match)
            instrument = search_result["data"][0]
            token = instrument.get("symboltoken")

            # Fetch LTP
            ltp_data = self._smart.ltpData(
                exchange=exchange,
                tradingsymbol=symbol,
                symboltoken=token
            )

            if ltp_data and ltp_data.get("data"):
                ltp = float(ltp_data["data"].get("ltp", 0))
                self._logger.debug(f"LTP | {exchange}:{symbol} = {ltp}")
                return ltp

        except Exception as e:
            self._logger.error(f"get_ltp failed for {symbol} on Angel: {e}")
        return None

    def get_nifty_spot(self) -> Optional[float]:
        """Fetch Nifty 50 spot index price from Angel One."""
        if not self._smart:
            return None
        try:
            # Nifty 50 index token on Angel is "99926000"
            ltp_data = self._smart.ltpData(
                exchange="NSE",
                tradingsymbol="Nifty 50",
                symboltoken="99926000"
            )
            if ltp_data and ltp_data.get("data"):
                return float(ltp_data["data"].get("ltp", 0))
        except Exception as e:
            self._logger.error(f"get_nifty_spot failed on Angel: {e}")
        return None

    def get_option_chain_atm_strike(
        self,
        index_ltp: float,
        option_type: str,
        expiry_date: str
    ) -> Optional[str]:
        """
        Build ATM option symbol for Angel One.
        Angel symbol format: NIFTY24JUL22100CE (same as NSE)
        """
        atm_strike = round(index_ltp / NIFTY_STRIKE_INTERVAL) * NIFTY_STRIKE_INTERVAL
        symbol = f"NIFTY{expiry_date}{int(atm_strike)}{option_type}"
        self._logger.debug(
            f"ATM option | Spot: {index_ltp:.2f} → Strike: {atm_strike} | Symbol: {symbol}"
        )
        return symbol

    def _get_symbol_token(self, symbol: str, exchange: str = "NFO") -> Optional[str]:
        """
        Helper: resolve a trading symbol to Angel One's internal token ID.
        This token is required for order placement on Angel One.
        """
        try:
            result = self._smart.searchScrip(exchange=exchange, searchscrip=symbol)
            if result and result.get("data"):
                for item in result["data"]:
                    if item.get("tradingsymbol") == symbol:
                        return str(item.get("symboltoken"))
                # Fallback: return first result's token
                return str(result["data"][0].get("symboltoken"))
        except Exception as e:
            self._logger.error(f"Symbol token lookup failed for {symbol}: {e}")
        return None

    # ----------------------------------------------------------
    # ORDER MANAGEMENT
    # ----------------------------------------------------------

    def place_order(self, order: OrderRequest) -> OrderResponse:
        """Place an order via Angel One SmartAPI."""
        if not self._smart or not self.is_logged_in:
            return OrderResponse(
                success=False, order_id=None,
                symbol=order.symbol, transaction_type=order.transaction_type,
                quantity=order.quantity, fill_price=0.0,
                status="ERROR", message="Not logged in to Angel One"
            )

        try:
            # Angel requires the numeric symboltoken
            token = self._get_symbol_token(order.symbol, order.exchange)
            if not token:
                raise ValueError(f"Could not resolve token for symbol: {order.symbol}")

            self._logger.info(f"Placing Angel order: {order}")

            order_params = {
                "variety":         self.VARIETY_NORMAL,
                "tradingsymbol":   order.symbol,
                "symboltoken":     token,
                "transactiontype": order.transaction_type.upper(),
                "exchange":        order.exchange.upper(),
                "ordertype":       order.order_type.upper(),
                "producttype":     self.PRODUCT_MIS,
                "duration":        "DAY",
                "quantity":        str(order.quantity),
            }

            if order.order_type == "LIMIT":
                order_params["price"] = str(round(order.price, 2))

            response_data = self._smart.placeOrder(order_params)

            if response_data and response_data.get("status"):
                order_id = response_data.get("data", {}).get("orderid", "UNKNOWN")
                # Give Angel One a moment to fill the market order
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
                    message=f"Angel order placed. ID: {order_id}",
                    raw_response=response_data
                )
            else:
                error_msg = response_data.get("message", "Unknown error") if response_data else "No response"
                response = OrderResponse(
                    success=False, order_id=None,
                    symbol=order.symbol, transaction_type=order.transaction_type,
                    quantity=order.quantity, fill_price=0.0,
                    status="REJECTED", message=error_msg,
                    raw_response=response_data
                )

        except Exception as e:
            self._logger.error(f"Angel order failed for {self.subscriber_id}: {e}")
            response = OrderResponse(
                success=False, order_id=None,
                symbol=order.symbol, transaction_type=order.transaction_type,
                quantity=order.quantity, fill_price=0.0,
                status="ERROR", message=str(e)
            )

        self.log_order(order, response)
        return response

    def _get_order_fill_price(self, order_id: str) -> Optional[float]:
        """Fetch average fill price for a completed Angel One order."""
        try:
            orders = self._smart.orderBook()
            if orders and orders.get("data"):
                for o in orders["data"]:
                    if str(o.get("orderid")) == str(order_id):
                        return float(o.get("averageprice", 0))
        except Exception as e:
            self._logger.warning(f"Could not fetch Angel fill price for {order_id}: {e}")
        return None

    def cancel_order(self, order_id: str, variety: str = "NORMAL") -> bool:
        if not self._smart:
            return False
        try:
            result = self._smart.cancelOrder(order_id=order_id, variety=variety)
            success = bool(result and result.get("status"))
            if success:
                self._logger.info(f"Angel order {order_id} cancelled")
            return success
        except Exception as e:
            self._logger.error(f"Angel cancel order failed: {e}")
            return False

    def get_order_status(self, order_id: str) -> Optional[OrderResponse]:
        if not self._smart:
            return None
        try:
            orders = self._smart.orderBook()
            if orders and orders.get("data"):
                for o in orders["data"]:
                    if str(o.get("orderid")) == str(order_id):
                        return OrderResponse(
                            success=True,
                            order_id=order_id,
                            symbol=o.get("tradingsymbol", ""),
                            transaction_type=o.get("transactiontype", ""),
                            quantity=int(o.get("quantity", 0)),
                            fill_price=float(o.get("averageprice", 0)),
                            status=o.get("status", "UNKNOWN"),
                            message=o.get("text", ""),
                            raw_response=o
                        )
        except Exception as e:
            self._logger.error(f"Angel get_order_status failed: {e}")
        return None

    # ----------------------------------------------------------
    # POSITIONS & PORTFOLIO
    # ----------------------------------------------------------

    def get_positions(self) -> List[Position]:
        if not self._smart:
            return []
        try:
            raw = self._smart.position()
            positions = []
            if raw and raw.get("data"):
                for p in raw["data"]:
                    qty = int(p.get("netqty", 0))
                    if qty != 0:
                        positions.append(Position(
                            symbol=p.get("tradingsymbol", ""),
                            exchange=p.get("exchange", ""),
                            quantity=qty,
                            average_price=float(p.get("netavgprice", 0)),
                            last_price=float(p.get("ltp", 0)),
                            unrealised_pnl=float(p.get("unrealisedprofitandloss", 0)),
                            product=p.get("producttype", "INTRADAY")
                        ))
            return positions
        except Exception as e:
            self._logger.error(f"get_positions failed for Angel {self.subscriber_id}: {e}")
            return []

    def get_account_info(self) -> Optional[AccountInfo]:
        if not self._smart:
            return None
        try:
            rms = self._smart.rmsLimit()
            if rms and rms.get("data"):
                d = rms["data"]
                return AccountInfo(
                    client_id=self._client_id,
                    broker="angel",
                    available_margin=float(d.get("availablecash", 0)),
                    used_margin=float(d.get("utiliseddebits", 0)),
                    total_balance=float(d.get("net", 0)),
                    is_connected=self.is_logged_in
                )
        except Exception as e:
            self._logger.error(f"get_account_info failed for Angel: {e}")
        return None

    def close_all_positions(self) -> Dict[str, bool]:
        """Close ALL open positions for this Angel subscriber at EOD."""
        results = {}
        positions = self.get_positions()

        if not positions:
            self._logger.info(f"No open positions to close for {self.subscriber_id}")
            return results

        self._logger.warning(
            f"FLATTEN ALL (Angel) | {len(positions)} positions | {self.subscriber_id}"
        )

        for pos in positions:
            try:
                close_side = "SELL" if pos.quantity > 0 else "BUY"
                close_qty  = abs(pos.quantity)
                close_order = OrderRequest(
                    symbol=pos.symbol,
                    exchange=pos.exchange,
                    transaction_type=close_side,
                    quantity=close_qty,
                    order_type="MARKET",
                    product="INTRADAY",
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
        """Calculate today's realised P&L from Angel trade book."""
        if not self._smart:
            return 0.0
        try:
            positions = self._smart.position()
            total_pnl = 0.0
            if positions and positions.get("data"):
                for p in positions["data"]:
                    total_pnl += float(p.get("realisedprofitandloss", 0))
                    total_pnl += float(p.get("unrealisedprofitandloss", 0))
            return total_pnl
        except Exception as e:
            self._logger.error(f"get_todays_pnl failed for Angel {self.subscriber_id}: {e}")
            return 0.0

    def terminate(self):
        """Logout from Angel One session cleanly."""
        if self._smart:
            try:
                self._smart.terminateSession(self._client_id)
                self._logger.info(f"Angel session terminated for {self.subscriber_id}")
            except Exception as e:
                self._logger.warning(f"Angel logout failed: {e}")
        self.is_logged_in = False
