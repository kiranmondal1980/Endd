"""
telegram_reporter.py — Automated Telegram P&L Reporter
========================================================
Sends automated messages to:
  1. Your Telegram admin chat (you personally)
  2. Subscriber broadcast channel (all paid subscribers see aggregate)
  3. Individual subscriber chats (their personal P&L, if chat_id is set)

MESSAGES SENT EACH DAY:
  07:50 AM — Morning market brief (VIX, Nifty level, today's outlook)
  During day — Trade execution alerts (entry/exit notifications)
  15:50 PM — End-of-day P&L summary for all subscribers

HOW TELEGRAM BOT WORKS:
  1. Create a bot via @BotFather on Telegram
  2. Get the bot token
  3. Add the bot to your subscriber channel as admin
  4. Use chat_id of channel for broadcasts, individual chat_id for DMs

Run this file standalone to send a test message:
    python telegram_reporter.py --test
"""

import sys
import asyncio
import argparse
from datetime import datetime
from typing import Dict, Optional
from loguru import logger
import pytz

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

from config import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_CHAT_ID,
    TELEGRAM_CHANNEL_ID, TIMEZONE, SubscriberProfile
)

IST = pytz.timezone(TIMEZONE)


# ============================================================
# MESSAGE TEMPLATES
# ============================================================

def _morning_brief_text(vix: float, nifty_spot: float, date_str: str) -> str:
    vix_emoji = "🟢" if vix < 15 else ("🟡" if vix < 20 else "🔴")
    vix_note  = (
        "Full position size today." if vix < 15
        else ("Reduced position size (50%)." if vix < 20
              else "⚠️ HIGH VIX — trading paused today.")
    )
    return (
        f"🌅 *Good Morning — Market Brief*\n"
        f"📅 {date_str}\n\n"
        f"{vix_emoji} India VIX: *{vix:.1f}* — {vix_note}\n"
        f"📈 Nifty Spot: *{nifty_spot:.1f}*\n\n"
        f"⚙️ Algorithm is live. Scanning every 5 minutes.\n"
        f"📊 End-of-day P&L report at 3:50 PM.\n\n"
        f"_Trade with discipline. Trust the system._"
    )


def _eod_report_text(
    total_trades: int,
    wins: int,
    losses: int,
    total_pnl: float,
    subscriber_count: int,
    date_str: str
) -> str:
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    pnl_emoji = "✅" if total_pnl >= 0 else "🔴"
    return (
        f"📊 *End-of-Day Report — {date_str}*\n\n"
        f"{pnl_emoji} Total Net P&L: *INR {total_pnl:+,.0f}*\n"
        f"📈 Total Trades: {total_trades}  (W: {wins} / L: {losses})\n"
        f"🎯 Win Rate: *{win_rate:.1f}%*\n"
        f"👥 Active Accounts: {subscriber_count}\n\n"
        f"_Individual P&L reports sent to your private chat._\n"
        f"_Full trade log available in your subscriber portal._"
    )


def _individual_pnl_text(name: str, pnl: float, trade_count: int, date_str: str) -> str:
    emoji = "✅" if pnl >= 0 else "📉"
    return (
        f"{emoji} *Your Daily P&L — {date_str}*\n\n"
        f"Hello {name.split()[0]}!\n\n"
        f"Today's Net P&L: *INR {pnl:+,.0f}*\n"
        f"Trades Executed: {trade_count}\n\n"
        f"_Your positions were squared off at 3:20 PM._\n"
        f"_Full trade log: Check your Zerodha/Angel P&L section._\n\n"
        f"See you tomorrow! 🚀"
    )


def _trade_entry_text(
    subscriber_name: str,
    symbol: str,
    option_type: str,
    quantity: int,
    fill_price: float,
    nifty_spot: float
) -> str:
    direction = "📈 CALL (CE)" if option_type == "CE" else "📉 PUT (PE)"
    return (
        f"⚡ *Trade Entered*\n"
        f"Direction: {direction}\n"
        f"Symbol: `{symbol}`\n"
        f"Qty: {quantity} | Fill: INR {fill_price:.2f}\n"
        f"Nifty Spot: {nifty_spot:.1f}"
    )


def _trade_exit_text(
    symbol: str,
    reason: str,
    pnl: float
) -> str:
    emoji = "✅" if pnl >= 0 else "🛑"
    reason_map = {
        "PROFIT_TARGET": "Profit target hit (+15%)",
        "STOP_LOSS":     "Stop loss triggered (-10%)",
        "EOD_FLATTEN":   "End-of-day square-off",
        "VIX_SPIKE":     "VIX spike — safety exit",
        "CIRCUIT_BREAKER": "Daily loss limit reached"
    }
    return (
        f"{emoji} *Trade Exited*\n"
        f"Symbol: `{symbol}`\n"
        f"Reason: {reason_map.get(reason, reason)}\n"
        f"P&L: *INR {pnl:+,.0f}*"
    )


def _admin_alert_text(message: str) -> str:
    return f"🚨 *ADMIN ALERT*\n\n{message}\n\n_Time: {datetime.now(IST).strftime('%H:%M:%S IST')}_"


# ============================================================
# TELEGRAM REPORTER CLASS
# ============================================================

class TelegramReporter:
    """
    Sends Telegram messages for all automated notifications.
    Uses python-telegram-bot v21 (async).
    """

    def __init__(self):
        if not TELEGRAM_BOT_TOKEN:
            logger.warning(
                "TELEGRAM_BOT_TOKEN not set. "
                "Telegram notifications will be DISABLED."
            )
            self._bot = None
        else:
            self._bot = Bot(token=TELEGRAM_BOT_TOKEN)

        self._log = logger.bind(component="TelegramReporter")

    # ----------------------------------------------------------
    # INTERNAL: Send with retry
    # ----------------------------------------------------------

    def _send(self, chat_id: str, text: str, parse_mode: str = ParseMode.MARKDOWN) -> bool:
        """Send a message. Returns True on success."""
        if not self._bot or not chat_id:
            self._log.warning(f"Cannot send — bot or chat_id not configured.")
            return False

        async def _do_send():
            await self._bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode
            )

        try:
            asyncio.run(_do_send())
            return True
        except TelegramError as e:
            self._log.error(f"Telegram send failed to {chat_id}: {e}")
            return False
        except Exception as e:
            self._log.error(f"Unexpected error sending Telegram message: {e}")
            return False

    # ----------------------------------------------------------
    # PUBLIC MESSAGE METHODS
    # ----------------------------------------------------------

    def send_morning_brief(self, vix: float, nifty_spot: float) -> bool:
        """07:50 AM — Send morning brief to subscriber channel."""
        date_str = datetime.now(IST).strftime("%A, %d %B %Y")
        text = _morning_brief_text(vix, nifty_spot, date_str)
        self._log.info(f"Sending morning brief | VIX={vix:.1f} | Spot={nifty_spot:.1f}")
        return self._send(TELEGRAM_CHANNEL_ID, text)

    def send_daily_pnl_report(
        self,
        pnl_data: Dict[str, float],
        subscriber_profiles: Dict[str, Optional[SubscriberProfile]]
    ) -> None:
        """
        15:50 PM — Send end-of-day P&L report.
        1. Aggregate report to channel
        2. Individual reports to each subscriber's personal chat
        """
        date_str = datetime.now(IST).strftime("%d %b %Y")
        total_pnl = sum(pnl_data.values())
        total_trades = sum(
            p.daily_trade_count
            for p in subscriber_profiles.values()
            if p is not None
        )
        wins   = sum(1 for pnl in pnl_data.values() if pnl > 0)
        losses = sum(1 for pnl in pnl_data.values() if pnl <= 0)

        # ---- Channel aggregate report ----
        channel_text = _eod_report_text(
            total_trades=total_trades,
            wins=wins,
            losses=losses,
            total_pnl=total_pnl,
            subscriber_count=len(pnl_data),
            date_str=date_str
        )
        self._send(TELEGRAM_CHANNEL_ID, channel_text)
        self._log.info(f"Sent aggregate EOD report to channel | Total P&L: {total_pnl:.2f}")

        # ---- Individual reports ----
        for sub_id, pnl in pnl_data.items():
            profile = subscriber_profiles.get(sub_id)
            if profile and profile.telegram_chat_id:
                individual_text = _individual_pnl_text(
                    name=profile.name,
                    pnl=pnl,
                    trade_count=profile.daily_trade_count,
                    date_str=date_str
                )
                success = self._send(profile.telegram_chat_id, individual_text)
                if success:
                    self._log.debug(f"Individual report sent to {profile.name} ({sub_id})")

    def send_trade_entry_alert(
        self,
        subscriber_id: str,
        subscriber_name: str,
        symbol: str,
        option_type: str,
        quantity: int,
        fill_price: float,
        nifty_spot: float,
        chat_id: Optional[str] = None
    ) -> bool:
        """Send real-time trade entry notification to a subscriber."""
        text = _trade_entry_text(
            subscriber_name=subscriber_name,
            symbol=symbol,
            option_type=option_type,
            quantity=quantity,
            fill_price=fill_price,
            nifty_spot=nifty_spot
        )
        target = chat_id or TELEGRAM_ADMIN_CHAT_ID
        self._log.info(f"[{subscriber_id}] Trade entry alert: {symbol}")
        return self._send(target, text)

    def send_trade_exit_alert(
        self,
        subscriber_id: str,
        symbol: str,
        reason: str,
        pnl: float,
        chat_id: Optional[str] = None
    ) -> bool:
        """Send real-time trade exit notification."""
        text = _trade_exit_text(symbol=symbol, reason=reason, pnl=pnl)
        target = chat_id or TELEGRAM_ADMIN_CHAT_ID
        self._log.info(f"[{subscriber_id}] Trade exit alert: {symbol} | P&L: {pnl:.2f}")
        return self._send(target, text)

    def send_admin_alert(self, message: str) -> bool:
        """Send a critical alert to the admin (Kiran's personal Telegram)."""
        text = _admin_alert_text(message)
        self._log.warning(f"Admin alert: {message}")
        return self._send(TELEGRAM_ADMIN_CHAT_ID, text)

    def send_circuit_breaker_alert(self, subscriber_id: str, name: str, pnl: float) -> bool:
        """Alert when a subscriber's circuit breaker activates."""
        msg = (
            f"🔴 Circuit breaker activated for *{name}* (`{subscriber_id}`)\n"
            f"Daily P&L: *INR {pnl:+,.0f}*\n"
            f"All trading HALTED for this account today.\n"
            f"Will reset at next market open."
        )
        return self._send(TELEGRAM_ADMIN_CHAT_ID, msg)

    def send_test_message(self) -> bool:
        """Send a test ping to verify bot configuration."""
        text = (
            f"✅ *Nifty Scalper Bot — Test Message*\n\n"
            f"Bot is configured correctly!\n"
            f"Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}\n\n"
            f"_If you see this, Telegram notifications are working._"
        )
        admin_ok   = self._send(TELEGRAM_ADMIN_CHAT_ID, text)
        channel_ok = self._send(TELEGRAM_CHANNEL_ID, text)
        return admin_ok and channel_ok


# ============================================================
# STANDALONE TEST RUNNER
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nifty Scalper Telegram Reporter")
    parser.add_argument("--test", action="store_true", help="Send a test message")
    parser.add_argument("--morning", action="store_true", help="Send morning brief")
    parser.add_argument("--eod", action="store_true", help="Send sample EOD report")
    args = parser.parse_args()

    reporter = TelegramReporter()

    if args.test:
        print("Sending test message...")
        ok = reporter.send_test_message()
        print(f"Test message {'sent ✅' if ok else 'FAILED ❌'}")

    elif args.morning:
        ok = reporter.send_morning_brief(vix=13.5, nifty_spot=22450.0)
        print(f"Morning brief {'sent ✅' if ok else 'FAILED ❌'}")

    elif args.eod:
        sample_pnl = {"SUB_001": 1250.0, "SUB_002": -340.0, "SUB_003": 870.0}
        reporter.send_daily_pnl_report(
            pnl_data=sample_pnl,
            subscriber_profiles={k: None for k in sample_pnl}
        )
        print("EOD report sent.")

    else:
        parser.print_help()
