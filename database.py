"""
database.py — Trade Log Database (SQLite via SQLAlchemy)
=========================================================
Stores every trade entry, exit, and daily summary.
SQLite is used for simplicity — zero setup, no external server.
Easily upgradeable to PostgreSQL (just change the connection string).
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
from sqlalchemy import (
    create_engine, Column, String, Float, Integer,
    DateTime, Boolean, Text, event
)
from sqlalchemy.orm import DeclarativeBase, Session
from loguru import logger


class Base(DeclarativeBase):
    pass


class TradeEntry(Base):
    """One row = one trade entry (buy of an option)."""
    __tablename__ = "trade_entries"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    subscriber_id  = Column(String(50), nullable=False, index=True)
    broker         = Column(String(20), nullable=False)
    symbol         = Column(String(50), nullable=False)
    option_type    = Column(String(2),  nullable=False)   # CE or PE
    quantity       = Column(Integer,    nullable=False)
    fill_price     = Column(Float,      nullable=False)
    vix_at_entry   = Column(Float)
    rsi_at_entry   = Column(Float)
    nifty_spot     = Column(Float)
    entry_time     = Column(DateTime,   default=datetime.utcnow)
    is_closed      = Column(Boolean,    default=False)


class TradeExit(Base):
    """One row = one trade exit (sell/close of a position)."""
    __tablename__ = "trade_exits"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    subscriber_id  = Column(String(50), nullable=False, index=True)
    broker         = Column(String(20), nullable=False)
    exit_reason    = Column(String(30), nullable=False)
    pnl            = Column(Float,      nullable=False)
    exit_time      = Column(DateTime,   default=datetime.utcnow)


class DailySummary(Base):
    """End-of-day P&L summary per subscriber."""
    __tablename__ = "daily_summaries"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    subscriber_id  = Column(String(50), nullable=False, index=True)
    trade_date     = Column(String(10), nullable=False)   # YYYY-MM-DD
    total_trades   = Column(Integer,    default=0)
    wins           = Column(Integer,    default=0)
    losses         = Column(Integer,    default=0)
    gross_pnl      = Column(Float,      default=0.0)
    brokerage      = Column(Float,      default=0.0)
    net_pnl        = Column(Float,      default=0.0)
    win_rate_pct   = Column(Float,      default=0.0)
    vix_avg        = Column(Float,      default=0.0)
    created_at     = Column(DateTime,   default=datetime.utcnow)


class SystemLog(Base):
    """Log important system events (errors, alerts, circuit breakers)."""
    __tablename__ = "system_logs"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    level          = Column(String(10), nullable=False)   # INFO/WARNING/ERROR/CRITICAL
    component      = Column(String(50))
    message        = Column(Text, nullable=False)
    subscriber_id  = Column(String(50), nullable=True)
    created_at     = Column(DateTime, default=datetime.utcnow)


class TradeDatabase:
    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._engine  = None
        self._logger  = logger.bind(component="Database")

    def initialize(self):
        db_url = f"sqlite:///{self._db_path}"
        self._engine = create_engine(db_url, echo=False)

        # Enable WAL mode for better concurrent read performance
        @event.listens_for(self._engine, "connect")
        def set_wal_mode(dbapi_con, _):
            dbapi_con.execute("PRAGMA journal_mode=WAL")

        Base.metadata.create_all(self._engine)
        self._logger.info(f"Database initialised at {self._db_path}")

    def log_entry(self, subscriber_id: str, broker: str, symbol: str,
                  option_type: str, quantity: int, fill_price: float,
                  vix: float, rsi: float, nifty_spot: float):
        with Session(self._engine) as session:
            session.add(TradeEntry(
                subscriber_id=subscriber_id, broker=broker,
                symbol=symbol, option_type=option_type,
                quantity=quantity, fill_price=fill_price,
                vix_at_entry=vix, rsi_at_entry=rsi, nifty_spot=nifty_spot
            ))
            session.commit()

    def log_exit(self, subscriber_id: str, broker: str, reason: str, pnl: float):
        with Session(self._engine) as session:
            session.add(TradeExit(
                subscriber_id=subscriber_id, broker=broker,
                exit_reason=reason, pnl=pnl
            ))
            session.commit()

    def log_system_event(self, level: str, message: str,
                         component: str = "", subscriber_id: str = ""):
        with Session(self._engine) as session:
            session.add(SystemLog(
                level=level, component=component,
                message=message, subscriber_id=subscriber_id
            ))
            session.commit()

    def get_todays_trades(self, subscriber_id: str) -> List[dict]:
        today = datetime.utcnow().date().isoformat()
        with Session(self._engine) as session:
            entries = session.query(TradeEntry).filter(
                TradeEntry.subscriber_id == subscriber_id,
                TradeEntry.entry_time >= today
            ).all()
            return [
                {
                    "symbol": e.symbol,
                    "option_type": e.option_type,
                    "quantity": e.quantity,
                    "fill_price": e.fill_price,
                    "entry_time": e.entry_time.isoformat() if e.entry_time else "",
                }
                for e in entries
            ]
