"""Base class for all strategies."""

import logging
import time
from abc import ABC, abstractmethod
from typing import List

from execution_ledger import ledger

logger = logging.getLogger(__name__)


class BaseStrategy(ABC):
    def __init__(self, name: str, exchange, capital: float):
        self.name = name
        self.exchange = exchange
        self.capital = capital
        self.total_pnl = 0.0
        self.pnl_history: List[float] = []
        self.peak_capital = capital
        self.trade_count = 0
        self.last_run = 0
        self.is_active = True
        self.weight = 1.0
        self.portfolio = None
        self.strategy_key = ""

    def set_weight(self, weight: float):
        self.weight = weight

    def _risk_strategy_name(self) -> str:
        return self.strategy_key or self.name.lower()

    def request_trade_approval(self, symbol: str, side: str, order_type: str,
                               amount: float, price: float = None,
                               reduce_only: bool = False, risk_context: dict = None) -> dict:
        if self.portfolio is None:
            return {"approved": True, "reason": "portfolio_unavailable", "details": {}, "limits": {}}
        decision = self.portfolio.request_trade_approval(
            strategy_name=self._risk_strategy_name(),
            symbol=symbol,
            side=side,
            order_type=order_type,
            amount=amount,
            price=price,
            reduce_only=reduce_only,
            risk_context=risk_context or {},
        )
        if not decision.get("approved", False):
            logger.warning(f"[{self.name}] Risk rejected {order_type} {side} {symbol}: {decision.get('reason')}")
        return decision

    def trigger_protection(self, reason: str, details: dict = None):
        if self.portfolio is None:
            return
        self.portfolio.enter_protection_mode(reason, strategy=self._risk_strategy_name(), details=details or {})
        ledger.record_event("strategy_protection", {
            "strategy": self._risk_strategy_name(),
            "reason": reason,
            "details": dict(details or {}),
        })

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def get_check_interval(self) -> int:
        pass

    def should_run(self) -> bool:
        return self.is_active and (time.time() - self.last_run >= self.get_check_interval())

    def get_unrealized_pnl(self) -> float:
        return 0.0

    def current_strategy_notional(self) -> float:
        return 0.0

    def max_strategy_notional(self) -> float:
        return float(self.capital or 0.0)

    def get_effective_pnl(self) -> float:
        return float(self.total_pnl or 0) + float(self.get_unrealized_pnl() or 0)

    def _drawdown_for_pnl(self, effective_pnl: float) -> float:
        current = self.capital + effective_pnl
        if current > self.peak_capital:
            self.peak_capital = current
        if self.peak_capital <= 0:
            return 0.0
        return (self.peak_capital - current) / self.peak_capital

    def get_drawdown(self) -> float:
        return self._drawdown_for_pnl(self.get_effective_pnl())

    def export_state(self) -> dict:
        return {
            "name": self.name,
            "capital": self.capital,
            "total_pnl": self.total_pnl,
            "pnl_history": list(self.pnl_history),
            "peak_capital": self.peak_capital,
            "trade_count": self.trade_count,
            "last_run": self.last_run,
            "is_active": self.is_active,
        }

    def import_state(self, state: dict):
        state = state or {}
        self.total_pnl = float(state.get("total_pnl", 0.0) or 0.0)
        self.pnl_history = list(state.get("pnl_history", []))
        self.peak_capital = float(state.get("peak_capital", self.capital) or self.capital)
        self.trade_count = int(state.get("trade_count", 0) or 0)
        self.last_run = float(state.get("last_run", 0) or 0)
        self.is_active = bool(state.get("is_active", True))

    def get_status(self) -> dict:
        realized = float(self.total_pnl or 0)
        unrealized = float(self.get_unrealized_pnl() or 0)
        effective = realized + unrealized
        return {
            "name": self.name,
            "capital": self.capital,
            "total_pnl": realized,
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "effective_pnl": effective,
            "drawdown": self._drawdown_for_pnl(effective),
            "trades": self.trade_count,
            "active": self.is_active,
            "execution_mode": getattr(self, "position", {}).get("execution_mode", "UNKNOWN") if hasattr(self, "position") and getattr(self, "position", None) else "UNKNOWN",
        }
