"""
风控模块
- 全局/单策略回撤监控
- ADX市场状态检测
- 策略权重动态调整
- 预交易风险审批接口
"""

import time
import logging
import numpy as np
from typing import Dict
from config import RISK as CFG
from notifier import notifier

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, exchange, initial_capital: float):
        self.exchange = exchange
        self.initial_capital = initial_capital
        self.peak_capital = initial_capital
        self.daily_start = initial_capital
        self.daily_reset_ts = time.time()

        self.market_state = "unknown"
        self.prev_state = "unknown"
        self.adx_value = 0.0

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            if value is None:
                return float(default)
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _approve(self, reason: str = "approved", details: dict = None, limits: dict = None) -> dict:
        return {
            "approved": True,
            "reason": reason,
            "details": details or {},
            "limits": limits or {},
        }

    def _reject(self, reason: str, details: dict = None, limits: dict = None) -> dict:
        return {
            "approved": False,
            "reason": reason,
            "details": details or {},
            "limits": limits or {},
        }

    def pre_trade_check(
        self,
        strategy_name: str,
        symbol: str,
        side: str,
        order_type: str,
        amount: float,
        price: float = None,
        reduce_only: bool = False,
        account_state: dict = None,
        exposure_state: dict = None,
        portfolio_state: dict = None,
        risk_context: dict = None,
    ) -> dict:
        account_state = dict(account_state or {})
        exposure_state = dict(exposure_state or {})
        portfolio_state = dict(portfolio_state or {})
        risk_context = dict(risk_context or {})

        normalized_amount = self._safe_float(amount)
        normalized_price = self._safe_float(price, default=0.0) if price is not None else None
        normalized_order_type = str(order_type or "market").lower()
        normalized_side = str(side or "").lower()
        normalized_symbol = str(symbol or "")
        normalized_strategy = str(strategy_name or "")

        limits = {
            "projected_total_notional": self._safe_float(exposure_state.get("projected_total_notional"), default=-1.0),
            "max_total_notional": self._safe_float(exposure_state.get("max_total_notional"), default=-1.0),
            "projected_symbol_notional": self._safe_float(exposure_state.get("projected_symbol_notional"), default=-1.0),
            "max_symbol_notional": self._safe_float(exposure_state.get("max_symbol_notional"), default=-1.0),
            "projected_strategy_notional": self._safe_float(exposure_state.get("projected_strategy_notional"), default=-1.0),
            "max_strategy_notional": self._safe_float(exposure_state.get("max_strategy_notional"), default=-1.0),
            "worst_case_total_notional": self._safe_float(exposure_state.get("worst_case_total_notional"), default=-1.0),
            "worst_case_symbol_notional": self._safe_float(exposure_state.get("worst_case_symbol_notional"), default=-1.0),
            "worst_case_strategy_notional": self._safe_float(exposure_state.get("worst_case_strategy_notional"), default=-1.0),
        }
        details = {
            "strategy": normalized_strategy,
            "symbol": normalized_symbol,
            "side": normalized_side,
            "order_type": normalized_order_type,
            "amount": normalized_amount,
            "price": normalized_price,
            "reduce_only": bool(reduce_only),
            "market_state": self.market_state,
            "risk_context": risk_context,
            "account": {
                "equity": self._safe_float(account_state.get("equity", account_state.get("total", 0.0))),
                "free": self._safe_float(account_state.get("free", 0.0)),
                "used": self._safe_float(account_state.get("used", 0.0)),
            },
            "portfolio": {
                "recovery_blocked": bool(portfolio_state.get("recovery_blocked", False)),
                "circuit_breaker_tripped": bool(portfolio_state.get("circuit_breaker_tripped", False)),
                "ws_stale": bool(portfolio_state.get("ws_stale", False)),
                "reconciliation_ok": bool(portfolio_state.get("reconciliation_ok", True)),
                "strategy_blocked": bool(portfolio_state.get("strategy_blocked", False)),
                "strategy_block_reason": str(portfolio_state.get("strategy_block_reason", "") or ""),
            },
            "exposure": {
                "current_total_notional": self._safe_float(exposure_state.get("current_total_notional", 0.0)),
                "current_symbol_notional": self._safe_float(exposure_state.get("current_symbol_notional", 0.0)),
                "current_strategy_notional": self._safe_float(exposure_state.get("current_strategy_notional", 0.0)),
                "pending_open_order_notional": self._safe_float(exposure_state.get("pending_open_order_notional", 0.0)),
                "pending_total_open_order_notional": self._safe_float(exposure_state.get("pending_total_open_order_notional", 0.0)),
                "pending_symbol_open_order_notional": self._safe_float(exposure_state.get("pending_symbol_open_order_notional", 0.0)),
                "pending_strategy_open_order_notional": self._safe_float(exposure_state.get("pending_strategy_open_order_notional", 0.0)),
                "paired_requested_notional": self._safe_float(exposure_state.get("paired_requested_notional", 0.0)),
                "effective_requested_notional": self._safe_float(exposure_state.get("effective_requested_notional", 0.0)),
            },
        }

        if normalized_amount <= 0:
            return self._reject("invalid_amount", details=details, limits=limits)

        if not bool(reduce_only) and normalized_order_type not in ("market", "limit"):
            return self._reject("unsupported_order_type", details=details, limits=limits)

        if normalized_order_type == "limit" and not bool(reduce_only):
            if normalized_price is None or normalized_price <= 0:
                return self._reject("invalid_limit_price", details=details, limits=limits)

        if details["portfolio"]["recovery_blocked"]:
            return self._reject("recovery_blocked", details=details, limits=limits)

        if not bool(reduce_only) and details["portfolio"]["strategy_blocked"]:
            return self._reject("strategy_blocked", details=details, limits=limits)

        if details["portfolio"]["circuit_breaker_tripped"]:
            return self._reject("circuit_breaker_tripped", details=details, limits=limits)

        if not details["portfolio"]["reconciliation_ok"]:
            return self._reject("reconciliation_not_ok", details=details, limits=limits)

        if details["portfolio"]["ws_stale"]:
            return self._reject("websocket_stale", details=details, limits=limits)

        equity = details["account"]["equity"]
        free = details["account"]["free"]
        if equity <= 0:
            return self._reject("invalid_equity", details=details, limits=limits)
        if not bool(reduce_only) and free <= 0:
            return self._reject("insufficient_free_balance", details=details, limits=limits)

        limit_pairs = (
            ("projected_total_notional", "max_total_notional", "total_notional_limit_exceeded"),
            ("projected_symbol_notional", "max_symbol_notional", "symbol_notional_limit_exceeded"),
            ("projected_strategy_notional", "max_strategy_notional", "strategy_notional_limit_exceeded"),
            ("worst_case_total_notional", "max_total_notional", "worst_case_total_notional_limit_exceeded"),
            ("worst_case_symbol_notional", "max_symbol_notional", "worst_case_symbol_notional_limit_exceeded"),
            ("worst_case_strategy_notional", "max_strategy_notional", "worst_case_strategy_notional_limit_exceeded"),
        )
        for projected_key, max_key, reason in limit_pairs:
            projected = limits.get(projected_key, -1.0)
            maximum = limits.get(max_key, -1.0)
            if projected >= 0 and maximum > 0 and projected - 1e-8 > maximum:
                return self._reject(reason, details=details, limits=limits)

        if normalized_order_type == "market" and normalized_price is None:
            details["price_required_later"] = True

        return self._approve(details=details, limits=limits)

    def export_state(self) -> dict:
        return {
            "initial_capital": self.initial_capital,
            "peak_capital": self.peak_capital,
            "daily_start": self.daily_start,
            "daily_reset_ts": self.daily_reset_ts,
            "market_state": self.market_state,
            "prev_state": self.prev_state,
            "adx_value": self.adx_value,
        }

    def import_state(self, state: dict):
        state = state or {}
        self.peak_capital = float(state.get("peak_capital", self.initial_capital) or self.initial_capital)
        self.daily_start = float(state.get("daily_start", self.initial_capital) or self.initial_capital)
        self.daily_reset_ts = float(state.get("daily_reset_ts", time.time()) or time.time())
        self.market_state = state.get("market_state", "unknown") or "unknown"
        self.prev_state = state.get("prev_state", "unknown") or "unknown"
        self.adx_value = float(state.get("adx_value", 0.0) or 0.0)

    def reset_baseline(self, equity: float):
        equity = float(equity or 0)
        if equity <= 0:
            raise ValueError("equity must be positive to reset risk baseline")
        self.initial_capital = equity
        self.peak_capital = equity
        self.daily_start = equity
        self.daily_reset_ts = time.time()

    def check_global(self, equity: float) -> dict:
        """全局风控检查 -> {safe, action, details}"""
        if equity > self.peak_capital:
            self.peak_capital = equity

        dd = (self.peak_capital - equity) / self.peak_capital if self.peak_capital > 0 else 0

        # 每日重置
        if time.time() - self.daily_reset_ts > 86400:
            self.daily_start = equity
            self.daily_reset_ts = time.time()

        daily_loss = (self.daily_start - equity) / self.daily_start if self.daily_start > 0 else 0

        # 紧急止损
        if dd >= CFG["emergency_stop_loss"]:
            msg = f"紧急止损: 回撤 {dd:.2%} >= {CFG['emergency_stop_loss']:.2%}"
            notifier.risk_emergency(msg)
            return {"safe": False, "action": "STOP_ALL", "details": msg}

        # 最大回撤
        if dd >= CFG["max_total_drawdown"]:
            msg = f"最大回撤: {dd:.2%} >= {CFG['max_total_drawdown']:.2%}"
            notifier.risk_alert("最大回撤", msg, "暂停所有策略")
            return {"safe": False, "action": "REDUCE_ALL", "details": msg}

        # 日内亏损
        if daily_loss >= CFG["max_daily_loss"]:
            msg = f"日内亏损: {daily_loss:.2%} >= {CFG['max_daily_loss']:.2%}"
            notifier.risk_alert("日内亏损", msg, "当日暂停")
            return {"safe": False, "action": "PAUSE_TODAY", "details": msg}

        return {
            "safe": True,
            "action": "CONTINUE",
            "details": f"DD: {dd:.2%} | Daily: {daily_loss:.2%}",
        }

    def check_strategy(self, name: str, drawdown: float) -> dict:
        """单策略风控"""
        if drawdown >= CFG["max_strategy_drawdown"]:
            msg = f"{name} 回撤 {drawdown:.2%} 超限"
            notifier.risk_alert("策略回撤", msg, f"暂停 {name}")
            return {"safe": False, "action": "PAUSE_STRATEGY", "details": msg}
        return {"safe": True, "action": "CONTINUE", "details": ""}

    def detect_market_state(self, symbol: str) -> str:
        """ADX市场状态检测"""
        try:
            ohlcv = self.exchange.get_ohlcv(symbol, "1h", limit=CFG["adx_period"] * 3)
            if len(ohlcv) < CFG["adx_period"] + 2:
                return "unknown"

            highs = np.array([c[2] for c in ohlcv])
            lows = np.array([c[3] for c in ohlcv])
            closes = np.array([c[4] for c in ohlcv])

            self.adx_value = self._calc_adx(highs, lows, closes, CFG["adx_period"])

            self.prev_state = self.market_state
            if self.adx_value > CFG["adx_trend_threshold"]:
                self.market_state = "trend"
            elif self.adx_value < CFG["adx_range_threshold"]:
                self.market_state = "range"
            else:
                self.market_state = "transition"

            if self.market_state != self.prev_state:
                notifier.market_state_change(self.prev_state, self.market_state, self.adx_value)

            return self.market_state
        except Exception as e:
            logger.error(f"[Risk] ADX error: {e}")
            return "unknown"

    def _calc_adx(self, highs, lows, closes, period: int) -> float:
        n = len(highs)
        if n < period + 1:
            return 0

        tr = np.zeros(n)
        plus_dm = np.zeros(n)
        minus_dm = np.zeros(n)

        for i in range(1, n):
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            up = highs[i] - highs[i - 1]
            down = lows[i - 1] - lows[i]
            plus_dm[i] = up if (up > down and up > 0) else 0
            minus_dm[i] = down if (down > up and down > 0) else 0

        atr = np.zeros(n)
        atr[period] = np.mean(tr[1:period + 1])
        sp = np.mean(plus_dm[1:period + 1])
        sm = np.mean(minus_dm[1:period + 1])

        plus_di = np.zeros(n)
        minus_di = np.zeros(n)

        for i in range(period + 1, n):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
            sp = (sp * (period - 1) + plus_dm[i]) / period
            sm = (sm * (period - 1) + minus_dm[i]) / period
            if atr[i] > 0:
                plus_di[i] = 100 * sp / atr[i]
                minus_di[i] = 100 * sm / atr[i]

        dx = np.zeros(n)
        for i in range(period, n):
            s = plus_di[i] + minus_di[i]
            if s > 0:
                dx[i] = 100 * abs(plus_di[i] - minus_di[i]) / s

        valid = dx[period:]
        if len(valid) < period:
            return float(np.mean(valid)) if len(valid) > 0 else 0
        return float(np.mean(valid[-period:]))

    def get_weight_adjustment(self) -> Dict[str, float]:
        """根据市场状态调整策略权重"""
        if self.market_state == "trend":
            return {"funding_arb": 1.0, "dynamic_grid": 0.5, "trend_dca": 1.5}
        elif self.market_state == "range":
            return {"funding_arb": 1.0, "dynamic_grid": 1.3, "trend_dca": 0.5}
        return {"funding_arb": 1.0, "dynamic_grid": 1.0, "trend_dca": 1.0}
