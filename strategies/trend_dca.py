"""Strategy 3: trend-following DCA."""

import logging
import time
from typing import Optional

import numpy as np

from config import TREND_DCA as CFG
from notifier import notifier
from execution_ledger import ledger
from strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


class TrendDCAStrategy(BaseStrategy):
    def __init__(self, exchange, capital: float):
        super().__init__("TrendDCA", exchange, capital)
        self.symbol = CFG["symbol"]
        self.signal: Optional[str] = None
        self.position: Optional[dict] = None
        self.layers_filled = 0
        self.peak_price = 0.0

    def export_state(self) -> dict:
        state = super().export_state()
        state.update({
            "symbol": self.symbol,
            "signal": self.signal,
            "position": self.position,
            "layers_filled": self.layers_filled,
            "peak_price": self.peak_price,
        })
        return state

    def import_state(self, state: dict):
        super().import_state(state)
        state = state or {}
        self.signal = state.get("signal")
        self.position = state.get("position")
        self.layers_filled = int(state.get("layers_filled", 0) or 0)
        self.peak_price = float(state.get("peak_price", 0.0) or 0.0)

    def _calc_stop_price(self, avg_price: float, side: str) -> float:
        avg_price = float(avg_price or 0)
        if avg_price <= 0:
            return 0.0
        if side == "long":
            return avg_price * (1 - CFG["stop_loss_pct"])
        return avg_price * (1 + CFG["stop_loss_pct"])

    def _protective_stop_spec(self) -> Optional[dict]:
        if not self.position:
            return None
        total_amount = float(self.position.get("total_amount", 0) or 0)
        side = str(self.position.get("side") or "").lower()
        stop_price = self._calc_stop_price(self.position.get("avg_price", 0), side)
        if total_amount <= 0 or stop_price <= 0 or side not in ("long", "short"):
            return None
        return {
            "side": "sell" if side == "long" else "buy",
            "amount": total_amount,
            "stop_price": stop_price,
        }

    @staticmethod
    def _order_field(order: dict, *keys, default=None):
        sources = [order]
        if isinstance(order, dict):
            sources.append(order.get("info"))
        for source in sources:
            if not isinstance(source, dict):
                continue
            for key in keys:
                value = source.get(key)
                if value not in (None, ""):
                    return value
        return default

    def _order_float_field(self, order: dict, *keys, default: float = 0.0) -> float:
        value = self._order_field(order, *keys, default=default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _order_flag(self, order: dict, *keys) -> bool:
        value = self._order_field(order, *keys, default=None)
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        if text in ("true", "1", "yes"):
            return True
        if text in ("false", "0", "no", ""):
            return False
        return bool(value)

    def _sync_partial_close_state(self, remaining_amount: float):
        if not self.position:
            return
        remaining_amount = float(remaining_amount or 0)
        current_total = float(self.position.get("total_amount", 0) or 0)
        if remaining_amount <= 0 or current_total <= 0:
            return
        ratio = min(max(remaining_amount / current_total, 0.0), 1.0)
        new_layers = []
        weighted_total = 0.0
        for layer in self.position.get("layers", []):
            price = float(layer.get("price", 0) or 0)
            layer_amount = float(layer.get("amount", 0) or 0) * ratio
            if price <= 0 or layer_amount <= 0:
                continue
            new_layers.append({"price": price, "amount": layer_amount})
            weighted_total += price * layer_amount
        if not new_layers:
            avg_price = float(self.position.get("avg_price", 0) or 0)
            if avg_price > 0:
                new_layers = [{"price": avg_price, "amount": remaining_amount}]
                weighted_total = avg_price * remaining_amount
        self.position["layers"] = new_layers
        self.position["total_amount"] = remaining_amount
        if weighted_total > 0 and remaining_amount > 0:
            self.position["avg_price"] = weighted_total / remaining_amount
        self.layers_filled = len(new_layers)

    def _emergency_flatten_unprotected_position(self, context: str) -> bool:
        if not self.position:
            return True
        side = str(self.position.get("side") or "").lower()
        amount = float(self.position.get("total_amount", 0) or 0)
        if amount <= 0 or side not in ("long", "short"):
            return False

        close_side = "sell" if side == "long" else "buy"
        logger.error(f"[{self.name}] {context}: protective stop unavailable, attempting emergency flat")
        order = self.exchange.market_order(self.symbol, close_side, amount, reduce_only=True)
        if not order or not order.get("execution_ok"):
            notifier.error(
                self.name,
                f"{self.symbol}: {context} left an unprotected position and emergency flat failed. Manual action required.",
            )
            return False

        close_price = float(order.get("average", 0) or 0)
        close_amount = float(order.get("filled", 0) or 0)
        if close_price <= 0 or close_amount <= 0:
            notifier.error(
                self.name,
                f"{self.symbol}: {context} emergency flat confirmation incomplete. Manual action required.",
            )
            return False

        tolerance = max(amount * 0.005, 1e-4)
        if abs(close_amount - amount) > tolerance:
            live = self._get_live_position()
            if live:
                self._sync_partial_close_state(float(live.get("contracts", close_amount) or close_amount))
            notifier.error(
                self.name,
                f"{self.symbol}: {context} emergency flat was partial and residual exposure remains. Manual action required.",
            )
            return False

        self._finalize_close(order, f"{context}_EMERGENCY_FLAT")
        return True

    def _sync_protective_stop(self):
        if not self.position:
            return True
        spec = self._protective_stop_spec()
        if not spec:
            return False
        existing_order_id = str(self.position.get("protective_stop_order_id") or "")
        if existing_order_id and not self.exchange.cancel_order(self.symbol, existing_order_id):
            self.trigger_protection("trend_dca_protective_stop_cancel_failed", {
                "order_id": existing_order_id,
            })
            return False
        stop_order = self.exchange.place_protective_stop(
            self.symbol,
            self.position["side"],
            spec["amount"],
            spec["stop_price"],
        )
        if not stop_order or not stop_order.get("execution_ok"):
            self.trigger_protection("trend_dca_protective_stop_failed", {
                "stop_price": spec["stop_price"],
                "amount": spec["amount"],
            })
            return False
        self.position["protective_stop_order_id"] = stop_order.get("id")
        self.position["protective_stop_price"] = spec["stop_price"]
        self.position["protective_stop_status"] = stop_order.get("status", "open")
        return True

    def _validate_protective_stop(self):
        if not self.position:
            return True
        spec = self._protective_stop_spec()
        if not spec:
            self.trigger_protection("trend_dca_protective_stop_invalid", {
                "reason": "invalid_local_spec",
            })
            return False
        order_id = str(self.position.get("protective_stop_order_id") or "")
        if not order_id:
            self.trigger_protection("trend_dca_protective_stop_missing", {
                "reason": "missing_order_id",
            })
            return False
        open_orders = self.exchange.get_open_orders(self.symbol)
        live = next((o for o in open_orders if str(o.get("id") or "") == order_id), None)
        if not live:
            self.trigger_protection("trend_dca_protective_stop_missing", {
                "reason": "order_not_found",
                "order_id": order_id,
            })
            return False
        live_side = str(self._order_field(live, "side", default="") or "").lower()
        live_amount = self._order_float_field(live, "remaining", "amount", "qty", "origQty", default=0.0)
        live_stop_price = self._order_float_field(
            live,
            "stop_price",
            "stopPrice",
            "triggerPrice",
            "trigger_price",
            "price",
            default=0.0,
        )
        live_reduce_only = self._order_flag(live, "reduceOnly", "reduce_only")
        amount_tol = max(spec["amount"] * 0.005, 1e-6)
        price_tol = max(spec["stop_price"] * 0.001, 1e-6)
        issues = []
        if live_side != spec["side"]:
            issues.append(f"side={live_side or 'missing'} expected={spec['side']}")
        if not live_reduce_only:
            issues.append("reduce_only=false")
        if abs(live_amount - spec["amount"]) > amount_tol:
            issues.append(f"amount={live_amount:.6f} expected={spec['amount']:.6f}")
        if abs(live_stop_price - spec["stop_price"]) > price_tol:
            issues.append(f"stop_price={live_stop_price:.6f} expected={spec['stop_price']:.6f}")
        if issues:
            self.trigger_protection("trend_dca_protective_stop_invalid", {
                "order_id": order_id,
                "issues": issues,
            })
            return False
        self.position["protective_stop_price"] = spec["stop_price"]
        self.position["protective_stop_status"] = str(live.get("status", "open") or "open")
        return True

    def _clear_protective_stop(self):
        if not self.position:
            return True
        existing_order_id = str(self.position.get("protective_stop_order_id") or "")
        if existing_order_id and not self.exchange.cancel_order(self.symbol, existing_order_id):
            self.position["protective_stop_status"] = "cancel_failed"
            return False
        self.position["protective_stop_order_id"] = ""
        self.position["protective_stop_price"] = 0.0
        self.position["protective_stop_status"] = "cancelled"
        return True

    def _finalize_close(self, order: dict, reason: str):
        if not self.position:
            return

        side = self.position["side"]
        avg = float(self.position.get("avg_price", 0) or 0)
        close_price = float(order.get("average", 0) or 0)
        close_amount = float(order.get("filled", 0) or 0)

        fees_paid = float(self.position.get("fees_paid", 0) or 0) + float(order.get("fee_cost", 0) or 0)
        if side == "long":
            pnl = (close_price - avg) * close_amount
        else:
            pnl = (avg - close_price) * close_amount
        pnl -= fees_paid

        ledger.record_order(self.name, self.symbol, order, {"action": "close", "reason": reason})

        self.total_pnl += pnl
        self.pnl_history.append(pnl)
        self.trade_count += 1

        notifier.trade_close(
            self.name,
            self.symbol,
            side.upper(),
            pnl,
            reason,
            f"Layers: {self.layers_filled}/{CFG['dca_layers']} | "
            f"Avg: `{avg:.2f}` -> `{close_price:.2f}` | "
            f"Exec: `{order.get('execution_mode', 'UNKNOWN')}`",
        )

        if not self._clear_protective_stop():
            notifier.error(self.name, f"{self.symbol}: close confirmed but protective stop cancel failed")

        self.position = None
        self.layers_filled = 0
        self.peak_price = 0.0
        if reason == "STOP_LOSS":
            self.signal = None

        self._ensure_position_consistency(allow_missing=False)

    def get_check_interval(self) -> int:
        return CFG["check_interval"]

    def run(self):
        self.last_run = time.time()
        try:
            market = self._analyze_market()
            market_bias = market["bias"]
            prev_signal = self.signal
            self.signal = market_bias

            if market_bias and market_bias != prev_signal:
                logger.info(
                    f"[{self.name}] Bias {market_bias.upper()} | "
                    f"fast={market['fast']:.2f} slow={market['slow']:.2f} "
                    f"entry_ready={market['entry_ready']}"
                )

            if self.position and market_bias and market_bias != self.position["side"]:
                self._close("SIGNAL_REVERSAL")
                return

            if not self.position and market_bias and market["entry_ready"]:
                self._open_first(market_bias)
                return

            if self.position:
                if not self._validate_protective_stop():
                    return
                self._manage_position(market_bias)
        except Exception as e:
            logger.error(f"[{self.name}] Error: {e}")
            notifier.error(self.name, str(e))

    def _analyze_market(self) -> dict:
        limit = max(CFG["fast_ma"], CFG["slow_ma"], 20) + 6
        ohlcv = self.exchange.get_ohlcv(self.symbol, CFG["timeframe"], limit=limit)

        if len(ohlcv) < CFG["slow_ma"] + 3:
            return {
                "bias": self.signal,
                "entry_ready": False,
                "fast": 0.0,
                "slow": 0.0,
            }

        closes = np.array([c[4] for c in ohlcv], dtype=float)
        fast = float(np.mean(closes[-CFG["fast_ma"]:]))
        slow = float(np.mean(closes[-CFG["slow_ma"]:]))
        prev_fast = float(np.mean(closes[-CFG["fast_ma"] - 1:-1]))
        prev_slow = float(np.mean(closes[-CFG["slow_ma"] - 1:-1]))
        last_close = float(closes[-1])
        prev_close = float(closes[-2])

        pullback_band = max(CFG["layer_spacing_pct"] * 0.75, 0.005)
        bias = None
        entry_ready = False

        trend_up = fast > slow and fast >= prev_fast and slow >= prev_slow
        trend_down = fast < slow and fast <= prev_fast and slow <= prev_slow

        if trend_up:
            bias = "long"
            near_fast = fast <= last_close <= fast * (1 + pullback_band)
            recovering = last_close >= prev_close or last_close >= slow
            entry_ready = near_fast and recovering
        elif trend_down:
            bias = "short"
            near_fast = fast * (1 - pullback_band) <= last_close <= fast
            recovering = last_close <= prev_close or last_close <= slow
            entry_ready = near_fast and recovering

        if self.weight < 1.0:
            entry_ready = False

        return {
            "bias": bias,
            "entry_ready": entry_ready,
            "fast": fast,
            "slow": slow,
        }

    def _get_live_position(self) -> Optional[dict]:
        positions = self.exchange.get_positions(self.symbol)
        return next(
            (p for p in positions if p.get("symbol") == self.symbol and float(p.get("contracts", 0) or 0) > 0),
            None,
        )

    def get_unrealized_pnl(self) -> float:
        live = self._get_live_position()
        if not live:
            return 0.0
        return float(live.get("unrealized_pnl", 0) or 0)

    def current_strategy_notional(self) -> float:
        return self._position_notional()

    def max_strategy_notional(self) -> float:
        return self._max_position_budget()

    def _ensure_position_consistency(
        self,
        expected_side: Optional[str] = None,
        expected_amount: Optional[float] = None,
        allow_missing: bool = False,
    ):
        live = self._get_live_position()
        if not self.position:
            if live and not allow_missing:
                self.trigger_protection("trend_dca_residual_exchange_position", {
                    "live_side": live.get("side"),
                    "live_amount": float(live.get("contracts", 0) or 0),
                })
                raise ValueError(
                    f"Residual exchange position detected: {live.get('side')}={float(live.get('contracts', 0) or 0):.6f}"
                )
            return None

        if not live:
            if allow_missing:
                return None
            self.trigger_protection("trend_dca_missing_exchange_position", {
                "local_side": self.position.get("side"),
                "local_amount": float(self.position.get("total_amount", 0) or 0),
            })
            raise ValueError("Local position exists but exchange position is missing")

        local_side = self.position.get("side")
        live_side = live.get("side")
        if local_side and live_side != local_side:
            self.trigger_protection("trend_dca_side_mismatch", {
                "local_side": local_side,
                "exchange_side": live_side,
            })
            raise ValueError(f"Position side mismatch: local={local_side} exchange={live_side}")
        if expected_side and live_side != expected_side:
            self.trigger_protection("trend_dca_expected_side_mismatch", {
                "expected_side": expected_side,
                "exchange_side": live_side,
            })
            raise ValueError(f"Position side mismatch: expected={expected_side} exchange={live_side}")

        live_amount = float(live.get("contracts", 0) or 0)
        local_amount = float(self.position.get("total_amount", 0) or 0)
        target_amount = float(expected_amount if expected_amount is not None else local_amount)
        if abs(live_amount - target_amount) > 1e-4:
            self.trigger_protection("trend_dca_amount_mismatch", {
                "local_amount": target_amount,
                "exchange_amount": live_amount,
            })
            raise ValueError(f"Position amount mismatch: local={target_amount:.6f} exchange={live_amount:.6f}")
        return live

    def _max_position_budget(self) -> float:
        return self.capital * 0.95

    def _position_notional(self) -> float:
        if not self.position:
            return 0.0
        return sum(
            float(layer.get("price", 0) or 0) * float(layer.get("amount", 0) or 0)
            for layer in self.position.get("layers", [])
        )

    def _remaining_position_budget(self) -> float:
        return max(self._max_position_budget() - self._position_notional(), 0.0)

    def _requested_layer_budget(self) -> float:
        scaled_base = self.capital * CFG["base_amount_pct"] * max(self.weight, 0.25)
        return scaled_base * (CFG["layer_multiplier"] ** self.layers_filled)

    def _open_first(self, direction: str):
        self._ensure_position_consistency(allow_missing=False)
        price = self.exchange.get_price(self.symbol)
        base_usdt = min(
            self.capital * CFG["base_amount_pct"] * max(self.weight, 0.25),
            self._max_position_budget(),
        )
        if base_usdt <= 0:
            logger.warning(f"[{self.name}] Initial layer skipped: no budget available")
            return

        amount = base_usdt / price
        self.exchange.set_leverage(self.symbol, CFG["leverage"])
        self.exchange.set_margin_mode(self.symbol, "isolated")

        side = "buy" if direction == "long" else "sell"
        approval = self.request_trade_approval(
            self.symbol, side, "market", amount, price=price,
            risk_context={"action": "open_first", "direction": direction},
        )
        if not approval.get("approved", False):
            notifier.risk_alert("DCA开仓被风控拒绝", approval.get("reason", "unknown"), "跳过开仓")
            return
        order = self.exchange.market_order(self.symbol, side, amount)

        if not order or not order.get("execution_ok"):
            logger.warning(f"[{self.name}] First layer not confirmed")
            return

        filled_price = float(order.get("average", 0) or 0)
        filled_amount = float(order.get("filled", 0) or 0)
        if filled_price <= 0 or filled_amount <= 0:
            logger.warning(f"[{self.name}] First layer fill incomplete")
            return

        self.position = {
            "side": direction,
            "total_amount": filled_amount,
            "avg_price": filled_price,
            "layers": [{"price": filled_price, "amount": filled_amount}],
            "execution_mode": order.get("execution_mode", "UNKNOWN"),
            "fees_paid": float(order.get("fee_cost", 0) or 0),
            "slippage_pct": float(order.get("slippage_pct", 0) or 0),
            "protective_stop_order_id": "",
            "protective_stop_price": 0.0,
            "protective_stop_status": "pending",
        }
        ledger.record_order(self.name, self.symbol, order, {"action": "open_first", "direction": direction})
        if not self._sync_protective_stop():
            logger.warning(f"[{self.name}] Protective stop missing after open")
            if not self._emergency_flatten_unprotected_position("OPEN_FIRST"):
                notifier.error(self.name, f"{self.symbol}: first layer opened without a valid protective stop")
            return
        self._ensure_position_consistency(expected_side=direction, expected_amount=filled_amount)
        self.layers_filled = 1
        self.trade_count += 1
        self.peak_price = 0.0

        notifier.trade_open(
            self.name,
            self.symbol,
            direction.upper(),
            filled_amount,
            filled_price,
            f"Signal: MA{CFG['fast_ma']}/MA{CFG['slow_ma']} trend pullback | "
            f"Layer 1/{CFG['dca_layers']} | Exec: `{order.get('execution_mode', 'UNKNOWN')}`",
        )

    def _manage_position(self, market_bias: Optional[str]):
        self._ensure_position_consistency()
        price = self.exchange.get_price(self.symbol)
        avg = self.position["avg_price"]
        side = self.position["side"]

        pnl_pct = (price - avg) / avg if side == "long" else (avg - price) / avg

        if pnl_pct >= CFG["take_profit_pct"]:
            self._close("TAKE_PROFIT")
            return

        if pnl_pct >= CFG["trailing_stop_threshold"]:
            if side == "long":
                self.peak_price = max(self.peak_price, price)
                drawback = (self.peak_price - price) / self.peak_price if self.peak_price > 0 else 0
            else:
                if self.peak_price == 0:
                    self.peak_price = price
                self.peak_price = min(self.peak_price, price)
                drawback = (price - self.peak_price) / self.peak_price if self.peak_price > 0 else 0

            if drawback >= CFG["trailing_stop_pct"]:
                self._close(f"TRAILING_STOP peak={self.peak_price:.2f} drawback={drawback:.2%}")
                return
        else:
            self.peak_price = 0.0

        if pnl_pct <= -CFG["stop_loss_pct"]:
            self._close("STOP_LOSS")
            return

        if (
            pnl_pct < 0
            and self.layers_filled < CFG["dca_layers"]
            and market_bias == side
            and self.weight >= 1.0
        ):
            self.peak_price = 0.0
            target_layer = int(abs(pnl_pct) / CFG["layer_spacing_pct"]) + 1
            target_layer = min(target_layer, CFG["dca_layers"])
            if target_layer > self.layers_filled:
                self._add_layer(price)

    def _add_layer(self, price: float):
        self._ensure_position_consistency()
        remaining_budget = self._remaining_position_budget()
        if remaining_budget <= 0:
            logger.warning(f"[{self.name}] DCA layer skipped: position budget exhausted")
            return

        layer_usdt = min(self._requested_layer_budget(), remaining_budget)
        if layer_usdt <= 0:
            logger.warning(f"[{self.name}] DCA layer skipped: invalid layer budget")
            return

        amount = layer_usdt / price
        side_str = "buy" if self.position["side"] == "long" else "sell"
        approval = self.request_trade_approval(
            self.symbol, side_str, "market", amount, price=price,
            risk_context={"action": "add_layer", "layer": self.layers_filled + 1},
        )
        if not approval.get("approved", False):
            notifier.risk_alert("DCA加仓被风控拒绝", approval.get("reason", "unknown"), "跳过加仓")
            return
        order = self.exchange.market_order(self.symbol, side_str, amount)

        if not order or not order.get("execution_ok"):
            logger.warning(f"[{self.name}] DCA layer not confirmed")
            return

        filled_amount = float(order.get("filled", 0) or 0)
        filled_price = float(order.get("average", 0) or 0)
        if filled_amount <= 0 or filled_price <= 0:
            logger.warning(f"[{self.name}] DCA layer fill incomplete")
            return

        old_total = self.position["total_amount"]
        old_avg = self.position["avg_price"]
        new_total = old_total + filled_amount
        if new_total <= 0:
            logger.warning(f"[{self.name}] DCA layer produced invalid total amount")
            return

        new_avg = (old_avg * old_total + filled_price * filled_amount) / new_total
        self.position["total_amount"] = new_total
        self.position["avg_price"] = new_avg
        self.position["layers"].append({"price": filled_price, "amount": filled_amount})
        self.position["execution_mode"] = order.get(
            "execution_mode",
            self.position.get("execution_mode", "UNKNOWN"),
        )
        self.position["fees_paid"] = float(self.position.get("fees_paid", 0) or 0) + float(order.get("fee_cost", 0) or 0)
        self.position["slippage_pct"] = float(self.position.get("slippage_pct", 0) or 0) + float(order.get("slippage_pct", 0) or 0)
        ledger.record_order(self.name, self.symbol, order, {"action": "add_layer", "layer": self.layers_filled + 1})
        if not self._sync_protective_stop():
            logger.warning(f"[{self.name}] Protective stop missing after DCA layer")
            if not self._emergency_flatten_unprotected_position("ADD_LAYER"):
                notifier.error(self.name, f"{self.symbol}: layer added without a valid protective stop")
            return
        self._ensure_position_consistency(expected_side=self.position["side"], expected_amount=new_total)
        self.layers_filled += 1
        self.trade_count += 1
        self.peak_price = 0.0

        notifier.dca_layer(
            self.symbol,
            self.layers_filled,
            CFG["dca_layers"],
            filled_amount,
            filled_price,
            new_avg,
        )

    def _close(self, reason: str):
        if not self.position:
            return

        self._ensure_position_consistency()
        side = self.position["side"]
        amount = self.position["total_amount"]

        close_side = "sell" if side == "long" else "buy"
        order = self.exchange.market_order(self.symbol, close_side, amount, reduce_only=True)

        if not order or not order.get("execution_ok"):
            logger.warning(f"[{self.name}] Close not confirmed")
            if not self._validate_protective_stop():
                notifier.error(self.name, f"{self.symbol}: close failed and no valid protective stop remains")
            return

        close_price = float(order.get("average", 0) or 0)
        close_amount = float(order.get("filled", 0) or 0)
        if close_price <= 0 or close_amount <= 0:
            logger.warning(f"[{self.name}] Close fill incomplete")
            if not self._validate_protective_stop():
                notifier.error(self.name, f"{self.symbol}: close confirmation incomplete and protective stop is invalid")
            return

        tolerance = max(float(amount or 0) * 0.005, 1e-4)
        if abs(close_amount - float(amount or 0)) > tolerance:
            live = self._get_live_position()
            remaining_amount = float(live.get("contracts", 0) or 0) if live else max(float(amount or 0) - close_amount, 0.0)
            if remaining_amount > 1e-6:
                self._sync_partial_close_state(remaining_amount)
                self.trigger_protection("trend_dca_partial_close", {
                    "requested_amount": float(amount or 0),
                    "filled_amount": close_amount,
                    "remaining_amount": remaining_amount,
                })
                if not self._sync_protective_stop():
                    self._emergency_flatten_unprotected_position("PARTIAL_CLOSE")
                return

        self._finalize_close(order, reason)

    def stop(self):
        logger.info(f"[{self.name}] Stopping...")
        if self.position:
            self._close("STRATEGY_STOP")
        else:
            self._ensure_position_consistency(allow_missing=False)

    def get_status(self) -> dict:
        base = super().get_status()
        base.update({
            "signal": self.signal or "FLAT",
            "layers": f"{self.layers_filled}/{CFG['dca_layers']}",
            "position": bool(self.position),
        })
        return base
