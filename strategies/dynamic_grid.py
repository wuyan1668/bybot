"""Strategy 2: dynamic grid trading."""

import logging
import time
from typing import Dict, List

import numpy as np

from config import DYNAMIC_GRID as CFG
from notifier import notifier
from execution_ledger import ledger
from strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


class DynamicGridStrategy(BaseStrategy):
    def __init__(self, exchange, capital: float):
        super().__init__("DynamicGrid", exchange, capital)
        self.symbol = CFG["symbol"]
        self.grid_center = 0.0
        self.grid_lines: List[float] = []
        self.active_orders: Dict[str, dict] = {}
        self.open_legs: Dict[str, dict] = {}
        self.grid_profits: List[float] = []

    def export_state(self) -> dict:
        state = super().export_state()
        state.update({
            "symbol": self.symbol,
            "grid_center": self.grid_center,
            "grid_lines": list(self.grid_lines),
            "active_orders": self.active_orders,
            "open_legs": self.open_legs,
            "grid_profits": list(self.grid_profits),
        })
        return state

    def import_state(self, state: dict):
        super().import_state(state)
        state = state or {}
        self.grid_center = float(state.get("grid_center", 0.0) or 0.0)
        self.grid_lines = list(state.get("grid_lines", []))
        self.active_orders = dict(state.get("active_orders", {}))
        self.open_legs = dict(state.get("open_legs", {}))
        self.grid_profits = list(state.get("grid_profits", []))

    def get_check_interval(self) -> int:
        return CFG["update_interval"]

    def run(self):
        self.last_run = time.time()
        try:
            price = self.exchange.get_price(self.symbol)

            self._assert_state_consistency()

            if self.grid_center == 0:
                self._ensure_flat("DynamicGrid init blocked")
                self._init_grid(price)
                return

            self._ensure_grid_orders_present(price)
            self._maintain_grid(price)
            self._assert_state_consistency()

            if not self.open_legs and self.grid_center > 0:
                drift = abs(price - self.grid_center) / self.grid_center
                if drift > CFG["recenter_threshold"]:
                    logger.info(f"[{self.name}] Drift {drift:.2%}, resetting grid")
                    self._reset_grid(price)
        except Exception as e:
            logger.error(f"[{self.name}] Error: {e}")
            notifier.error(self.name, str(e))

    def _calc_atr(self) -> float:
        try:
            ohlcv = self.exchange.get_ohlcv(self.symbol, "1h", limit=CFG["atr_period"] + 2)
            if len(ohlcv) < 3:
                return 0
            trs = []
            for i in range(1, len(ohlcv)):
                high = ohlcv[i][2]
                low = ohlcv[i][3]
                prev_close = ohlcv[i - 1][4]
                trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
            return float(np.mean(trs[-CFG["atr_period"]:]))
        except Exception:
            return 0

    def _calc_spacing(self, price: float) -> float:
        atr = self._calc_atr()
        if atr > 0:
            return max((atr * CFG["atr_multiplier"]) / price, CFG["grid_spacing_pct"])
        return CFG["grid_spacing_pct"]

    def _build_grid_lines(self, center_price: float) -> List[float]:
        spacing = self._calc_spacing(center_price)
        half = CFG["grid_count"] // 2
        lines = []
        for i in range(-half, half + 1):
            if i == 0:
                continue
            lines.append(center_price * (1 + i * spacing))
        lines.sort()
        return lines

    def _live_positions(self) -> List[dict]:
        return [
            p for p in self.exchange.get_positions(self.symbol)
            if p.get("symbol") == self.symbol and float(p.get("contracts", 0) or 0) > 0
        ]

    def get_unrealized_pnl(self) -> float:
        return sum(float(pos.get("unrealized_pnl", 0) or 0) for pos in self._live_positions())

    def current_strategy_notional(self) -> float:
        return sum(
            abs(float(leg.get("entry_price", 0) or 0) * float(leg.get("amount", 0) or 0))
            for leg in self.open_legs.values()
        )

    def max_strategy_notional(self) -> float:
        entry_slots = int(CFG.get("max_open_orders", 0) or 0)
        if entry_slots <= 0:
            entry_slots = int(CFG.get("grid_count", 0) or 0)
        order_usdt = float(self.capital or 0.0) * float(CFG.get("order_amount_pct", 0) or 0) * max(float(self.weight or 0.0), 0.0)
        return max(entry_slots, 0) * order_usdt * 2.0

    @staticmethod
    def _position_totals(positions: List[dict]) -> dict:
        totals = {"long": 0.0, "short": 0.0}
        for pos in positions:
            side = str(pos.get("side") or "").lower()
            contracts = float(pos.get("contracts", 0) or 0)
            if side in totals and contracts > 0:
                totals[side] += contracts
        return totals

    def _normalize_live_order(self, order: dict) -> dict:
        info = order or {}
        return {
            "id": str(info.get("id") or ""),
            "side": str(info.get("side") or "").lower(),
            "price": float(info.get("price", 0) or 0),
            "amount": float(info.get("amount", 0) or info.get("remaining", 0) or 0),
            "status": str(info.get("status") or "").lower(),
            "reduce_only": bool(info.get("reduceOnly", False)),
        }

    def _active_order_issue(self) -> str:
        if not self.active_orders:
            return ""
        live_orders = self.exchange.get_open_orders(self.symbol)
        live_by_id = {
            item["id"]: item
            for item in (self._normalize_live_order(order) for order in live_orders)
            if item["id"] and item["status"] in ("", "open", "new")
        }
        for order_id, expected in self.active_orders.items():
            live = live_by_id.get(str(order_id))
            if not live:
                continue
            expected_side = str(expected.get("side") or "").lower()
            expected_price = float(expected.get("price", 0) or 0)
            expected_amount = float(expected.get("amount", 0) or 0)
            expected_reduce_only = expected.get("role", "entry") == "exit"
            if expected_side and live["side"] and live["side"] != expected_side:
                return f"Active order side mismatch: {order_id} local={expected_side} exchange={live['side']}"
            if expected_price > 0 and live["price"] > 0 and abs(live["price"] - expected_price) > 1e-2:
                return f"Active order price mismatch: {order_id} local={expected_price:.8f} exchange={live['price']:.8f}"
            if expected_amount > 0 and live["amount"] > 0 and abs(live["amount"] - expected_amount) > 1e-6:
                return f"Active order amount mismatch: {order_id} local={expected_amount:.8f} exchange={live['amount']:.8f}"
            if expected_reduce_only and not live["reduce_only"]:
                return f"Exit order is not reduce-only on exchange: {order_id}"
        return ""

    def _assert_state_consistency(self):
        inventory_issue = self._get_inventory_issue()
        if inventory_issue:
            self.trigger_protection("grid_inventory_mismatch", {"issue": inventory_issue})
            raise ValueError(inventory_issue)
        active_order_issue = self._active_order_issue()
        if active_order_issue:
            self.trigger_protection("grid_active_order_mismatch", {"issue": active_order_issue})
            raise ValueError(active_order_issue)

    def _tracked_totals(self) -> dict:
        totals = {"long": 0.0, "short": 0.0}
        for leg in self.open_legs.values():
            side = str(leg.get("side") or "").lower()
            amount = float(leg.get("amount", 0) or 0)
            if side in totals and amount > 0:
                totals[side] += amount
        return totals

    @staticmethod
    def _totals_match(left: dict, right: dict, tol: float = 1e-4) -> bool:
        return (
            abs(float(left.get("long", 0) or 0) - float(right.get("long", 0) or 0)) <= tol
            and abs(float(left.get("short", 0) or 0) - float(right.get("short", 0) or 0)) <= tol
        )

    def _format_live_positions(self, positions: List[dict]) -> str:
        return ", ".join(
            f"{p.get('side')}={float(p.get('contracts', 0) or 0):.6f}"
            for p in positions
        )

    def _get_inventory_issue(self) -> str:
        live_positions = self._live_positions()
        tracked_totals = self._tracked_totals()
        live_totals = self._position_totals(live_positions)

        if not self.open_legs and not live_positions:
            return ""
        if not self.open_legs and live_positions:
            return f"Residual exchange exposure detected: {self._format_live_positions(live_positions)}"
        if self.open_legs and not live_positions:
            return "Tracked grid inventory exists but exchange position is missing"
        if not self._totals_match(tracked_totals, live_totals):
            return (
                "Tracked grid inventory mismatch: "
                f"tracked long={tracked_totals['long']:.6f} short={tracked_totals['short']:.6f}, "
                f"exchange long={live_totals['long']:.6f} short={live_totals['short']:.6f}"
            )
        missing_orders = []
        for leg_id, leg in self.open_legs.items():
            exit_order_id = str(leg.get("exit_order_id") or "")
            if not exit_order_id or exit_order_id not in self.active_orders:
                missing_orders.append(str(leg_id))
        if missing_orders:
            return f"Legs missing exit orders: {', '.join(missing_orders[:5])}"
        return ""

    def _ensure_flat(self, reason: str):
        issue = self._get_inventory_issue()
        if issue:
            raise ValueError(f"{reason}: {issue}")
        if self.open_legs:
            raise ValueError(f"{reason}: tracked grid inventory exists")

    def _record_active_order(self, order: dict, side: str, amount: float, price: float,
                             role: str = "entry", leg_id: str = "", entry_side: str = "",
                             entry_price: float = 0.0):
        if not order or not order.get("execution_ok"):
            return
        order_id = str(order.get("id") or "")
        order_price = float(order.get("price", price) or price)
        order_amount = float(order.get("amount", amount) or amount)
        if not order_id or order_price <= 0 or order_amount <= 0:
            raise ValueError(f"Invalid {role} order confirmation")
        self.active_orders[order_id] = {
            "price": order_price,
            "side": side,
            "amount": order_amount,
            "role": role,
            "leg_id": leg_id,
            "entry_side": entry_side,
            "entry_price": entry_price,
            "execution_mode": order.get("execution_mode", "UNKNOWN"),
            "fee_cost": float(order.get("fee_cost", 0) or 0),
            "slippage_pct": float(order.get("slippage_pct", 0) or 0),
        }
        ledger.record_order(self.name, self.symbol, order, {"role": role, "side": side, "price": price})

    def _ensure_grid_orders_present(self, current_price: float):
        if not self.grid_lines:
            raise ValueError("Grid state invalid: missing grid lines")
        if self.active_orders:
            return
        live_orders = self.exchange.get_open_orders(self.symbol)
        live_ids = {str(order.get("id")) for order in live_orders if order.get("id")}
        if live_ids:
            raise ValueError(
                f"Grid state mismatch: local active_orders empty but exchange has {len(live_ids)} open orders"
            )
        if self.open_legs:
            raise ValueError("Grid state invalid: open inventory exists but no active orders are tracked")
        logger.warning(f"[{self.name}] No active grid orders found, rebuilding grid")
        self._place_orders(current_price)
        if not self.active_orders:
            raise ValueError("Grid rebuild failed: no confirmed active orders")

    def rebuild_from_recovery(self, current_price: float):
        self._ensure_flat("DynamicGrid rebuild blocked")
        self.exchange.cancel_all_orders(self.symbol)
        self.active_orders.clear()
        self.open_legs.clear()
        self.grid_center = 0.0
        self.grid_lines = []
        self._init_grid(current_price)

    def _init_grid(self, price: float):
        logger.info(f"[{self.name}] Init grid @ {price:.2f}")

        self.exchange.set_leverage(self.symbol, CFG["leverage"])
        self.exchange.set_margin_mode(self.symbol, "isolated")

        self.grid_center = price
        self.grid_lines = self._build_grid_lines(price)
        self._place_orders(price)

        spacing = self._calc_spacing(price)
        notifier.trade_open(
            self.name, self.symbol, "GRID",
            0, price,
            f"Grid lines `{len(self.grid_lines)}` | "
            f"Spacing `{spacing:.4%}` | "
            f"Range `{self.grid_lines[0]:.2f} - {self.grid_lines[-1]:.2f}`"
        )

    def _selected_grid_lines(self, current_price: float) -> List[float]:
        lines = list(self.grid_lines)
        max_open_orders = int(CFG.get("max_open_orders", 0) or 0)
        if max_open_orders > 0 and len(lines) > max_open_orders:
            lines = sorted(
                sorted(lines, key=lambda line: abs(line - current_price))[:max_open_orders]
            )
        return lines

    def _place_orders(self, current_price: float):
        self._ensure_flat("Cannot place grid orders while residual position exists")
        order_usdt = self.capital * CFG["order_amount_pct"] * self.weight

        for grid_price in self._selected_grid_lines(current_price):
            amount = order_usdt / grid_price
            side = "buy" if grid_price < current_price else "sell"
            approval = self.request_trade_approval(
                self.symbol, side, "limit", amount, price=grid_price,
                risk_context={"action": "grid_entry", "grid_price": grid_price},
            )
            if not approval.get("approved", False):
                logger.warning(f"[{self.name}] Grid order blocked by risk: {side} {amount}@{grid_price} | {approval.get('reason')}")
                continue
            order = self.exchange.limit_order(self.symbol, side, amount, grid_price)
            if not order or not order.get("execution_ok"):
                logger.warning(f"[{self.name}] Grid order not confirmed: {side} {amount}@{grid_price}")
                continue
            self._record_active_order(order, side, amount, grid_price, role="entry")

        logger.info(f"[{self.name}] Placed {len(self.active_orders)} grid orders")

    def _calc_counter_price(self, entry_price: float, entry_side: str) -> float:
        spacing_abs = entry_price * self._calc_spacing(max(entry_price, self.grid_center or entry_price))
        if entry_side == "buy":
            return entry_price + spacing_abs
        return entry_price - spacing_abs

    def _replace_entry_order(self, side: str, amount: float, price: float):
        approval = self.request_trade_approval(
            self.symbol, side, "limit", amount, price=price,
            risk_context={"action": "grid_replace_entry"},
        )
        if not approval.get("approved", False):
            raise ValueError(f"Replacement entry risk rejected: {approval.get('reason', 'unknown')}")
        order = self.exchange.limit_order(self.symbol, side, amount, price)
        if not order or not order.get("execution_ok"):
            raise ValueError(f"Replacement entry order not confirmed: {side} {amount}@{price}")
        self._record_active_order(order, side, amount, price, role="entry")

    def _replace_exit_order(self, leg: dict):
        counter_side = "sell" if leg["side"] == "long" else "buy"
        order = self.exchange.limit_order(
            self.symbol, counter_side, leg["amount"], leg["exit_price"], reduce_only=True
        )
        if not order or not order.get("execution_ok"):
            raise ValueError(
                f"Replacement exit order not confirmed: {counter_side} {leg['amount']}@{leg['exit_price']}"
            )
        leg["exit_order_id"] = str(order.get("id") or "")
        leg["execution_mode"] = order.get("execution_mode", leg.get("execution_mode", "UNKNOWN"))
        self._record_active_order(
            order,
            counter_side,
            leg["amount"],
            leg["exit_price"],
            role="exit",
            leg_id=leg["leg_id"],
            entry_side=leg["entry_side"],
            entry_price=leg["entry_price"],
        )

    def _handle_rejected_order(self, order_id: str, info: dict):
        role = info.get("role", "entry")
        if role == "entry":
            logger.warning(f"[{self.name}] Replacing rejected grid entry: {order_id}")
            self._replace_entry_order(info["side"], float(info["amount"]), float(info["price"]))
            return

        leg_id = str(info.get("leg_id") or "")
        leg = self.open_legs.get(leg_id)
        if not leg:
            raise ValueError(f"Missing leg for rejected exit order: {order_id}")
        logger.warning(f"[{self.name}] Replacing rejected grid exit: {order_id}")
        self._replace_exit_order(leg)

    def _handle_filled_entry(self, order_id: str, info: dict, classified: dict):
        filled_amount = float(classified.get("filled", 0) or info.get("amount", 0) or 0)
        filled_price = float(
            classified.get("average", 0) or classified.get("price", 0) or info.get("price", 0) or 0
        )
        if filled_amount <= 0 or filled_price <= 0:
            raise ValueError(f"Filled grid entry missing price/amount: {order_id}")

        leg_id = str(order_id)
        entry_side = info["side"]
        position_side = "long" if entry_side == "buy" else "short"
        counter_side = "sell" if entry_side == "buy" else "buy"
        counter_price = self._calc_counter_price(filled_price, entry_side)

        counter_order = self.exchange.limit_order(
            self.symbol, counter_side, filled_amount, counter_price, reduce_only=True
        )
        if not counter_order or not counter_order.get("execution_ok"):
            raise ValueError(
                f"Counter order not confirmed after filled grid entry: {counter_side} {filled_amount}@{counter_price}"
            )

        self.open_legs[leg_id] = {
            "leg_id": leg_id,
            "side": position_side,
            "amount": filled_amount,
            "entry_price": filled_price,
            "entry_side": entry_side,
            "entry_order_id": order_id,
            "exit_price": counter_price,
            "exit_order_id": str(counter_order.get("id") or ""),
            "execution_mode": counter_order.get("execution_mode", info.get("execution_mode", "UNKNOWN")),
        }
        self._record_active_order(
            counter_order,
            counter_side,
            filled_amount,
            counter_price,
            role="exit",
            leg_id=leg_id,
            entry_side=entry_side,
            entry_price=filled_price,
        )
        logger.info(
            f"[{self.name}] Entry filled: {entry_side.upper()} @ {filled_price:.2f} "
            f"-> exit {counter_side.upper()} @ {counter_price:.2f}"
        )

    def _handle_filled_exit(self, order_id: str, info: dict, classified: dict):
        leg_id = str(info.get("leg_id") or "")
        leg = self.open_legs.get(leg_id)
        if not leg:
            raise ValueError(f"Missing leg for filled exit order: {order_id}")

        close_amount = float(classified.get("filled", 0) or info.get("amount", 0) or leg["amount"])
        close_price = float(
            classified.get("average", 0) or classified.get("price", 0) or info.get("price", 0) or leg["exit_price"]
        )
        if close_amount <= 0 or close_price <= 0:
            raise ValueError(f"Filled grid exit missing price/amount: {order_id}")

        if leg["side"] == "long":
            pnl = (close_price - leg["entry_price"]) * close_amount
        else:
            pnl = (leg["entry_price"] - close_price) * close_amount

        fee_cost = float(classified.get("fee_cost", 0) or info.get("fee_cost", 0) or 0)
        pnl = pnl - fee_cost
        self.grid_profits.append(pnl)
        self.total_pnl = sum(self.grid_profits)
        self.trade_count += 1
        
        remaining_leg_amount = round(leg["amount"] - close_amount, 8)
        if remaining_leg_amount > 0:
            leg["amount"] = remaining_leg_amount
            logger.info(f"[{self.name}] Partial exit filled: {close_amount} @ {close_price:.2f}, remaining {remaining_leg_amount}")
            self._replace_exit_order(leg)
        else:
            self.open_legs.pop(leg_id, None)

        self._replace_entry_order(leg["entry_side"], close_amount, leg["entry_price"])

        logger.info(
            f"[{self.name}] Exit filled ({close_amount}): {info['side'].upper()} @ {close_price:.2f} "
            f"-> entry restored {leg['entry_side'].upper()} @ {leg['entry_price']:.2f}"
        )
        notifier.grid_fill(self.symbol, leg["entry_side"], leg["entry_price"], pnl)

    def _maintain_grid(self, current_price: float):
        del current_price
        try:
            open_orders = self.exchange.get_open_orders(self.symbol)
            open_ids = {str(order.get("id")) for order in open_orders if order.get("id")}

            for order_id in list(self.active_orders.keys()):
                if str(order_id) in open_ids:
                    continue

                info = self.active_orders.get(order_id)
                if not info:
                    continue

                classified = self.exchange.classify_order(
                    order_id,
                    self.symbol,
                    expected_amount=float(info.get("amount", 0) or 0),
                    expected_price=float(info.get("price", 0) or 0),
                )
                state = str(classified.get("execution_state") or "unknown").lower()

                if state in ("open", "new"):
                    continue
                if state in ("uncertain", "unknown"):
                    self.trigger_protection("grid_order_uncertain", {
                        "order_id": order_id,
                        "role": info.get("role", "entry"),
                        "state": state,
                    })
                    raise ValueError(
                        f"Grid order requires manual review: id={order_id} "
                        f"role={info.get('role', 'entry')} state={state}"
                    )

                self.active_orders.pop(order_id, None)

                if state == "rejected":
                    self._handle_rejected_order(order_id, info)
                    continue
                if state not in ("filled", "partial"):
                    raise ValueError(
                        f"Unexpected grid order state: id={order_id} role={info.get('role', 'entry')} state={state}"
                    )

                if info.get("role", "entry") == "entry":
                    self._handle_filled_entry(order_id, info, classified)
                    if state == "partial":
                        remaining = float(classified.get("remaining", 0) or 0)
                        if remaining > 0:
                            logger.info(f"[{self.name}] Entry partial fill: replacing remainder {remaining} @ {info['price']}")
                            self._replace_entry_order(info["side"], remaining, float(info["price"]))
                else:
                    self._handle_filled_exit(order_id, info, classified)

        except Exception as e:
            logger.error(f"[{self.name}] Maintain error: {e}")
            raise

    def _reset_grid(self, new_center: float):
        if self.open_legs:
            raise ValueError("Grid reset blocked: tracked grid inventory exists")
        self.exchange.cancel_all_orders(self.symbol)
        self.active_orders.clear()
        self._ensure_flat("Grid reset blocked")
        self.grid_center = new_center
        self.grid_lines = self._build_grid_lines(new_center)
        self._place_orders(new_center)
        logger.info(f"[{self.name}] Grid reset @ {new_center:.2f}")

    def stop(self):
        logger.info(f"[{self.name}] Stopping...")
        self.exchange.cancel_all_orders(self.symbol)
        if not self.exchange.close_position(self.symbol):
            raise ValueError("Grid stop failed to close exchange position")
        self.active_orders.clear()
        self.open_legs.clear()
