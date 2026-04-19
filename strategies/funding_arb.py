"""Strategy 1: funding-rate arbitrage."""

import logging
import time
from typing import Dict, Optional

from config import EXCHANGE, FUNDING_ARB as CFG
from notifier import notifier
from execution_ledger import ledger
from strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


class FundingArbStrategy(BaseStrategy):
    def __init__(self, exchange, capital: float):
        super().__init__("FundingArb", exchange, capital)
        self.positions: Dict[str, dict] = {}

    @staticmethod
    def _spot_asset(symbol: str) -> str:
        base = symbol.split(":")[0]
        return base.split("/")[0]

    def configured_symbols(self) -> list[str]:
        return list(CFG.get("symbols", []))

    def is_live_blocked(self) -> bool:
        return EXCHANGE.get("mode") == "live" and CFG.get("live_confirm") != "ENABLE_FUNDING_ARB_LIVE"

    def export_state(self) -> dict:
        state = super().export_state()
        state.update({
            "positions": self.positions,
        })
        return state

    def import_state(self, state: dict):
        super().import_state(state)
        loaded = dict((state or {}).get("positions", {}))
        self.positions = {}
        for symbol, position in loaded.items():
            item = dict(position or {})
            item.setdefault("spot_amount", float(item.get("amount", 0) or 0))
            item.setdefault("realized_rebalance_pnl", 0.0)
            item.setdefault("recovery_pending", bool(item.get("funding_estimated")))
            item.setdefault("spot_asset", self._spot_asset(symbol))
            item.setdefault("actual_funding_collected", 0.0)
            item.setdefault("funding_reconciliation_status", "unverified")
            item.setdefault("last_funding_sync_ts", 0)
            item.setdefault("fees_paid", 0.0)
            item.setdefault("slippage_pct", 0.0)
            self.positions[symbol] = item

    def _get_live_spot_balance(self, symbol: str) -> float:
        asset = self._spot_asset(symbol)
        spot = self.exchange.get_spot_balance(asset)
        if not spot:
            return 0.0
        return float(spot.get("total", 0) or 0)

    def _hedge_amount_tolerance(self, expected_amount: float) -> float:
        expected_amount = abs(float(expected_amount or 0))
        rel_tol = max(float(CFG.get("hedge_rel_tolerance", 0.00001) or 0.0), 0.0)
        abs_tol = max(float(CFG.get("hedge_abs_tolerance", 0.001) or 0.0), 0.0)
        return max(abs_tol, expected_amount * rel_tol)

    def _sync_actual_funding(self, symbol: str, position: dict):
        last_sync_ts = float(position.get("last_funding_sync_ts", 0) or 0)
        since_ms = int(last_sync_ts * 1000) + 1 if last_sync_ts > 0 else None
        funding = self.exchange.get_funding_income(symbol, since=since_ms)
        if not funding.get("ok"):
            position["funding_reconciliation_status"] = "unverified"
            notifier.error(self.name, f"{symbol}: funding reconciliation unavailable ({funding.get('error', 'unknown')})")
            return

        items = list(funding.get("items", []) or [])
        delta = sum(float(item.get("amount", 0) or 0) for item in items)
        if delta:
            position["actual_funding_collected"] = float(position.get("actual_funding_collected", 0) or 0) + delta
            ledger.record_funding(self.name, symbol, delta, {
                "items": items,
                "source": funding.get("source"),
            })

        position["funding_reconciliation_status"] = "verified"
        if items:
            latest_ts = max(int(item.get("timestamp", 0) or 0) for item in items)
            if latest_ts > 0:
                position["last_funding_sync_ts"] = latest_ts / 1000

    def _ensure_hedge_consistency(self, symbol: str, position: dict):
        live = self._get_live_position(symbol)
        live_spot = self._get_live_spot_balance(symbol)
        expected_short = float(position.get("amount", 0) or 0)
        expected_spot = float(position.get("spot_amount", expected_short) or expected_short)
        if not live:
            self.trigger_protection("funding_hedge_missing", {
                "symbol": symbol,
                "expected_short": expected_short,
            })
            raise ValueError(f"{symbol}: hedge missing futures short position")
        live_short = float(live.get("contracts", 0) or 0)
        short_gap = abs(live_short - expected_short)
        short_tol = self._hedge_amount_tolerance(expected_short)
        if short_gap > short_tol:
            self.trigger_protection("funding_futures_hedge_mismatch", {
                "symbol": symbol,
                "expected_short": expected_short,
                "exchange_short": live_short,
                "delta": live_short - expected_short,
                "tolerance": short_tol,
            })
            raise ValueError(
                f"{symbol}: futures hedge mismatch local={expected_short:.6f} "
                f"exchange={live_short:.6f} delta={live_short - expected_short:+.6f} "
                f"tol={short_tol:.6f}"
            )
        spot_gap = abs(live_spot - expected_spot)
        spot_tol = self._hedge_amount_tolerance(expected_spot)
        if spot_gap > spot_tol:
            self.trigger_protection("funding_spot_hedge_mismatch", {
                "symbol": symbol,
                "expected_spot": expected_spot,
                "exchange_spot": live_spot,
                "delta": live_spot - expected_spot,
                "tolerance": spot_tol,
            })
            raise ValueError(
                f"{symbol}: spot hedge mismatch local={expected_spot:.6f} "
                f"exchange={live_spot:.6f} delta={live_spot - expected_spot:+.6f} "
                f"tol={spot_tol:.6f}"
            )

    def get_check_interval(self) -> int:
        return CFG["check_interval"]

    def get_unrealized_pnl(self) -> float:
        if not self.positions:
            return 0.0
        tracked_symbols = set(self.positions.keys())
        live_positions = self.exchange.get_positions()
        futures_unrealized = sum(
            float(pos.get("unrealized_pnl", 0) or 0)
            for pos in live_positions
            if pos.get("symbol") in tracked_symbols
        )
        spot_unrealized = 0.0
        for symbol, position in self.positions.items():
            spot_amount = float(position.get("spot_amount", position.get("amount", 0)) or 0)
            spot_entry_price = float(position.get("spot_entry_price", position.get("entry_price", 0)) or 0)
            if spot_amount <= 0 or spot_entry_price <= 0:
                continue
            try:
                mark_price = float(self.exchange.get_price(symbol) or 0)
            except Exception:
                mark_price = 0.0
            if mark_price <= 0:
                continue
            spot_unrealized += (mark_price - spot_entry_price) * spot_amount
        return futures_unrealized + spot_unrealized

    def current_strategy_notional(self) -> float:
        total = 0.0
        for position in self.positions.values():
            futures_amount = float(position.get("amount", 0) or 0)
            futures_entry = float(position.get("entry_price", 0) or 0)
            spot_amount = float(position.get("spot_amount", futures_amount) or futures_amount)
            spot_entry = float(position.get("spot_entry_price", futures_entry) or futures_entry)
            total += abs(futures_amount * futures_entry)
            total += abs(spot_amount * spot_entry)
        return total

    def max_strategy_notional(self) -> float:
        max_positions = max(int(CFG.get("max_positions", 1) or 1), 1)
        position_ratio = max(float(CFG.get("position_ratio", 0) or 0), 0.0)
        weight = max(float(self.weight or 0.0), 0.0)
        return float(self.capital or 0.0) * position_ratio * max_positions * weight * 2.0

    def _position_ready_for_new_opens(self) -> bool:
        if not self.positions:
            return True
        return all(not pos.get("recovery_pending", False) for pos in self.positions.values())

    def _get_live_position(self, symbol: str) -> Optional[dict]:
        positions = self.exchange.get_positions(symbol)
        return next(
            (p for p in positions if p.get("symbol") == symbol and float(p.get("contracts", 0) or 0) > 0),
            None,
        )

    def _rollback_spot_open(self, symbol: str, amount: float) -> bool:
        rollback = self.exchange.spot_market_order(symbol, "sell", amount, reduce_only=True)
        return bool(rollback and rollback.get("execution_ok"))

    def _neutralize_open_mismatch(self, symbol: str, spot_amount: float, hedge_amount: float) -> tuple[float, float]:
        gap = float(spot_amount or 0) - float(hedge_amount or 0)
        if abs(gap) <= 1e-6:
            return spot_amount, hedge_amount

        if gap > 0:
            rollback = self.exchange.spot_market_order(symbol, "sell", gap, reduce_only=True)
            if rollback and rollback.get("execution_ok"):
                logger.warning(f"[{self.name}] Trimmed excess spot after hedge mismatch: {symbol} amount={gap:.6f}")
                return hedge_amount, hedge_amount
            notifier.error(self.name, f"{symbol}: excess spot exposure remains after hedge mismatch ({gap:.6f})")
            return spot_amount, hedge_amount

        rollback = self.exchange.market_order(symbol, "buy", abs(gap), reduce_only=True)
        if rollback and rollback.get("execution_ok"):
            logger.warning(f"[{self.name}] Trimmed excess futures short after hedge mismatch: {symbol} amount={abs(gap):.6f}")
            return spot_amount, spot_amount
        notifier.error(self.name, f"{symbol}: excess futures short remains after hedge mismatch ({abs(gap):.6f})")
        return spot_amount, hedge_amount

    def _restore_short_hedge(self, symbol: str, amount: float, close_price: float, position: dict) -> bool:
        reopen = self.exchange.market_order(symbol, "sell", amount)
        if not reopen or not reopen.get("execution_ok"):
            return False

        reopened_price = float(reopen.get("average", 0) or 0)
        reopened_amount = float(reopen.get("filled", 0) or 0)
        if reopened_price <= 0 or reopened_amount <= 0:
            return False

        realized_now = (float(position.get("entry_price", 0) or 0) - close_price) * amount
        position["realized_rebalance_pnl"] = float(position.get("realized_rebalance_pnl", 0) or 0) + realized_now
        position["entry_price"] = reopened_price
        position["amount"] = reopened_amount
        position["open_time"] = time.time()
        position["execution_mode"] = reopen.get("execution_mode", position.get("execution_mode", "UNKNOWN"))
        position["recovery_pending"] = False

        adjusted_spot_amount, adjusted_hedge_amount = self._neutralize_open_mismatch(
            symbol,
            float(position.get("spot_amount", reopened_amount) or reopened_amount),
            reopened_amount,
        )
        position["spot_amount"] = adjusted_spot_amount
        position["amount"] = adjusted_hedge_amount
        return True

    def run(self):
        self.last_run = time.time()
        if self.is_live_blocked():
            logger.warning(f"[{self.name}] Live mode blocked until FUNDING_LIVE_CONFIRM=ENABLE_FUNDING_ARB_LIVE")
            return

        allow_new_opens = self._position_ready_for_new_opens()
        if not allow_new_opens:
            logger.warning(f"[{self.name}] Recovered funding state detected, only managing existing positions")

        for symbol in CFG["symbols"]:
            try:
                self._process_symbol(symbol, allow_new_opens=allow_new_opens)
            except Exception as e:
                logger.error(f"[{self.name}] Error {symbol}: {e}")
                notifier.error(self.name, f"{symbol}: {e}")

    def _process_symbol(self, symbol: str, allow_new_opens: bool = True):
        rate_info = self.exchange.get_funding_rate(symbol)
        rate = rate_info["rate"]
        logger.info(f"[{self.name}] {symbol} rate: {rate:.6f}")

        if symbol in self.positions:
            self._ensure_hedge_consistency(symbol, self.positions[symbol])
            self._check_close(symbol, rate)
        elif allow_new_opens:
            self._check_open(symbol, rate)

    def _check_open(self, symbol: str, rate: float):
        if len(self.positions) >= CFG["max_positions"]:
            return
        if rate < CFG["min_funding_rate"]:
            return

        live = self._get_live_position(symbol)
        if live:
            logger.warning(
                f"[{self.name}] Skip open for {symbol}: residual futures position "
                f"{live.get('side')}={float(live.get('contracts', 0) or 0):.6f}"
            )
            return

        alloc = self.capital * CFG["position_ratio"] * self.weight
        price = self.exchange.get_price(symbol)
        amount = alloc / price
        if amount <= 0:
            return

        logger.info(f"[{self.name}] Opening: {symbol} rate={rate:.6f} amount={amount:.6f}")

        approval = self.request_trade_approval(
            symbol, "sell", "market", amount, price=price,
            risk_context={
                "action": "funding_open",
                "funding_rate": rate,
                "paired_spot_leg": True,
                "paired_notional_multiplier": 1.0,
            },
        )
        if not approval.get("approved", False):
            notifier.risk_alert("FundingArb开仓被风控拒绝", approval.get("reason", "unknown"), f"跳过 {symbol}")
            return

        self.exchange.set_leverage(symbol, CFG["leverage"])
        self.exchange.set_margin_mode(symbol, "isolated")

        spot_before = self._get_live_spot_balance(symbol)
        spot_order = self.exchange.spot_market_order(symbol, "buy", amount)
        if not spot_order or not spot_order.get("execution_ok"):
            spot_after = self._get_live_spot_balance(symbol)
            unexpected_fill = max(spot_after - spot_before, 0.0)
            if unexpected_fill > 1e-6:
                logger.warning(
                    f"[{self.name}] Spot open confirmation missing but wallet increased: "
                    f"{symbol} delta={unexpected_fill:.6f}, attempting rollback"
                )
                if not self._rollback_spot_open(symbol, unexpected_fill):
                    notifier.error(
                        self.name,
                        f"Spot open confirmation missing for {symbol}. Wallet increased by "
                        f"{unexpected_fill:.6f} and rollback failed. Manual action required.",
                    )
            logger.warning(f"[{self.name}] Spot open not confirmed: {symbol}")
            return

        spot_filled_price = float(spot_order.get("average", 0) or spot_order.get("price", 0) or price)
        spot_filled_amount = float(spot_order.get("filled", 0) or amount)
        if spot_filled_amount <= 0:
            logger.warning(f"[{self.name}] Spot open fill incomplete: {symbol}")
            return

        hedge_order = self.exchange.market_order(symbol, "sell", spot_filled_amount)
        if not hedge_order or not hedge_order.get("execution_ok"):
            logger.warning(f"[{self.name}] Futures open not confirmed, rolling back spot: {symbol}")
            if not self._rollback_spot_open(symbol, spot_filled_amount):
                notifier.error(
                    self.name,
                    f"Spot filled but futures short failed for {symbol}. Spot rollback also failed. Manual action required.",
                )
            return

        filled_price = float(hedge_order.get("average", 0) or 0)
        filled_amount = float(hedge_order.get("filled", 0) or 0)
        if filled_price <= 0 or filled_amount <= 0:
            logger.warning(f"[{self.name}] Open fill incomplete: {symbol}")
            if not self._rollback_spot_open(symbol, spot_filled_amount):
                notifier.error(
                    self.name,
                    f"Hedge confirmation incomplete for {symbol}. Spot rollback failed. Manual action required.",
                )
            return

        spot_filled_amount, filled_amount = self._neutralize_open_mismatch(symbol, spot_filled_amount, filled_amount)

        open_ts = time.time()
        self.positions[symbol] = {
            "amount": filled_amount,
            "spot_amount": spot_filled_amount,
            "entry_price": filled_price,
            "spot_entry_price": spot_filled_price,
            "entry_rate": rate,
            "current_rate": rate,
            "open_time": open_ts,
            "collected": 0.0,
            "funding_estimated": False,
            "recovery_pending": False,
            "realized_rebalance_pnl": 0.0,
            "execution_mode": hedge_order.get("execution_mode", "UNKNOWN"),
            "spot_asset": self._spot_asset(symbol),
            "actual_funding_collected": 0.0,
            "funding_reconciliation_status": "unverified",
            "last_funding_sync_ts": open_ts,
            "fees_paid": float(spot_order.get("fee_cost", 0) or 0) + float(hedge_order.get("fee_cost", 0) or 0),
            "slippage_pct": float(spot_order.get("slippage_pct", 0) or 0) + float(hedge_order.get("slippage_pct", 0) or 0),
            "spot_order_id": spot_order.get("id"),
            "hedge_order_id": hedge_order.get("id"),
        }
        ledger.record_order(self.name, symbol, spot_order, {"leg": "spot_open"})
        ledger.record_order(self.name, symbol, hedge_order, {"leg": "futures_hedge_open"})
        self.trade_count += 1

        notifier.trade_open(
            self.name,
            symbol,
            "SHORT+SPOT_LONG",
            filled_amount,
            filled_price,
            f"Rate: `{rate:.6f}` | Est. 8h income: `{rate * alloc:.4f} USDT` | "
            f"Exec: `{hedge_order.get('execution_mode', 'UNKNOWN')}`",
        )

    def _check_close(self, symbol: str, rate: float):
        pos = self.positions[symbol]
        pos["current_rate"] = rate

        hours = (time.time() - pos["open_time"]) / 3600
        periods = int(hours / 8)
        estimated = periods * pos["current_rate"] * pos["amount"] * pos["entry_price"]
        pos["collected"] = estimated
        self._sync_actual_funding(symbol, pos)

        should_close = False
        reason = ""
        if rate < CFG["close_threshold"]:
            should_close = True
            reason = f"funding below threshold ({rate:.6f})"
        if rate < 0:
            should_close = True
            reason = f"funding turned negative ({rate:.6f})"

        if should_close:
            self._close_position(symbol, reason)

    def _close_position(self, symbol: str, reason: str):
        if symbol not in self.positions:
            return

        pos = self.positions[symbol]
        hedge_amount = float(pos.get("amount", 0) or 0)
        spot_amount = float(pos.get("spot_amount", hedge_amount) or hedge_amount)
        if hedge_amount <= 0 or spot_amount <= 0:
            logger.warning(f"[{self.name}] Invalid close state for {symbol}")
            return

        hedge_close = self.exchange.market_order(symbol, "buy", hedge_amount, reduce_only=True)
        if not hedge_close or not hedge_close.get("execution_ok"):
            logger.warning(f"[{self.name}] Futures close not confirmed: {symbol}")
            return

        close_price = float(hedge_close.get("average", 0) or 0)
        close_amount = float(hedge_close.get("filled", 0) or 0)
        if close_price <= 0 or close_amount <= 0:
            logger.warning(f"[{self.name}] Futures close fill incomplete: {symbol}")
            return

        spot_close = self.exchange.spot_market_order(symbol, "sell", spot_amount, reduce_only=True)
        if not spot_close or not spot_close.get("execution_ok"):
            logger.warning(f"[{self.name}] Spot close not confirmed, restoring short hedge: {symbol}")
            if self._restore_short_hedge(symbol, close_amount, close_price, pos):
                notifier.error(
                    self.name,
                    f"Spot close failed for {symbol}. Futures short was restored to keep the hedge intact.",
                )
            else:
                notifier.error(
                    self.name,
                    f"Futures close succeeded but spot sell failed for {symbol}. Hedge restore failed. Manual action required.",
                )
            return

        spot_close_price = float(spot_close.get("average", 0) or spot_close.get("price", 0) or close_price)
        spot_close_amount = float(spot_close.get("filled", 0) or spot_amount)

        futures_pnl = (float(pos.get("entry_price", 0) or 0) - close_price) * close_amount
        spot_pnl = (spot_close_price - float(pos.get("spot_entry_price", close_price) or close_price)) * spot_close_amount
        carry_pnl = float(pos.get("realized_rebalance_pnl", 0) or 0)
        funding_pnl = float(pos.get("actual_funding_collected", 0) or 0)
        if pos.get("funding_reconciliation_status") != "verified":
            funding_pnl = float(pos.get("collected", 0) or 0)
        close_fees = float(hedge_close.get("fee_cost", 0) or 0) + float(spot_close.get("fee_cost", 0) or 0)
        total_fees = float(pos.get("fees_paid", 0) or 0) + close_fees
        total_pnl = futures_pnl + spot_pnl + funding_pnl + carry_pnl - total_fees

        ledger.record_order(self.name, symbol, hedge_close, {"leg": "futures_hedge_close", "reason": reason})
        ledger.record_order(self.name, symbol, spot_close, {"leg": "spot_close", "reason": reason})

        self.total_pnl += total_pnl
        self.pnl_history.append(total_pnl)
        self.trade_count += 1

        notifier.trade_close(
            self.name,
            symbol,
            "SHORT+SPOT_LONG",
            total_pnl,
            reason,
            f"Futures: `{futures_pnl:+.4f}` | Spot: `{spot_pnl:+.4f}` | "
            f"Funding: `{funding_pnl:+.4f}` ({pos.get('funding_reconciliation_status', 'unknown')}) | "
            f"Fees: `{total_fees:+.4f}` | Rebalance: `{carry_pnl:+.4f}` | "
            f"Exec: `{hedge_close.get('execution_mode', 'UNKNOWN')}`",
        )

        del self.positions[symbol]

    def stop(self):
        logger.info(f"[{self.name}] Stopping...")
        for symbol in list(self.positions.keys()):
            self._close_position(symbol, "STRATEGY_STOP")
