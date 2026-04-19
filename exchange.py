"""
交易所客户端 - 支持 Binance / Bybit USDT-M 永续合约
Demo 模式:
  Bybit   -> ccxt.enable_demo_trading(True), 使用真实API Key
  Binance -> 手动覆盖URL到 demo-fapi.binance.com, 使用真实API Key

Symbol 格式统一: BTC/USDT:USDT
"""

import time
import logging
import ccxt
from typing import Optional, List, Dict, Callable
from config import EXCHANGE, ORDER_GUARD, WEBSOCKET
from notifier import notifier
from ws_events import WebSocketEventMonitor

logger = logging.getLogger(__name__)

_BINANCE_DEMO_URLS = {
    "fapiPublic": "https://demo-fapi.binance.com/fapi/v1",
    "fapiPrivate": "https://demo-fapi.binance.com/fapi/v1",
    "fapiPrivateV2": "https://demo-fapi.binance.com/fapi/v2",
    "fapiData": "https://demo-fapi.binance.com/futures/data",
}

_ACCOUNT_WIDE_BALANCE_TYPES = {
    "UNIFIED",
    "PORTFOLIO",
    "PORTFOLIO_MARGIN",
    "PORTFOLIO_MARGIN_PRO",
}
_STABLE_ASSETS = {"USDT", "USDC"}


class ExchangeClient:
    def __init__(self):
        self.name = EXCHANGE["name"]
        self.mode = EXCHANGE["mode"]
        if bool(EXCHANGE.get("dry_run", False)):
            raise RuntimeError("DRY_RUN has been removed. Set DRY_RUN=false to place orders directly on the exchange.")
        self.dry_run = False
        self.live_confirm = EXCHANGE.get("live_confirm", "")
        self.client: Optional[ccxt.Exchange] = None
        self.ready = False
        self.init_error = ""
        self._symbol_info: Dict[str, dict] = {}
        self._symbol_leverage: Dict[str, int] = {}
        self._dry_run_seq = 0
        self._dry_run_positions: Dict[str, dict] = {}
        self._dry_run_orders: Dict[str, dict] = {}
        self._dry_run_order_history: Dict[str, dict] = {}
        self._dry_run_spot_balances: Dict[str, dict] = {}
        self._failure_callback: Optional[Callable[[str, str], None]] = None
        self._success_callback: Optional[Callable[[str], None]] = None
        self.ws_monitor = WebSocketEventMonitor(
            self.name,
            {
                "enabled": WEBSOCKET.get("enabled", False),
                "api_key": EXCHANGE.get("api_key", ""),
                "api_secret": EXCHANGE.get("api_secret", ""),
            },
        )
        self._init_client()

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            if value is None:
                return float(default)
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def attach_runtime_callbacks(self, on_failure=None, on_success=None):
        self._failure_callback = on_failure
        self._success_callback = on_success

    def _report_failure(self, category: str, detail: str):
        if not callable(self._failure_callback):
            return
        try:
            self._failure_callback(category, detail)
        except Exception as e:
            logger.debug(f"[Exchange] Failure callback error: {e}")

    def _report_success(self, category: str):
        if not callable(self._success_callback):
            return
        try:
            self._success_callback(category)
        except Exception as e:
            logger.debug(f"[Exchange] Success callback error: {e}")

    def export_dry_run_state(self) -> dict:
        return {}

    def import_dry_run_state(self, state: dict):
        return

    def _next_dry_run_id(self, order_type: str, symbol: str) -> str:
        self._dry_run_seq += 1
        return f"dryrun-{order_type}-{symbol.replace('/', '').replace(':', '')}-{self._dry_run_seq}"

    @staticmethod
    def _dry_run_position_key(symbol: str, side: str) -> str:
        return f"{symbol}|{side}"

    def _record_dry_run_order(self, order: dict, open_order: bool = False):
        order_id = str((order or {}).get("id") or "")
        if not order_id:
            return
        normalized = dict(order or {})
        self._dry_run_order_history[order_id] = normalized
        if open_order:
            self._dry_run_orders[order_id] = dict(normalized)
        else:
            self._dry_run_orders.pop(order_id, None)

    def _dry_run_spot_balance(self, asset: str) -> dict:
        asset = str(asset or "").upper()
        if not asset:
            return {"asset": asset, "total": 0.0, "free": 0.0, "used": 0.0, "dry_run": True}
        stored = dict(self._dry_run_spot_balances.get(asset, {}) or {})
        return {
            "asset": asset,
            "total": float(stored.get("total", 0) or 0),
            "free": float(stored.get("free", stored.get("total", 0)) or 0),
            "used": float(stored.get("used", 0) or 0),
            "dry_run": True,
        }

    def _set_dry_run_spot_balance(self, asset: str, total: float):
        asset = str(asset or "").upper()
        total = max(float(total or 0), 0.0)
        if total <= 1e-12:
            self._dry_run_spot_balances.pop(asset, None)
            return
        self._dry_run_spot_balances[asset] = {
            "total": total,
            "free": total,
            "used": 0.0,
        }

    def _apply_dry_run_spot_fill(self, symbol: str, side: str, amount: float) -> float:
        spot_symbol = self._spot_market_symbol(symbol)
        base_asset, _ = self._spot_symbol_assets(spot_symbol)
        balance = self._dry_run_spot_balance(base_asset)
        current_total = float(balance.get("total", 0) or 0)
        if str(side or "").lower() == "buy":
            filled = max(float(amount or 0), 0.0)
            self._set_dry_run_spot_balance(base_asset, current_total + filled)
            return filled

        filled = min(max(float(amount or 0), 0.0), current_total)
        self._set_dry_run_spot_balance(base_asset, current_total - filled)
        return filled

    def _apply_dry_run_position_fill(self, symbol: str, side: str, amount: float,
                                     price: float, reduce_only: bool = False) -> float:
        amount = max(float(amount or 0), 0.0)
        price = float(price or 0)
        if amount <= 0:
            return 0.0

        side = str(side or "").lower()
        target_side = "long" if side == "buy" else "short"
        opposite_side = "short" if target_side == "long" else "long"

        if reduce_only:
            key = self._dry_run_position_key(symbol, opposite_side)
            existing = dict(self._dry_run_positions.get(key, {}) or {})
            contracts = float(existing.get("contracts", 0) or 0)
            filled = min(amount, contracts)
            remaining = contracts - filled
            if remaining <= 1e-8:
                self._dry_run_positions.pop(key, None)
            elif filled > 0:
                existing["contracts"] = remaining
                self._dry_run_positions[key] = existing
            return filled

        key = self._dry_run_position_key(symbol, target_side)
        existing = dict(self._dry_run_positions.get(key, {}) or {})
        current_contracts = float(existing.get("contracts", 0) or 0)
        new_contracts = current_contracts + amount
        entry_price = price
        if current_contracts > 0 and float(existing.get("entry_price", 0) or 0) > 0 and price > 0:
            entry_price = (
                current_contracts * float(existing.get("entry_price", 0) or 0) + amount * price
            ) / new_contracts
        leverage = max(int(self._symbol_leverage.get(symbol, 1) or 1), 1)
        self._dry_run_positions[key] = {
            "symbol": symbol,
            "side": target_side,
            "contracts": new_contracts,
            "entry_price": entry_price,
            "leverage": leverage,
        }
        return amount

    def _sync_dry_run_orders(self, symbol: str = None):
        if not self.dry_run or not self._dry_run_orders:
            return

        for order_id, order in list(self._dry_run_orders.items()):
            order_symbol = str(order.get("symbol") or "")
            if symbol and order_symbol != symbol:
                continue

            status = str(order.get("status") or "").lower()
            if status not in ("open", "new", ""):
                self._record_dry_run_order(order, open_order=False)
                continue

            try:
                mark_price = float(self.get_price(order_symbol) or 0)
            except Exception:
                continue
            if mark_price <= 0:
                continue

            order_type = str(order.get("type") or "").lower()
            side = str(order.get("side") or "").lower()
            order_price = float(order.get("price", 0) or 0)
            stop_price = float(order.get("stop_price", order_price) or order_price or 0)
            trigger = False
            fill_price = order_price if order_price > 0 else mark_price

            if order_type == "limit":
                if side == "buy" and mark_price <= order_price + 1e-8:
                    trigger = True
                elif side == "sell" and mark_price + 1e-8 >= order_price:
                    trigger = True
            elif order_type == "stop_market":
                fill_price = mark_price
                if side == "sell" and mark_price <= stop_price - 1e-8:
                    trigger = True
                elif side == "buy" and mark_price + 1e-8 >= stop_price:
                    trigger = True

            if not trigger:
                continue

            amount = float(order.get("amount", 0) or 0)
            market_type = str(order.get("market_type") or "swap").lower()
            reduce_only = bool(order.get("reduceOnly", False))
            if market_type == "spot":
                filled = self._apply_dry_run_spot_fill(order_symbol, side, amount)
            else:
                filled = self._apply_dry_run_position_fill(order_symbol, side, amount, fill_price, reduce_only=reduce_only)

            updated = dict(order)
            updated["filled"] = filled
            updated["remaining"] = max(amount - filled, 0.0)
            updated["average"] = fill_price if filled > 0 else 0.0
            updated["status"] = "closed" if updated["remaining"] <= 1e-8 else "open"
            updated["execution_state"] = "filled" if updated["status"] == "closed" else "partial"
            updated["execution_ok"] = updated["status"] == "closed"
            updated["timestamp"] = int(time.time() * 1000)
            self._record_dry_run_order(updated, open_order=updated["status"] in ("open", "new"))

    def _extract_balance_account_type(self, balance: dict) -> str:
        info = balance.get("info") if isinstance(balance, dict) else None
        if not isinstance(info, dict):
            return ""

        account_type = info.get("accountType")
        if account_type:
            return str(account_type).upper()

        result = info.get("result")
        if isinstance(result, dict):
            entries = result.get("list")
            if isinstance(entries, list) and entries:
                account_type = (entries[0] or {}).get("accountType")
                if account_type:
                    return str(account_type).upper()
        return ""

    def _iter_raw_balance_assets(self, balance: dict) -> List[dict]:
        items: List[dict] = []
        info = balance.get("info") if isinstance(balance, dict) else None
        if not isinstance(info, dict):
            return items

        result = info.get("result")
        if isinstance(result, dict):
            entries = result.get("list")
            if isinstance(entries, list):
                for entry in entries:
                    coins = (entry or {}).get("coin")
                    if not isinstance(coins, list):
                        continue
                    for coin in coins:
                        if not isinstance(coin, dict):
                            continue
                        raw_total = coin.get("equity")
                        if raw_total is None:
                            raw_total = coin.get("walletBalance")
                        if raw_total is None:
                            raw_total = coin.get("total")
                        items.append({
                            "asset": coin.get("coin"),
                            "total": self._safe_float(raw_total),
                        })

        assets = info.get("assets")
        if isinstance(assets, list):
            for asset in assets:
                if not isinstance(asset, dict):
                    continue
                raw_total = asset.get("walletBalance")
                if raw_total is None:
                    raw_total = asset.get("balance")
                if raw_total is None:
                    raw_total = asset.get("total")
                items.append({
                    "asset": asset.get("asset") or asset.get("coin"),
                    "total": self._safe_float(raw_total),
                })
        return items

    def _balance_has_non_stable_assets(self, balance: dict) -> bool:
        reserved = {"info", "free", "used", "total", "debt", "timestamp", "datetime"}
        if isinstance(balance, dict):
            for currency, item in balance.items():
                if currency in reserved or not isinstance(item, dict):
                    continue
                asset = str(currency or "").upper()
                if not asset or asset in _STABLE_ASSETS:
                    continue
                total = self._safe_float(item.get("total"))
                if total <= 0:
                    total = self._safe_float(item.get("free"))
                if total <= 0:
                    total = self._safe_float(item.get("used"))
                if total > 1e-8:
                    return True

        for item in self._iter_raw_balance_assets(balance):
            asset = str(item.get("asset") or "").upper()
            if asset and asset not in _STABLE_ASSETS and self._safe_float(item.get("total")) > 1e-8:
                return True
        return False

    def _balance_includes_spot_assets(self, balance: dict, account_type: str = "") -> bool:
        normalized_type = str(account_type or self._extract_balance_account_type(balance)).upper()
        if normalized_type in _ACCOUNT_WIDE_BALANCE_TYPES:
            return True
        return self._balance_has_non_stable_assets(balance)

    def _extract_account_balance(self, balance: dict) -> dict:
        usdt = balance.get("USDT", {}) if isinstance(balance, dict) else {}
        account_type = self._extract_balance_account_type(balance)
        snapshot = {
            "total": self._safe_float((usdt or {}).get("total")),
            "free": self._safe_float((usdt or {}).get("free")),
            "used": self._safe_float((usdt or {}).get("used")),
            "account_type": account_type,
        }

        info = balance.get("info") if isinstance(balance, dict) else None

        if self.name == "binance" and snapshot["total"] == 0 and isinstance(info, dict):
            for asset in info.get("assets", []):
                if asset.get("asset") == "USDT":
                    snapshot["total"] = self._safe_float(asset.get("walletBalance"))
                    snapshot["free"] = self._safe_float(asset.get("availableBalance"))
                    snapshot["used"] = max(snapshot["total"] - snapshot["free"], 0.0)
                    break

        if self.name == "bybit" and isinstance(info, dict):
            result = info.get("result")
            if isinstance(result, dict):
                entries = result.get("list")
                if isinstance(entries, list) and entries:
                    account = entries[0] or {}
                    total_equity = self._safe_float(account.get("totalEquity"), default=-1.0)
                    total_wallet = self._safe_float(account.get("totalWalletBalance"), default=-1.0)
                    available = self._safe_float(account.get("totalAvailableBalance"), default=-1.0)
                    if total_equity > 0:
                        snapshot["total"] = total_equity
                    elif total_wallet > 0:
                        snapshot["total"] = total_wallet
                    if available >= 0:
                        snapshot["free"] = available
                        snapshot["used"] = max(snapshot["total"] - snapshot["free"], 0.0)

        snapshot["includes_spot_assets"] = self._balance_includes_spot_assets(balance, account_type=account_type)
        return snapshot

    def _init_client(self):
        common = {
            "apiKey": EXCHANGE["api_key"],
            "secret": EXCHANGE["api_secret"],
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
                "defaultSubType": "linear",
                "adjustForTimeDifference": True,
                "recvWindow": 10000,
            },
        }

        if self.name == "bybit":
            self.client = ccxt.bybit(common)
        elif self.name == "binance":
            self.client = ccxt.binance(common)
        else:
            raise ValueError(f"Unsupported exchange: {self.name}")

        # Demo 模式
        if self.mode == "demo":
            if self.name == "bybit":
                self.client.enable_demo_trading(True)
                logger.info("[Exchange] Bybit demo trading enabled")
            elif self.name == "binance":
                urls = self.client.urls
                if "api" in urls and isinstance(urls["api"], dict):
                    for key, url in _BINANCE_DEMO_URLS.items():
                        if key in urls["api"]:
                            urls["api"][key] = url
                logger.info("[Exchange] Binance demo -> demo-fapi.binance.com")

        try:
            self.client.load_markets()
            self._cache_symbols()
            env_tag = "DEMO" if self.mode == "demo" else "LIVE"
            exec_tag = "DRY-RUN" if self.dry_run else "EXECUTE"
            logger.info(f"[Exchange] {self.name.upper()} {env_tag} {exec_tag} | {len(self.client.markets)} markets")
            self.ws_monitor.start()
            self.ready = True
            self.init_error = ""
        except Exception as e:
            self.ready = False
            self.init_error = str(e)
            logger.error(f"[Exchange] Load markets failed: {e}")
            notifier.error("Exchange", f"Connect failed: {e}")
            raise RuntimeError(f"Exchange initialization failed: {self.name} {self.mode} - {e}") from e

    def _cache_symbols(self):
        for sym, m in self.client.markets.items():
            if m.get("linear") and m.get("active"):
                self._symbol_info[sym] = {
                    "amt_prec": m.get("precision", {}).get("amount", 8),
                    "prc_prec": m.get("precision", {}).get("price", 8),
                    "min_amt": m.get("limits", {}).get("amount", {}).get("min", 0),
                    "min_cost": m.get("limits", {}).get("cost", {}).get("min", 0),
                }

    def _get_symbol_meta(self, symbol: str) -> dict:
        info = self._symbol_info.get(symbol)
        if info:
            return info
        market = self.client.market(symbol)
        info = {
            "amt_prec": market.get("precision", {}).get("amount", 8),
            "prc_prec": market.get("precision", {}).get("price", 8),
            "min_amt": market.get("limits", {}).get("amount", {}).get("min", 0),
            "min_cost": market.get("limits", {}).get("cost", {}).get("min", 0),
        }
        self._symbol_info[symbol] = info
        return info

    @staticmethod
    def _spot_market_symbol(symbol: str) -> str:
        return str(symbol or "").split(":")[0]

    @staticmethod
    def _base_asset_from_symbol(symbol: str) -> str:
        market_symbol = ExchangeClient._spot_market_symbol(symbol)
        return market_symbol.split("/", 1)[0] if "/" in market_symbol else market_symbol

    @staticmethod
    def _quote_asset_from_symbol(symbol: str) -> str:
        market_symbol = ExchangeClient._spot_market_symbol(symbol)
        return market_symbol.split("/", 1)[1] if "/" in market_symbol else ""

    def _compose_guard_balance(self) -> dict:
        swap_balance = self.get_balance() or {"total": 0.0, "free": 0.0, "used": 0.0}
        spot_assets = self.get_spot_exposure(min_usdt_value=1.0)
        swap_total = self._safe_float(swap_balance.get("total"))
        spot_total = sum(self._safe_float(item.get("value_usdt")) for item in spot_assets)
        includes_spot_assets = bool(swap_balance.get("includes_spot_assets", False))
        total_equity = swap_total if includes_spot_assets else swap_total + spot_total
        return {
            "total": total_equity,
            "swap_total": swap_total,
            "swap_free": self._safe_float(swap_balance.get("free")),
            "swap_used": self._safe_float(swap_balance.get("used")),
            "spot_total": spot_total,
            "spot_assets": spot_assets,
            "includes_spot_assets": includes_spot_assets,
        }

    def _spot_symbol_notional(self, symbol: str, ref_price: Optional[float] = None) -> float:
        asset = self._base_asset_from_symbol(symbol).upper()
        if not asset or asset in _STABLE_ASSETS:
            return 0.0

        spot = self.get_spot_balance(asset) or {}
        total = self._safe_float(spot.get("total"))
        if total <= 0:
            return 0.0

        price = self._safe_float(ref_price, default=0.0)
        if price <= 0:
            try:
                price = self._safe_float(self.get_price(symbol))
            except Exception:
                price = 0.0
        if price <= 0:
            return 0.0
        return total * price

    def _open_order_notionals(self, symbol: str, fallback_price: float = 0.0) -> tuple[float, float]:
        total = 0.0
        symbol_total = 0.0
        for open_order in self.get_open_orders():
            if bool(open_order.get("reduceOnly")):
                continue
            status = str(open_order.get("status", "")).lower()
            if status and status not in ("open", "new"):
                continue
            order_symbol = str(open_order.get("symbol") or "")
            order_notional = self._open_order_notional(
                open_order,
                fallback_price=fallback_price if order_symbol == symbol else 0.0,
            )
            if order_notional <= 0:
                continue
            total += order_notional
            if order_symbol == symbol:
                symbol_total += order_notional
        return total, symbol_total

    @staticmethod
    def _estimate_notional(amount: float, price: float) -> float:
        return abs(float(amount or 0) * float(price or 0))

    @staticmethod
    def _open_order_notional(order: dict, fallback_price: float = 0.0) -> float:
        info = order or {}
        amount = float(info.get("remaining", 0) or info.get("amount", 0) or 0)
        price = float(info.get("price", 0) or fallback_price or 0)
        if amount <= 0 or price <= 0:
            return 0.0
        return abs(amount * price)

    def _get_configured_leverage(self, symbol: str) -> int:
        return max(int(self._symbol_leverage.get(symbol, 1) or 1), 1)

    def _prepare_order_request(self, symbol: str, amount: float,
                               price: Optional[float] = None) -> tuple[str, float, Optional[str], Optional[float]]:
        fa = self.client.amount_to_precision(symbol, amount)
        normalized_amount = float(fa)
        fp = None
        normalized_price = None
        if price is not None:
            fp = self.client.price_to_precision(symbol, price)
            normalized_price = float(fp)
        return fa, normalized_amount, fp, normalized_price

    def _enforce_order_guard(self, symbol: str, side: str, amount: float,
                             price: Optional[float] = None, reduce_only: bool = False,
                             order_type: str = "market", market_type: str = "swap"):
        if amount <= 0:
            raise ValueError("order amount rounded to zero")

        guard_symbol = self._spot_market_symbol(symbol) if str(market_type or "swap").lower() == "spot" else symbol
        meta = self._get_symbol_meta(guard_symbol)
        ref_price = float(price if price is not None else self.get_price(symbol))
        if ref_price <= 0:
            raise ValueError("reference price unavailable")

        notional = self._estimate_notional(amount, ref_price)
        min_amt = float(meta.get("min_amt", 0) or 0)
        min_cost = float(meta.get("min_cost", 0) or 0)
        if min_amt > 0 and amount + 1e-12 < min_amt:
            raise ValueError(f"amount {amount} below exchange min amount {min_amt}")
        if min_cost > 0 and notional + 1e-8 < min_cost:
            raise ValueError(f"notional {notional:.6f} below exchange min cost {min_cost}")

        side = str(side or "").lower()
        market_type = str(market_type or "swap").lower()
        if reduce_only or not ORDER_GUARD.get("enabled", True):
            return

        balance = self._compose_guard_balance()
        if float(balance.get("total", 0) or 0) <= 0:
            raise ValueError("order guard cannot validate without account balance")

        equity = float(balance.get("total", 0) or 0)
        positions = self.get_positions()
        symbol_position_notional = sum(
            abs(float(p.get("notional", 0) or 0))
            for p in positions
            if p.get("symbol") == symbol
        )
        total_position_notional = sum(abs(float(p.get("notional", 0) or 0)) for p in positions)
        total_open_notional, symbol_open_notional = self._open_order_notionals(symbol, fallback_price=ref_price)
        spot_total_notional = float(balance.get("spot_total", 0) or 0)
        spot_symbol_notional = self._spot_symbol_notional(symbol, ref_price=ref_price)

        if market_type == "spot":
            quote_asset = self._quote_asset_from_symbol(symbol)
            if self.dry_run:
                quote_free = float(balance.get("swap_free", 0) or 0)
            else:
                quote_balance = self.get_spot_balance(quote_asset) or {}
                quote_free = self._safe_float(quote_balance.get("free"))
            if side == "buy" and quote_free + 1e-8 < notional:
                raise ValueError(
                    f"spot order guard blocked {side}: quote free {quote_free:.2f} < required {notional:.2f}"
                )
        else:
            free = float(balance.get("swap_free", 0) or 0)
            margin_equity = float(balance.get("swap_total", 0) or equity)
            leverage = self._get_configured_leverage(symbol)
            required_margin = notional / leverage
            min_free = margin_equity * float(ORDER_GUARD.get("min_free_balance_pct", 0) or 0)

            if free - required_margin + 1e-8 < min_free:
                raise ValueError(
                    f"order guard blocked {order_type} {side}: free {free:.2f}, "
                    f"required {required_margin:.2f}, min_free {min_free:.2f}"
                )

        projected_symbol = symbol_position_notional + spot_symbol_notional + symbol_open_notional + notional
        projected_total = total_position_notional + spot_total_notional + total_open_notional + notional
        max_symbol = equity * float(ORDER_GUARD.get("max_symbol_notional_pct", 0) or 0)
        max_total = equity * float(ORDER_GUARD.get("max_total_notional_pct", 0) or 0)

        if max_symbol > 0 and projected_symbol - 1e-8 > max_symbol:
            raise ValueError(
                f"order guard blocked {symbol}: projected symbol notional {projected_symbol:.2f} > {max_symbol:.2f}"
            )
        if max_total > 0 and projected_total - 1e-8 > max_total:
            raise ValueError(
                f"order guard blocked total exposure: projected total notional {projected_total:.2f} > {max_total:.2f}"
            )

    def _execution_mode_label(self) -> str:
        env_tag = "DEMO" if self.mode == "demo" else "LIVE"
        exec_tag = "DRY-RUN" if self.dry_run else "EXECUTE"
        return f"{env_tag}/{exec_tag}"

    @staticmethod
    def _extract_fee_cost(info: dict) -> float:
        fee = info.get("fee") if isinstance(info, dict) else None
        if isinstance(fee, dict):
            return float(fee.get("cost", 0) or 0)
        fees = info.get("fees") if isinstance(info, dict) else None
        if isinstance(fees, list):
            return sum(float(item.get("cost", 0) or 0) for item in fees if isinstance(item, dict))
        return 0.0

    @staticmethod
    def _calc_slippage_pct(requested_price: Optional[float], average: float) -> float:
        requested = float(requested_price or 0)
        actual = float(average or 0)
        if requested <= 0 or actual <= 0:
            return 0.0
        return (actual - requested) / requested

    def _augment_execution_metrics(self, normalized: dict, requested_amount: float,
                                   requested_price: Optional[float] = None) -> dict:
        normalized["requested_amount"] = float(requested_amount or 0)
        normalized["requested_price"] = float(requested_price or 0) if requested_price is not None else None
        normalized["fee_cost"] = self._extract_fee_cost(normalized)
        normalized["slippage_pct"] = self._calc_slippage_pct(requested_price, float(normalized.get("average", 0) or 0))
        normalized["timestamp"] = normalized.get("timestamp") or int(time.time() * 1000)
        return normalized

    def _simulate_fee_cost(self, amount: float, price: float) -> float:
        notional = abs(float(amount or 0) * float(price or 0))
        return notional * 0.0005

    def _mark_ws_event(self, category: str, payload):
        if self.ws_monitor:
            self.ws_monitor.mark_event(category, payload)

    def _simulate_order(self, symbol: str, side: str, amount: float,
                        price: Optional[float] = None, reduce_only: bool = False,
                        order_type: str = "market") -> dict:
        now_ms = int(time.time() * 1000)
        market_price = float(price if price is not None else self.get_price(symbol))
        return {
            "id": self._next_dry_run_id(order_type, symbol),
            "clientOrderId": None,
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "price": market_price if order_type == "limit" else market_price,
            "average": market_price,
            "amount": float(amount),
            "filled": float(amount) if order_type == "market" else 0.0,
            "remaining": 0.0 if order_type == "market" else float(amount),
            "status": "closed" if order_type == "market" else "open",
            "reduceOnly": reduce_only,
            "dry_run": True,
            "execution_mode": self._execution_mode_label(),
            "execution_state": "filled" if order_type == "market" else "open",
            "execution_ok": True,
            "fee_cost": self._simulate_fee_cost(amount, market_price) if order_type == "market" else 0.0,
            "slippage_pct": 0.0,
            "timestamp": now_ms,
            "requested_amount": float(amount),
            "requested_price": market_price if price is not None else None,
        }

    def _normalize_execution(self, order: dict, expected_amount: float,
                             expected_price: Optional[float] = None,
                             is_market: bool = True) -> Optional[dict]:
        info = order or {}
        order_id = info.get("id")
        if not order_id:
            return None

        status = (info.get("status") or "").lower()
        filled = float(info.get("filled", 0) or 0)
        amount = float(info.get("amount", expected_amount) or expected_amount or 0)
        remaining = float(info.get("remaining", max(amount - filled, 0)) or 0)
        price = float(info.get("price", expected_price or 0) or expected_price or 0)
        average = float(info.get("average", price) or price)

        if amount <= 0:
            amount = float(expected_amount or 0)
        if not status:
            status = "closed" if is_market and remaining <= 0 else "open"

        normalized = dict(info)
        normalized.update({
            "id": order_id,
            "status": status,
            "amount": amount,
            "filled": filled,
            "remaining": remaining,
            "price": price,
            "average": average,
            "execution_mode": self._execution_mode_label(),
            "execution_state": "uncertain",
            "execution_ok": False,
        })
        normalized = self._augment_execution_metrics(normalized, expected_amount, expected_price)

        if is_market:
            if filled > 0 and average > 0 and status in ("closed", "filled"):
                normalized["execution_state"] = "filled"
                normalized["execution_ok"] = True
            elif filled > 0 and average > 0:
                normalized["execution_state"] = "partial"
            elif status in ("canceled", "cancelled", "rejected", "expired"):
                normalized["execution_state"] = "rejected"
        else:
            if status in ("open", "new"):
                normalized["execution_state"] = "open"
                normalized["execution_ok"] = True
            elif filled > 0 and remaining > 0:
                normalized["execution_state"] = "partial"
            elif filled > 0 and remaining <= 0:
                normalized["execution_state"] = "filled"
                normalized["execution_ok"] = True
            elif status in ("canceled", "cancelled", "rejected", "expired"):
                normalized["execution_state"] = "rejected"
        return normalized

    def _fetch_order(self, order_id: str, symbol: str) -> Optional[dict]:
        if self.dry_run:
            self._sync_dry_run_orders(symbol)
            stored = self._dry_run_orders.get(str(order_id)) or self._dry_run_order_history.get(str(order_id))
            return dict(stored or {}) if stored else None
        try:
            return self.client.fetch_order(order_id, symbol)
        except Exception:
            return None

    def get_order(self, order_id: str, symbol: str) -> Optional[dict]:
        return self._fetch_order(order_id, symbol)

    def classify_order(self, order_id: str, symbol: str, expected_amount: float = 0,
                       expected_price: Optional[float] = None) -> dict:
        if self.dry_run:
            self._sync_dry_run_orders(symbol)
        order = self._fetch_order(order_id, symbol)
        if not order:
            return {"id": order_id, "execution_state": "unknown", "execution_ok": False}
        return self._normalize_execution(
            order,
            expected_amount,
            expected_price=expected_price,
            is_market=False,
        ) or {"id": order_id, "execution_state": "unknown", "execution_ok": False}

    def _confirm_order_state(self, symbol: str, order: dict, expected_amount: float,
                             expected_price: Optional[float] = None,
                             is_market: bool = True, side: Optional[str] = None,
                             reduce_only: bool = False,
                             before_positions: Optional[List[dict]] = None) -> Optional[dict]:
        normalized = self._normalize_execution(order, expected_amount, expected_price=expected_price, is_market=is_market)
        if not normalized:
            return None
        if normalized.get("execution_ok"):
            return normalized

        order_id = normalized.get("id")
        fetched = self._fetch_order(order_id, symbol) if order_id else None
        if fetched:
            fetched_normalized = self._normalize_execution(fetched, expected_amount, expected_price=expected_price, is_market=is_market)
            if fetched_normalized:
                normalized = fetched_normalized
                if normalized.get("execution_ok"):
                    return normalized

        if is_market:
            positions = self.get_positions(symbol)
            matched_side = "long" if side == "buy" else "short"
            matched_positions = [
                p for p in positions
                if p.get("side") == matched_side and float(p.get("contracts", 0) or 0) > 0
            ]
            before_positions = before_positions or []
            before_long = sum(float(p.get("contracts", 0) or 0) for p in before_positions if p.get("side") == "long")
            before_short = sum(float(p.get("contracts", 0) or 0) for p in before_positions if p.get("side") == "short")
            after_long = sum(float(p.get("contracts", 0) or 0) for p in positions if p.get("side") == "long")
            after_short = sum(float(p.get("contracts", 0) or 0) for p in positions if p.get("side") == "short")
            before_same = before_long if matched_side == "long" else before_short
            after_same = after_long if matched_side == "long" else after_short
            before_opp = before_short if matched_side == "long" else before_long
            after_opp = after_short if matched_side == "long" else after_long

            if not reduce_only and after_same > before_same + 1e-6:
                avg_entry = 0.0
                if matched_positions:
                    avg_entry = float(matched_positions[-1].get("entry_price", 0) or 0)
                normalized["filled"] = max(after_same - before_same, float(normalized.get("filled", 0) or 0))
                normalized["amount"] = max(expected_amount, float(normalized.get("amount", expected_amount) or expected_amount))
                normalized["remaining"] = max(float(normalized.get("amount", expected_amount) or expected_amount) - normalized["filled"], 0.0)
                if avg_entry > 0:
                    normalized["average"] = avg_entry
                    normalized["price"] = avg_entry
                normalized["execution_state"] = "filled"
                normalized["execution_ok"] = True
                return normalized

            if reduce_only and after_opp + 1e-6 < before_opp:
                reduced = max(before_opp - after_opp, float(normalized.get("filled", 0) or 0))
                normalized["filled"] = reduced
                normalized["amount"] = max(expected_amount, float(normalized.get("amount", expected_amount) or expected_amount))
                normalized["remaining"] = max(float(normalized.get("amount", expected_amount) or expected_amount) - reduced, 0.0)
                normalized["execution_state"] = "filled" if after_opp <= 1e-6 else "partial"
                normalized["execution_ok"] = normalized["execution_state"] == "filled"
                return normalized

            if any(float(p.get("contracts", 0) or 0) > 0 for p in matched_positions):
                normalized["execution_state"] = normalized.get("execution_state") or "partial"
        else:
            open_orders = self.get_open_orders(symbol)
            if order_id and any(str(o.get("id")) == str(order_id) for o in open_orders):
                normalized["execution_state"] = "open"
                normalized["execution_ok"] = True

        return normalized

    def _log_dry_run(self, action: str, symbol: str, side: Optional[str] = None,
                     amount: Optional[float] = None, price: Optional[float] = None,
                     extra: str = ""):
        details = [f"[Exchange] DRY-RUN {action}: {symbol}"]
        if side:
            details.append(side.upper())
        if amount is not None:
            details.append(f"amount={amount}")
        if price is not None:
            details.append(f"price={price}")
        if extra:
            details.append(extra)
        logger.info(" | ".join(details))

    # ── 行情 ──

    def get_price(self, symbol: str) -> float:
        return float(self.client.fetch_ticker(symbol)["last"])

    def get_ohlcv(self, symbol: str, timeframe: str = "1h", limit: int = 100) -> list:
        return self.client.fetch_ohlcv(symbol, timeframe, limit=limit)

    def get_orderbook(self, symbol: str, limit: int = 10) -> dict:
        return self.client.fetch_order_book(symbol, limit)

    def get_funding_rate(self, symbol: str) -> dict:
        try:
            info = self.client.fetch_funding_rate(symbol)
            return {
                "symbol": symbol,
                "rate": float(info.get("fundingRate", 0)),
                "next_time": info.get("fundingDatetime"),
            }
        except Exception as e:
            logger.error(f"[Exchange] Funding rate error {symbol}: {e}")
            return {"symbol": symbol, "rate": 0, "next_time": None}

    def get_funding_income(self, symbol: str = None, since: Optional[int] = None, limit: int = 50) -> dict:
        try:
            incomes = []
            if hasattr(self.client, "fetch_funding_history"):
                history = self.client.fetch_funding_history(symbol, since=since, limit=limit)
                for item in history or []:
                    incomes.append({
                        "symbol": item.get("symbol", symbol),
                        "amount": float(item.get("amount", 0) or 0),
                        "timestamp": item.get("timestamp"),
                        "info": item,
                    })
            return {
                "ok": True,
                "symbol": symbol,
                "items": incomes,
                "total": sum(float(item.get("amount", 0) or 0) for item in incomes),
                "source": "fetch_funding_history",
            }
        except Exception as e:
            logger.warning(f"[Exchange] Funding income unavailable {symbol}: {e}")
            return {
                "ok": False,
                "symbol": symbol,
                "items": [],
                "total": 0.0,
                "error": str(e),
                "source": "unavailable",
            }

    def get_trade_fills(self, symbol: str = None, since: Optional[int] = None, limit: int = 50) -> list:
        try:
            trades = self.client.fetch_my_trades(symbol, since=since, limit=limit)
            return trades or []
        except Exception as e:
            logger.warning(f"[Exchange] Trade fills unavailable {symbol}: {e}")
            return []

    def get_fee_summary(self, symbol: str = None, since: Optional[int] = None, limit: int = 50) -> dict:
        trades = self.get_trade_fills(symbol, since=since, limit=limit)
        total_fee = 0.0
        for trade in trades:
            total_fee += self._extract_fee_cost(trade)
        return {
            "symbol": symbol,
            "count": len(trades),
            "fee_cost": total_fee,
            "trades": trades,
        }

    # ── 账户 ──

    def get_balance(self) -> Optional[dict]:
        try:
            balance = self.client.fetch_balance({"type": "swap"})
            snapshot = self._extract_account_balance(balance)
            self._mark_ws_event("balance", {
                "total": snapshot["total"],
                "free": snapshot["free"],
                "used": snapshot["used"],
            })
            return snapshot
        except Exception as e:
            logger.error(f"[Exchange] Balance error: {e}")
            return None

    def get_spot_balance(self, asset: str = None) -> Optional[dict]:
        try:
            if self.dry_run:
                if asset:
                    return self._dry_run_spot_balance(asset)
                assets = {
                    key: self._dry_run_spot_balance(key)
                    for key in sorted(self._dry_run_spot_balances.keys())
                    if float((self._dry_run_spot_balances.get(key) or {}).get("total", 0) or 0) > 0
                }
                return {"assets": assets, "dry_run": True}
            balance = self.client.fetch_balance({"type": "spot"})
            if asset:
                item = balance.get(asset, {})
                return {
                    "asset": asset,
                    "total": float(item.get("total", 0) or 0),
                    "free": float(item.get("free", 0) or 0),
                    "used": float(item.get("used", 0) or 0),
                }
            result = {}
            for key, value in balance.items():
                if isinstance(value, dict) and any(k in value for k in ("total", "free", "used")):
                    result[key] = {
                        "total": float(value.get("total", 0) or 0),
                        "free": float(value.get("free", 0) or 0),
                        "used": float(value.get("used", 0) or 0),
                    }
            return {"assets": result}
        except Exception as e:
            logger.error(f"[Exchange] Spot balance error: {e}")
            return None

    @staticmethod
    def _spot_symbol_assets(spot_symbol: str) -> tuple[str, str]:
        base_asset, quote_asset = spot_symbol.split("/", 1)
        return base_asset.strip(), quote_asset.strip()

    def _spot_balance_snapshot(self, spot_symbol: str) -> dict:
        base_asset, quote_asset = self._spot_symbol_assets(spot_symbol)
        base = self.get_spot_balance(base_asset) or {}
        quote = self.get_spot_balance(quote_asset) or {}
        return {
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "base_total": float(base.get("total", 0) or 0),
            "quote_total": float(quote.get("total", 0) or 0),
        }

    def _confirm_spot_order_state(
        self,
        spot_symbol: str,
        order: dict,
        expected_amount: float,
        side: str,
        before_snapshot: Optional[dict] = None,
    ) -> Optional[dict]:
        normalized = self._normalize_execution(order, expected_amount, is_market=True)
        if not normalized:
            return None
        if normalized.get("execution_ok"):
            return normalized

        order_id = normalized.get("id")
        fetched = self._fetch_order(order_id, spot_symbol) if order_id else None
        if fetched:
            fetched_normalized = self._normalize_execution(fetched, expected_amount, is_market=True)
            if fetched_normalized:
                normalized = fetched_normalized
                if normalized.get("execution_ok"):
                    return normalized

        if not before_snapshot:
            return normalized

        after_snapshot = self._spot_balance_snapshot(spot_symbol)
        base_delta = float(after_snapshot.get("base_total", 0) or 0) - float(before_snapshot.get("base_total", 0) or 0)
        quote_delta = float(after_snapshot.get("quote_total", 0) or 0) - float(before_snapshot.get("quote_total", 0) or 0)
        filled = base_delta if side == "buy" else -base_delta
        if filled <= 1e-8:
            return normalized

        requested = max(float(normalized.get("amount", expected_amount) or expected_amount), float(expected_amount or 0))
        fee_tolerance = max(requested * 0.005, 1e-8)
        normalized["filled"] = max(float(normalized.get("filled", 0) or 0), filled)
        normalized["amount"] = requested
        normalized["remaining"] = max(requested - float(normalized["filled"]), 0.0)

        implied_avg = 0.0
        if side == "buy" and quote_delta < 0:
            implied_avg = abs(quote_delta) / max(float(normalized["filled"]), 1e-12)
        elif side == "sell" and quote_delta > 0:
            implied_avg = quote_delta / max(float(normalized["filled"]), 1e-12)
        if implied_avg > 0:
            normalized["average"] = implied_avg
            normalized["price"] = implied_avg

        is_full_fill = requested - float(normalized["filled"]) <= fee_tolerance
        normalized["execution_state"] = "filled" if is_full_fill else "partial"
        normalized["execution_ok"] = is_full_fill
        normalized["balance_delta_confirmed"] = True
        return normalized

    def _spot_price_symbol(self, asset: str) -> Optional[str]:
        if not asset:
            return None
        candidates = (
            f"{asset}/USDT",
            f"{asset}/USDT:USDT",
            f"{asset}/USDC",
            f"{asset}/USDC:USDC",
        )
        for symbol in candidates:
            market = self.client.markets.get(symbol)
            if market and market.get("active"):
                return symbol
        return None

    def get_spot_exposure(self, min_usdt_value: float = 1.0) -> List[dict]:
        snapshot = self.get_spot_balance()
        if not snapshot:
            return []

        assets = snapshot.get("assets", {})
        exposures: List[dict] = []
        for asset, item in assets.items():
            total = float((item or {}).get("total", 0) or 0)
            if total <= 0:
                continue

            price = 0.0
            if asset in ("USDT", "USDC"):
                price = 1.0
            else:
                symbol = self._spot_price_symbol(asset)
                if not symbol:
                    continue
                try:
                    price = self.get_price(symbol)
                except Exception as e:
                    logger.warning(f"[Exchange] Spot valuation skipped {asset}: {e}")
                    continue

            value_usdt = total * price
            if value_usdt < min_usdt_value:
                continue

            exposures.append({
                "asset": asset,
                "total": total,
                "free": float((item or {}).get("free", 0) or 0),
                "used": float((item or {}).get("used", 0) or 0),
                "price": price,
                "value_usdt": value_usdt,
            })

        exposures.sort(key=lambda item: float(item.get("value_usdt", 0) or 0), reverse=True)
        return exposures

    def get_positions(self, symbol: str = None) -> List[dict]:
        if self.dry_run:
            self._sync_dry_run_orders(symbol)
            result = []
            for position in self._dry_run_positions.values():
                if not isinstance(position, dict):
                    continue
                if symbol and position.get("symbol") != symbol:
                    continue
                contracts = float(position.get("contracts", 0) or 0)
                if contracts <= 0:
                    continue
                entry_price = float(position.get("entry_price", 0) or 0)
                try:
                    mark_price = float(self.get_price(position.get("symbol")) or 0)
                except Exception:
                    mark_price = entry_price
                if str(position.get("side") or "") == "long":
                    unrealized_pnl = (mark_price - entry_price) * contracts
                else:
                    unrealized_pnl = (entry_price - mark_price) * contracts
                result.append({
                    "symbol": position.get("symbol"),
                    "side": position.get("side"),
                    "contracts": contracts,
                    "entry_price": entry_price,
                    "unrealized_pnl": unrealized_pnl,
                    "leverage": int(position.get("leverage", self._get_configured_leverage(position.get("symbol"))) or 1),
                    "notional": contracts * (mark_price or entry_price),
                })
            self._mark_ws_event("positions", result[-20:])
            self._report_success("position_fetch")
            return result
        try:
            positions = self.client.fetch_positions([symbol] if symbol else None)
            result = []
            for p in positions:
                c = float(p.get("contracts", 0) or 0)
                if c == 0:
                    continue
                result.append({
                    "symbol": p["symbol"],
                    "side": p["side"],
                    "contracts": c,
                    "entry_price": float(p.get("entryPrice", 0) or 0),
                    "unrealized_pnl": float(p.get("unrealizedPnl", 0) or 0),
                    "leverage": int(p.get("leverage", 1) or 1),
                    "notional": float(p.get("notional", 0) or 0),
                })
            self._mark_ws_event("positions", result[-20:])
            self._report_success("position_fetch")
            return result
        except Exception as e:
            self._report_failure("position_fetch", f"positions {symbol or '*'}: {e}")
            logger.error(f"[Exchange] Positions error: {e}")
            return []

    # ── 交易 ──

    def set_leverage(self, symbol: str, leverage: int):
        self._symbol_leverage[symbol] = max(int(leverage or 1), 1)
        if self.dry_run:
            self._log_dry_run("SET_LEVERAGE", symbol, amount=leverage)
            return
        try:
            self.client.set_leverage(leverage, symbol)
        except Exception as e:
            msg = str(e).lower()
            if "no need" not in msg and "not modified" not in msg and "same" not in msg:
                logger.warning(f"[Exchange] Leverage: {e}")

    def set_margin_mode(self, symbol: str, mode: str = "isolated"):
        if self.dry_run:
            self._log_dry_run("SET_MARGIN_MODE", symbol, extra=f"mode={mode}")
            return
        try:
            self.client.set_margin_mode(mode, symbol)
        except Exception as e:
            msg = str(e).lower()
            if "no need" not in msg and "not modified" not in msg and "same" not in msg:
                logger.warning(f"[Exchange] Margin mode: {e}")

    def spot_market_order(self, symbol: str, side: str, amount: float, reduce_only: bool = False) -> Optional[dict]:
        """Execute a spot market order."""
        if self.mode == "demo" and self.name == "binance":
            logger.error(f"[Exchange] Spot market order unavailable on Binance demo: {symbol} {side} {amount}")
            notifier.error("Exchange", f"Spot market unavailable on Binance demo: {symbol} {side}")
            return None

        guard_passed = False
        try:
            spot_symbol = self._spot_market_symbol(symbol)
            fa = self.client.amount_to_precision(spot_symbol, amount)
            normalized_amount = float(fa)
            ref_price = self.get_price(symbol)
            self._enforce_order_guard(
                symbol,
                side,
                normalized_amount,
                price=ref_price,
                reduce_only=reduce_only,
                order_type="spot_market",
                market_type="spot",
            )
            guard_passed = True
            before_snapshot = self._spot_balance_snapshot(spot_symbol)

            if self.dry_run:
                self._log_dry_run("SPOT_MARKET", spot_symbol, side=side, amount=normalized_amount)
                self._report_success("order_submit")
                self._report_success("order_confirm")
                simulated = self._augment_execution_metrics(
                    self._simulate_order(spot_symbol, side, normalized_amount, price=ref_price, order_type="market"),
                    normalized_amount,
                    ref_price,
                )
                simulated["market_type"] = "spot"
                simulated["filled"] = self._apply_dry_run_spot_fill(spot_symbol, side, normalized_amount)
                simulated["remaining"] = max(normalized_amount - float(simulated.get("filled", 0) or 0), 0.0)
                self._record_dry_run_order(simulated, open_order=False)
                return simulated

            try:
                if side == "buy":
                    order = self.client.create_market_buy_order(spot_symbol, fa, params={"type": "spot"})
                else:
                    order = self.client.create_market_sell_order(spot_symbol, fa, params={"type": "spot"})
                self._report_success("order_submit")
            except Exception as e:
                self._report_failure("order_submit", f"spot market {side} {symbol}: {e}")
                raise

            normalized = self._confirm_spot_order_state(
                spot_symbol,
                order,
                normalized_amount,
                side,
                before_snapshot=before_snapshot,
            )
            if not normalized:
                self._report_failure("order_confirm", f"spot market {side} {symbol}: confirmation missing")
                raise ValueError("spot order confirmation missing")

            avg = float(normalized.get("average", 0) or normalized.get("price", 0) or 0)
            filled = float(normalized.get("filled", 0) or 0)
            if filled > 0 and avg <= 0:
                avg = self.get_price(symbol)
                normalized["average"] = avg
                normalized["price"] = avg
            if not normalized.get("execution_ok"):
                state = normalized.get("execution_state", "unknown")
                self._report_failure("order_confirm", f"spot market {side} {symbol}: {state}")
                raise ValueError(f"spot order unconfirmed: {state}")
            self._report_success("order_confirm")
            normalized = self._augment_execution_metrics(normalized, normalized_amount, avg or self.get_price(symbol))
            logger.info(f"[Exchange] SPOT MARKET {side.upper()}: {spot_symbol} | {normalized.get('filled')} @ {avg:.2f}")
            self._mark_ws_event("orders", [normalized])
            return normalized
        except ValueError as e:
            if guard_passed:
                logger.error(f"[Exchange] Spot market order failed: {symbol} {side} {amount} - {e}")
                notifier.error("Exchange", f"Spot order failed: {symbol} {side}\n{e}")
            else:
                logger.warning(f"[Exchange] Spot market order blocked: {symbol} {side} {amount} - {e}")
            return None
        except Exception as e:
            logger.error(f"[Exchange] Spot market order failed: {symbol} {side} {amount} - {e}")
            notifier.error("Exchange", f"Spot order failed: {symbol} {side}\n{e}")
            return None

    def market_order(self, symbol: str, side: str, amount: float,
                     reduce_only: bool = False) -> Optional[dict]:
        params = {"reduceOnly": True} if reduce_only else {}
        guard_passed = False
        try:
            fa, normalized_amount, _, _ = self._prepare_order_request(symbol, amount)
            self._enforce_order_guard(
                symbol, side, normalized_amount, reduce_only=reduce_only, order_type="market"
            )
            guard_passed = True
            before_positions = self.get_positions(symbol)
            if self.dry_run:
                self._log_dry_run("MARKET", symbol, side=side, amount=normalized_amount, extra=f"reduce_only={reduce_only}")
                self._report_success("order_submit")
                self._report_success("order_confirm")
                simulated = self._augment_execution_metrics(
                    self._simulate_order(symbol, side, normalized_amount, reduce_only=reduce_only, order_type="market"),
                    normalized_amount,
                    self.get_price(symbol),
                )
                fill_price = float(simulated.get("average", 0) or simulated.get("price", 0) or self.get_price(symbol))
                filled = self._apply_dry_run_position_fill(symbol, side, normalized_amount, fill_price, reduce_only=reduce_only)
                simulated["filled"] = filled
                simulated["remaining"] = max(normalized_amount - filled, 0.0)
                simulated["execution_state"] = "filled" if simulated["remaining"] <= 1e-8 else "partial"
                simulated["execution_ok"] = simulated["execution_state"] == "filled"
                simulated["status"] = "closed" if simulated["remaining"] <= 1e-8 else "open"
                simulated["market_type"] = "swap"
                self._record_dry_run_order(simulated, open_order=False)
                return simulated
            try:
                if side == "buy":
                    order = self.client.create_market_buy_order(symbol, fa, params=params)
                else:
                    order = self.client.create_market_sell_order(symbol, fa, params=params)
                self._report_success("order_submit")
            except Exception as e:
                self._report_failure("order_submit", f"market {side} {symbol}: {e}")
                raise
            normalized = self._confirm_order_state(
                symbol,
                order,
                normalized_amount,
                is_market=True,
                side=side,
                reduce_only=reduce_only,
                before_positions=before_positions,
            )
            if not normalized or not normalized.get("execution_ok"):
                state = normalized.get("execution_state", "unknown") if normalized else "missing"
                self._report_failure("order_confirm", f"market {side} {symbol}: {state}")
                raise ValueError(f"market order unconfirmed: {state}")
            self._report_success("order_confirm")
            avg = float(normalized.get("average", 0) or 0)
            filled = float(normalized.get("filled", 0) or 0)
            if reduce_only and filled > 0 and avg <= 0:
                avg = self.get_price(symbol)
                normalized["average"] = avg
                normalized["price"] = avg
            logger.info(f"[Exchange] MARKET {side.upper()}: {symbol} | {filled} @ {avg:.2f}")
            normalized = self._augment_execution_metrics(normalized, normalized_amount, self.get_price(symbol))
            self._mark_ws_event("orders", [normalized])
            return normalized
        except ValueError as e:
            if guard_passed:
                logger.error(f"[Exchange] Market order failed: {symbol} {side} {amount} - {e}")
                notifier.error("Exchange", f"Order failed: {symbol} {side}\n{e}")
            else:
                logger.warning(f"[Exchange] Market order blocked: {symbol} {side} {amount} - {e}")
            return None
        except Exception as e:
            logger.error(f"[Exchange] Market order failed: {symbol} {side} {amount} - {e}")
            notifier.error("Exchange", f"Order failed: {symbol} {side}\n{e}")
            return None

    def limit_order(self, symbol: str, side: str, amount: float, price: float,
                    reduce_only: bool = False) -> Optional[dict]:
        params = {"reduceOnly": True} if reduce_only else {}
        guard_passed = False
        try:
            fa, normalized_amount, fp, normalized_price = self._prepare_order_request(symbol, amount, price)
            self._enforce_order_guard(
                symbol, side, normalized_amount, price=normalized_price, reduce_only=reduce_only, order_type="limit"
            )
            guard_passed = True
            if self.dry_run:
                self._log_dry_run("LIMIT", symbol, side=side, amount=normalized_amount, price=normalized_price, extra=f"reduce_only={reduce_only}")
                self._report_success("order_submit")
                self._report_success("order_confirm")
                simulated = self._augment_execution_metrics(
                    self._simulate_order(symbol, side, normalized_amount, price=normalized_price, reduce_only=reduce_only, order_type="limit"),
                    normalized_amount,
                    normalized_price,
                )
                simulated["market_type"] = "swap"
                self._record_dry_run_order(simulated, open_order=True)
                return simulated
            try:
                if side == "buy":
                    order = self.client.create_limit_buy_order(symbol, fa, fp, params=params)
                else:
                    order = self.client.create_limit_sell_order(symbol, fa, fp, params=params)
                self._report_success("order_submit")
            except Exception as e:
                self._report_failure("order_submit", f"limit {side} {symbol}: {e}")
                raise
            normalized = self._confirm_order_state(
                symbol, order, normalized_amount, expected_price=normalized_price, is_market=False, side=side
            )
            if not normalized or not normalized.get("execution_ok"):
                state = normalized.get("execution_state", "unknown") if normalized else "missing"
                order_id = normalized.get("id") if normalized else None
                self._report_failure("order_confirm", f"limit {side} {symbol}: {state}")
                raise ValueError(f"limit order placement unconfirmed: state={state} order_id={order_id}")
            self._report_success("order_confirm")
            logger.info(f"[Exchange] LIMIT {side.upper()}: {symbol} | {normalized_amount} @ {normalized_price}")
            normalized = self._augment_execution_metrics(normalized, normalized_amount, normalized_price)
            self._mark_ws_event("orders", [normalized])
            return normalized
        except ValueError as e:
            if guard_passed:
                logger.error(f"[Exchange] Limit failed: {symbol} {side} {amount}@{price} - {e}")
                notifier.error("Exchange", f"Limit failed: {symbol} {side}\n{e}")
            else:
                logger.warning(f"[Exchange] Limit order blocked: {symbol} {side} {amount}@{price} - {e}")
            return None
        except Exception as e:
            logger.error(f"[Exchange] Limit failed: {symbol} {side} {amount}@{price} - {e}")
            notifier.error("Exchange", f"Limit failed: {symbol} {side}\n{e}")
            return None

    def place_protective_stop(self, symbol: str, position_side: str, amount: float,
                              stop_price: float) -> Optional[dict]:
        close_side = "sell" if position_side == "long" else "buy"
        guard_passed = False
        try:
            fa, normalized_amount, fp, normalized_price = self._prepare_order_request(symbol, amount, stop_price)
            self._enforce_order_guard(
                symbol, close_side, normalized_amount, price=normalized_price, reduce_only=True, order_type="stop_market"
            )
            guard_passed = True
            if self.dry_run:
                self._log_dry_run(
                    "PROTECTIVE_STOP", symbol, side=close_side,
                    amount=normalized_amount, price=normalized_price, extra="reduce_only=True"
                )
                self._report_success("order_submit")
                self._report_success("order_confirm")
                order = self._simulate_order(symbol, close_side, normalized_amount, price=normalized_price, reduce_only=True, order_type="stop_market")
                order["status"] = "open"
                order["execution_state"] = "open"
                order["protective_stop"] = True
                order["stop_price"] = normalized_price
                order["market_type"] = "swap"
                order = self._augment_execution_metrics(order, normalized_amount, normalized_price)
                self._record_dry_run_order(order, open_order=True)
                return order

            params = {"reduceOnly": True}
            if self.name == "binance":
                params.update({
                    "stopPrice": fp,
                    "workingType": "MARK_PRICE",
                })
            elif self.name == "bybit":
                params.update({
                    "triggerPrice": fp,
                    "triggerBy": "MarkPrice",
                })
            else:
                params.update({
                    "stopPrice": fp,
                    "triggerPrice": fp,
                })
            try:
                order = self.client.create_order(symbol, "stop_market", close_side, fa, None, params=params)
                self._report_success("order_submit")
            except Exception as e:
                self._report_failure("order_submit", f"protective stop {close_side} {symbol}: {e}")
                raise
            normalized = self._normalize_execution(order, normalized_amount, expected_price=normalized_price, is_market=False)
            if not normalized or not normalized.get("id"):
                self._report_failure("order_confirm", f"protective stop {close_side} {symbol}: missing_order_id")
                raise ValueError("protective stop placement unconfirmed")
            normalized["execution_mode"] = self._execution_mode_label()
            normalized["execution_state"] = "open"
            normalized["execution_ok"] = True
            normalized["protective_stop"] = True
            normalized["stop_price"] = normalized_price
            self._report_success("order_confirm")
            logger.info(f"[Exchange] PROTECTIVE STOP {close_side.upper()}: {symbol} | {normalized_amount} @ {normalized_price}")
            self._mark_ws_event("orders", [normalized])
            return normalized
        except ValueError as e:
            if guard_passed:
                logger.error(f"[Exchange] Protective stop failed: {symbol} {position_side} {amount}@{stop_price} - {e}")
                notifier.error("Exchange", f"Protective stop failed: {symbol}\n{e}")
            else:
                logger.warning(f"[Exchange] Protective stop blocked: {symbol} {position_side} {amount}@{stop_price} - {e}")
            return None
        except Exception as e:
            logger.error(f"[Exchange] Protective stop failed: {symbol} {position_side} {amount}@{stop_price} - {e}")
            notifier.error("Exchange", f"Protective stop failed: {symbol}\n{e}")
            return None

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        if not order_id:
            return True
        if self.dry_run:
            self._log_dry_run("CANCEL_ORDER", symbol, extra=f"id={order_id}")
            self._sync_dry_run_orders(symbol)
            order = dict(self._dry_run_orders.pop(str(order_id), None) or self._dry_run_order_history.get(str(order_id), {}) or {})
            if order:
                order["status"] = "canceled"
                order["execution_state"] = "rejected" if float(order.get("filled", 0) or 0) <= 0 else "partial"
                order["execution_ok"] = False
                order["timestamp"] = int(time.time() * 1000)
                self._record_dry_run_order(order, open_order=False)
            return True
        try:
            self.client.cancel_order(order_id, symbol)
            logger.info(f"[Exchange] Cancelled order {order_id}: {symbol}")
            return True
        except Exception as e:
            msg = str(e).lower()
            benign = (
                "not found",
                "unknown order",
                "does not exist",
                "already closed",
                "already filled",
                "ordernotexists",
            )
            if any(token in msg for token in benign):
                logger.info(f"[Exchange] Cancel already resolved order {order_id}: {symbol}")
                return True
            logger.error(f"[Exchange] Cancel order error {symbol} {order_id}: {e}")
            return False

    def cancel_all_orders(self, symbol: str) -> int:
        if self.dry_run:
            open_orders = self.get_open_orders(symbol)
            self._log_dry_run("CANCEL_ALL", symbol, extra=f"count={len(open_orders)}")
            for order in open_orders:
                order_id = str(order.get("id") or "")
                if order_id:
                    self.cancel_order(symbol, order_id)
            return len(open_orders)
        try:
            orders = self.client.fetch_open_orders(symbol)
            for o in orders:
                self.client.cancel_order(o["id"], symbol)
            if orders:
                logger.info(f"[Exchange] Cancelled {len(orders)} orders: {symbol}")
            return len(orders)
        except Exception as e:
            logger.error(f"[Exchange] Cancel error {symbol}: {e}")
            return 0

    def get_open_orders(self, symbol: str = None) -> list:
        if self.dry_run:
            self._sync_dry_run_orders(symbol)
            orders = [
                dict(order)
                for order in self._dry_run_orders.values()
                if not symbol or order.get("symbol") == symbol
            ]
            self._mark_ws_event("orders", orders[-20:] if isinstance(orders, list) else orders)
            return orders
        try:
            orders = self.client.fetch_open_orders(symbol)
            self._mark_ws_event("orders", orders[-20:] if isinstance(orders, list) else orders)
            return orders
        except Exception as e:
            logger.error(f"[Exchange] Open orders error: {e}")
            return []

    def get_ws_status(self) -> dict:
        return self.ws_monitor.get_status()

    def get_all_open_orders(self, symbols: List[str]) -> Dict[str, list]:
        result = {}
        for symbol in symbols:
            result[symbol] = self.get_open_orders(symbol)
        return result

    def close_position(self, symbol: str) -> bool:
        positions = self.get_positions(symbol)
        if not positions:
            return True
        ok = True
        for pos in positions:
            side = "sell" if pos["side"] == "long" else "buy"
            if not self.market_order(symbol, side, pos["contracts"], reduce_only=True):
                ok = False
        return ok

    def close_all_positions(self) -> bool:
        ok = True
        for pos in self.get_positions():
            side = "sell" if pos["side"] == "long" else "buy"
            if not self.market_order(pos["symbol"], side, pos["contracts"], reduce_only=True):
                ok = False
        return ok
