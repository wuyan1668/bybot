"""
组合策略 v2.0 - 主入口
职责: 初始化、调度策略、风控检查、状态报告
"""

import time
import signal
import logging
from typing import Optional

from config import (
    TOTAL_CAPITAL, ALLOCATION, CHECK_INTERVAL,
    LOG_LEVEL, LOG_FILE, EXCHANGE, TELEGRAM, RISK, DYNAMIC_GRID, RUNTIME, OPERATOR, RISK_CONTROL, ORDER_GUARD, WEBSOCKET,
)
from exchange import ExchangeClient
from instance_lock import SingleInstanceLock
from strategies import FundingArbStrategy, DynamicGridStrategy, TrendDCAStrategy
from risk_manager import RiskManager
from notifier import notifier
from execution_ledger import ledger
from config_validator import validate_config
from circuit_breaker import CircuitBreaker
from state_store import get_state_file, get_state_namespace, load_state, save_state

# ==================== 日志 ====================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


class PortfolioManager:
    def __init__(self):
        self.running = True
        self.start_time = time.time()
        self.cycle = 0
        self.recovery_blocked = False
        self.recovery_reason = ""
        self.blocked_strategies = {}
        self.environment_mode = EXCHANGE.get("mode", "demo").upper()
        self.execution_mode = "EXECUTE"
        self.banner_execution_mode = self.execution_mode
        self.last_account_balance = None
        self.last_risk_snapshot = {}
        self.last_reconciliation = {}
        self.last_rehearsal_cycle = 0
        self._pending_orders_cache = {"ts": 0.0, "orders": []}
        self.circuit_breaker = CircuitBreaker()

        # 初始化交易所
        self.exchange = ExchangeClient()

        # 风控
        self.risk = RiskManager(self.exchange, TOTAL_CAPITAL)

        # 资金分配
        cap = {
            "funding_arb": TOTAL_CAPITAL * ALLOCATION["funding_arb"],
            "dynamic_grid": TOTAL_CAPITAL * ALLOCATION["dynamic_grid"],
            "trend_dca": TOTAL_CAPITAL * ALLOCATION["trend_dca"],
        }

        # 初始化策略
        self.strategies = {
            "funding_arb": FundingArbStrategy(self.exchange, cap["funding_arb"]),
            "dynamic_grid": DynamicGridStrategy(self.exchange, cap["dynamic_grid"]),
            "trend_dca": TrendDCAStrategy(self.exchange, cap["trend_dca"]),
        }
        self.paused = {k: False for k in self.strategies}

        # 状态报告间隔 (每30分钟)
        self.report_interval = 1800
        self.last_report = 0

        # 信号处理
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

        # 启动通知
        mode = "🔧 模拟盘 (Testnet)" if EXCHANGE["mode"] == "demo" else "🔴 实盘"
        exec_mode = "🧪 DRY-RUN" if EXCHANGE.get("dry_run", True) else "✅ 实际执行"
        tg_status = "✅ 已开启" if notifier.enabled else "❌ 未配置"
        config_summary = (
            f"交易所: {self.exchange.name.upper()}\n"
            f"模式: {mode}\n"
            f"执行: {exec_mode}\n"
            f"环境标签: {self.environment_mode}/{self.execution_mode}\n"
            f"通知: {tg_status} ({TELEGRAM['notify_level']})\n"
            f"总资金: {TOTAL_CAPITAL} USDT\n"
            f"─────────────────────\n"
            f"资金费率套利: {cap['funding_arb']:.0f} USDT ({ALLOCATION['funding_arb']:.0%})\n"
            f"动态网格:     {cap['dynamic_grid']:.0f} USDT ({ALLOCATION['dynamic_grid']:.0%})\n"
            f"趋势DCA:      {cap['trend_dca']:.0f} USDT ({ALLOCATION['trend_dca']:.0%})\n"
            f"─────────────────────\n"
            f"最大回撤: {TOTAL_CAPITAL * 0.15:.0f} USDT (15%)\n"
            f"紧急止损: {TOTAL_CAPITAL * 0.20:.0f} USDT (20%)"
        )

        self._validate_startup_safety()
        self._attach_runtime_services()
        self._print_banner(mode, cap)
        logger.info(f"  State: {get_state_file()}")
        notifier.startup(config_summary)
        self._recover_runtime_state()
        self._maybe_reset_risk_baseline()
        self._save_state()

    def _attach_runtime_services(self):
        self.exchange.attach_runtime_callbacks(self._record_failure, self._record_success)
        for name, strat in self.strategies.items():
            setattr(strat, "portfolio", self)
            setattr(strat, "strategy_key", name)

    def _print_banner(self, mode: str, cap: dict):
        logger.info("=" * 55)
        logger.info("  COMBO STRATEGY v2.0")
        logger.info(f"  Mode: {mode}")
        logger.info(f"  Execution: {self.environment_mode}/{self.banner_execution_mode}")
        logger.info(f"  Exchange: {self.exchange.name.upper()}")
        logger.info(f"  Capital: {TOTAL_CAPITAL} USDT")
        logger.info(f"  FundingArb:  {cap['funding_arb']:.0f} USDT")
        logger.info(f"  DynamicGrid: {cap['dynamic_grid']:.0f} USDT")
        logger.info(f"  TrendDCA:    {cap['trend_dca']:.0f} USDT")
        logger.info(f"  FundingSymbols: {', '.join(self.strategies['funding_arb'].configured_symbols())}")
        logger.info(f"  GridSymbol: {self.strategies['dynamic_grid'].symbol}")
        logger.info(f"  DCASymbol: {self.strategies['trend_dca'].symbol}")
        logger.info("=" * 55)

    def _validate_startup_safety(self):
        validation = validate_config()
        issues = list(validation.get("errors", []))
        for warning in validation.get("warnings", []):
            logger.warning(f"Config warning: {warning}")
            notifier.risk_alert("配置警告", warning, "检查参数")
        if validation.get("errors"):
            for error in validation.get("errors", []):
                logger.error(f"Config error: {error}")
                notifier.risk_alert("配置错误", error, "拒绝启动交易")
        if not EXCHANGE.get("api_key") or not EXCHANGE.get("api_secret"):
            issues.append("缺少 API_KEY/API_SECRET")

        if EXCHANGE.get("dry_run", False):
            issues.append("宸茬Щ闄?DRY_RUN锛岀▼搴忓彧鍏佽浜ゆ槗鎵€鐩磋繛涓嬪崟锛岃璁剧疆 DRY_RUN=false")

        if EXCHANGE["mode"] == "live":
            if EXCHANGE.get("live_confirm") != "I_UNDERSTAND_LIVE_TRADING":
                issues.append("未设置 LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING")
            if not notifier.enabled:
                issues.append("实盘模式要求配置 Telegram 关键告警")
            if self.strategies["funding_arb"].is_live_blocked():
                self.paused["funding_arb"] = True
                self.blocked_strategies["funding_arb"] = "实盘模式默认禁用 funding_arb，需设置 FUNDING_LIVE_CONFIRM=ENABLE_FUNDING_ARB_LIVE"

        grid_symbol = self.strategies["dynamic_grid"].symbol
        dca_symbol = self.strategies["trend_dca"].symbol
        if grid_symbol == dca_symbol:
            if RUNTIME.get("allow_shared_symbols", False):
                logger.warning(
                    f"Shared symbol acknowledged: GRID_SYMBOL and DCA_SYMBOL are both {grid_symbol}. "
                    "TrendDCA will still be paused while grid orders exist."
                )
            else:
                logger.warning(
                    f"Shared symbol detected: GRID_SYMBOL and DCA_SYMBOL are both {grid_symbol}. "
                    "This reduces effective strategy coverage because TrendDCA will be paused while grid orders exist. "
                    "Set DCA_SYMBOL to another contract or ALLOW_SHARED_SYMBOLS=true to acknowledge."
                )

        if issues:
            self._enter_recovery_blocked("; ".join(issues))


    def _export_runtime_state(self) -> dict:
        return {
            "portfolio": {
                "cycle": self.cycle,
                "start_time": self.start_time,
                "last_report": self.last_report,
                "paused": self.paused,
                "recovery_blocked": self.recovery_blocked,
                "recovery_reason": self.recovery_reason,
                "blocked_strategies": self.blocked_strategies,
                "exchange": self.exchange.name,
                "mode": EXCHANGE["mode"],
                "execution_mode": self.execution_mode,
                "environment_mode": self.environment_mode,
            },
            "risk": self.risk.export_state(),
            "circuit_breaker": self.circuit_breaker.export_state(),
            "account": {
                "balance": self.last_account_balance,
                "risk_snapshot": self.last_risk_snapshot,
                "last_reconciliation": self.last_reconciliation,
            },
            "strategies": {
                name: strat.export_state()
                for name, strat in self.strategies.items()
            },
        }

    def _restore_runtime_state(self, snapshot: dict):
        portfolio = snapshot.get("portfolio", {})
        self.cycle = int(portfolio.get("cycle", 0) or 0)
        self.start_time = float(portfolio.get("start_time", self.start_time) or self.start_time)
        self.last_report = float(portfolio.get("last_report", 0) or 0)
        paused = portfolio.get("paused", {}) or {}
        for name in self.paused:
            self.paused[name] = bool(paused.get(name, False))
        self.recovery_blocked = bool(portfolio.get("recovery_blocked", False))
        self.recovery_reason = portfolio.get("recovery_reason", "") or ""
        self.blocked_strategies = dict(portfolio.get("blocked_strategies", {}) or {})
        self.execution_mode = portfolio.get("execution_mode", self.execution_mode) or self.execution_mode
        self.environment_mode = portfolio.get("environment_mode", self.environment_mode) or self.environment_mode
        self.banner_execution_mode = "EXECUTE"
        self.execution_mode = self.banner_execution_mode

        self.risk.import_state(snapshot.get("risk", {}))
        self.circuit_breaker.import_state(snapshot.get("circuit_breaker", {}))

        account = snapshot.get("account", {}) or {}
        self.last_account_balance = account.get("balance")
        self.last_risk_snapshot = dict(account.get("risk_snapshot", {}) or {})
        self.last_reconciliation = dict(account.get("last_reconciliation", {}) or {})
        for name, strat in self.strategies.items():
            strat.import_state((snapshot.get("strategies", {}) or {}).get(name, {}))

    def _save_state(self):
        try:
            save_state(self._export_runtime_state())
        except Exception as e:
            logger.error(f"State save failed: {e}")

    def _record_failure(self, category: str, detail: str, strategy: str = None):
        result = self.circuit_breaker.record_failure(category)
        ledger.record_event("failure", {
            "category": category,
            "detail": detail,
            "strategy": strategy,
            "count": result.get("count"),
            "threshold": result.get("threshold"),
        })
        if result.get("tripped"):
            action = "暂停全部策略"
            if strategy and strategy in self.paused:
                self.paused[strategy] = True
                self.blocked_strategies[strategy] = result.get("reason", detail)
                action = f"暂停 {strategy}"
            else:
                for name in self.paused:
                    self.paused[name] = True
            notifier.risk_alert("连续异常熔断", result.get("reason", detail), action)

    def _record_success(self, category: str):
        self.circuit_breaker.reset(category)

    def _maybe_reset_risk_baseline(self):
        if not RISK_CONTROL.get("auto_reset_baseline", False):
            return

        balance, positions, open_orders = self._get_reconciliation_inputs(load_state())
        equity = float((balance or {}).get("total", 0) or 0)
        if equity <= 0:
            return
        if positions or any(open_orders.values()):
            logger.warning("Risk baseline reset skipped: open positions or orders detected")
            return

        current_baseline = float(self.risk.peak_capital or self.risk.initial_capital or 0)
        if current_baseline <= 0:
            return
        if equity >= current_baseline * (1 - float(RISK.get("emergency_stop_loss", 0.2) or 0.2)):
            return

        if EXCHANGE.get("mode") == "live" and RISK_CONTROL.get("baseline_reset_confirm") != "I_CONFIRM_RESET_RISK_BASELINE":
            logger.warning("Risk baseline reset skipped in live mode: missing confirmation")
            return

        old_baseline = current_baseline
        self.risk.reset_baseline(equity)
        self.last_account_balance = balance
        notifier.risk_alert(
            "风控基准重置",
            f"账户空仓且权益 {equity:.2f} 低于旧基准 {old_baseline:.2f}，已自动重置风控基准",
            "更新 initial/peak/daily 基准"
        )
        ledger.record_event("risk_baseline_reset", {
            "old_baseline": old_baseline,
            "new_baseline": equity,
            "mode": EXCHANGE.get("mode"),
        })

    @staticmethod
    def _pending_order_key(order: dict) -> str:
        order_id = str((order or {}).get("id") or "").strip()
        if order_id:
            return f"id:{order_id}"
        symbol = str((order or {}).get("symbol") or "")
        side = str((order or {}).get("side") or "")
        price = float((order or {}).get("price", 0) or 0)
        amount = float((order or {}).get("amount", 0) or 0)
        return f"synthetic:{symbol}:{side}:{price:.8f}:{amount:.8f}"

    def _strategy_symbols(self, strategy_name: str) -> set[str]:
        strategy = self.strategies.get(strategy_name)
        if strategy is None:
            return set()

        symbols = set()
        symbol = getattr(strategy, "symbol", None)
        if symbol:
            symbols.add(symbol)

        if strategy_name == "funding_arb":
            symbols.update(getattr(strategy, "configured_symbols", lambda: [])())
            symbols.update((getattr(strategy, "positions", {}) or {}).keys())

        return {sym for sym in symbols if sym}

    def _normalize_pending_order(self, order: dict, symbol: str = None, source: str = "exchange") -> Optional[dict]:
        info = dict(order or {})
        order_symbol = str(symbol or info.get("symbol") or "").strip()
        if not order_symbol:
            return None

        status = str(info.get("status") or "").lower()
        if status and status not in ("open", "new"):
            return None

        role = str(info.get("role") or "").lower()
        reduce_only = bool(info.get("reduceOnly", info.get("reduce_only", False)))
        if role == "exit":
            reduce_only = True

        amount = float(info.get("remaining", 0) or info.get("amount", 0) or 0)
        price = float(info.get("price", 0) or 0)
        if amount <= 0 or price <= 0:
            return None

        return {
            "id": str(info.get("id") or "").strip(),
            "symbol": order_symbol,
            "side": str(info.get("side") or "").lower(),
            "amount": amount,
            "price": price,
            "status": status or "open",
            "reduce_only": reduce_only,
            "role": role,
            "strategy": str(info.get("strategy") or "").strip(),
            "source": source,
            "notional": abs(amount * price),
        }

    def _infer_pending_order_strategy(self, order: dict) -> str:
        order_id = str((order or {}).get("id") or "").strip()
        grid = self.strategies.get("dynamic_grid")
        if grid and order_id and order_id in (getattr(grid, "active_orders", {}) or {}):
            return "dynamic_grid"

        symbol = str((order or {}).get("symbol") or "")
        if not symbol:
            return ""

        candidates = [
            name for name in self.strategies
            if symbol in self._strategy_symbols(name)
        ]
        if len(candidates) == 1:
            return candidates[0]
        return ""

    def _collect_pending_orders(self, cache_ttl: float = 1.0) -> list[dict]:
        cache_ts = float(self._pending_orders_cache.get("ts", 0) or 0)
        if time.time() - cache_ts > cache_ttl:
            live_pending = {}
            for order in self.exchange.get_open_orders():
                normalized = self._normalize_pending_order(order, source="exchange")
                if not normalized or normalized["reduce_only"]:
                    continue
                normalized["strategy"] = normalized["strategy"] or self._infer_pending_order_strategy(normalized)
                live_pending[self._pending_order_key(normalized)] = normalized
            self._pending_orders_cache = {
                "ts": time.time(),
                "orders": list(live_pending.values()),
            }

        pending = {
            self._pending_order_key(item): dict(item)
            for item in (self._pending_orders_cache.get("orders", []) or [])
        }

        grid = self.strategies.get("dynamic_grid")
        if grid:
            for order_id, tracked in (getattr(grid, "active_orders", {}) or {}).items():
                local_order = dict(tracked or {})
                local_order.setdefault("id", order_id)
                local_order.setdefault("symbol", grid.symbol)
                local_order.setdefault("strategy", "dynamic_grid")
                normalized = self._normalize_pending_order(local_order, source="dynamic_grid")
                if not normalized or normalized["reduce_only"]:
                    continue
                pending.setdefault(self._pending_order_key(normalized), normalized)

        return list(pending.values())

    def _compose_equity_balance(self) -> dict:
        swap_balance = self.exchange.get_balance() or {"total": 0.0, "free": 0.0, "used": 0.0}
        spot_assets = self.exchange.get_spot_exposure(min_usdt_value=1.0)
        balance_total = float(swap_balance.get("total", 0) or 0)
        spot_total = sum(float(item.get("value_usdt", 0) or 0) for item in spot_assets)
        includes_spot_assets = bool(swap_balance.get("includes_spot_assets", False))

        combined = dict(swap_balance)
        combined["balance_total"] = balance_total
        combined["swap_total"] = balance_total
        combined["spot_total"] = spot_total
        combined["spot_assets"] = spot_assets
        combined["spot_included_in_total"] = includes_spot_assets
        combined["double_count_offset"] = spot_total if includes_spot_assets else 0.0
        combined["total"] = balance_total if includes_spot_assets else balance_total + spot_total
        return combined

    def _funding_spot_notional_totals(self, symbol: str = None) -> tuple[float, float]:
        strategy = self.strategies.get("funding_arb")
        if strategy is None:
            return 0.0, 0.0

        total = 0.0
        symbol_total = 0.0
        for pos_symbol, position in (getattr(strategy, "positions", {}) or {}).items():
            spot_amount = float(position.get("spot_amount", position.get("amount", 0)) or 0)
            if spot_amount <= 0:
                continue
            spot_price = float(position.get("spot_entry_price", 0) or position.get("entry_price", 0) or 0)
            if spot_price <= 0:
                try:
                    spot_price = float(self.exchange.get_price(pos_symbol) or 0)
                except Exception:
                    spot_price = 0.0
            if spot_price <= 0:
                continue
            notional = abs(spot_amount * spot_price)
            total += notional
            if symbol and pos_symbol == symbol:
                symbol_total += notional
        return total, symbol_total

    def _get_reconciliation_inputs(self, snapshot: dict):
        symbols = set()
        snapshot_strategies = (snapshot or {}).get("strategies", {}) or {}
        snapshot_funding = (snapshot_strategies.get("funding_arb", {}) or {}).get("positions", {}) or {}
        symbols.update(snapshot_funding.keys())
        symbols.update(self.strategies["funding_arb"].positions.keys())
        symbols.add(self.strategies["dynamic_grid"].symbol)
        symbols.add(self.strategies["trend_dca"].symbol)
        positions = self.exchange.get_positions()
        orders = self.exchange.get_all_open_orders(sorted(symbols))
        balance = self._compose_equity_balance()
        return balance, positions, orders

    def _build_portfolio_state(self, strategy_name: str = None) -> dict:
        ws_status = self.exchange.get_ws_status()
        ws_stale = False
        if ws_status.get("enabled"):
            ws_stale = self.exchange.ws_monitor.stale(int(WEBSOCKET.get("stale_after_sec", 180) or 180))
        reconciliation_ok = not bool((self.last_reconciliation or {}).get("global"))
        strategy_blocked = bool(strategy_name and self.paused.get(strategy_name, False))
        strategy_block_reason = self.blocked_strategies.get(strategy_name, "") if strategy_name else ""
        if strategy_blocked and not strategy_block_reason:
            strategy_block_reason = "strategy paused"
        return {
            "recovery_blocked": bool(self.recovery_blocked),
            "circuit_breaker_tripped": bool(self.circuit_breaker.is_tripped()),
            "ws_stale": bool(ws_stale),
            "reconciliation_ok": bool(reconciliation_ok),
            "strategy_blocked": strategy_blocked,
            "strategy_block_reason": strategy_block_reason,
        }

    def _build_account_state(self) -> dict:
        balance = self.last_account_balance if isinstance(self.last_account_balance, dict) else None
        if balance is None:
            balance = self._compose_equity_balance()
            self.last_account_balance = balance
        return {
            "equity": float(balance.get("total", 0) or 0),
            "free": float(balance.get("free", 0) or 0),
            "used": float(balance.get("used", 0) or 0),
        }

    def _build_exposure_state(self, strategy_name: str, symbol: str, side: str,
                              amount: float, price: float = None, reduce_only: bool = False,
                              risk_context: dict = None) -> dict:
        snapshot = self.last_risk_snapshot or self._get_account_snapshot()
        self.last_risk_snapshot = snapshot
        account_state = self._build_account_state()
        risk_context = dict(risk_context or {})
        price = float(price or self.exchange.get_price(symbol) or 0)
        requested_notional = abs(float(amount or 0) * price)
        positions = list(snapshot.get("positions", []) or [])
        current_total = sum(abs(float(p.get("notional", 0) or 0)) for p in positions)
        current_symbol = sum(abs(float(p.get("notional", 0) or 0)) for p in positions if p.get("symbol") == symbol)
        funding_spot_total, funding_spot_symbol = self._funding_spot_notional_totals(symbol)
        current_total += funding_spot_total
        current_symbol += funding_spot_symbol
        strategy = self.strategies.get(strategy_name)
        strategy_symbols = self._strategy_symbols(strategy_name)
        strategy_symbols.add(symbol)
        strategy_current = 0.0
        if strategy is not None:
            strategy_current = float(getattr(strategy, "current_strategy_notional", lambda: 0.0)() or 0.0)
        pending_orders = self._collect_pending_orders()
        pending_total_open_order_notional = sum(
            float(item.get("notional", 0) or 0) for item in pending_orders
        )
        pending_symbol_open_order_notional = sum(
            float(item.get("notional", 0) or 0)
            for item in pending_orders
            if item.get("symbol") == symbol
        )
        pending_strategy_open_order_notional = sum(
            float(item.get("notional", 0) or 0)
            for item in pending_orders
            if item.get("strategy") == strategy_name
            or (not item.get("strategy") and item.get("symbol") in strategy_symbols)
        )
        paired_notional_multiplier = float(risk_context.get("paired_notional_multiplier", 0.0) or 0.0)
        if risk_context.get("paired_spot_leg") and paired_notional_multiplier <= 0:
            paired_notional_multiplier = 1.0
        paired_requested_notional = 0.0
        if not reduce_only and paired_notional_multiplier > 0:
            paired_requested_notional = requested_notional * max(paired_notional_multiplier, 0.0)
        equity = float(account_state.get("equity", 0) or 0)
        max_symbol = float(equity * float(ORDER_GUARD.get("max_symbol_notional_pct", 0) or 0))
        max_total = float(equity * float(ORDER_GUARD.get("max_total_notional_pct", 0) or 0))
        max_strategy = float(
            getattr(strategy, "max_strategy_notional", lambda: getattr(strategy, "capital", 0.0))() or 0.0
        )

        projected_total = current_total + pending_total_open_order_notional
        projected_symbol = current_symbol + pending_symbol_open_order_notional
        projected_strategy = strategy_current + pending_strategy_open_order_notional
        if not reduce_only:
            effective_requested_notional = requested_notional + paired_requested_notional
            projected_total += effective_requested_notional
            projected_symbol += effective_requested_notional
            projected_strategy += effective_requested_notional
        else:
            effective_requested_notional = requested_notional

        return {
            "current_total_notional": current_total,
            "current_symbol_notional": current_symbol,
            "current_strategy_notional": strategy_current,
            "pending_open_order_notional": pending_symbol_open_order_notional,
            "pending_total_open_order_notional": pending_total_open_order_notional,
            "pending_symbol_open_order_notional": pending_symbol_open_order_notional,
            "pending_strategy_open_order_notional": pending_strategy_open_order_notional,
            "requested_notional": requested_notional,
            "paired_requested_notional": paired_requested_notional,
            "effective_requested_notional": effective_requested_notional,
            "projected_total_notional": projected_total,
            "projected_symbol_notional": projected_symbol,
            "projected_strategy_notional": projected_strategy,
            "max_total_notional": max_total,
            "max_symbol_notional": max_symbol,
            "max_strategy_notional": max_strategy,
            "reduce_only": bool(reduce_only),
            "worst_case_total_notional": projected_total,
            "worst_case_symbol_notional": projected_symbol,
            "worst_case_strategy_notional": projected_strategy,
        }

    def request_trade_approval(self, strategy_name: str, symbol: str, side: str, order_type: str,
                               amount: float, price: float = None, reduce_only: bool = False,
                               risk_context: dict = None) -> dict:
        account_state = self._build_account_state()
        exposure_state = self._build_exposure_state(
            strategy_name,
            symbol,
            side,
            amount,
            price=price,
            reduce_only=reduce_only,
            risk_context=risk_context,
        )
        portfolio_state = self._build_portfolio_state(strategy_name)
        decision = self.risk.pre_trade_check(
            strategy_name=strategy_name,
            symbol=symbol,
            side=side,
            order_type=order_type,
            amount=amount,
            price=price,
            reduce_only=reduce_only,
            account_state=account_state,
            exposure_state=exposure_state,
            portfolio_state=portfolio_state,
            risk_context=risk_context,
        )
        if not decision.get("approved", False):
            ledger.record_risk_rejection(strategy_name, symbol, str(decision.get("reason", "rejected")), decision)
        return decision

    def enter_protection_mode(self, reason: str, strategy: str = None, details: dict = None):
        payload = dict(details or {})
        if strategy:
            self.paused[strategy] = True
            self.blocked_strategies[strategy] = reason
            scope = strategy
        else:
            self.recovery_blocked = True
            self.recovery_reason = reason
            for name in self.paused:
                self.paused[name] = True
            scope = "global"
        ledger.record_protection_event(scope, reason, payload, strategy=strategy)
        notifier.protection_mode(scope, reason, str(payload)[:500])

    @staticmethod
    def _is_close(a: float, b: float, tol: float = 1e-6) -> bool:
        return abs(a - b) <= tol

    def _normalize_order(self, order: dict) -> dict:
        info = order or {}
        amount = float(info.get("amount", 0) or info.get("remaining", 0) or info.get("filled", 0) or 0)
        price = float(info.get("price", 0) or 0)
        status = (info.get("status") or "").lower()
        return {
            "id": info.get("id"),
            "side": (info.get("side") or "").lower(),
            "price": price,
            "amount": amount,
            "status": status,
        }

    @staticmethod
    def _is_uncertain_order(order: dict) -> bool:
        state = (order or {}).get("execution_state") or ""
        return state in ("partial", "uncertain")

    def _classify_grid_missing_order(self, symbol: str, order_id: str, expected: dict,
                                     live_grid_orders: dict, grid_live_position: Optional[dict]) -> str:
        expected_side = (expected.get("side") or "").lower()
        expected_amount = float(expected.get("amount", 0) or 0)
        expected_price = float(expected.get("price", 0) or 0)
        classified = self.exchange.classify_order(
            order_id,
            symbol,
            expected_amount=expected_amount,
            expected_price=expected_price,
        )
        state = (classified or {}).get("execution_state") or "unknown"
        filled = float((classified or {}).get("filled", 0) or 0)

        if state == "rejected":
            return f"挂单已撤销/拒绝: {order_id}"
        if state == "filled":
            if grid_live_position and float(grid_live_position.get("contracts", 0) or 0) > 0:
                return f"挂单已成交且存在残余仓位需人工复核: {order_id}"
            return f"挂单已成交但缺少对手单恢复: {order_id}"
        if state == "partial" or filled > 0:
            return f"挂单部分成交需人工复核: {order_id}"
        if state in ("open", "new"):
            side_matches = sum(1 for order in live_grid_orders.values() if order.get("side") == expected_side)
            return f"挂单状态异常需人工复核: {order_id} ({expected_side} side open={side_matches})"
        if self._is_uncertain_order(expected):
            return f"挂单状态不确定需人工复核: {order_id}"
        return f"挂单丢失需人工复核: {order_id}"

    def _reconcile_snapshot(self, snapshot: dict, balance: dict, positions: list, open_orders: dict) -> dict:
        global_mismatches = []
        strategy_mismatches = {name: [] for name in self.strategies}
        strategies = snapshot.get("strategies", {}) or {}

        funding_state = strategies.get("funding_arb", {}) or {}
        funding_expected = funding_state.get("positions", {}) or {}
        funding_symbols = set(funding_expected.keys())
        expected_spot_assets = {
            str((pos.get("spot_asset") or symbol.split(":")[0].split("/")[0]) or "").upper()
            for symbol, pos in funding_expected.items()
        }
        live_spot_assets = {
            str(item.get("asset") or "").upper(): item
            for item in (balance or {}).get("spot_assets", [])
        }
        live_positions = {p["symbol"]: p for p in positions}
        for symbol, pos in funding_expected.items():
            live = live_positions.get(symbol)
            if not live:
                strategy_mismatches["funding_arb"].append(f"缺少持仓: {symbol}")
                continue
            if live.get("side") != "short":
                strategy_mismatches["funding_arb"].append(f"持仓方向不匹配: {symbol}")
            if not self._is_close(float(live.get("contracts", 0) or 0), float(pos.get("amount", 0) or 0), 1e-4):
                strategy_mismatches["funding_arb"].append(f"持仓数量不匹配: {symbol}")
            if pos.get("funding_estimated"):
                logger.warning(f"FundingArb recovered with estimated funding only: {symbol}")
            if not self.exchange.dry_run:
                asset = pos.get("spot_asset") or symbol.split(":")[0].split("/")[0]
                spot = self.exchange.get_spot_balance(asset)
                expected_spot = float(pos.get("spot_amount", pos.get("amount", 0)) or 0)
                live_spot = float((spot or {}).get("total", 0) or 0)
                if spot is None:
                    strategy_mismatches["funding_arb"].append(f"现货余额读取失败: {symbol}")
                elif not self._is_close(live_spot, expected_spot, 1e-4):
                    strategy_mismatches["funding_arb"].append(f"现货数量不匹配: {symbol} {asset}")

        for symbol in funding_symbols:
            live_positions.pop(symbol, None)

        unexpected_spot_assets = sorted(
            asset for asset in live_spot_assets
            if asset not in expected_spot_assets and asset not in ("USDT", "USDC")
        )
        for asset in unexpected_spot_assets:
            details = live_spot_assets.get(asset, {})
            global_mismatches.append(
                f"鏈瘑鍒殑鐜拌揣璧勪骇: {asset} {float(details.get('total', 0) or 0):.6f}"
            )

        grid_state = strategies.get("dynamic_grid", {}) or {}
        grid_symbol = self.strategies["dynamic_grid"].symbol
        grid_expected_orders = grid_state.get("active_orders", {}) or {}
        live_grid_orders = {
            order["id"]: self._normalize_order(order)
            for order in open_orders.get(grid_symbol, [])
            if order.get("id") and (order.get("status") or "").lower() in ("open", "new")
        }
        grid_live_position = next((p for p in positions if p["symbol"] == grid_symbol), None)
        missing_grid_orders = sorted(set(grid_expected_orders.keys()) - set(live_grid_orders.keys()))
        extra_grid_orders = sorted(set(live_grid_orders.keys()) - set(grid_expected_orders.keys()))
        if missing_grid_orders:
            for oid in missing_grid_orders[:5]:
                strategy_mismatches["dynamic_grid"].append(
                    self._classify_grid_missing_order(grid_symbol, oid, grid_expected_orders.get(oid, {}), live_grid_orders, grid_live_position)
                )
            if len(missing_grid_orders) > 5:
                strategy_mismatches["dynamic_grid"].append(f"其余挂单丢失需人工复核: {len(missing_grid_orders) - 5}")
        if extra_grid_orders:
            strategy_mismatches["dynamic_grid"].append(f"存在额外挂单: {len(extra_grid_orders)}")
        if grid_live_position and float(grid_live_position.get("contracts", 0) or 0) > 0:
            strategy_mismatches["dynamic_grid"].append(
                f"存在残余仓位: {grid_symbol} {grid_live_position.get('side')} {float(grid_live_position.get('contracts', 0) or 0):.6f}"
            )
            live_positions.pop(grid_symbol, None)

        for oid, expected in grid_expected_orders.items():
            live = live_grid_orders.get(oid)
            if not live:
                continue
            expected_side = (expected.get("side") or "").lower()
            if expected_side and live["side"] != expected_side:
                strategy_mismatches["dynamic_grid"].append(f"挂单方向不匹配: {oid}")
            if not self._is_close(float(live["price"] or 0), float(expected.get("price", 0) or 0), 1e-2):
                strategy_mismatches["dynamic_grid"].append(f"挂单价格不匹配: {oid}")
            if not self._is_close(float(live["amount"] or 0), float(expected.get("amount", 0) or 0), 1e-6):
                strategy_mismatches["dynamic_grid"].append(f"挂单数量不匹配: {oid}")

        dca_state = strategies.get("trend_dca", {}) or {}
        dca_expected = dca_state.get("position")
        dca_symbol = self.strategies["trend_dca"].symbol
        live_dca = next((p for p in positions if p["symbol"] == dca_symbol), None)
        if dca_expected and not live_dca:
            strategy_mismatches["trend_dca"].append(f"缺少持仓: {dca_symbol}")
        elif dca_expected and live_dca:
            expected_side = dca_expected.get("side")
            live_side = live_dca.get("side")
            if expected_side == "long" and live_side != "long":
                strategy_mismatches["trend_dca"].append(f"持仓方向不匹配: {dca_symbol}")
            if expected_side == "short" and live_side != "short":
                strategy_mismatches["trend_dca"].append(f"持仓方向不匹配: {dca_symbol}")
            if not self._is_close(float(live_dca.get("contracts", 0) or 0), float(dca_expected.get("total_amount", 0) or 0), 1e-4):
                strategy_mismatches["trend_dca"].append(f"持仓数量不匹配: {dca_symbol}")
            live_positions.pop(dca_symbol, None)
        elif live_dca and not dca_expected:
            strategy_mismatches["trend_dca"].append(f"存在交易所持仓但本地未记录: {dca_symbol}")
            live_positions.pop(dca_symbol, None)

        if dca_expected:
            for layer in dca_expected.get("layers", []):
                if self._is_uncertain_order(layer):
                    strategy_mismatches["trend_dca"].append(f"DCA 层状态不确定需人工复核: {dca_symbol}")
                    break

        unexpected_positions = sorted(live_positions.keys())
        for symbol in unexpected_positions:
            global_mismatches.append(f"未识别的交易所持仓: {symbol}")

        if not snapshot.get("saved_at") and (positions or any(open_orders.values())):
            global_mismatches.append("发现交易所已有持仓或挂单，但本地没有可恢复快照")

        strategy_mismatches = {
            name: reasons
            for name, reasons in strategy_mismatches.items()
            if reasons
        }
        return {
            "global": global_mismatches,
            "strategies": strategy_mismatches,
        }

    def _enter_recovery_blocked(self, reason: str):
        self.recovery_blocked = True
        self.recovery_reason = reason
        for name in self.paused:
            self.paused[name] = True
        logger.warning(f"Recovery blocked: {reason}")
        notifier.risk_alert("启动恢复失败", reason, "暂停全部策略")

    def _apply_strategy_recovery_blocks(self, strategy_mismatches: dict):
        recovery_blocks = {
            name: "; ".join(reasons)
            for name, reasons in (strategy_mismatches or {}).items()
            if reasons
        }
        self.blocked_strategies.update(recovery_blocks)
        for name, reason in recovery_blocks.items():
            self.paused[name] = True
            logger.warning(f"Recovery paused strategy {name}: {reason}")
            notifier.risk_alert("启动恢复告警", f"{name}: {reason}", f"暂停 {name}")

    def _maybe_rebuild_dynamic_grid(self):
        confirm = DYNAMIC_GRID.get("rebuild_confirm", "")
        if confirm != "REBUILD_DYNAMIC_GRID":
            return
        reason = self.blocked_strategies.get("dynamic_grid", "")
        if not reason:
            return
        if "挂单" not in reason and "grid" not in reason.lower():
            return
        price = self.exchange.get_price(self.strategies["dynamic_grid"].symbol)
        self.strategies["dynamic_grid"].rebuild_from_recovery(price)
        self.paused["dynamic_grid"] = False
        self.blocked_strategies.pop("dynamic_grid", None)
        self.last_reconciliation = {}
        logger.warning("DynamicGrid recovery rebuild executed from operator confirmation")
        notifier.risk_alert("恢复重建", "dynamic_grid 已清空旧网格并重建", "恢复 dynamic_grid")

    def _recover_runtime_state(self):
        snapshot = load_state()
        balance, positions, open_orders = self._get_reconciliation_inputs(snapshot)

        has_snapshot = bool(snapshot.get("saved_at"))
        has_live_exposure = bool(positions) or any(open_orders.values()) or bool((balance or {}).get("spot_assets"))

        if not has_snapshot and not has_live_exposure:
            logger.info("No runtime snapshot and no live exposure detected")
            return

        reconciliation = self._reconcile_snapshot(snapshot, balance, positions, open_orders)
        self.last_reconciliation = reconciliation
        global_mismatches = reconciliation["global"]
        strategy_mismatches = reconciliation["strategies"]
        if global_mismatches:
            self.enter_protection_mode("; ".join(global_mismatches), details={"source": "reconciliation", "global": global_mismatches})
            return

        if has_snapshot:
            self._restore_runtime_state(snapshot)
            logger.info("Runtime state restored from snapshot")
            if strategy_mismatches:
                self._apply_strategy_recovery_blocks(strategy_mismatches)
            self._maybe_rebuild_dynamic_grid()
            if balance:
                logger.info(
                    f"Recovery balance: total={balance['total']:.4f} free={balance['free']:.4f} used={balance['used']:.4f}"
                )

    def _handle_operator_action(self):
        action = (OPERATOR.get("action") or "").strip()
        if not action:
            return
        confirm = (OPERATOR.get("confirm") or "").strip()
        logger.warning(f"Operator action requested: {action}")
        ledger.record_operator_action(action, {"confirm": bool(confirm)})

        if action == "REPORT_RECONCILIATION":
            snapshot = load_state()
            balance, positions, orders = self._get_reconciliation_inputs(snapshot)
            self.last_reconciliation = self._reconcile_snapshot(snapshot, balance, positions, orders)
            notifier.risk_alert("人工对账报告", str(self.last_reconciliation)[:500], "仅报告")
            return

        if action.startswith("PAUSE_STRATEGY:"):
            name = action.split(":", 1)[1]
            if name in self.paused:
                self.paused[name] = True
                self.blocked_strategies[name] = "operator pause"
                notifier.risk_alert("人工操作", f"暂停 {name}", "已暂停")
            return

        if action.startswith("RESUME_STRATEGY:"):
            name = action.split(":", 1)[1]
            if name in self.paused:
                self.paused[name] = False
                self.blocked_strategies.pop(name, None)
                notifier.risk_alert("人工操作", f"恢复 {name}", "已恢复")
            return

        if confirm != "I_CONFIRM_OPERATOR_ACTION":
            notifier.risk_alert("人工操作被拒绝", action, "缺少 OPERATOR_CONFIRM=I_CONFIRM_OPERATOR_ACTION")
            return

        if action.startswith("CANCEL_STRATEGY_ORDERS:"):
            name = action.split(":", 1)[1]
            strat = self.strategies.get(name)
            symbol = getattr(strat, "symbol", None)
            if symbol:
                count = self.exchange.cancel_all_orders(symbol)
                notifier.risk_alert("人工撤单", f"{name} {symbol} 撤单 {count} 个", "已执行")
            return

        if action.startswith("CLOSE_STRATEGY_POSITION:"):
            name = action.split(":", 1)[1]
            strat = self.strategies.get(name)
            symbol = getattr(strat, "symbol", None)
            if symbol:
                ok = self.exchange.close_position(symbol)
                notifier.risk_alert("人工平仓", f"{name} {symbol} ok={ok}", "已执行")
            return

        if action == "REBUILD_DYNAMIC_GRID":
            price = self.exchange.get_price(self.strategies["dynamic_grid"].symbol)
            self.strategies["dynamic_grid"].rebuild_from_recovery(price)
            self.paused["dynamic_grid"] = False
            self.blocked_strategies.pop("dynamic_grid", None)
            notifier.risk_alert("人工重建", "dynamic_grid 已重建", "已执行")

    def _handle_symbol_conflicts(self):
        funding = self.strategies["funding_arb"]
        grid = self.strategies["dynamic_grid"]
        dca = self.strategies["trend_dca"]

        funding_symbols = set(getattr(funding, "configured_symbols", lambda: [])()) | set((funding.positions or {}).keys())

        for strategy_name, symbol in (("dynamic_grid", grid.symbol), ("trend_dca", dca.symbol)):
            if symbol in funding_symbols:
                self.paused[strategy_name] = True
                self.blocked_strategies[strategy_name] = f"symbol conflict: {symbol} also configured in funding_arb"
                logger.warning(f"Symbol conflict paused {strategy_name}: {symbol} overlaps funding_arb")
            elif self.blocked_strategies.get(strategy_name, "") == f"symbol conflict: {symbol} also configured in funding_arb":
                self.blocked_strategies.pop(strategy_name, None)
                self.paused[strategy_name] = False
                logger.info(f"Symbol conflict cleared for {strategy_name}: {symbol} no longer overlaps funding_arb")

        if grid.symbol != dca.symbol:
            return
        symbol = grid.symbol
        positions = self.exchange.get_positions(symbol)
        open_orders = self.exchange.get_open_orders(symbol)
        has_position = any(float(p.get("contracts", 0) or 0) > 0 for p in positions)
        has_grid_orders = bool(open_orders or grid.active_orders)
        dca_local_position = bool(dca.position)
        dca_live_position = any(
            p.get("side") in ("long", "short") and float(p.get("contracts", 0) or 0) > 0
            for p in positions
        )

        if dca_local_position or dca_live_position:
            was_paused = self.paused.get("dynamic_grid")
            self.paused["dynamic_grid"] = True
            self.blocked_strategies["dynamic_grid"] = f"symbol conflict: {symbol} has TrendDCA/exchange position"
            if not was_paused:
                logger.warning(f"Symbol conflict paused dynamic_grid: {symbol} has position")
            return

        if has_grid_orders:
            was_paused = self.paused.get("trend_dca")
            self.paused["trend_dca"] = True
            self.blocked_strategies["trend_dca"] = f"symbol conflict: {symbol} has grid open orders"
            if not was_paused:
                logger.warning(f"Symbol conflict paused trend_dca: {symbol} has grid orders")
            return

        if self.blocked_strategies.get("dynamic_grid", "").startswith("symbol conflict:"):
            self.blocked_strategies.pop("dynamic_grid", None)
            self.paused["dynamic_grid"] = False
            logger.info(f"Symbol conflict cleared for dynamic_grid: {symbol}")
        if self.blocked_strategies.get("trend_dca", "").startswith("symbol conflict:"):
            self.blocked_strategies.pop("trend_dca", None)
            self.paused["trend_dca"] = False
            logger.info(f"Symbol conflict cleared for trend_dca: {symbol}")

    def run(self):
        """主循环"""
        logger.info("Main loop started")

        while self.running:
            try:
                self.cycle += 1
                self._maybe_rehearse_exception()
                t0 = time.time()

                # 1) 全局风控
                if self.circuit_breaker.is_tripped():
                    logger.warning(f"Circuit breaker active: {self.circuit_breaker.last_reason}")
                    self._save_state()
                    time.sleep(CHECK_INTERVAL)
                    continue
                risk = self._check_risk()
                if not risk["safe"]:
                    self._handle_risk(risk)
                    self._save_state()
                    time.sleep(CHECK_INTERVAL)
                    continue

                # 2) 市场状态 (每10周期)
                if self.cycle % 10 == 0:
                    ws = self.exchange.get_ws_status()
                    if ws.get("enabled"):
                        if self.exchange.ws_monitor.stale(int(WEBSOCKET.get("stale_after_sec", 180) or 180)):
                            self._record_failure("websocket_stale", "websocket event stream stale")
                        else:
                            self._record_success("websocket_stale")
                    state = self.risk.detect_market_state("BTC/USDT:USDT")
                    adj = self.risk.get_weight_adjustment()
                    logger.info(
                        f"Market: {state} | ADX: {self.risk.adx_value:.1f} | "
                        f"Grid={adj['dynamic_grid']:.1f}x DCA={adj['trend_dca']:.1f}x"
                    )
                    
                    self.strategies["dynamic_grid"].set_weight(adj.get("dynamic_grid", 1.0))
                    self.strategies["trend_dca"].set_weight(adj.get("trend_dca", 1.0))
                    self.strategies["funding_arb"].set_weight(adj.get("funding_arb", 1.0))
                    
                    self._save_state()

                # 3) 运行策略
                if self.recovery_blocked:
                    logger.warning(f"Recovery blocked mode active: {self.recovery_reason}")
                else:
                    self._handle_operator_action()
                    self._handle_symbol_conflicts()
                    for name, strat in self.strategies.items():
                        if self.paused[name]:
                            continue

                        # 单策略风控
                        sr = self.risk.check_strategy(name, strat.get_drawdown())
                        if not sr["safe"]:
                            self.paused[name] = True
                            strat.stop()
                            self._save_state()
                            continue

                        if strat.should_run():
                            try:
                                strat.run()
                                self._record_success("strategy_exception")
                            except Exception as strat_error:
                                self._record_failure("strategy_exception", str(strat_error), strategy=name)
                                raise
                            self._save_state()

                # 4) 定时状态报告
                if time.time() - self.last_report >= self.report_interval:
                    self._send_report()
                    self.last_report = time.time()
                    self._save_state()

                # 5) 控制台状态 (每5周期)
                if self.cycle % 5 == 0:
                    self._print_status()

                self._save_state()
                elapsed = time.time() - t0
                time.sleep(max(0, CHECK_INTERVAL - elapsed))

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True)
                notifier.error("Main", str(e))
                self._save_state()
                time.sleep(10)

        self._shutdown()

    def _get_account_snapshot(self) -> dict:
        balance = self._compose_equity_balance()
        positions = self.exchange.get_positions()
        return {
            "balance": balance,
            "positions": positions,
            "open_position_count": len(positions),
            "unrealized_pnl": sum(float(p.get("unrealized_pnl", 0) or 0) for p in positions),
            "notional": sum(abs(float(p.get("notional", 0) or 0)) for p in positions),
        }

    def _check_risk(self) -> dict:
        try:
            snapshot = self._get_account_snapshot()
            bal = snapshot["balance"]
            self.last_account_balance = bal
            self.last_risk_snapshot = snapshot
            if bal is None or bal["total"] <= 0:
                logger.warning("Risk check skipped: balance unavailable")
                self._record_failure("balance_fetch", "balance unavailable")
                return {"safe": True, "action": "CONTINUE", "details": "balance unavailable"}
            self._record_success("balance_fetch")
            return self.risk.check_global(bal["total"])
        except Exception as e:
            logger.error(f"Risk check error: {e}")
            self._record_failure("balance_fetch", str(e))
            return {"safe": True, "action": "CONTINUE", "details": "error"}

    def _maybe_rehearse_exception(self):
        target_cycle = int(RISK.get("rehearsal_exception_cycle", 0) or 0)
        if target_cycle <= 0 or self.last_rehearsal_cycle == target_cycle:
            return
        if self.cycle == target_cycle:
            self.last_rehearsal_cycle = target_cycle
            logger.warning(f"Rehearsal exception triggered at cycle {target_cycle}")
            raise RuntimeError(f"rehearsal exception at cycle {target_cycle}")

    def _handle_risk(self, result: dict):
        action = result["action"]
        logger.warning(f"RISK: {action} | {result['details']}")

        self._flatten_exposure_for_risk(action, result["details"])

        if action == "STOP_ALL":
            self.running = False
        elif action in ("REDUCE_ALL", "PAUSE_TODAY"):
            for name in self.strategies:
                self.paused[name] = True
                self.blocked_strategies[name] = f"risk {action.lower()}: {result['details']}"

    def _flatten_exposure_for_risk(self, action: str, details: str):
        failures = []
        cancelled_orders = 0
        symbols = set()

        funding = self.strategies.get("funding_arb")
        if funding:
            symbols.update(getattr(funding, "configured_symbols", lambda: [])())
            symbols.update((getattr(funding, "positions", {}) or {}).keys())

        for strat in self.strategies.values():
            symbol = getattr(strat, "symbol", None)
            if symbol:
                symbols.add(symbol)

        live_positions = self.exchange.get_positions()
        for pos in live_positions:
            if pos.get("symbol"):
                symbols.add(pos["symbol"])

        for order in self.exchange.get_open_orders():
            if order.get("symbol"):
                symbols.add(order["symbol"])

        for name, strat in self.strategies.items():
            try:
                strat.stop()
            except Exception as exc:
                failures.append(f"{name}.stop={exc}")

        for symbol in sorted(item for item in symbols if item):
            try:
                cancelled_orders += int(self.exchange.cancel_all_orders(symbol) or 0)
            except Exception as exc:
                failures.append(f"cancel {symbol}={exc}")

        if not self.exchange.close_all_positions():
            failures.append("close_all_positions failed")

        ledger.record_event("risk_flatten", {
            "action": action,
            "details": details,
            "cancelled_orders": cancelled_orders,
            "symbols": sorted(item for item in symbols if item),
            "failures": list(failures),
        })

        if failures:
            msg = "; ".join(failures)
            logger.error(f"Risk flatten issues: {msg}")
            notifier.error("Risk", f"{action}: flatten issues\n{msg}")

    def _strategy_accounting_summary(self) -> dict:
        realized = 0.0
        estimated = 0.0
        ledger_summary = ledger.summarize()
        for name, strat in self.strategies.items():
            realized += float(strat.total_pnl or 0)
            if name == "funding_arb":
                estimated += sum(float((pos or {}).get("collected", 0) or 0) for pos in strat.positions.values())
        account = self.last_risk_snapshot or {}
        unrealized = float(account.get("unrealized_pnl", 0) or 0)
        return {
            "realized": realized,
            "unrealized": unrealized,
            "estimated": estimated,
            "funding_actual": ledger_summary.get("funding_actual", 0.0),
            "fees": ledger_summary.get("fees", 0.0),
            "net": realized + unrealized + ledger_summary.get("funding_actual", 0.0) - ledger_summary.get("fees", 0.0),
            "order_count": ledger_summary.get("order_count", 0.0),
        }

    def _format_positions_summary(self) -> list:
        account = self.last_risk_snapshot or {}
        positions = list(account.get("positions", []) or [])
        if not positions:
            return ["持仓明细: 无"]

        lines = ["持仓明细:"]
        for pos in positions[:5]:
            lines.append(
                f"- {pos.get('symbol')} | {str(pos.get('side', '')).upper()} | "
                f"数量 {float(pos.get('contracts', 0) or 0):.6f} | "
                f"开仓价 {float(pos.get('entry_price', 0) or 0):.2f} | "
                f"未实现 {float(pos.get('unrealized_pnl', 0) or 0):+.2f}"
            )
        extra = len(positions) - 5
        if extra > 0:
            lines.append(f"- 其余持仓: {extra} 个")
        return lines

    def _strategy_status_line(self, name: str, strat) -> str:
        s = strat.get_status()
        reason = self.blocked_strategies.get(name, "")
        status = "运行中"
        if self.recovery_blocked:
            status = "组合阻断"
        elif self.paused[name]:
            status = "已暂停"
        suffix = f" | 原因: {reason}" if reason else ""
        return (
            f"{s['name']:12s} | 状态: {status} | PnL: {s['total_pnl']:+8.2f} | "
            f"DD: {s['drawdown']:.2%} | 交易数: {s['trades']} | 执行:{s.get('execution_mode', 'N/A')}{suffix}"
        )

    def _send_report(self):
        """发送Telegram状态报告"""
        runtime_h = (time.time() - self.start_time) / 3600
        total_trades = sum(s.trade_count for s in self.strategies.values())
        pnl = self._strategy_accounting_summary()
        account = self.last_risk_snapshot or {}
        balance = (self.last_account_balance or {}) if isinstance(self.last_account_balance, dict) else {}

        lines = [
            f"机器人状态: {'恢复阻断' if self.recovery_blocked else '正常运行'}",
            f"运行时长: {runtime_h:.1f}h | 主循环: #{self.cycle}",
            f"环境模式: {EXCHANGE['mode']} | 执行模式: {self.banner_execution_mode}",
            f"市场状态: {self.risk.market_state} | ADX: {self.risk.adx_value:.1f}",
            (
                f"账户资金: 总额 {float(balance.get('total', 0) or 0):.2f} | "
                f"可用 {float(balance.get('free', 0) or 0):.2f} | "
                f"占用 {float(balance.get('used', 0) or 0):.2f}"
            ),
            (
                f"收益汇总: 已实现 {pnl['realized']:+.2f} | 未实现 {pnl['unrealized']:+.2f} | "
                f"实收资金费 {pnl['funding_actual']:+.2f} | 估算资金费 {pnl['estimated']:+.2f}"
            ),
            f"执行成本: 手续费 {pnl['fees']:+.2f} | 净收益 {pnl['net']:+.2f} | 订单 {int(pnl['order_count'])}",
            (
                f"仓位统计: {int(account.get('open_position_count', 0) or 0)} 个 | "
                f"名义敞口 {float(account.get('notional', 0) or 0):.2f}"
            ),
            "─────────────────────",
        ]

        if self.recovery_blocked:
            lines.append(f"恢复保护原因: {self.recovery_reason}")
            lines.append("─────────────────────")
        elif self.blocked_strategies:
            lines.append("策略暂停原因:")
            for name, reason in self.blocked_strategies.items():
                lines.append(f"- {name}: {reason}")
            lines.append("─────────────────────")

        lines.extend(self._format_positions_summary())
        lines.append("─────────────────────")
        lines.append("策略状态:")
        for name, strat in self.strategies.items():
            lines.append(self._strategy_status_line(name, strat))

        lines.append("─────────────────────")
        lines.append(
            f"总计 | 已实现 {pnl['realized']:+.2f} | 未实现 {pnl['unrealized']:+.2f} | "
            f"估算 {pnl['estimated']:+.2f} | 总交易数 {total_trades}"
        )

        report = "\n".join(lines)
        notifier.status_report(report)

    def _print_status(self):
        runtime_h = (time.time() - self.start_time) / 3600
        pnl = self._strategy_accounting_summary()
        balance = (self.last_account_balance or {}) if isinstance(self.last_account_balance, dict) else {}
        account = self.last_risk_snapshot or {}

        logger.info("─" * 50)
        logger.info(f"  机器人状态: {'恢复阻断' if self.recovery_blocked else '正常运行'} | 循环 #{self.cycle} | 运行 {runtime_h:.1f}h")
        logger.info(f"  环境:{EXCHANGE['mode']} | 执行:{self.banner_execution_mode} | 市场:{self.risk.market_state} ADX:{self.risk.adx_value:.1f}")
        logger.info(
            f"  账户 total:{float(balance.get('total', 0) or 0):.2f} | "
            f"free:{float(balance.get('free', 0) or 0):.2f} | used:{float(balance.get('used', 0) or 0):.2f}"
        )
        logger.info(
            f"  收益 已实现:{pnl['realized']:+.2f} | 未实现:{pnl['unrealized']:+.2f} | "
            f"估算:{pnl['estimated']:+.2f} | 持仓:{int(account.get('open_position_count', 0) or 0)}"
        )
        if self.recovery_blocked:
            logger.info(f"  RECOVERY BLOCKED | {self.recovery_reason}")
        elif self.blocked_strategies:
            for name, reason in self.blocked_strategies.items():
                logger.info(f"  PAUSED {name} | {reason}")

        for line in self._format_positions_summary():
            logger.info(f"  {line}")

        for name, strat in self.strategies.items():
            logger.info(f"  {self._strategy_status_line(name, strat)}")

        logger.info(
            f"  {'TOTAL':12s} | 已实现:{pnl['realized']:+8.2f} | "
            f"未实现:{pnl['unrealized']:+8.2f} | 估算:{pnl['estimated']:+8.2f}"
        )
        logger.info("─" * 50)

    def _on_signal(self, signum=None, frame=None):
        logger.info("Shutdown signal received")
        self.running = False

    def _shutdown(self):
        logger.info("Shutting down all strategies...")

        total_pnl = sum(s.total_pnl for s in self.strategies.values())
        total_trades = sum(s.trade_count for s in self.strategies.values())
        runtime_h = (time.time() - self.start_time) / 3600

        for name, strat in self.strategies.items():
            try:
                strat.stop()
                logger.info(f"  {name} stopped")
            except Exception as e:
                logger.error(f"  {name} stop error: {e}")

        self._save_state()

        pnl_summary = (
            f"运行时长: {runtime_h:.1f}h\n"
            f"总交易数: {total_trades}\n"
            f"总盈亏:   {total_pnl:+.4f} USDT"
        )
        notifier.shutdown("用户中止" if self.running is False else "异常退出", pnl_summary)
        logger.info("All stopped. Goodbye.")


if __name__ == "__main__":
    print("""
    ╔════════════════════════════════════════════╗
    ║   COMBO STRATEGY v2.0                      ║
    ║   资金费率套利 + 动态网格 + 趋势DCA        ║
    ║                                            ║
    ║   [!] 确认 .env 已正确配置                 ║
    ║   [!] 首次运行请使用 TRADING_MODE=demo     ║
    ║   [!] Ctrl+C 优雅退出                      ║
    ╚════════════════════════════════════════════╝
    """)

    instance_lock = SingleInstanceLock(f"combo_strategy_{get_state_namespace()}")
    try:
        instance_lock.acquire()
    except RuntimeError as e:
        logger.error(str(e))
        raise SystemExit(str(e))

    try:
        manager = PortfolioManager()
        manager.run()
    finally:
        instance_lock.release()
