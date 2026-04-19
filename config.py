"""Load runtime configuration from the local .env file."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = os.getenv("BOT_ENV_FILE", "").strip()
env_path = Path(ENV_FILE).expanduser() if ENV_FILE else BASE_DIR / ".env"
if not env_path.is_absolute():
    env_path = (BASE_DIR / env_path).resolve()
if not env_path.exists():
    print(f"Missing .env file: {env_path}")
    print("Copy .env.example to .env or point BOT_ENV_FILE to a valid env file.")
    sys.exit(1)

load_dotenv(env_path)


def _get(key: str, default=None) -> str:
    return os.getenv(key, default)


def _float(key: str, default: float = 0.0) -> float:
    return float(_get(key, str(default)))


def _int(key: str, default: int = 0) -> int:
    return int(_get(key, str(default)))


def _bool(key: str, default: bool = False) -> bool:
    return _get(key, str(default)).lower() in ("true", "1", "yes")


def _list(key: str, default: str = "") -> list:
    raw = _get(key, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


EXCHANGE = {
    "name": _get("EXCHANGE_NAME", "bybit"),
    "api_key": _get("API_KEY", ""),
    "api_secret": _get("API_SECRET", ""),
    "mode": _get("TRADING_MODE", "demo"),
    "dry_run": _bool("DRY_RUN", False),
    "live_confirm": _get("LIVE_TRADING_CONFIRM", ""),
}


TELEGRAM = {
    "bot_token": _get("TELEGRAM_BOT_TOKEN", ""),
    "chat_id": _get("TELEGRAM_CHAT_ID", ""),
    "notify_level": _get("NOTIFY_LEVEL", "all"),
}


RUNTIME = {
    "allow_shared_symbols": _bool("ALLOW_SHARED_SYMBOLS", False),
    "operator_action": _get("OPERATOR_ACTION", ""),
    "operator_confirm": _get("OPERATOR_CONFIRM", ""),
}


ORDER_GUARD = {
    "enabled": _bool("ORDER_GUARD_ENABLED", True),
    "min_free_balance_pct": _float("MIN_FREE_BALANCE_PCT", 0.05),
    "max_symbol_notional_pct": _float("MAX_SYMBOL_NOTIONAL_PCT", 0.60),
    "max_total_notional_pct": _float("MAX_TOTAL_NOTIONAL_PCT", 1.50),
}


TOTAL_CAPITAL = _float("TOTAL_CAPITAL", 10000)


ALLOCATION = {
    "funding_arb": _float("ALLOC_FUNDING_ARB", 0.50),
    "dynamic_grid": _float("ALLOC_DYNAMIC_GRID", 0.30),
    "trend_dca": _float("ALLOC_TREND_DCA", 0.20),
}


RISK = {
    "max_total_drawdown": _float("MAX_TOTAL_DRAWDOWN", 0.15),
    "max_strategy_drawdown": _float("MAX_STRATEGY_DRAWDOWN", 0.10),
    "max_daily_loss": _float("MAX_DAILY_LOSS", 0.05),
    "emergency_stop_loss": _float("EMERGENCY_STOP_LOSS", 0.20),
    "rehearsal_exception_cycle": _int("REHEARSAL_EXCEPTION_CYCLE", 0),
    "adx_period": _int("ADX_PERIOD", 14),
    "adx_trend_threshold": _int("ADX_TREND_THRESHOLD", 25),
    "adx_range_threshold": _int("ADX_RANGE_THRESHOLD", 20),
    "max_consecutive_order_failures": _int("MAX_CONSECUTIVE_ORDER_FAILURES", 3),
    "max_consecutive_data_failures": _int("MAX_CONSECUTIVE_DATA_FAILURES", 3),
    "max_consecutive_strategy_failures": _int("MAX_CONSECUTIVE_STRATEGY_FAILURES", 3),
    "circuit_breaker_cooldown_sec": _int("CIRCUIT_BREAKER_COOLDOWN_SEC", 900),
}


RISK_CONTROL = {
    "auto_reset_baseline": _bool("AUTO_RESET_RISK_BASELINE", False),
    "baseline_reset_confirm": _get("RISK_BASELINE_RESET_CONFIRM", ""),
}


CHECK_INTERVAL = _int("CHECK_INTERVAL", 60)


FUNDING_ARB = {
    "symbols": _list("FUNDING_SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT"),
    "min_funding_rate": _float("FUNDING_MIN_RATE", 0.0005),
    "leverage": _int("FUNDING_LEVERAGE", 1),
    "position_ratio": _float("FUNDING_POSITION_RATIO", 0.45),
    "check_interval": _int("FUNDING_CHECK_INTERVAL", 300),
    "close_threshold": _float("FUNDING_CLOSE_THRESHOLD", 0.0001),
    "max_positions": _int("FUNDING_MAX_POSITIONS", 2),
    "hedge_rel_tolerance": _float("FUNDING_HEDGE_REL_TOLERANCE", 0.00001),
    "hedge_abs_tolerance": _float("FUNDING_HEDGE_ABS_TOLERANCE", 0.001),
    "live_confirm": _get("FUNDING_LIVE_CONFIRM", ""),
}


DYNAMIC_GRID = {
    "symbol": _get("GRID_SYMBOL", "BTC/USDT:USDT"),
    "grid_count": _int("GRID_COUNT", 10),
    "grid_spacing_pct": _float("GRID_SPACING_PCT", 0.005),
    "atr_period": _int("GRID_ATR_PERIOD", 14),
    "atr_multiplier": _float("GRID_ATR_MULTIPLIER", 1.5),
    "order_amount_pct": _float("GRID_ORDER_AMOUNT_PCT", 0.08),
    "leverage": _int("GRID_LEVERAGE", 3),
    "max_open_orders": _int("GRID_MAX_OPEN_ORDERS", 10),
    "recenter_threshold": _float("GRID_RECENTER_THRESHOLD", 0.03),
    "update_interval": _int("GRID_UPDATE_INTERVAL", 120),
    "rebuild_confirm": _get("GRID_REBUILD_CONFIRM", ""),
}


TREND_DCA = {
    "symbol": _get("DCA_SYMBOL", "BTC/USDT:USDT"),
    "fast_ma": _int("DCA_FAST_MA", 7),
    "slow_ma": _int("DCA_SLOW_MA", 25),
    "timeframe": _get("DCA_TIMEFRAME", "4h"),
    "dca_layers": _int("DCA_LAYERS", 5),
    "layer_spacing_pct": _float("DCA_LAYER_SPACING_PCT", 0.02),
    "layer_multiplier": _float("DCA_LAYER_MULTIPLIER", 1.5),
    "base_amount_pct": _float("DCA_BASE_AMOUNT_PCT", 0.10),
    "leverage": _int("DCA_LEVERAGE", 2),
    "take_profit_pct": _float("DCA_TAKE_PROFIT_PCT", 0.03),
    "trailing_stop_pct": _float("DCA_TRAILING_STOP_PCT", 0.005),
    "trailing_stop_threshold": _float("DCA_TRAILING_STOP_THRESHOLD", 0.015),
    "stop_loss_pct": _float("DCA_STOP_LOSS_PCT", 0.08),
    "check_interval": _int("DCA_CHECK_INTERVAL", 300),
}


WEBSOCKET = {
    "enabled": _bool("WEBSOCKET_ENABLED", False),
    "stale_after_sec": _int("WEBSOCKET_STALE_AFTER_SEC", 180),
}


OPERATOR = {
    "action": _get("OPERATOR_ACTION", ""),
    "confirm": _get("OPERATOR_CONFIRM", ""),
}


LOG_LEVEL = _get("LOG_LEVEL", "INFO")
LOG_FILE = _get("LOG_FILE", "combo_strategy.log")
