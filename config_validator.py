from typing import Dict, List

from config import (
    ALLOCATION,
    DYNAMIC_GRID,
    EXCHANGE,
    FUNDING_ARB,
    ORDER_GUARD,
    RISK,
    TREND_DCA,
)


def validate_config() -> Dict[str, List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    alloc_sum = sum(float(v or 0) for v in ALLOCATION.values())
    if abs(alloc_sum - 1.0) > 1e-6:
        errors.append(f"资金分配比例之和必须等于 1.0，当前为 {alloc_sum:.4f}")

    if EXCHANGE.get("mode") == "live":
        if EXCHANGE.get("dry_run", True):
            errors.append("实盘模式禁止 DRY_RUN=true")
        if EXCHANGE.get("live_confirm") != "I_UNDERSTAND_LIVE_TRADING":
            errors.append("未设置 LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING")
        if FUNDING_ARB.get("live_confirm") != "ENABLE_FUNDING_ARB_LIVE":
            warnings.append("FundingArb 默认仍会在实盘中被暂停，需显式开启")

    if float(ORDER_GUARD.get("min_free_balance_pct", 0) or 0) < 0:
        errors.append("MIN_FREE_BALANCE_PCT 不能小于 0")
    if float(ORDER_GUARD.get("max_symbol_notional_pct", 0) or 0) <= 0:
        errors.append("MAX_SYMBOL_NOTIONAL_PCT 必须大于 0")
    if float(ORDER_GUARD.get("max_total_notional_pct", 0) or 0) <= 0:
        errors.append("MAX_TOTAL_NOTIONAL_PCT 必须大于 0")

    if int(DYNAMIC_GRID.get("grid_count", 0) or 0) < 2:
        errors.append("GRID_COUNT 必须至少为 2")
    if float(DYNAMIC_GRID.get("grid_spacing_pct", 0) or 0) <= 0:
        errors.append("GRID_SPACING_PCT 必须大于 0")
    if int(DYNAMIC_GRID.get("leverage", 0) or 0) < 1:
        errors.append("GRID_LEVERAGE 必须大于等于 1")

    if int(TREND_DCA.get("dca_layers", 0) or 0) < 1:
        errors.append("DCA_LAYERS 必须大于等于 1")
    if float(TREND_DCA.get("take_profit_pct", 0) or 0) <= 0:
        errors.append("DCA_TAKE_PROFIT_PCT 必须大于 0")
    if float(TREND_DCA.get("stop_loss_pct", 0) or 0) <= 0:
        errors.append("DCA_STOP_LOSS_PCT 必须大于 0")
    if float(TREND_DCA.get("trailing_stop_pct", 0) or 0) <= 0:
        errors.append("DCA_TRAILING_STOP_PCT 必须大于 0")
    if float(TREND_DCA.get("trailing_stop_threshold", 0) or 0) <= float(TREND_DCA.get("trailing_stop_pct", 0) or 0):
        warnings.append("建议 DCA_TRAILING_STOP_THRESHOLD 大于 DCA_TRAILING_STOP_PCT")

    if float(RISK.get("max_total_drawdown", 0) or 0) <= 0:
        errors.append("MAX_TOTAL_DRAWDOWN 必须大于 0")
    if float(RISK.get("emergency_stop_loss", 0) or 0) < float(RISK.get("max_total_drawdown", 0) or 0):
        warnings.append("建议 EMERGENCY_STOP_LOSS 大于等于 MAX_TOTAL_DRAWDOWN")

    grid_symbol = DYNAMIC_GRID.get("symbol")
    dca_symbol = TREND_DCA.get("symbol")
    funding_symbols = set(FUNDING_ARB.get("symbols", []) or [])
    if grid_symbol == dca_symbol:
        errors.append("GRID_SYMBOL 与 DCA_SYMBOL 不能相同；实盘前必须拆分标的")
    overlaps = sorted(symbol for symbol in (grid_symbol, dca_symbol) if symbol in funding_symbols)
    if overlaps:
        errors.append(f"FundingArb symbols 不能与 Grid/DCA 重叠: {', '.join(overlaps)}")

    return {"errors": errors, "warnings": warnings}
