import time
from typing import Dict

from config import RISK as CFG


class CircuitBreaker:
    def __init__(self):
        self.failures: Dict[str, int] = {}
        self.tripped_until = 0.0
        self.last_reason = ""

    def export_state(self) -> dict:
        return {
            "failures": dict(self.failures),
            "tripped_until": self.tripped_until,
            "last_reason": self.last_reason,
        }

    def import_state(self, state: dict):
        state = state or {}
        self.failures = dict(state.get("failures", {}) or {})
        self.tripped_until = float(state.get("tripped_until", 0) or 0)
        self.last_reason = state.get("last_reason", "") or ""

    def reset(self, category: str):
        self.failures[category] = 0

    def record_failure(self, category: str) -> dict:
        self.failures[category] = int(self.failures.get(category, 0) or 0) + 1
        count = self.failures[category]
        order_limit = int(CFG.get("max_consecutive_order_failures", 3) or 3)
        data_limit = int(CFG.get("max_consecutive_data_failures", 3) or 3)
        strategy_limit = int(CFG.get("max_consecutive_strategy_failures", 3) or 3)

        threshold = strategy_limit
        if category in ("order_submit", "order_confirm"):
            threshold = order_limit
        elif category in ("balance_fetch", "position_fetch", "reconciliation", "websocket_stale"):
            threshold = data_limit

        tripped = count >= threshold
        if tripped:
            cooldown = int(CFG.get("circuit_breaker_cooldown_sec", 900) or 900)
            self.tripped_until = time.time() + cooldown
            self.last_reason = f"{category} consecutive failures={count}"
        return {
            "category": category,
            "count": count,
            "threshold": threshold,
            "tripped": tripped,
            "reason": self.last_reason if tripped else "",
        }

    def is_tripped(self) -> bool:
        return time.time() < self.tripped_until
