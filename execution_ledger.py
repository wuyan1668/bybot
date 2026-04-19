import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import EXCHANGE

LEDGER_ROOT = Path(__file__).parent / "state"


def _ledger_namespace() -> str:
    exec_tag = "dryrun" if EXCHANGE.get("dry_run", True) else "execute"
    base = f"{EXCHANGE.get('name', 'exchange')}_{EXCHANGE.get('mode', 'demo')}_{exec_tag}"
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in base).strip("._").lower() or "default"


def _ledger_dir() -> Path:
    return LEDGER_ROOT / _ledger_namespace()


def _ledger_file() -> Path:
    return _ledger_dir() / "execution_ledger.json"


def _default_payload() -> Dict[str, Any]:
    return {
        "saved_at": None,
        "orders": [],
        "funding": [],
        "events": [],
        "operator_actions": [],
        "risk_rejections": [],
        "protection_events": [],
    }


class ExecutionLedger:
    def __init__(self):
        self.path = _ledger_file()
        self.payload = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return _default_payload()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            payload = _default_payload()
            if isinstance(data, dict):
                payload.update(data)
            return payload
        except Exception:
            return _default_payload()

    def _save(self):
        state_dir = _ledger_dir()
        state_dir.mkdir(parents=True, exist_ok=True)
        payload = dict(self.payload)
        payload["saved_at"] = time.time()
        fd, tmp_path = tempfile.mkstemp(prefix="execution_ledger_", suffix=".json", dir=str(state_dir))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                json.dump(payload, tmp, ensure_ascii=False, indent=2, sort_keys=True)
                tmp.flush()
                os.fsync(tmp.fileno())
            os.replace(tmp_path, self.path)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _append(self, key: str, record: Dict[str, Any], limit: int = 5000):
        bucket = list(self.payload.get(key, []))
        bucket.append(record)
        if len(bucket) > limit:
            bucket = bucket[-limit:]
        self.payload[key] = bucket
        self._save()

    def record_order(self, strategy: str, symbol: str, order: Dict[str, Any], context: Optional[Dict[str, Any]] = None):
        order = dict(order or {})
        context = dict(context or {})
        record = {
            "ts": time.time(),
            "strategy": strategy,
            "symbol": symbol,
            "context": context,
            "order": order,
            "fee_cost": float(order.get("fee_cost", 0) or 0),
            "slippage_pct": float(order.get("slippage_pct", 0) or 0),
            "execution_state": order.get("execution_state", "unknown"),
            "execution_ok": bool(order.get("execution_ok", False)),
        }
        self._append("orders", record)

    def record_funding(self, strategy: str, symbol: str, amount: float, details: Optional[Dict[str, Any]] = None):
        record = {
            "ts": time.time(),
            "strategy": strategy,
            "symbol": symbol,
            "amount": float(amount or 0),
            "details": dict(details or {}),
        }
        self._append("funding", record)

    def record_event(self, category: str, details: Dict[str, Any]):
        record = {
            "ts": time.time(),
            "category": category,
            "details": dict(details or {}),
        }
        self._append("events", record)

    def record_operator_action(self, action: str, details: Dict[str, Any]):
        record = {
            "ts": time.time(),
            "action": action,
            "details": dict(details or {}),
        }
        self._append("operator_actions", record)

    def record_risk_rejection(self, strategy: str, symbol: str, reason: str,
                              details: Optional[Dict[str, Any]] = None):
        record = {
            "ts": time.time(),
            "strategy": strategy,
            "symbol": symbol,
            "reason": reason,
            "details": dict(details or {}),
        }
        self._append("risk_rejections", record)

    def record_protection_event(self, scope: str, reason: str,
                                details: Optional[Dict[str, Any]] = None,
                                strategy: Optional[str] = None):
        record = {
            "ts": time.time(),
            "scope": scope,
            "strategy": strategy,
            "reason": reason,
            "details": dict(details or {}),
        }
        self._append("protection_events", record)

    def summarize(self) -> Dict[str, float]:
        orders: List[Dict[str, Any]] = list(self.payload.get("orders", []))
        funding: List[Dict[str, Any]] = list(self.payload.get("funding", []))
        return {
            "fees": sum(float(item.get("fee_cost", 0) or 0) for item in orders),
            "slippage_pct_sum": sum(float(item.get("slippage_pct", 0) or 0) for item in orders),
            "funding_actual": sum(float(item.get("amount", 0) or 0) for item in funding),
            "order_count": float(len(orders)),
        }


ledger = ExecutionLedger()
