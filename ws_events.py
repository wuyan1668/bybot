import asyncio
import logging
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

try:
    import ccxt.pro as ccxtpro  # type: ignore
    HAS_CCXTPRO = True
except Exception:
    ccxtpro = None
    HAS_CCXTPRO = False


class WebSocketEventMonitor:
    def __init__(self, exchange_name: str, config: Dict[str, Any]):
        self.exchange_name = exchange_name
        self.config = dict(config or {})
        self.enabled = bool(self.config.get("enabled", False)) and HAS_CCXTPRO
        self.available = HAS_CCXTPRO
        self.last_event_ts = 0.0
        self.last_error = ""
        self.event_counts = {
            "orders": 0,
            "positions": 0,
            "balance": 0,
        }
        self.cache = {
            "orders": [],
            "positions": [],
            "balance": {},
        }
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start(self):
        if not self.enabled or self._thread:
            return
        self._thread = threading.Thread(target=self._run, name="ws-event-monitor", daemon=True)
        self._thread.start()
        logger.info("[WS] Event monitor started")

    def stop(self):
        self._stop.set()

    def mark_event(self, category: str, payload: Any):
        self.last_event_ts = time.time()
        if category in self.event_counts:
            self.event_counts[category] += 1
        self.cache[category] = payload

    def stale(self, max_idle_sec: int) -> bool:
        if not self.enabled:
            return False
        if self.last_event_ts <= 0:
            return True
        return time.time() - self.last_event_ts > max_idle_sec

    def get_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "available": self.available,
            "last_event_ts": self.last_event_ts,
            "last_error": self.last_error,
            "event_counts": dict(self.event_counts),
        }

    def _run(self):
        try:
            asyncio.run(self._watch())
        except Exception as e:
            self.last_error = str(e)
            logger.warning(f"[WS] Monitor stopped: {e}")

    async def _watch(self):
        if not HAS_CCXTPRO:
            return
        exchange_class = getattr(ccxtpro, self.exchange_name, None)
        if exchange_class is None:
            self.last_error = f"ccxt.pro missing exchange: {self.exchange_name}"
            return
        client = exchange_class({
            "apiKey": self.config.get("api_key", ""),
            "secret": self.config.get("api_secret", ""),
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
                "defaultSubType": "linear",
            },
        })
        try:
            while not self._stop.is_set():
                tasks = []
                if hasattr(client, "watch_orders"):
                    tasks.append(asyncio.create_task(client.watch_orders()))
                if hasattr(client, "watch_positions"):
                    tasks.append(asyncio.create_task(client.watch_positions()))
                if hasattr(client, "watch_balance"):
                    tasks.append(asyncio.create_task(client.watch_balance()))
                if not tasks:
                    self.last_error = "watch_orders/watch_positions/watch_balance unavailable"
                    return
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=30)
                for task in pending:
                    task.cancel()
                for task in done:
                    result = task.result()
                    if isinstance(result, list) and result:
                        sample = result[0]
                        if isinstance(sample, dict) and "symbol" in sample and "side" in sample and "status" in sample:
                            self.mark_event("orders", result[-20:])
                        elif isinstance(sample, dict) and "contracts" in sample:
                            self.mark_event("positions", result[-20:])
                    elif isinstance(result, dict):
                        self.mark_event("balance", result)
        finally:
            await client.close()
