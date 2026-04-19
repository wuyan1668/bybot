import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any, Dict

from config import EXCHANGE

STATE_ROOT = Path(__file__).parent / "state"
LEGACY_STATE_FILE = STATE_ROOT / "runtime_state.json"
LEGACY_BACKUP_FILE = STATE_ROOT / "runtime_state.bak.json"


def get_state_namespace() -> str:
    override = os.getenv("BOT_STATE_NAMESPACE", "").strip()
    if override:
        base = override
    else:
        exec_tag = "execute"
        base = f"{EXCHANGE.get('name', 'exchange')}_{EXCHANGE.get('mode', 'demo')}_{exec_tag}"
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", base).strip("._").lower()
    return safe or "default"


def _state_paths():
    namespace_dir = STATE_ROOT / get_state_namespace()
    return {
        "dir": namespace_dir,
        "file": namespace_dir / "runtime_state.json",
        "backup": namespace_dir / "runtime_state.bak.json",
    }


def get_state_file() -> Path:
    return _state_paths()["file"]


def default_state() -> Dict[str, Any]:
    return {
        "saved_at": None,
        "portfolio": {
            "paused": {},
            "recovery_blocked": False,
            "recovery_reason": "",
            "blocked_strategies": {},
        },
        "risk": {},
        "strategies": {},
    }


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else default_state()
    except Exception:
        return default_state()


def _state_matches_current_runtime(snapshot: Dict[str, Any]) -> bool:
    portfolio = snapshot.get("portfolio", {}) or {}
    runtime_mode = str(portfolio.get("mode", "") or "").lower()
    runtime_exec = str(portfolio.get("execution_mode", "") or "").upper()
    runtime_exchange = str(portfolio.get("exchange", "") or "").lower()
    expected_mode = str(EXCHANGE.get("mode", "demo") or "").lower()
    expected_exec = "EXECUTE"
    expected_exchange = str(EXCHANGE.get("name", "") or "").lower()

    if runtime_mode and runtime_mode != expected_mode:
        return False
    if runtime_exec and runtime_exec != expected_exec:
        return False
    if runtime_exchange and runtime_exchange != expected_exchange:
        return False
    return True


def load_state() -> Dict[str, Any]:
    paths = _state_paths()

    if paths["file"].exists():
        data = _load_json(paths["file"])
        state = default_state()
        state.update(data)
        return state

    if LEGACY_STATE_FILE.exists():
        legacy = _load_json(LEGACY_STATE_FILE)
        if _state_matches_current_runtime(legacy):
            state = default_state()
            state.update(legacy)
            return state

    return default_state()


def save_state(snapshot: Dict[str, Any]) -> None:
    paths = _state_paths()
    state_dir = paths["dir"]
    state_file = paths["file"]
    backup_file = paths["backup"]

    state_dir.mkdir(parents=True, exist_ok=True)

    payload = default_state()
    payload.update(snapshot or {})
    payload["saved_at"] = time.time()

    if state_file.exists():
        try:
            backup_file.write_text(state_file.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            pass

    fd, tmp_path = tempfile.mkstemp(prefix="runtime_state_", suffix=".json", dir=str(state_dir))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
            json.dump(payload, tmp, ensure_ascii=False, indent=2, sort_keys=True)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_path, state_file)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
