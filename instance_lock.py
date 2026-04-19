import atexit
import json
import logging
import os
import re
import socket
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

if os.name == "nt":
    import msvcrt
else:
    import fcntl


class SingleInstanceLock:
    def __init__(self, name: str, directory: Optional[Path] = None):
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._") or "instance"
        base_dir = Path(directory) if directory else Path(__file__).resolve().parent / "state" / "locks"
        self.path = base_dir / f"{safe_name}.lock"
        self._fh = None
        self._locked = False

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = self.path.open("a+b")

        try:
            self._lock_file(fh)
        except OSError as exc:
            details = self._read_details()
            fh.close()
            message = "Another combo strategy instance is already running"
            if details:
                message = f"{message}: {details}"
            raise RuntimeError(message) from exc

        payload = {
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "started_at": time.time(),
            "cwd": os.getcwd(),
        }
        data = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
        fh.seek(0)
        fh.truncate()
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())

        self._fh = fh
        self._locked = True
        atexit.register(self.release)
        logger.info(f"Instance lock acquired: {self.path.name}")
        return self

    def release(self):
        if not self._fh or not self._locked:
            return

        try:
            self._fh.seek(0)
            self._fh.write(b" ")
            self._fh.truncate()
            self._fh.flush()
            os.fsync(self._fh.fileno())
        except OSError:
            pass

        try:
            self._unlock_file(self._fh)
        finally:
            try:
                self._fh.close()
            finally:
                self._fh = None
                self._locked = False

        try:
            self.path.unlink()
        except OSError:
            pass

    def _read_details(self) -> str:
        try:
            raw = self.path.read_text(encoding="utf-8", errors="ignore").strip()
            if not raw:
                return ""
            info = json.loads(raw)
            pid = info.get("pid", "?")
            host = info.get("hostname", "?")
            cwd = info.get("cwd", "?")
            return f"pid={pid} host={host} cwd={cwd}"
        except Exception:
            return ""

    def _lock_file(self, fh):
        fh.seek(0)
        if os.name == "nt":
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock_file(self, fh):
        fh.seek(0)
        if os.name == "nt":
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
