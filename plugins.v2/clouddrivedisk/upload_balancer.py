import json
import logging
import math
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple


WINDOW_SECONDS = 24 * 60 * 60
BYTES_PER_GB = 1_000_000_000
logger = logging.getLogger(__name__)


def _normalize_path(path: Any) -> str:
    value = str(path or "").strip()
    if not value or "\\" in value:
        raise ValueError("path must be a POSIX path starting with /")
    parts = [part for part in value.split("/") if part]
    if any(part in (".", "..") for part in parts):
        raise ValueError("path cannot contain . or .. segments")
    return "/" + "/".join(parts) if parts else "/"


def parse_rules(value: Any) -> List[Dict[str, Any]]:
    """Parse ordered path-prefix rules from the plugin form's JSON field."""
    if not value or not str(value).strip():
        return []
    data = json.loads(value) if isinstance(value, str) else value
    if not isinstance(data, list):
        raise ValueError("rules must be a JSON array")

    rules: List[Dict[str, Any]] = []
    for rule in data:
        if not isinstance(rule, dict):
            raise ValueError("each rule must be an object")
        prefix = _normalize_path(rule.get("prefix"))
        branches = rule.get("branches")
        if not isinstance(branches, list) or not branches:
            raise ValueError(f"{prefix} must have at least one branch")
        parsed_branches = []
        for branch in branches:
            if not isinstance(branch, dict):
                raise ValueError(f"branches for {prefix} must be objects")
            branch_path = _normalize_path(branch.get("path"))
            try:
                capacity_gb = float(branch.get("capacity_gb"))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"capacity for {branch_path} must be positive GB") from exc
            if not math.isfinite(capacity_gb) or capacity_gb <= 0:
                raise ValueError(f"capacity for {branch_path} must be positive GB")
            parsed_branches.append(
                {
                    "path": branch_path,
                    "capacity_bytes": int(capacity_gb * BYTES_PER_GB),
                }
            )
        rules.append({"prefix": prefix, "branches": parsed_branches})
    return rules


class UploadReservation:
    def __init__(self, token: int, folder: str, size: int) -> None:
        self.token = token
        self.folder = folder
        self.size = size


class UploadBalancer:
    def __init__(
        self,
        rules: List[Dict[str, Any]],
        records: Any,
        save_records: Callable[[List[Dict[str, Any]]], None],
    ) -> None:
        self.rules = rules
        self._save_records = save_records
        self._lock = threading.RLock()
        self._next_token = 0
        self._reservations: Dict[int, UploadReservation] = {}
        self._records = self._valid_records(records)
        with self._lock:
            self._prune(time.time())

    @staticmethod
    def _valid_records(records: Any) -> List[Dict[str, Any]]:
        if not isinstance(records, list):
            return []
        valid = []
        for record in records:
            if not isinstance(record, dict):
                continue
            try:
                timestamp = float(record["timestamp"])
                size = int(record["size"])
                folder = _normalize_path(record["folder"])
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(timestamp) and size >= 0:
                valid.append({"timestamp": timestamp, "folder": folder, "size": size})
        return valid

    def _prune(self, now: float) -> None:
        cutoff = now - WINDOW_SECONDS
        pruned = [record for record in self._records if record["timestamp"] > cutoff]
        if len(pruned) != len(self._records):
            self._records = pruned
            self._persist()

    def _persist(self) -> None:
        try:
            self._save_records(list(self._records))
        except Exception:
            logger.exception("Failed to persist CloudDrive upload accounting records")

    @staticmethod
    def _matches(path: str, prefix: str) -> bool:
        return path == prefix or path.startswith(prefix.rstrip("/") + "/")

    def reserve(
        self, target_path: str, size: int
    ) -> Tuple[str, Optional[UploadReservation]]:
        original = str(target_path or "")
        normalized = _normalize_path(original)
        trailing_slash = original.endswith("/")
        size = max(0, int(size))

        with self._lock:
            now = time.time()
            self._prune(now)
            rule = next(
                (
                    candidate
                    for candidate in self.rules
                    if self._matches(normalized, candidate["prefix"])
                ),
                None,
            )
            if rule is None:
                return original, None

            relative = normalized[len(rule["prefix"]) :].lstrip("/")
            usage: Dict[str, int] = {}
            for record in self._records:
                usage[record["folder"]] = usage.get(record["folder"], 0) + record["size"]
            for reservation in self._reservations.values():
                usage[reservation.folder] = usage.get(reservation.folder, 0) + reservation.size

            for branch in rule["branches"]:
                folder = branch["path"]
                if usage.get(folder, 0) + size > branch["capacity_bytes"]:
                    continue
                routed_path = folder + (f"/{relative}" if relative else "")
                if trailing_slash and routed_path != "/":
                    routed_path += "/"
                self._next_token += 1
                reservation = UploadReservation(self._next_token, folder, size)
                self._reservations[reservation.token] = reservation
                return routed_path, reservation

            return original, None

    def finish(self, reservation: Optional[UploadReservation], success: bool) -> None:
        if reservation is None:
            return
        with self._lock:
            if self._reservations.pop(reservation.token, None) is None:
                return
            if success:
                self._records.append(
                    {
                        "timestamp": time.time(),
                        "folder": reservation.folder,
                        "size": reservation.size,
                    }
                )
            self._prune(time.time())
            self._persist()
