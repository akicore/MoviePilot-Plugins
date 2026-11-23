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

        logger.info(
            "【CloudDrive】开始检查上传分流：原始目标路径=%s，文件大小=%d 字节，规则数=%d",
            original,
            size,
            len(self.rules),
        )

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
                logger.info(
                    "【CloudDrive】上传路径未匹配任何分流规则：原始目标路径=%s，最终使用原路径",
                    original,
                )
                return original, None

            logger.info(
                "【CloudDrive】上传路径匹配分流规则：原始目标路径=%s，命中前缀=%s",
                original,
                rule["prefix"],
            )

            relative = normalized[len(rule["prefix"]) :].lstrip("/")
            usage: Dict[str, int] = {}
            for record in self._records:
                usage[record["folder"]] = usage.get(record["folder"], 0) + record["size"]
            for reservation in self._reservations.values():
                usage[reservation.folder] = usage.get(reservation.folder, 0) + reservation.size

            for branch in rule["branches"]:
                folder = branch["path"]
                current_usage = usage.get(folder, 0)
                capacity = branch["capacity_bytes"]
                if current_usage + size > capacity:
                    logger.info(
                        "【CloudDrive】分流目录额度不足，尝试下一目录：目录=%s，近24小时已用=%d 字节，本次=%d 字节，额度=%d 字节",
                        folder,
                        current_usage,
                        size,
                        capacity,
                    )
                    continue
                routed_path = folder + (f"/{relative}" if relative else "")
                if trailing_slash and routed_path != "/":
                    routed_path += "/"
                self._next_token += 1
                reservation = UploadReservation(self._next_token, folder, size)
                self._reservations[reservation.token] = reservation
                logger.info(
                    "【CloudDrive】上传分流已选择目录：规则前缀=%s，分支目录=%s，最终目标路径=%s，额度占用=%d/%d 字节",
                    rule["prefix"],
                    folder,
                    routed_path,
                    current_usage + size,
                    capacity,
                )
                return routed_path, reservation

            logger.warning(
                "【CloudDrive】匹配规则下所有分流目录额度均不足：原始目标路径=%s，规则前缀=%s，最终回退原路径",
                original,
                rule["prefix"],
            )
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
                logger.info(
                    "【CloudDrive】分流上传成功并计入额度：目录=%s，本次=%d 字节，24小时记录数=%d",
                    reservation.folder,
                    reservation.size,
                    len(self._records),
                )
            else:
                logger.warning(
                    "【CloudDrive】分流上传未成功，本次额度预留已释放：目录=%s，本次=%d 字节",
                    reservation.folder,
                    reservation.size,
                )
            self._prune(time.time())
            self._persist()

    def get_usage_stats(self) -> Dict[str, Any]:
        """Return configured branch quotas and successful rolling-24-hour usage."""
        with self._lock:
            self._prune(time.time())
            usage: Dict[str, int] = {}
            for record in self._records:
                folder = record["folder"]
                usage[folder] = usage.get(folder, 0) + record["size"]

            rules = []
            total_capacity = 0
            configured_folders = set()
            for rule in self.rules:
                branches = []
                for branch in rule["branches"]:
                    capacity = branch["capacity_bytes"]
                    used = usage.get(branch["path"], 0)
                    branches.append(
                        {
                            "path": branch["path"],
                            "capacity_bytes": capacity,
                            "used_bytes": used,
                            "remaining_bytes": max(0, capacity - used),
                        }
                    )
                    total_capacity += capacity
                    configured_folders.add(branch["path"])
                rules.append({"prefix": rule["prefix"], "branches": branches})

            total_used = sum(usage.get(folder, 0) for folder in configured_folders)
            logger.info(
                "【CloudDrive】刷新24小时分流额度统计：有效记录数=%d，总额度=%d 字节，总已用=%d 字节，目录用量=%s",
                len(self._records),
                total_capacity,
                total_used,
                {
                    folder: usage.get(folder, 0)
                    for folder in sorted(configured_folders)
                },
            )
            return {
                "rules": rules,
                "total_capacity_bytes": total_capacity,
                "total_used_bytes": total_used,
                "total_remaining_bytes": max(0, total_capacity - total_used),
            }
