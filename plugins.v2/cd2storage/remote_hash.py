"""On-demand hashing without blocking a remote upload's read requests."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import IntEnum
import hashlib
from os import fstat
from pathlib import Path
from threading import Event, Lock
from time import monotonic
from typing import Callable, Tuple

from .remote_upload import RemoteUploadCancelled


class HashType(IntEnum):
    MD5 = 1
    SHA1 = 2
    PIKPAK_SHA1 = 3


@dataclass(frozen=True)
class HashResult:
    hash_type: HashType
    block_size: int
    hash_value: str
    block_hashes: Tuple[str, ...] = ()


class RemoteHashCache:
    """Keep verified results only for one source file and its upload retries."""

    MAX_REQUESTS = 8

    def __init__(self, path: Path, file_size: int) -> None:
        self.path = path
        self.file_size = file_size
        self._signature = self._signature_of(path.stat())
        if self._signature[2] != file_size:
            raise OSError("Source file size changed before remote upload")
        self._lock = Lock()
        self._results = {}

    @staticmethod
    def _signature_of(stat):
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns

    def validate(self, stat=None) -> None:
        if (
            self._signature_of(self.path.stat() if stat is None else stat)
            != self._signature
        ):
            raise OSError("Source file changed during remote hashing")

    @staticmethod
    def key(hash_type: int, block_size: int):
        kind = HashType(hash_type)  # Reject unknown algorithms explicitly.
        if block_size < 0:
            raise ValueError("Negative hash block size")
        return kind, block_size if kind == HashType.MD5 else 0

    def get(self, key):
        self.validate()
        with self._lock:
            return self._results.get(key)

    def put(self, result: HashResult) -> None:
        self.validate()
        with self._lock:
            self._results[(result.hash_type, result.block_size)] = result
            if result.block_size:
                self._results[(result.hash_type, 0)] = HashResult(
                    result.hash_type, 0, result.hash_value
                )

    def known_hashes(self):
        self.validate()
        with self._lock:
            return {
                kind: result.hash_value
                for (kind, block_size), result in self._results.items()
                if block_size == 0
            }


class RemoteHashDispatcher:
    """One hash worker per file, with bounded jobs and cancellable progress RPCs."""

    READ_BYTES = 1024 * 1024
    MAX_BLOCKS = 262144
    PROGRESS_INTERVAL = 0.5

    def __init__(
        self,
        cache: RemoteHashCache,
        send_progress: Callable,
        check_cancelled: Callable[[], None],
        on_result: Callable[[HashResult, bool, float], None],
    ) -> None:
        self._cache = cache
        self._send_progress = send_progress
        self._check_cancelled = check_cancelled
        self._on_result = on_result
        self._stopped = Event()
        self._rpc_lock = Lock()
        self._rpc = None
        self._pending = {}
        self._requested = set()
        self.last_progress = 0.0
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="cd2-remote-hash"
        )

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def _ensure_running(self) -> None:
        if self._stopped.is_set():
            raise RemoteUploadCancelled("Remote hash stopped")
        self._check_cancelled()

    def submit(self, hash_type: int, block_size: int = 0) -> bool:
        self.poll()
        key = self._cache.key(hash_type, block_size)
        if key in self._pending:
            return False
        if (
            key not in self._requested
            and len(self._requested) >= self._cache.MAX_REQUESTS
        ):
            raise ValueError("Too many distinct hash requests for one upload")
        if key[1] and (self._cache.file_size + key[1] - 1) // key[1] > self.MAX_BLOCKS:
            raise ValueError("Requested MD5 block hash list exceeds memory budget")
        self._cache.validate()
        self._requested.add(key)
        self._pending[key] = self._executor.submit(self._run, key)
        return True

    def _report(self, kind, bytes_hashed, hash_value="", block_hashes=()) -> None:
        self._ensure_running()
        with self._rpc_lock:
            if self._stopped.is_set():
                raise RemoteUploadCancelled("Remote hash stopped")
            rpc = self._send_progress(kind, bytes_hashed, hash_value, block_hashes)
            self._rpc = rpc
        try:
            rpc.result()
            self.last_progress = monotonic()
        finally:
            with self._rpc_lock:
                self._rpc = None

    def _run(self, key) -> None:
        self._ensure_running()
        started = monotonic()
        result = self._cache.get(key)
        cached = result is not None
        if result is None:
            self._report(key[0], 0)
            result = self._compute(*key)
            self._cache.put(result)
        self._ensure_running()
        self._cache.validate()
        self._report(
            result.hash_type,
            self._cache.file_size,
            result.hash_value,
            result.block_hashes,
        )
        self._on_result(result, cached, monotonic() - started)

    @staticmethod
    def _pikpak_segment_size(total: int) -> int:
        if total <= (128 << 20):
            return 256 << 10
        if total <= (256 << 20):
            return 512 << 10
        if total <= (512 << 20):
            return 1024 << 10
        return 2048 << 10

    def _compute(self, kind, block_size) -> HashResult:
        total = self._cache.file_size
        pikpak = kind == HashType.PIKPAK_SHA1
        digest = (
            hashlib.md5(usedforsecurity=False)
            if kind == HashType.MD5
            else hashlib.sha1()
        )
        segment_size = self._pikpak_segment_size(total) if pikpak else block_size
        segment = None
        in_segment = 0
        blocks = []
        done = 0
        last_report = monotonic()
        with self._cache.path.open("rb") as source:
            self._cache.validate(fstat(source.fileno()))
            while done < total:
                self._ensure_running()
                self._cache.validate(fstat(source.fileno()))
                length = min(self.READ_BYTES, total - done)
                if segment_size:
                    length = min(length, segment_size - in_segment)
                data = source.read(length)
                if len(data) != length:
                    raise OSError("Short source read during remote hashing")
                if not pikpak:
                    digest.update(data)
                if segment_size:
                    if segment is None:
                        segment = (
                            hashlib.sha1()
                            if pikpak
                            else hashlib.md5(usedforsecurity=False)
                        )
                    segment.update(data)
                    in_segment += len(data)
                done += len(data)
                if segment_size and (in_segment == segment_size or done == total):
                    if pikpak:
                        digest.update(segment.digest())
                    else:
                        blocks.append(segment.hexdigest())
                    segment, in_segment = None, 0
                now = monotonic()
                if now - last_report >= self.PROGRESS_INTERVAL:
                    self._report(kind, done)
                    last_report = monotonic()
            self._cache.validate(fstat(source.fileno()))
        self._ensure_running()
        value = digest.hexdigest()
        return HashResult(
            kind, block_size, value.upper() if pikpak else value, tuple(blocks)
        )

    def poll(self) -> None:
        self._ensure_running()
        for key, job in list(self._pending.items()):
            if job.done():
                job.result()
                del self._pending[key]

    def close(self) -> None:
        with self._rpc_lock:
            self._stopped.set()
            rpc = self._rpc
        if rpc is not None:
            rpc.cancel()
        for job in self._pending.values():
            job.cancel()
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._pending.clear()
