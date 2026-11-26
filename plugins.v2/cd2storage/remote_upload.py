"""Bounded, cancellable handling of one remote upload's read requests."""

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
import os
from os import fstat
from pathlib import Path
from threading import Event, Lock
from time import monotonic
from typing import Any, Callable, Dict, List, Tuple


class RemoteUploadCancelled(Exception):
    """The caller or another read task has stopped this upload."""


@dataclass(frozen=True)
class ReadResult:
    offset: int
    length: int
    read_seconds: float
    rpc_seconds: float


class RemoteReadDispatcher:
    """One open source per upload; workers read independent offsets with pread.

    send_data starts a cancellable RPC and returns its Future. RPC deadlines
    belong to that callable. Capacity is acquired before reading any file data.
    Platforms without pread serialize seek/read on the same unbuffered handle.
    """

    MAX_WORKERS = 4
    MAX_BUFFER_BYTES = 64 * 1024 * 1024

    def __init__(
        self,
        path: Path,
        file_size: int,
        send_data: Callable[[int, bytes, bool, bool], Any],
        check_cancelled: Callable[[], None],
        on_progress: Callable[[float], None],
        on_result: Callable[[ReadResult], None],
    ) -> None:
        self._path = path
        self._file_size = file_size
        self._send_data = send_data
        self._check_cancelled = check_cancelled
        self._on_progress = on_progress
        self._on_result = on_result
        self._signature = self._file_signature(path.stat())
        if self._signature[2] != file_size:
            raise OSError("Source file size changed before remote upload")
        self._stopped = Event()
        self._rpc_lock = Lock()
        self._active_rpcs = set()
        self._pending: Dict[Future, int] = {}
        self._buffer_bytes = 0
        self._ranges: List[Tuple[int, int]] = []
        self._last_progress = 0.0
        self.completed_bytes = 0
        self.completed_requests = 0
        self.max_active_rpcs = 0
        self._read_lock = Lock()
        self._pread = getattr(os, "pread", None)
        self._source = path.open("rb", buffering=0)
        try:
            if self._file_signature(fstat(self._source.fileno())) != self._signature:
                raise OSError("Source file changed before remote upload")
            self._executor = ThreadPoolExecutor(
                max_workers=self.MAX_WORKERS, thread_name_prefix="cd2-remote-read"
            )
        except BaseException:
            self._source.close()
            raise

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @staticmethod
    def _file_signature(stat) -> Tuple[int, int, int, int]:
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns

    def _ensure_running(self) -> None:
        if self._stopped.is_set():
            raise RemoteUploadCancelled("Remote upload stopped")
        self._check_cancelled()

    def submit(self, offset: int, length: int, lazy_read: bool) -> None:
        self.poll()
        if offset < 0 or offset > self._file_size or length < 0:
            raise ValueError(f"Invalid remote read range: {offset}+{length}")
        if length == 0 and self._file_size != 0:
            raise ValueError("Empty read request for a nonempty file")
        # A request extending past EOF receives only the remaining bytes.
        length = min(length, self._file_size - offset)
        if length > self.MAX_BUFFER_BYTES:
            raise ValueError(
                f"Remote read requires {length} bytes; "
                f"per-upload data budget is {self.MAX_BUFFER_BYTES} bytes"
            )
        while (
            len(self._pending) >= self.MAX_WORKERS
            or self._buffer_bytes + length > self.MAX_BUFFER_BYTES
        ):
            wait(self._pending, timeout=0.1, return_when=FIRST_COMPLETED)
            self.poll()
        self._ensure_running()
        task = self._executor.submit(self._read_and_send, offset, length, lazy_read)
        self._pending[task] = length
        self._buffer_bytes += length

    def _read_and_send(self, offset: int, length: int, lazy_read: bool) -> ReadResult:
        if self._stopped.is_set():
            raise RemoteUploadCancelled("Remote upload stopped")
        started = monotonic()
        source_fd = self._source.fileno()
        # Check the path too: fstat alone cannot detect a replaced source path
        # once this upload has pinned the original inode with an open handle.
        if (
            self._file_signature(self._path.stat()) != self._signature
            or self._file_signature(fstat(source_fd)) != self._signature
        ):
            raise OSError("Source file changed during remote upload")
        data = self._read_at(offset, length)
        if len(data) != length:
            raise OSError(f"Short source read at {offset}: {len(data)}/{length}")
        if self._file_signature(fstat(source_fd)) != self._signature:
            raise OSError("Source file changed while reading remote upload data")
        read_finished = monotonic()
        with self._rpc_lock:
            if self._stopped.is_set():
                raise RemoteUploadCancelled("Remote upload stopped")
            rpc = self._send_data(
                offset, data, offset + length >= self._file_size, lazy_read
            )
            self._active_rpcs.add(rpc)
            self.max_active_rpcs = max(
                self.max_active_rpcs, sum(not call.done() for call in self._active_rpcs)
            )
        # The protobuf RPC owns the outgoing payload now; release our copy.
        del data
        try:
            reply = rpc.result()
            if not reply.success:
                raise OSError(f"RemoteReadData rejected: {reply.error_message}")
            if reply.bytes_received != length:
                raise OSError(
                    f"RemoteReadData acknowledged {reply.bytes_received}/{length} "
                    f"bytes at {offset}"
                )
            return ReadResult(
                offset, length, read_finished - started, monotonic() - read_finished
            )
        finally:
            with self._rpc_lock:
                self._active_rpcs.discard(rpc)

    def _read_at(self, offset: int, length: int) -> bytes:
        """Read only the requested range, retrying partial reads without prefetch."""
        chunks = []
        remaining = length
        while remaining:
            if self._stopped.is_set():
                raise RemoteUploadCancelled("Remote upload stopped")
            if self._pread is not None:
                chunk = self._pread(self._source.fileno(), remaining, offset)
            else:
                # Compatibility path: keep seek/read atomic, never the RPC wait.
                with self._read_lock:
                    self._source.seek(offset)
                    chunk = self._source.read(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            offset += len(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def poll(self) -> None:
        """Surface worker errors/cancellation even when CD2 sends no messages."""
        self._ensure_running()
        done = [task for task in self._pending if task.done()]
        # Resolve every result first: a failure must not look like progress.
        results = [task.result() for task in done]
        for task, result in zip(done, results):
            self._buffer_bytes -= self._pending.pop(task)
            self.completed_requests += 1
            self._record_range(result.offset, result.offset + result.length)
            self._on_result(result)
        if self._file_size:
            # 100% is reserved for CD2's FINISH and all RPC acknowledgements.
            progress = min(99.0, self.completed_bytes * 100.0 / self._file_size)
            if progress > self._last_progress:
                self._last_progress = progress
                self._on_progress(progress)

    def _record_range(self, start: int, end: int) -> None:
        if start == end:
            return
        ranges = []
        for left, right in self._ranges:
            if right < start:
                ranges.append((left, right))
            elif end < left:
                ranges.append((start, end))
                start, end = left, right
            else:
                start, end = min(start, left), max(end, right)
        ranges.append((start, end))
        self._ranges = ranges
        self.completed_bytes = sum(right - left for left, right in ranges)

    def finish(self) -> None:
        while self._pending:
            wait(self._pending, timeout=0.1, return_when=FIRST_COMPLETED)
            self.poll()
        self._ensure_running()

    def close(self) -> None:
        """Cancel RPCs and join all workers before closing the pinned source."""
        with self._rpc_lock:
            self._stopped.set()
            active_rpcs = list(self._active_rpcs)
        try:
            for rpc in active_rpcs:
                rpc.cancel()
            for task in self._pending:
                task.cancel()
        finally:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._source.close()
            self._pending.clear()
            self._buffer_bytes = 0
