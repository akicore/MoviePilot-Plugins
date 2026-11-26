"""One remote-upload request channel shared by all files of a client."""

from queue import Empty, Full, Queue
from threading import Lock, Thread
from time import monotonic
from typing import Callable, Optional


class RemoteUploadSession:
    """A bounded mailbox; a full/failed mailbox never blocks other files."""

    def __init__(self, upload_id: str, path: str) -> None:
        self.upload_id = upload_id
        self.path = path
        self.queue = Queue(maxsize=32)
        self.error: Optional[Exception] = None
        self.received = 0
        self.last_received = monotonic()

    def deliver(self, message) -> None:
        if self.error is not None:
            return
        try:
            self.queue.put_nowait(message)
            self.received += 1
            self.last_received = monotonic()
        except Full:
            self.error = RuntimeError(
                f"Remote upload request queue full: upload={self.upload_id} "
                f"path={self.path}"
            )

    def messages(
        self,
        check_callback: Callable[[], None],
        timeout: float = 300,
        activity_time: Optional[Callable[[], float]] = None,
    ):
        while True:
            deadline = monotonic() + timeout
            while True:
                if self.error is not None:
                    raise self.error
                check_callback()
                # A long hash scan is active while CD2 acknowledges progress,
                # even if no new server request arrives during that scan.
                if activity_time is not None:
                    deadline = max(deadline, activity_time() + timeout)
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Remote upload idle for {timeout}s: "
                        f"upload={self.upload_id} path={self.path}"
                    )
                try:
                    message = self.queue.get(timeout=min(0.1, remaining))
                    break
                except Empty:
                    pass
            yield message


class RemoteUploadChannel:
    """Serialize session registration, never the transfer of file data.

    StartRemoteUpload may emit requests before its reply reaches the caller.
    Buffer those requests while a start is in progress, then bind them to the
    returned upload_id. A channel error fails its active sessions explicitly;
    the next upload can open a replacement using the same device identifier.
    """

    MAX_EARLY_MESSAGES = 64

    def __init__(self, client, device_id: str, logger) -> None:
        self._client = client
        self._device_id = device_id
        self._logger = logger
        self._lock = Lock()
        self._start_lock = Lock()
        self._sessions = {}
        self._early = None
        self._early_error = None
        self._stream = None
        self._reader = None
        self._failure = None
        self._closed = False

    def _ensure_channel(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("Remote upload channel is closed")
            if self._stream is not None and self._failure is None:
                return
            old_stream, old_reader = self._stream, self._reader
        if old_stream is not None:
            old_stream.cancel()
        if old_reader is not None:
            old_reader.join(timeout=1)
            if old_reader.is_alive():
                raise RuntimeError("Previous remote upload reader has not stopped")
        stream = self._client.remote_upload_channel(device_id=self._device_id)
        with self._lock:
            if self._closed:
                stream.cancel()
                raise RuntimeError("Remote upload channel is closed")
            self._stream = stream
            self._failure = None
        try:
            stream.initial_metadata()
        except Exception:
            stream.cancel()
            with self._lock:
                self._stream = None
            raise
        with self._lock:
            if self._closed:
                stream.cancel()
                raise RuntimeError("Remote upload channel is closed")
            self._reader = Thread(
                target=self._receive,
                args=(stream,),
                daemon=True,
                name="cd2-upload-channel",
            )
            self._reader.start()
        self._logger.info(
            "【CloudDrive】远程上传共享通道已连接: device=%s", self._device_id
        )

    def start(self, path: str, start_upload: Callable) -> RemoteUploadSession:
        # Only the short start/registration phase is serialized across files.
        with self._start_lock:
            with self._lock:
                self._early = []
                self._early_error = None
            try:
                self._ensure_channel()
                started = start_upload()
                session = RemoteUploadSession(started.upload_id, path)
                with self._lock:
                    if self._closed or self._failure is not None:
                        session.error = self._failure or RuntimeError(
                            "Remote upload channel is closed"
                        )
                    elif self._early_error is not None:
                        session.error = self._early_error
                    if session.upload_id in self._sessions:
                        raise RuntimeError(
                            f"Duplicate remote upload ID: {session.upload_id}"
                        )
                    self._sessions[session.upload_id] = session
                    for message in self._early:
                        if message.upload_id == session.upload_id:
                            session.deliver(message)
                        else:
                            self._logger.debug(
                                "【CloudDrive】忽略已结束或未知会话消息: upload=%s",
                                message.upload_id,
                            )
                return session
            finally:
                with self._lock:
                    self._early = None
                    self._early_error = None

    def _receive(self, stream) -> None:
        try:
            for message in stream:
                with self._lock:
                    if self._closed or stream is not self._stream:
                        return
                    session = self._sessions.get(message.upload_id)
                    if session is not None:
                        session.deliver(message)
                    elif self._early is not None:
                        if len(self._early) < self.MAX_EARLY_MESSAGES:
                            self._early.append(message)
                        else:
                            self._early_error = RuntimeError(
                                "Too many requests before remote upload registration"
                            )
                    else:
                        self._logger.debug(
                            "【CloudDrive】忽略已结束或未知会话消息: upload=%s",
                            message.upload_id,
                        )
            error = ConnectionError("Remote upload request channel ended")
        except Exception as exc:
            error = exc
        with self._lock:
            if self._closed or stream is not self._stream:
                return
            self._failure = error
            for session in self._sessions.values():
                if session.error is None:
                    session.error = error
        self._logger.warning("【CloudDrive】远程上传共享通道中断: %s", error)

    def release(self, session: RemoteUploadSession) -> None:
        with self._lock:
            if self._sessions.get(session.upload_id) is session:
                del self._sessions[session.upload_id]

    def close(self) -> None:
        with self._lock:
            self._closed = True
            for session in self._sessions.values():
                session.error = RuntimeError("Remote upload channel stopped")
            stream, reader = self._stream, self._reader
        if stream is not None:
            try:
                stream.cancel()
            except Exception:
                pass
        if reader is not None:
            reader.join(timeout=1)
