"""Serialize directory creation and retain recent results during CD2 refresh lag."""

from collections import OrderedDict
from contextlib import contextmanager
from pathlib import PurePosixPath
from threading import Lock
from time import monotonic, sleep

import grpc

from .local_upload import normalize_path


class FolderManager:
    RECENT_SECONDS = 60
    MAX_RECENT = 1024
    VISIBILITY_ATTEMPTS = 3
    VISIBILITY_RETRY_SECONDS = 0.2

    def __init__(self, client, logger, shared_groups=()):
        self.client = client
        self.logger = logger
        self._guard = Lock()
        self._locks = {}
        self._recent = OrderedDict()
        self._aliases = [
            (normalize_path(prefix), group_index)
            for group_index, group in enumerate(shared_groups)
            for prefix in group
        ]

    def _key(self, path):
        for prefix, group_index in self._aliases:
            # Mount roots remain separate CD2 entries. Only their descendants
            # refer to the same provider objects through the declared aliases.
            if path.startswith(prefix + "/"):
                return ("shared", group_index, path[len(prefix):])
        return path

    def _find_peer(self, path, key):
        if not isinstance(key, tuple):
            return None
        found = None
        for prefix, group_index in self._aliases:
            if group_index != key[1]:
                continue
            peer_path = prefix + key[2]
            if peer_path == path:
                continue
            candidate = self._refresh_shared_path(peer_path, key)
            if candidate is not None:
                if found is not None and found.id != candidate.id:
                    raise OSError(f"CD2 共享入口返回不同目录，停止创建: {path}")
                found = candidate
        return found

    def _refresh_shared_path(self, path, key, require_parent=False):
        # Refresh from the mount downwards. Listing a deep parent directly can
        # fail with NOT_FOUND before forceRefresh gets a chance to run: CD2 has
        # not indexed a newly created ancestor through this account yet.
        prefix = next(prefix for prefix, group_index in self._aliases
                      if group_index == key[1] and path == prefix + key[2])
        parent = prefix
        parts = PurePosixPath(key[2]).parts[1:]
        for index, name in enumerate(parts):
            child = (PurePosixPath(parent) / name).as_posix()
            self.logger.debug(
                "【CloudDrive】刷新共享入口目录: parent=%s expected=%s", parent, child,
            )
            folder = self._find(parent, child)
            if folder is None:
                if require_parent and index < len(parts) - 1:
                    raise OSError(f"CD2 共享入口父目录刷新后仍不可见，停止创建: {child}")
                # An optional peer may genuinely have no such parent yet. Do
                # not try listing its nonexistent descendants, or create any.
                return None
            parent = child
        return folder

    def _for_path(self, folder, path):
        if normalize_path(folder.fullPathName) == path:
            return folder
        # A shared provider ID does not make the other account's CD2 path
        # addressable. Only return metadata actually read through that entry.
        key = self._key(path)
        for attempt in range(self.VISIBILITY_ATTEMPTS):
            result = self._refresh_shared_path(path, key)
            if result is not None:
                if result.id != folder.id:
                    raise OSError(
                        f"CD2 共享目录身份不一致，停止创建或上传: path={path} "
                        f"expected_id={folder.id} actual_id={result.id}"
                    )
                self.logger.info(
                    "【CloudDrive】刷新并复用共享目录: created_path=%s requested_path=%s id=%s",
                    folder.fullPathName, path, result.id,
                )
                return result
            if attempt + 1 < self.VISIBILITY_ATTEMPTS:
                sleep(self.VISIBILITY_RETRY_SECONDS)
        raise OSError(
            f"CD2 共享目录刷新后仍不可见，暂不重复创建: path={path} "
            f"existing_path={folder.fullPathName} id={folder.id}"
        )

    @contextmanager
    def _locked(self, path):
        # Count waiters too: a lock must not be replaced while anyone uses it.
        with self._guard:
            entry = self._locks.setdefault(path, [Lock(), 0])
            entry[1] += 1
        try:
            with entry[0]:
                yield
        finally:
            with self._guard:
                entry[1] -= 1
                if not entry[1]:
                    del self._locks[path]

    def _remember(self, path, folder):
        with self._guard:
            self._recent[path] = (monotonic() + self.RECENT_SECONDS, folder)
            self._recent.move_to_end(path)
            while len(self._recent) > self.MAX_RECENT:
                self._recent.popitem(last=False)

    def _recent_result(self, path):
        with self._guard:
            entry = self._recent.get(path)
            if entry is not None and entry[0] <= monotonic():
                del self._recent[path]
                entry = None
            return entry

    @staticmethod
    def _validate(folder, path):
        if (not folder.id or not folder.isDirectory
                or folder.name != PurePosixPath(path).name
                or normalize_path(folder.fullPathName) != path):
            raise OSError(
                f"CD2 返回非预期目录: expected={path} "
                f"actual={folder.fullPathName} name={folder.name} id={folder.id}"
            )
        return folder

    def _find(self, parent, path):
        # Never translate an RPC failure (including mid-stream failure) into an
        # empty listing. Complete the listing before making any create decision.
        entries = []
        try:
            entries.extend(self.client.get_sub_files(parent, force_refresh=True))
        except grpc.RpcError as error:
            if entries:
                # NOT_FOUND after receiving entries is not a missing parent.
                # Do not let the path-recovery logic turn a partial listing
                # into a fresh create decision.
                raise OSError(f"CD2 目录列举中途失败: {parent}") from error
            raise
        matches = [file for file in entries if file.name == PurePosixPath(path).name]
        if len(matches) > 1:
            raise OSError(f"CD2 返回多个同名目录项，无法安全选择: {path}")
        return self._validate(matches[0], path) if matches else None

    def ensure(self, parent, name):
        parent = normalize_path(parent)
        if (not name or name in (".", "..") or "/" in name
                or "\\" in name or "\x00" in name):
            raise ValueError("非法的目录名称")
        path = (PurePosixPath(parent) / name).as_posix()
        key = self._key(path)
        with self._locked(key):
            recent = self._recent_result(key)
            try:
                existing = self._find(parent, path)
            except grpc.RpcError as error:
                if not isinstance(key, tuple) or error.code() != grpc.StatusCode.NOT_FOUND:
                    raise
                # Retry only path resolution, never CreateFolder. Permission,
                # transport and partially failed streams must still fail closed.
                existing = self._refresh_shared_path(path, key, require_parent=True)
            if existing is not None:
                if recent is not None and recent[1] is not None and recent[1].id != existing.id:
                    raise OSError(
                        f"CD2 共享目录身份不一致，停止创建或上传: path={path} "
                        f"expected_id={recent[1].id} actual_id={existing.id}"
                    )
                self._remember(key, existing)
                self.logger.debug("【CloudDrive】复用目录: path=%s id=%s", path, existing.id)
                return existing
            if recent is not None and recent[1] is not None:
                self.logger.debug("【CloudDrive】复用刚创建目录: path=%s id=%s", path, recent[1].id)
                return self._for_path(recent[1], path)
            # A restart or expired cache must not create a second directory
            # merely because this mount's listing lags behind another mount.
            peer = self._find_peer(path, key)
            if peer is not None:
                self._remember(key, peer)
                return self._for_path(peer, path)
            if recent is not None:
                raise OSError(f"CD2 上次创建结果尚未确认，暂不重复创建: {path}")

            # Record before sending: a transport timeout has an unknown outcome.
            self._remember(key, None)
            self.logger.info("【CloudDrive】提交创建目录: parent=%s name=%s expected=%s", parent, name, path)
            try:
                result = self.client.create_folder(parent, name)
                if not result or not result.result or not result.result.success:
                    # A different client may have won the create. Only adopt a
                    # freshly listed exact match, never issue another create here.
                    existing = self._find(parent, path)
                    if existing is None:
                        detail = result.result.errorMessage if result and result.result else "空响应"
                        raise OSError(f"CD2 创建目录失败: {path} error={detail}")
                elif result.folderCreated.id:
                    existing = self._validate(result.folderCreated, path)
                else:
                    existing = self._find(parent, path)
                    if existing is None:
                        raise OSError(f"CD2 已接受创建但目录尚未确认: {path}")
            except Exception:
                # Restart the uncertainty window after a slow failed RPC too.
                self._remember(key, None)
                raise
            self._remember(key, existing)
            self.logger.info("【CloudDrive】目录创建确认: expected=%s actual=%s id=%s", path, existing.fullPathName, existing.id)
            return existing
