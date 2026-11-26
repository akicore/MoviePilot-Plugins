"""Serialize directory creation and retain recent results during CD2 refresh lag."""

from collections import OrderedDict
from contextlib import contextmanager
from pathlib import PurePosixPath
from threading import Lock
from time import monotonic

from .local_upload import normalize_path


class FolderManager:
    RECENT_SECONDS = 60
    MAX_RECENT = 1024

    def __init__(self, client, logger):
        self.client = client
        self.logger = logger
        self._guard = Lock()
        self._locks = {}
        self._recent = OrderedDict()

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
        entries = list(self.client.get_sub_files(parent, force_refresh=True))
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
        with self._locked(path):
            existing = self._find(parent, path)
            if existing is not None:
                self._remember(path, existing)
                self.logger.debug("【CloudDrive】复用目录: path=%s id=%s", path, existing.id)
                return existing
            recent = self._recent_result(path)
            if recent is not None:
                if recent[1] is None:
                    raise OSError(f"CD2 上次创建结果尚未确认，暂不重复创建: {path}")
                self.logger.debug("【CloudDrive】复用刚创建目录: path=%s id=%s", path, recent[1].id)
                return recent[1]

            # Record before sending: a transport timeout has an unknown outcome.
            self._remember(path, None)
            self.logger.info("【CloudDrive】提交创建目录: parent=%s name=%s expected=%s", parent, name, path)
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
            self._remember(path, existing)
            self.logger.info("【CloudDrive】目录创建确认: expected=%s actual=%s id=%s", path, existing.fullPathName, existing.id)
            return existing
