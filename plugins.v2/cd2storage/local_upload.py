"""CD2-owned local-source uploads; no payload bytes pass through MoviePilot."""

from dataclasses import dataclass
import json
from pathlib import PurePosixPath
from time import monotonic, sleep
from typing import Any, Callable, List, Optional

from grpc import RpcError, StatusCode
from google.protobuf.empty_pb2 import Empty
from clouddrive2_client.proto import clouddrive_pb2 as pb


def normalize_path(value: Any) -> str:
    """Accept explicit absolute POSIX paths only; never normalize traversal away."""
    if not isinstance(value, str) or not value.startswith("/"):
        raise ValueError("路径必须是以 / 开头的绝对 POSIX 路径")
    if "\\" in value or "\x00" in value or any(
        part in (".", "..") for part in value.split("/")
    ):
        raise ValueError("路径不能包含反斜杠、空字符、. 或 .. 路径段")
    return "/" + "/".join(part for part in value.split("/") if part)


@dataclass(frozen=True)
class LocalPathMapping:
    mp_prefix: str
    cd2_prefix: str


def parse_local_mappings(value: Any) -> List[LocalPathMapping]:
    """Map MP paths to *virtual* paths in an already-added CD2 local folder."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return []
    data = json.loads(value) if isinstance(value, str) else value
    if not isinstance(data, list):
        raise ValueError("本地上传路径映射必须是 JSON 数组")
    mappings = []
    seen = set()
    for entry in data:
        if not isinstance(entry, dict):
            raise ValueError("每组本地上传路径映射必须是对象")
        mp = normalize_path(entry.get("mp_prefix"))
        cd2 = normalize_path(entry.get("cd2_prefix"))
        if mp in seen:
            raise ValueError(f"MP 源路径前缀重复: {mp}")
        seen.add(mp)
        mappings.append(LocalPathMapping(mp, cd2))
    # More-specific bindings win, not /PTTemp matching /PTTemp2.
    return sorted(mappings, key=lambda mapping: len(mapping.mp_prefix), reverse=True)


def match_local_path(path: str, mappings: List[LocalPathMapping]):
    source = PurePosixPath(normalize_path(path))
    for mapping in mappings:
        try:
            relative = source.relative_to(mapping.mp_prefix)
        except ValueError:
            continue
        if not relative.parts:
            continue  # An upload must identify a file, not the mapped root itself.
        return mapping, (PurePosixPath(mapping.cd2_prefix) / relative).as_posix()
    return None


class LocalUploadRpc:
    """Small public-stub adapter; new operations have bounded RPC deadlines."""

    def __init__(self, client):
        self.client = client

    def call(self, method: str, request):
        token = self.client.jwt_token
        metadata = [("authorization", f"Bearer {token}")] if token else []
        return getattr(self.client.stub, method)(request, metadata=metadata, timeout=30)

    def find(self, path: str):
        return self.call("FindFileByPath", pb.FindFileByPathRequest(path=path))

    @staticmethod
    def checked(result, operation: str):
        if not result.success:
            raise OSError(f"{operation}: {result.errorMessage}")
        return result

    def copy(self, source: str, destination: str):
        # Copy directly into the requested cloud folder and replace an existing
        # same-name object, as MoviePilot has already decided the conflict policy.
        return self.checked(
            self.call("CopyFile", pb.CopyFileRequest(
                theFilePaths=[source], destPath=destination,
                conflictPolicy=pb.CopyFileRequest.Overwrite,
            )), "CD2 CopyFile 失败",
        )

    def list_files(self, directory: str):
        replies = self.call("GetSubFiles", pb.ListSubFileRequest(
            path=directory, forceRefresh=True,
        ))
        return [file for reply in replies for file in reply.subFiles]

    def cloud_file(self, path: str, size: Optional[int] = None):
        # Refresh the provider listing; reject ambiguous or redirected objects.
        path = normalize_path(path)
        parent = PurePosixPath(path).parent.as_posix()
        matches = [file for file in self.list_files(parent)
                   if file.name == PurePosixPath(path).name]
        if len(matches) > 1:
            raise OSError(f"CD2 云端文件同名，无法确认本次上传: {path}")
        if not matches:
            return None
        file = matches[0]
        if normalize_path(file.fullPathName) != path:
            raise OSError(f"CD2 返回非预期云端文件: expected={path} actual={file.fullPathName}")
        if (file.id and file.isCloudFile and not file.isLocal
                and not file.isDirectory and (size is None or file.size == size)):
            return file
        return None

    def uploads(self, path: str):
        """Match this file's full path, independently of server search filters."""
        path = normalize_path(path)
        result = []
        for page in range(100):
            response = self.call("GetUploadFileList", pb.GetUploadFileListRequest(
                getAll=False, itemsPerPage=100, pageNumber=page,
            ))
            result.extend(item for item in response.uploadFiles
                          if item.destPath.rstrip("/") == path)
            if len(response.uploadFiles) < 100 or (page + 1) * 100 >= response.totalCount:
                return result
        raise OSError("CD2 上传列表分页超过安全上限")

    def copy_tasks(self):
        return self.call("GetCopyTasks", Empty()).copyTasks

    def rename(self, path: str, name: str):
        return self.checked(self.call("RenameFile", pb.RenameFileRequest(
            theFilePath=path, newName=name,
        )), "CD2 RenameFile 失败")

    def cancel(self, source: str, destination: str, logger):
        # Cancel only an exact copy task. Upload tasks in a shared destination
        # cannot be unambiguously attributed to this request.
        try:
            self.call("CancelCopyTask", pb.CopyTaskRequest(
                sourcePath=source, destPath=destination,
            ))
        except Exception as error:
            logger.warning("【CloudDrive】取消本地复制任务失败: %s", error)
        # Upload-file tasks in a shared destination cannot be unambiguously
        # attributed to one CopyFile request. Never cancel other files there.


class LocalUpload:
    """Submit once, wait for positive completion evidence, then publish the name."""

    def __init__(
        self, rpc: LocalUploadRpc, logger, check_cancelled: Callable[[], None],
        progress: Callable[[float], None], timeout: float = 3600,
        interval: float = 2, stall_timeout: float = 1800,
    ):
        self.rpc = rpc
        self.logger = logger
        self.check_cancelled = check_cancelled
        self.progress = progress
        self.timeout = timeout
        self.interval = interval
        self.stall_timeout = stall_timeout

    @staticmethod
    def _upload_state(item):
        return (item.statusEnum, item.transferedBytes, item.size, item.errorMessage)

    def _matching_copies(self, source, directory, path):
        return [task for task in self.rpc.copy_tasks()
                if task.sourcePath.rstrip("/") == source
                and task.destPath.rstrip("/") in (directory, path)]

    def _wait(self, source: str, directory: str, path: str, size: int,
              baseline_uploads=None, baseline_copies=None, baseline_file=None):
        deadline = monotonic() + self.timeout
        last_state = None
        last_activity = monotonic()
        last_log = 0.0
        quiet_cloud_id = None
        observed_uploads = set()
        observed_copies = set()
        while monotonic() < deadline:
            self.check_cancelled()
            try:
                # A reused key must not hide an active task. Ignore only
                # terminal records that are unchanged since before submission.
                terminal = (
                    pb.UploadFileInfo.Finish, pb.UploadFileInfo.Cancelled,
                    pb.UploadFileInfo.Error, pb.UploadFileInfo.FatalError,
                    pb.UploadFileInfo.Ignored, pb.UploadFileInfo.Skipped,
                )
                uploads = []
                for item in self.rpc.uploads(path):
                    if (baseline_uploads is None or item.key in observed_uploads
                            or item.statusEnum not in terminal
                            or baseline_uploads.get(item.key) != self._upload_state(item)):
                        uploads.append(item)
                        observed_uploads.add(item.key)
                copies = []
                for task in self._matching_copies(source, directory, path):
                    identity = (task.sourcePath, task.destPath,
                                task.startTime.SerializeToString())
                    if (baseline_copies is None or identity in observed_copies
                            or task.status not in (pb.CopyTask.Completed, pb.CopyTask.Failed)
                            or task.SerializeToString() not in baseline_copies):
                        copies.append(task)
                        observed_copies.add(identity)
            except RpcError as error:
                if error.code() not in (StatusCode.UNAVAILABLE, StatusCode.DEADLINE_EXCEEDED):
                    raise
                quiet_cloud_id = None
                self.logger.debug("【CloudDrive】本地上传状态查询暂时失败: %s", error)
                self._pause(deadline)
                continue
            for task in copies:
                if (task.status == pb.CopyTask.Failed or task.failedFiles
                        or task.failedFolders or task.cancelledFiles or task.errors):
                    raise OSError(f"CD2 本地复制任务失败: {task}")
                if task.skippedFiles:
                    raise OSError("CD2 本地复制任务跳过文件，不能视为上传成功")
            for item in uploads:
                if item.statusEnum in (
                    pb.UploadFileInfo.Cancelled, pb.UploadFileInfo.Error,
                    pb.UploadFileInfo.FatalError, pb.UploadFileInfo.Ignored,
                    pb.UploadFileInfo.Skipped,
                ):
                    raise OSError(f"CD2 本地上传失败: {item.status} {item.errorMessage}")
            # Explicit success still requires a refreshed provider file. If a
            # fast upload disappeared between polls, require that same cloud ID
            # on two consecutive quiet polls, never queue disappearance alone.
            uploads_done = bool(uploads) and all(
                item.statusEnum == pb.UploadFileInfo.Finish for item in uploads
            )
            copies_done = bool(copies) and all(
                task.status == pb.CopyTask.Completed and task.uploadedFiles >= 1
                for task in copies
            )
            uploads_active = any(item.statusEnum != pb.UploadFileInfo.Finish for item in uploads)
            copies_active = any(task.status != pb.CopyTask.Completed for task in copies)
            if not uploads_active and not copies_active:
                file = self.rpc.cloud_file(path, size)
                baseline_changed = baseline_file is None or (
                    file is not None
                    and (file.id, file.writeTime.SerializeToString())
                    != (baseline_file.id, baseline_file.writeTime.SerializeToString())
                )
                if file and (uploads_done or copies_done or baseline_changed):
                    signature = (file.id, file.size, file.writeTime.SerializeToString())
                    if uploads_done or copies_done or quiet_cloud_id == signature:
                        self.logger.info(
                            "【CloudDrive】本地上传云端校验完成: path=%s id=%s size=%d",
                            path, file.id, size,
                        )
                        return
                    quiet_cloud_id = signature
                else:
                    quiet_cloud_id = None
            else:
                quiet_cloud_id = None
            state = (
                tuple((item.key, item.statusEnum, item.transferedBytes) for item in uploads),
                tuple((task.status, task.uploadedBytes, task.uploadedFiles) for task in copies),
            )
            now = monotonic()
            if state != last_state:
                last_state, last_activity = state, now
            if now - last_activity >= self.stall_timeout:
                raise TimeoutError(f"CD2 本地上传无进展: {path}")
            transferred = max((item.transferedBytes for item in uploads), default=0)
            if size:
                self.progress(min(99.0, transferred * 100 / size))
            if now - last_log >= 15:
                last_log = now
                self.logger.info(
                    "【CloudDrive】等待本地上传: source=%s dest=%s uploads=%d copies=%d bytes=%d/%d",
                    source, path, len(uploads), len(copies), transferred, size,
                )
            self._pause(deadline)
        raise TimeoutError(f"等待 CD2 本地上传超时: {path}")

    def _pause(self, deadline: float):
        # Cancellation stays responsive during the polling interval.
        until = min(deadline, monotonic() + self.interval)
        while monotonic() < until:
            self.check_cancelled()
            sleep(min(0.1, max(0, until - monotonic())))

    def run(self, source: str, destination: str, name: str, size: int) -> str:
        destination = normalize_path(destination)
        if not name or name in (".", "..") or "/" in name or "\\" in name or "\x00" in name:
            raise ValueError("非法的本地上传目标文件名")
        final_path = (PurePosixPath(destination) / name).as_posix()
        copied_path = (PurePosixPath(destination) / PurePosixPath(source).name).as_posix()
        self.check_cancelled()
        submitted = False
        try:
            self.check_cancelled()
            # Snapshot existing upload keys and destination metadata so a prior
            # completed task/file cannot be mistaken for this overwrite.
            baseline_uploads = {
                item.key: self._upload_state(item)
                for item in self.rpc.uploads(copied_path)
            }
            baseline_copies = {
                task.SerializeToString()
                for task in self._matching_copies(source, destination, copied_path)
            }
            baseline_file = self.rpc.cloud_file(copied_path)
            # CopyFile accepts a destination directory, not a destination
            # filename. For the normal same-name case this writes the final
            # path directly; only a requested MP rename needs RenameFile later.
            submitted = True
            response = self.rpc.copy(source, destination)
            if response.resultFilePaths and list(response.resultFilePaths) != [copied_path]:
                raise OSError(f"CD2 CopyFile 返回非预期路径: {list(response.resultFilePaths)}")
            self.logger.info("【CloudDrive】已直接提交本地上传: source=%s dest=%s final=%s",
                             source, copied_path, final_path)
            self._wait(
                source, destination, copied_path, size,
                baseline_uploads=baseline_uploads,
                baseline_copies=baseline_copies,
                baseline_file=baseline_file,
            )
            self.check_cancelled()
            if copied_path != final_path:
                renamed = self.rpc.rename(copied_path, name)
                if renamed.resultFilePaths and list(renamed.resultFilePaths) != [final_path]:
                    raise OSError("CD2 RenameFile 返回非预期路径")
                # Confirm the requested name is visible and complete.
                deadline = monotonic() + 30
                while monotonic() < deadline:
                    self.check_cancelled()
                    file = self.rpc.cloud_file(final_path, size)
                    if file:
                        self.progress(100)
                        return final_path
                    self._pause(deadline)
                raise OSError("CD2 重命名后最终文件未通过云端校验")
            self.progress(100)
            return final_path
        except BaseException:
            if submitted:
                self.rpc.cancel(source, destination, self.logger)
                self.logger.warning(
                    "【CloudDrive】本地上传未确认完成，不回退重传；CD2 目标路径=%s",
                    final_path,
                )
            raise
