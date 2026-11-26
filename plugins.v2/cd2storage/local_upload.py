"""CD2-owned local-source uploads; no payload bytes pass through MoviePilot."""

from dataclasses import dataclass
import json
from pathlib import Path, PurePosixPath
from time import monotonic, sleep
from typing import Any, Callable, List, Optional
from uuid import uuid4

from grpc import RpcError, StatusCode
from google.protobuf.empty_pb2 import Empty
from clouddrive2_client.proto import clouddrive_pb2 as pb

from .remote_upload import RemoteUploadCancelled


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
        # A unique staging directory makes Skip a fail-safe, never an overwrite.
        return self.checked(
            self.call("CopyFile", pb.CopyFileRequest(
                theFilePaths=[source], destPath=destination,
                conflictPolicy=pb.CopyFileRequest.Skip,
            )), "CD2 CopyFile 失败",
        )

    def list_files(self, directory: str):
        replies = self.call("GetSubFiles", pb.ListSubFileRequest(
            path=directory, forceRefresh=True,
        ))
        return [file for reply in replies for file in reply.subFiles]

    def cloud_file(self, path: str, size: int):
        # Force a provider listing instead of trusting FindFileByPath's cached
        # upload placeholder. The per-upload staging directory is uniquely owned.
        parent = PurePosixPath(path).parent.as_posix()
        for file in self.list_files(parent):
            if (file.name == PurePosixPath(path).name and file.id
                    and file.isCloudFile and not file.isLocal
                    and not file.isDirectory and file.size == size):
                return file
        return None

    def uploads(self, destination: str):
        """Use bounded pages; do not assume undocumented server filter semantics."""
        result = []
        for page in range(100):
            response = self.call("GetUploadFileList", pb.GetUploadFileListRequest(
                getAll=False, itemsPerPage=100, pageNumber=page, filter=destination,
            ))
            # `filter` is the API's search filter; confirm exact destination
            # locally so similar names (/stage and /stage-old) never mix.
            result.extend(response.uploadFiles)
            if len(response.uploadFiles) < 100 or (page + 1) * 100 >= response.totalCount:
                return [item for item in result if item.destPath.rstrip("/") == destination.rstrip("/")]
        raise OSError("CD2 上传列表分页超过安全上限")

    def copy_tasks(self):
        return self.call("GetCopyTasks", Empty()).copyTasks

    def rename(self, path: str, name: str):
        return self.checked(self.call("RenameFile", pb.RenameFileRequest(
            theFilePath=path, newName=name,
        )), "CD2 RenameFile 失败")

    def move(self, path: str, destination: str):
        # Do not overwrite an existing final file. The isolated staging file is
        # retained on conflict so that a concurrent upload cannot destroy data.
        return self.checked(self.call("MoveFile", pb.MoveFileRequest(
            theFilePaths=[path], destPath=destination,
            conflictPolicy=pb.MoveFileRequest.Skip,
        )), "CD2 MoveFile 失败")

    def delete_empty_directory(self, path: str):
        if self.list_files(path):
            return  # Never recursively delete data, even after an error.
        self.checked(self.call("DeleteFile", pb.FileRequest(path=path)), "删除空暂存目录失败")

    def cancel(self, source: str, directory: str, destination: str, logger):
        # Only our unique staging task, never all uploads or any MP source file.
        try:
            self.call("CancelCopyTask", pb.CopyTaskRequest(
                sourcePath=source, destPath=directory,
            ))
        except Exception as error:
            logger.warning("【CloudDrive】取消本地复制任务失败: %s", error)
        try:
            matches = [
                item for item in self.uploads(directory)
                if item.key and item.destPath.rstrip("/") == directory.rstrip("/")
            ]
            keys = [item.key for item in matches]
            if keys:
                self.call("CancelUploadFiles", pb.MultpleUploadFileKeyRequest(keys=keys))
        except Exception as error:
            logger.warning("【CloudDrive】取消本地上传任务失败: %s", error)


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

    def _wait(self, source: str, directory: str, path: str, size: int):
        deadline = monotonic() + self.timeout
        last_state = None
        last_activity = monotonic()
        last_log = 0.0
        quiet_cloud_id = None
        while monotonic() < deadline:
            self.check_cancelled()
            try:
                uploads = self.rpc.uploads(directory)
                # File copies may not create a folder-copy task at all.
                copies = [
                    task for task in self.rpc.copy_tasks()
                    if task.sourcePath.rstrip("/") == source
                    and task.destPath.rstrip("/") in (directory, path)
                ]
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
                if file:
                    if uploads_done or copies_done or quiet_cloud_id == file.id:
                        self.logger.info("【CloudDrive】本地上传云端校验完成: %s size=%d", path, size)
                        return
                    quiet_cloud_id = file.id
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

    def run(self, source: str, destination: str, name: str, size: int,
            create_directory: Callable[[Path], Any]) -> str:
        destination = normalize_path(destination)
        if not name or name in (".", "..") or "/" in name or "\\" in name or "\x00" in name:
            raise ValueError("非法的本地上传目标文件名")
        final_path = (PurePosixPath(destination) / name).as_posix()
        directory = (PurePosixPath(destination) / f".cd2storage-{uuid4().hex}").as_posix()
        copied_path = (PurePosixPath(directory) / PurePosixPath(source).name).as_posix()
        self.check_cancelled()
        if any(file.name == name for file in self.rpc.list_files(destination)):
            raise FileExistsError(f"目标文件已存在，拒绝覆盖: {final_path}")
        if not create_directory(Path(directory)):
            raise OSError(f"无法创建本地上传暂存目录: {directory}")
        submitted = False
        published = False
        try:
            self.check_cancelled()
            # Record before the RPC: a deadline/network failure has unknown outcome.
            submitted = True
            response = self.rpc.copy(source, directory)
            if response.resultFilePaths and list(response.resultFilePaths) != [copied_path]:
                raise OSError(f"CD2 CopyFile 返回非预期路径: {list(response.resultFilePaths)}")
            self.logger.info("【CloudDrive】已提交本地上传: source=%s staging=%s final=%s",
                             source, copied_path, final_path)
            self._wait(source, directory, copied_path, size)
            self.check_cancelled()
            if PurePosixPath(copied_path).name != name:
                renamed = self.rpc.rename(copied_path, name)
                copied_path = (PurePosixPath(directory) / name).as_posix()
                if renamed.resultFilePaths and list(renamed.resultFilePaths) != [copied_path]:
                    raise OSError("CD2 RenameFile 返回非预期路径")
            self.check_cancelled()
            # Check for existing files, then Skip handles races in the move RPC.
            if any(file.name == name for file in self.rpc.list_files(destination)):
                raise FileExistsError(f"目标文件已存在，保留云端暂存文件: {final_path}")
            moved = self.rpc.move(copied_path, destination)
            if moved.resultFilePaths and list(moved.resultFilePaths) != [final_path]:
                raise OSError("CD2 MoveFile 返回非预期路径")
            # resultFilePaths can be empty. A skipped move leaves our staging
            # file in place; do not mistake a pre-existing destination for ours.
            deadline = monotonic() + 30
            while monotonic() < deadline:
                self.check_cancelled()
                remaining = self.rpc.list_files(directory)
                file = self.rpc.cloud_file(final_path, size)
                if not remaining and file:
                    published = True
                    return final_path
                self._pause(deadline)
            raise OSError("CD2 MoveFile 未确认移走暂存文件或最终文件未通过云端校验")
        except BaseException:
            if submitted:
                self.rpc.cancel(source, directory, copied_path, self.logger)
                self.logger.warning(
                    "【CloudDrive】本地上传未确认完成，不回退重传；保留云端暂存目录供检查: %s", directory,
                )
            raise
        finally:
            # Empty-only cleanup. Failure/cancellation never deletes uploaded data.
            if published or not submitted:
                try:
                    self.rpc.delete_empty_directory(directory)
                except Exception as error:
                    self.logger.warning("【CloudDrive】清理空暂存目录失败: %s - %s", directory, error)
