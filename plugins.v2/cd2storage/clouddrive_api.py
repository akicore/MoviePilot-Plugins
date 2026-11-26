from enum import IntEnum
from pathlib import Path, PurePosixPath
from time import monotonic, sleep
from typing import Any, Dict, List, Literal, Optional
from uuid import NAMESPACE_DNS, getnode, uuid4, uuid5

from grpc import Call, RpcError, StatusCode
from httpx import RequestError
from httpx import stream as httpx_stream

from app.core.config import global_vars, settings
from app.log import logger
from app.modules.filemanager.storages import transfer_process
from app.schemas import FileItem, StorageUsage
from app.schemas.exception import StorageQueryError

from clouddrive2_client import CloudDriveClient
from clouddrive2_client.proto import clouddrive_pb2

from .folder_manager import FolderManager
from .local_upload import (
    LocalPathMapping, LocalUpload, LocalUploadRpc, match_local_path, normalize_path,
)
from .remote_channel import RemoteUploadChannel
from .remote_hash import HashResult, RemoteHashCache, RemoteHashDispatcher
from .remote_upload import ReadResult, RemoteReadDispatcher, RemoteUploadCancelled
from .upload_balancer import UploadBalancer


# 115/CD2：移动或「从含撇号的旧名改到其它名」时不稳定；先脱敏的是**源侧末段名/路径**
# 最终目标名 `new_name` 仍可含 ASCII 撇号（改为带撇号的展示名通常可用），本模块不禁止
APOSTROPHE_ESCAPE = "[Jw==]"


def escape_apostrophe_in_name(name: str) -> str:
    """
    将名称中的 ASCII 单引号替换为占位串，便于移动前或两步重命名第一步脱敏（针对**源名**）

    :param name (str): 原始文件名或目录名

    :return str: 替换后的名称
    """

    return name.replace("'", APOSTROPHE_ESCAPE)


def _posix_join_parent_name(parent: str, name: str) -> str:
    """
    将父目录路径与末段名称拼接为 POSIX 路径（根目录无双斜杠）

    :param parent (str): 父目录绝对路径
    :param name (str): 文件或目录名

    :return str: 完整路径
    """

    base = (parent or "/").rstrip("/") or "/"
    if base == "/":
        return f"/{name}"
    return f"{base}/{name}"


class UploadStatus(IntEnum):
    """
    上传状态枚举

    Attributes:
        FINISH: 完成
        ERROR: 错误
        FATAL_ERROR: 致命错误
    """

    CANCELLED = 2
    FINISH = 5
    SKIPPED = 6
    IGNORED = 8
    ERROR = 9
    FATAL_ERROR = 10


def _cloudfile_to_fileitem(
    f: Any, disk_name: str, parent_fileid: Optional[str] = None
) -> FileItem:
    """
    将 proto CloudDriveFile 转为 FileItem

    :param f (Any): proto CloudDriveFile
    :param disk_name (str): 磁盘名称
    :param parent_fileid (str): 父文件ID

    :return FileItem: 转换后的文件项
    """
    path_str = f.fullPathName or ""
    if f.isDirectory and path_str and not path_str.endswith("/"):
        path_str = path_str + "/"
    name = f.name or ""
    stem = Path(name).stem
    suffix = Path(name).suffix
    extension = suffix[1:] if suffix and not f.isDirectory else None
    modify_time = None
    if f.writeTime and f.writeTime.seconds:
        modify_time = f.writeTime.seconds
    return FileItem(
        storage=disk_name,
        fileid=f.id or "",
        parent_fileid=parent_fileid,
        name=name,
        basename=stem,
        extension=extension,
        type="dir" if f.isDirectory else "file",
        path=path_str,
        size=f.size if not f.isDirectory else None,
        modify_time=modify_time,
    )


class CloudDriveApi:
    """
    CloudDrive 储存操作实现
    """

    def __init__(
        self,
        client: CloudDriveClient,
        disk_name: str,
        download_base: Optional[str] = None,
        upload_mode: Literal["remote_upload", "direct_write", "local_upload"] = "direct_write",
        upload_balancer: Optional[UploadBalancer] = None,
        remote_device_id: Optional[str] = None,
        local_upload_mappings: Optional[List[LocalPathMapping]] = None,
    ) -> None:
        """
        初始化 CloudDrive API 封装

        :param client (CloudDriveClient): 已认证的 CloudDrive 客户端
        :param disk_name (str): 储存名称，与注册的 storage 一致
        :param download_base (str): 下载 URL 基地址
        :param upload_mode (str): 上传模式
        """
        self.client = client
        self._folders = FolderManager(client, logger)
        self._disk_name = disk_name
        self._download_base = (download_base or "http://127.0.0.1:19798").rstrip("/")
        self._upload_mode = upload_mode
        self._local_upload_mappings = local_upload_mappings or []
        self._upload_balancer = upload_balancer
        self._remote_channel = RemoteUploadChannel(
            client,
            remote_device_id
            or f"moviepilot-{uuid5(NAMESPACE_DNS, f'{getnode()}:{disk_name}')}",
            logger,
        )
        self.transtype = {"move": "移动", "copy": "复制"}

    def close(self) -> None:
        """Stop the shared request channel before closing its gRPC client."""
        self._remote_channel.close()

    def list(self, fileitem: FileItem) -> List[FileItem]:
        """
        列出目录下文件，或单文件则返回包含该文件的列表

        :param fileitem (FileItem): 目录或文件项

        :return List: FileItem 列表
        """
        if fileitem.type == "file":
            item = self.get_item(Path(fileitem.path))
            return [item] if item else []
        path = (fileitem.path or "/").rstrip("/") or "/"
        try:
            sub = self.client.get_sub_files(path, force_refresh=True)
            return [
                _cloudfile_to_fileitem(
                    f, self._disk_name, parent_fileid=fileitem.fileid
                )
                for f in sub
            ]
        except Exception as e:
            logger.error("【CloudDrive】列出目录失败 %s: %s", path, e)
            return []

    def iter_files(self, fileitem: FileItem) -> Optional[List[FileItem]]:
        """
        递归遍历目录，返回扁平文件列表

        :param fileitem (FileItem): 目录或文件项

        :return List: 所有文件项列表，失败返回 None
        """
        if fileitem.type == "file":
            item = self.get_item(Path(fileitem.path))
            return [item] if item else []
        items: List[FileItem] = []
        try:
            for child in self.list(fileitem):
                if child.type == "dir":
                    sub_list = self.iter_files(child)
                    if sub_list is not None:
                        items.extend(sub_list)
                else:
                    items.append(child)
            return items
        except Exception as e:
            logger.error("【CloudDrive】递归列表失败 %s: %s", fileitem.path, e)
            return None

    def get_item(self, path: Path) -> Optional[FileItem]:
        """
        按路径获取文件或目录项

        :param path (Path): 路径

        :return FileItem: 文件项，未找到返回 None
        """
        try:
            return self._query_item(path)
        except Exception as e:
            logger.error("【CloudDrive】查询文件失败 %s: %s", path.as_posix(), e)
            return None

    def get_item_strict(self, path: Path) -> Optional[FileItem]:
        """
        严格获取文件或目录，无法确认状态时抛出存储查询异常

        :param path (Path): 文件或目录路径

        :return FileItem: 文件项，确认不存在时返回 None

        :raises StorageQueryError: RPC 或接口异常导致无法确认文件状态
        """
        try:
            return self._query_item(path)
        except StorageQueryError:
            raise
        except Exception as e:
            raise StorageQueryError(
                f"【CloudDrive】查询文件信息失败: {path} - {e}"
            ) from e

    def _query_item(self, path: Path) -> Optional[FileItem]:
        """
        查询远端文件项

        :param path (Path): 文件或目录路径

        :return FileItem: 查询到的文件项，确认不存在时返回 None
        """
        path_str = path.as_posix()
        if not path_str or path_str == ".":
            path_str = "/"
        try:
            cloud_file = self.client.find_file_by_path(path_str)
            logger.info("【CloudDrive】查找文件成功 %s: %s", path_str, cloud_file)
        except RpcError as e:
            if not isinstance(e, Call) or e.code() != StatusCode.NOT_FOUND:
                raise
            try:
                cloud_file = self.client.get_file_detail_properties(path_str)
                logger.info(
                    "【CloudDrive】获取文件详情成功 %s: %s",
                    path_str,
                    cloud_file,
                )
            except RpcError as detail_error:
                if (
                    isinstance(detail_error, Call)
                    and detail_error.code() == StatusCode.NOT_FOUND
                ):
                    return None
                raise
        if not cloud_file or not cloud_file.id:
            return None
        return _cloudfile_to_fileitem(cloud_file, self._disk_name)

    def get_parent(self, fileitem: FileItem) -> Optional[FileItem]:
        """
        获取父目录项

        :param fileitem (FileItem): 文件项

        :return FileItem: 父目录项，如果不存在则返回 None
        """
        parent_path = Path(fileitem.path).parent
        if parent_path.as_posix() == "." or parent_path.as_posix() == "":
            parent_path = Path("/")
        return self.get_item(parent_path)

    def create_folder(self, fileitem: FileItem, name: str) -> Optional[FileItem]:
        """
        在指定目录下创建文件夹

        :param fileitem (FileItem): 父目录项
        :param name (str): 新文件夹名

        :return FileItem: 新目录的 FileItem，失败返回 None
        """
        try:
            folder = self._folders.ensure(fileitem.path or "/", name)
            return _cloudfile_to_fileitem(
                folder, self._disk_name, parent_fileid=fileitem.fileid
            )
        except Exception as e:
            logger.error(
                "【CloudDrive】获取或创建目录失败: parent=%s name=%s error=%s",
                fileitem.path, name, e,
            )
            return None

    def get_folder(self, path: Path) -> Optional[FileItem]:
        """Resolve every component through the same guarded create entry point."""
        try:
            normalized = normalize_path(path.as_posix())
        except ValueError as e:
            logger.error("【CloudDrive】非法目录路径 %s: %s", path, e)
            return None
        # CD2's virtual root need not have a provider ID. Listing it still has
        # to succeed before ensure() can resolve/create a child.
        fileitem = FileItem(
            storage=self._disk_name, fileid="", parent_fileid=None,
            name="", basename="", extension=None, type="dir", path="/",
            size=None, modify_time=None,
        )
        for part in PurePosixPath(normalized).parts[1:]:
            fileitem = self.create_folder(fileitem, part)
            if fileitem is None:
                return None
        return fileitem

    def detail(self, fileitem: FileItem) -> Optional[FileItem]:
        """
        获取文件详情

        :param fileitem (FileItem): 文件项

        :return FileItem: 包含详细信息的文件项，如果获取失败则返回None
        """
        return self.get_item(Path(fileitem.path))

    def delete(self, fileitem: FileItem) -> bool:
        """
        删除文件或目录

        :param fileitem (FileItem): 文件项

        :return bool: 删除成功返回 True，失败返回 False
        """
        try:
            result = self.client.delete_file(fileitem.path)
            return bool(result and result.success)
        except Exception as e:
            logger.error("【CloudDrive】删除失败 %s: %s", fileitem.path, e)
            return False

    def _disambiguate_safe_name(self, parent_dir: str, candidate: str) -> str:
        """
        若同目录已存在 candidate，则追加随机后缀避免覆盖

        :param parent_dir (str): 父目录路径
        :param candidate (str): 撇号脱敏后的候选名

        :return str: 唯一候选名
        """

        parent_item = self.get_item(Path(parent_dir))
        if not parent_item:
            return candidate
        existing = {it.name for it in self.list(parent_item)}
        if candidate not in existing:
            return candidate
        for _ in range(32):
            alt = f"{candidate}_cd2_{uuid4().hex[:8]}"
            if alt not in existing:
                logger.warning(
                    "【CloudDrive】撇号脱敏名与同目录冲突，已改用 %s",
                    alt,
                )
                return alt
        alt = f"{candidate}_cd2_{uuid4().hex[:16]}"
        logger.warning("【CloudDrive】撇号脱敏名与同目录冲突，已改用 %s", alt)
        return alt

    def _path_after_rename(self, parent_dir: str, basename: str, is_dir: bool) -> str:
        """
        在父目录下拼接重命名后的完整路径（目录保留末尾 /）

        :param parent_dir (str): 父目录路径
        :param basename (str): 新末段名
        :param is_dir (bool): 是否为目录

        :return str: 完整路径
        """

        joined = _posix_join_parent_name(parent_dir, basename)
        if is_dir and not joined.endswith("/"):
            return joined + "/"
        return joined

    def _verify_rename_succeeded(
        self,
        parent_str: str,
        new_basename: str,
        max_attempts: int = 8,
        delay_s: float = 0.25,
    ) -> bool:
        """
        列举父目录子项，判断是否已存在指定末段名

        :param parent_str (str): 父目录绝对路径
        :param new_basename (str): 重命名后的末段名
        :param max_attempts (int): 最大尝试次数（115 偶发延迟或 RPC 报错后实际已成功）
        :param delay_s (float): 两次尝试间隔秒数

        :return bool: 子项中存在同名则 True
        """

        parent_norm = (parent_str or "/").rstrip("/") or "/"
        for attempt in range(max_attempts):
            try:
                sub = self.client.get_sub_files(parent_norm, force_refresh=True)
                for f in sub:
                    if (f.name or "") == new_basename:
                        return True
            except Exception:
                pass
            if attempt < max_attempts - 1:
                sleep(delay_s)
        return False

    def _rename_with_verify(self, source_path: str, new_basename: str) -> bool:
        """
        重命名并在 API 返回失败时校验路径是否已更新（115/CD2 偶发假失败）

        :param source_path (str): 当前完整路径
        :param new_basename (str): 新名称（仅末段）

        :return bool: 成功返回 True
        """

        result: Any = None
        err: Optional[Exception] = None
        try:
            result = self.client.rename_file(source_path, new_basename)
        except Exception as e:
            err = e
            logger.debug("【CloudDrive】重命名调用异常（将尝试校验）: %s", e)

        ok_api = bool(result and result.success)
        if ok_api:
            return True

        parent = Path(source_path.rstrip("/")).parent
        parent_str = parent.as_posix()
        if parent_str in ("", "."):
            parent_str = "/"

        if self._verify_rename_succeeded(parent_str, new_basename):
            logger.warning(
                "【CloudDrive】重命名 RPC 未返回成功但已校验到目标文件存在，视为成功: %s -> %s",
                source_path,
                new_basename,
            )
            return True

        if err:
            logger.error(
                "【CloudDrive】重命名失败 %s -> %s: %s",
                source_path,
                new_basename,
                err,
            )
        else:
            logger.error(
                "【CloudDrive】重命名失败 %s -> %s: %s",
                source_path,
                new_basename,
                getattr(result, "errorMessage", "") if result else "",
            )
        return False

    def rename(self, fileitem: FileItem, name: str) -> bool:
        """
        重命名文件或目录

        若**旧名（源末段）**含撇号：先改为脱敏名再改为 ``name``，避免一步「含撇号旧名 → 其它名」触发的 API 问题
        若 ``name`` 本身含撇号，第二步仍会把它交给 CD2；规避的是**源路径上的撇号**，不是禁止目标名含撇号

        :param fileitem (FileItem): 文件项
        :param name (str): 新名称

        :return bool: 重命名成功返回 True，失败返回 False
        """
        if name == fileitem.name:
            return True

        raw_path = (fileitem.path or "/").rstrip("/") or "/"
        p = Path(raw_path)
        parent_dir = p.parent.as_posix() or "/"
        if parent_dir == ".":
            parent_dir = "/"
        old_name = fileitem.name or p.name
        is_dir = fileitem.type == "dir"
        current_path = fileitem.path or _posix_join_parent_name(parent_dir, old_name)

        if "'" not in old_name:
            return self._rename_with_verify(current_path, name)

        intermediate = self._disambiguate_safe_name(
            parent_dir, escape_apostrophe_in_name(old_name)
        )
        if not self._rename_with_verify(current_path, intermediate):
            return False
        if intermediate == name:
            return True
        mid_path = self._path_after_rename(parent_dir, intermediate, is_dir)
        # 第二步：源路径已不含撇号；目标名仍可含撇号（策略只约束源侧末段名）
        if not self._rename_with_verify(mid_path, name):
            logger.warning(
                "【CloudDrive】两步重命名第二步失败，对象已保留脱敏末段名且未自动回滚: %s（期望改为 %s）",
                intermediate,
                name,
            )
            return False
        return True

    def move(self, fileitem: FileItem, path: Path, new_name: str) -> bool:
        """
        移动文件或目录到目标位置

        若**被移动项当前末段名**含撇号，移动前先脱敏；移动后再 ``rename`` 为 ``new_name``
        ``new_name`` 可含撇号，规避的是**移动时源路径**中的撇号，与最终展示名是否含撇号无关

        :param fileitem (FileItem): 要移动的文件项
        :param path (Path): 目标目录路径
        :param new_name (str): 移动后的文件名

        :return bool: 成功返回 True，失败返回 False
        """
        dest_path = (
            Path(path).as_posix().rstrip("/") if path is not None else "/"
        ) or "/"

        raw_path = (fileitem.path or "/").rstrip("/") or "/"
        p = Path(raw_path)
        parent_dir = p.parent.as_posix() or "/"
        if parent_dir == ".":
            parent_dir = "/"
        current_name = fileitem.name or p.name
        is_dir = fileitem.type == "dir"
        current_path = fileitem.path or _posix_join_parent_name(
            parent_dir, current_name
        )

        working_name = current_name
        escaped_applied = False
        if "'" in current_name:
            safe = self._disambiguate_safe_name(
                parent_dir, escape_apostrophe_in_name(current_name)
            )
            if not self._rename_with_verify(current_path, safe):
                return False
            escaped_applied = True
            working_name = safe
            current_path = self._path_after_rename(parent_dir, safe, is_dir)

        try:
            result = self.client.move_file([current_path], dest_path)
            if not result or not result.success:
                if escaped_applied:
                    logger.warning(
                        "【CloudDrive】移动失败，源侧已脱敏的对象未移动且未回滚，当前末段: %s",
                        working_name,
                    )
                logger.error(
                    "【CloudDrive】移动失败 %s -> %s: %s",
                    current_path,
                    dest_path,
                    getattr(result, "errorMessage", "") if result else "",
                )
                return False
            if working_name != new_name:
                new_path = _posix_join_parent_name(dest_path, working_name)
                if is_dir and not new_path.endswith("/"):
                    new_path = new_path + "/"
                if not self._rename_with_verify(new_path, new_name):
                    if escaped_applied:
                        logger.warning(
                            "【CloudDrive】移动后重命名失败，目标目录中仍为脱敏末段名且未回滚: %s（期望 %s）",
                            working_name,
                            new_name,
                        )
                    return False
            return True
        except Exception as e:
            if escaped_applied:
                logger.warning(
                    "【CloudDrive】移动异常，源侧已脱敏的对象未移动且未回滚，当前末段: %s",
                    working_name,
                )
            logger.error(
                "【CloudDrive】移动失败 %s -> %s: %s",
                current_path,
                dest_path,
                e,
            )
            return False

    def copy(self, fileitem: FileItem, path: Path, new_name: str) -> bool:
        """
        复制文件或目录到目标位置

        :param fileitem (FileItem): 要复制的文件项
        :param path (Path): 目标目录路径
        :param new_name (str): 复制后的文件名

        :return bool: 成功返回 True，失败返回 False
        """
        dest_path = (
            Path(path).as_posix().rstrip("/") if path is not None else "/"
        ) or "/"
        try:
            result = self.client.copy_file([fileitem.path], dest_path)
            if not result or not result.success:
                logger.error(
                    "【CloudDrive】复制失败 %s -> %s: %s",
                    fileitem.path,
                    dest_path,
                    getattr(result, "errorMessage", "") if result else "",
                )
                return False
            if fileitem.name != new_name:
                new_path = f"{dest_path}/{fileitem.name}"
                rename_result = self.client.rename_file(new_path, new_name)
                if not rename_result or not rename_result.success:
                    logger.error(
                        "【CloudDrive】复制后重命名失败 %s -> %s: %s",
                        new_path,
                        new_name,
                        getattr(rename_result, "errorMessage", "")
                        if rename_result
                        else "",
                    )
                    return False
            return True
        except Exception as e:
            logger.error(
                "【CloudDrive】复制失败 %s -> %s: %s",
                fileitem.path,
                dest_path,
                e,
            )
            return False

    def download(
        self, fileitem: FileItem, path: Optional[Path] = None
    ) -> Optional[Path]:
        """
        下载文件到本地

        :param fileitem (FileItem): 文件项
        :param path (Path): 本地目录，None 则使用临时目录

        :return Path: 本地文件路径，失败返回 None
        """
        detail = self.get_item(Path(fileitem.path))
        if not detail:
            logger.error("【CloudDrive】获取文件详情失败: %s", fileitem.name)
            return None
        try:
            url_info = self.client.get_download_url(fileitem.path, get_direct_url=True)
        except Exception as e:
            logger.error("【CloudDrive】获取下载链接失败 %s: %s", fileitem.path, e)
            return None
        download_url = None
        # 直链时使用 API 返回的 userAgent 与 additionalHeaders（见 proto DownloadUrlPathInfo）
        headers: Dict[str, str] = {}
        if url_info.directUrl:
            download_url = url_info.directUrl
            headers["User-Agent"] = (
                url_info.userAgent if url_info.userAgent else settings.USER_AGENT
            )
            if getattr(url_info, "additionalHeaders", None):
                for k, v in url_info.additionalHeaders.items():
                    if k and v:
                        headers[k] = v
        elif url_info.downloadUrlPath:
            # 代理下载走 CloudDrive 服务，使用默认 UA
            headers["User-Agent"] = settings.USER_AGENT
            # 占位符替换：{SCHEME} {HOST} {PREVIEW}
            host_part = self._download_base.replace("http://", "").replace(
                "https://", ""
            )
            path_part = (
                url_info.downloadUrlPath.replace("{SCHEME}", "http")
                .replace("{HOST}", host_part)
                .replace("{PREVIEW}", "false")
            )
            download_url = self._download_base + path_part
        if not download_url:
            logger.error("【CloudDrive】下载链接为空: %s", fileitem.name)
            return None
        local_path = (path or settings.TEMP_PATH) / fileitem.name
        file_size = detail.size
        logger.info("【CloudDrive】开始下载: %s -> %s", fileitem.name, local_path)
        progress_callback = transfer_process(Path(fileitem.path).as_posix())
        try:
            with httpx_stream("GET", download_url, headers=headers) as r:
                r.raise_for_status()
                downloaded_size = 0
                with open(local_path, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=10 * 1024 * 1024):
                        if global_vars.is_transfer_stopped(fileitem.path):
                            logger.info("【CloudDrive】%s 下载已取消", fileitem.path)
                            r.close()
                            return None
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        if file_size:
                            progress = (downloaded_size * 100) / file_size
                            progress_callback(progress)
                progress_callback(100)
                logger.info("【CloudDrive】下载完成: %s", fileitem.name)
        except RequestError as e:
            logger.error("【CloudDrive】下载网络错误 %s: %s", fileitem.name, e)
            if local_path.exists():
                local_path.unlink()
            return None
        except Exception as e:
            logger.error("【CloudDrive】下载失败 %s: %s", fileitem.name, e)
            if local_path.exists():
                local_path.unlink()
            return None
        return local_path

    def _cancel_upload(self, upload_id: str) -> None:
        """
        安全取消远程上传会话，忽略取消过程中的异常

        :param upload_id (str): StartRemoteUpload 返回的上传会话 ID
        """
        try:
            self.client.remote_upload_control_cancel(upload_id)
        except Exception:
            pass

    def upload(
        self,
        target_dir: FileItem,
        local_path: Path,
        new_name: Optional[str] = None,
    ) -> Optional[FileItem]:
        """Upload through the common API entry point, applying balancing once."""
        balancer = self._upload_balancer
        original_path = target_dir.path or "/"
        routed_path = original_path
        file_size = None
        reservation = None
        path_changed = False
        result = None

        logger.info(
            "【CloudDrive】进入 CloudDriveApi.upload：原始目标目录=%s，本地文件=%s，分流器=%s",
            original_path,
            local_path,
            "已启用" if balancer else "未启用",
        )

        if balancer and target_dir.storage == self._disk_name:
            try:
                file_size = local_path.stat().st_size
                routed_path, reservation = balancer.reserve(original_path, file_size)
            except (OSError, TypeError, ValueError) as e:
                logger.warning(
                    "【CloudDrive】无法应用上传分流，将使用原目标目录：目标=%s，文件=%s，原因=%s",
                    original_path,
                    local_path,
                    e,
                )

        try:
            if routed_path != original_path:
                try:
                    target_dir.path = routed_path
                    path_changed = True
                except Exception as e:
                    logger.warning(
                        "【CloudDrive】无法修改上传目标目录，回退原目录并释放额度：原目录=%s，分流目录=%s，原因=%s",
                        original_path,
                        routed_path,
                        e,
                    )
                    if balancer:
                        balancer.finish(reservation, success=False)
                    reservation = None
                    routed_path = original_path

            logger.info(
                "【CloudDrive】执行上传：原始目标目录=%s，最终目标目录=%s，文件=%s，大小=%s 字节，已预留分流额度=%s",
                original_path,
                routed_path,
                local_path,
                file_size if file_size is not None else "未知",
                "是" if reservation else "否",
            )

            mode = (self._upload_mode or "direct_write").strip() or "direct_write"
            if mode == "direct_write":
                result = self._upload_direct_write(target_dir, local_path, new_name)
            elif mode == "local_upload":
                mapped = match_local_path(str(local_path), self._local_upload_mappings)
                if mapped is None:
                    logger.info(
                        "【CloudDrive】源路径没有匹配本地上传映射，回退远程上传: %s",
                        local_path,
                    )
                    result = self._upload_remote_upload(target_dir, local_path, new_name)
                else:
                    result = self._upload_local_source(
                        target_dir, local_path, mapped[1], new_name
                    )
            else:
                result = self._upload_remote_upload(target_dir, local_path, new_name)

            logger.info(
                "【CloudDrive】上传%s：原始目标目录=%s，实际目标目录=%s，返回文件路径=%s",
                "成功" if result is not None else "失败",
                original_path,
                routed_path,
                getattr(result, "path", "未知") if result is not None else "无",
            )
            return result
        finally:
            try:
                if path_changed:
                    target_dir.path = original_path
            finally:
                if balancer:
                    balancer.finish(reservation, success=result is not None)

    def _upload_local_source(
        self,
        target_dir: FileItem,
        local_path: Path,
        cd2_source_path: str,
        new_name: Optional[str] = None,
    ) -> Optional[FileItem]:
        """Ask CD2 to copy its configured local-folder source into the cloud."""
        try:
            stat = local_path.stat()
            if not local_path.is_file():
                raise OSError(f"本地上传源不是普通文件: {local_path}")
            rpc = LocalUploadRpc(self.client)
            source = rpc.find(cd2_source_path)
            if (
                not source
                or not source.id
                or not source.isLocal
                or source.isDirectory
                or source.size != stat.st_size
            ):
                raise OSError(
                    "CD2 本地映射源未验证通过（必须是已添加的本地文件夹内同大小文件）: "
                    f"{cd2_source_path}"
                )

            target_dir_path = (target_dir.path or "/").rstrip("/") or "/"
            destination = PurePosixPath(target_dir_path).as_posix()
            if destination == ".":
                destination = "/"
            target = rpc.find(destination)
            if normalize_path(target.fullPathName) != destination:
                raise OSError(
                    f"CD2 返回非预期上传目录: expected={destination} "
                    f"actual={target.fullPathName} id={target.id}"
                )
            logger.info(
                "【CloudDrive】本地上传目标确认: path=%s id=%s",
                destination, target.id,
            )
            # `readOnly` is present in newer CD2 schemas, but the bundled
            # clouddrive2-client protobuf may predate that field.
            if (
                not target.isDirectory
                or target.isLocal
                or getattr(target, "readOnly", False)
            ):
                raise OSError(f"本地上传目标必须是可写网盘目录: {destination}")
            target_name = new_name or local_path.name
            target_path = (PurePosixPath(destination) / target_name).as_posix()
            progress_callback = transfer_process(target_path)

            def check_cancelled() -> None:
                if global_vars.is_transfer_stopped(target_path):
                    raise RemoteUploadCancelled(target_path)

            uploader = LocalUpload(rpc, logger, check_cancelled, progress_callback)
            final_path = uploader.run(
                cd2_source_path,
                destination,
                target_name,
                stat.st_size,
            )
            result = self.get_item(Path(final_path))
            if (not result or result.type != "file" or result.size != stat.st_size
                    or normalize_path(result.path) != final_path):
                raise OSError(f"CD2 本地上传完成但目标文件未通过最终校验: {final_path}")
            progress_callback(100)
            return result
        except RemoteUploadCancelled:
            logger.info("【CloudDrive】本地上传已取消: %s", cd2_source_path)
            return None
        except Exception as e:
            logger.error(
                "【CloudDrive】CD2 本地上传失败（不自动切换远程重传）: source=%s error=%s",
                cd2_source_path,
                e,
            )
            return None

    def _upload_remote_upload(
        self,
        target_dir: FileItem,
        local_path: Path,
        new_name: Optional[str] = None,
    ) -> Optional[FileItem]:
        """
        上传文件到 CloudDrive（Remote Upload 协议）

        协议参考: https://www.clouddrive2.com/api/CloudDrive2_gRPC_API_Guide.html
        （远程上传协议：StartRemoteUpload → RemoteUploadChannel 流 → 响应 ReadData/HashData）

        :param target_dir (FileItem): 目标目录 FileItem
        :param local_path (Path): 本地文件路径
        :param new_name (str): 云端文件名，None 则用 local_path.name

        :return FileItem: 上传成功返回云端文件 FileItem，失败返回 None
        """
        max_retries = 3
        if not local_path.exists() or not local_path.is_file():
            logger.error("【CloudDrive】上传文件不存在或非文件: %s", local_path)
            return None
        file_size = local_path.stat().st_size
        target_name = new_name or local_path.name
        parent_path = (target_dir.path or "").rstrip("/") or "/"
        target_path = f"{parent_path.rstrip('/')}/{target_name}"

        hash_cache = RemoteHashCache(local_path, file_size)
        logger.info("【CloudDrive】远程上传按需计算哈希: %s", target_name)

        for attempt in range(1, max_retries + 1):
            if global_vars.is_transfer_stopped(target_path):
                logger.info("【CloudDrive】上传已取消: %s", target_path)
                return None
            result = self._upload_once(
                target_path=target_path,
                local_path=local_path,
                file_size=file_size,
                hash_cache=hash_cache,
            )
            if result is not None:
                return result
            if result is None and attempt < max_retries:
                if global_vars.is_transfer_stopped(target_path):
                    return None
                logger.warning(
                    "【CloudDrive】上传失败，第 %d/%d 次重试: %s",
                    attempt,
                    max_retries,
                    target_path,
                )
        logger.error(
            "【CloudDrive】上传最终失败（已重试 %d 次）: %s",
            max_retries,
            target_path,
        )
        return None

    def _wait_upload_complete(
        self,
        remote_path: str,
        timeout: int = 3600,
        interval: int = 5,
        stall_timeout: int = 1800,
    ) -> bool:
        """
        等待 CloudDrive2 将直写文件上传到云端完成

        通过轮询 get_upload_file_list 查找 destPath 匹配项的终态

        :param remote_path (str): 云端目标路径
        :param timeout (int): 总超时时间（秒）
        :param interval (int): 轮询间隔（秒）
        :param stall_timeout (int): 无进度超时（秒）

        :return bool: 成功完成返回 True，否则 False
        """
        terminal, finish_value = self.client.upload_terminal_enums()

        normalized = Path(remote_path).as_posix() if remote_path else "/"
        deadline = monotonic() + timeout
        found_ever = False
        last_observed_state = None
        last_activity_time = monotonic()

        while monotonic() < deadline:
            if global_vars.is_transfer_stopped(normalized):
                logger.info("【CloudDrive】等待云端上传已取消: %s", normalized)
                return False
            try:
                resp = self.client.get_upload_file_list(get_all=True)
                upload_files = getattr(resp, "uploadFiles", None) or []

                found = False
                for uf in upload_files:
                    raw_dest = getattr(uf, "destPath", "") or ""
                    dest = Path(raw_dest).as_posix() if raw_dest else "/"
                    if dest != normalized and dest.rstrip("/") != normalized.rstrip(
                        "/"
                    ):
                        continue
                    found = True
                    found_ever = True

                    status_enum = getattr(uf, "statusEnum", None)
                    status_str = getattr(uf, "status", "")
                    transferred = int(getattr(uf, "transferedBytes", 0) or 0)
                    total = int(getattr(uf, "size", 0) or 0)
                    observed_state = (status_enum, status_str, transferred, total)

                    if terminal and status_enum in terminal:
                        if finish_value is not None and status_enum == finish_value:
                            logger.info("【CloudDrive】云端上传完成: %s", normalized)
                            return True
                        logger.warning(
                            "【CloudDrive】云端上传终态非成功: %s, status=%s, enum=%s",
                            normalized,
                            status_str,
                            status_enum,
                        )
                        return False

                    now = monotonic()
                    if observed_state != last_observed_state:
                        last_activity_time = now
                        last_observed_state = observed_state

                    dynamic_stall_timeout = stall_timeout
                    status_lc = str(status_str).lower()
                    if status_lc == "waitingforpreprocessing":
                        dynamic_stall_timeout = None

                    if dynamic_stall_timeout is not None and (
                        now - last_activity_time >= dynamic_stall_timeout
                    ):
                        logger.warning(
                            "【CloudDrive】云端上传停滞超时(%ss 无状态/进度变化): %s, status=%s",
                            dynamic_stall_timeout,
                            normalized,
                            status_str,
                        )
                        return False

                    if total > 0:
                        logger.debug(
                            "【CloudDrive】等待云端上传: %s, status=%s, progress=%.1f%%, %d/%d",
                            normalized,
                            status_str,
                            transferred / total * 100,
                            transferred,
                            total,
                        )
                    else:
                        logger.debug(
                            "【CloudDrive】等待云端上传: %s, status=%s, transferred=%d",
                            normalized,
                            status_str,
                            transferred,
                        )
                    break

                if not found and found_ever:
                    logger.info(
                        "【CloudDrive】上传任务已从队列消失，视为完成: %s", normalized
                    )
                    return True
            except Exception as e:
                logger.debug("【CloudDrive】查询上传状态失败: %s, %s", normalized, e)

            sleep(max(int(interval), 1))

        logger.warning("【CloudDrive】等待云端上传超时(%ss): %s", timeout, normalized)
        return False

    def _upload_direct_write(
        self,
        target_dir: FileItem,
        local_path: Path,
        new_name: Optional[str] = None,
    ) -> Optional[FileItem]:
        """
        直写上传

        :param target_dir (FileItem): 目标目录 FileItem
        :param local_path (Path): 本地文件路径
        :param new_name (str): 云端文件名，None 则用 local_path.name

        :return FileItem: 上传成功返回云端文件 FileItem，失败返回 None
        """
        if not local_path.exists() or not local_path.is_file():
            logger.error("【CloudDrive】上传文件不存在或非文件: %s", local_path)
            return None

        target_name = new_name or local_path.name
        raw_dir = (target_dir.path or "/").rstrip("/") or "/"
        remote_dir = Path(raw_dir).as_posix() if raw_dir else "/"
        remote_path = (PurePosixPath(remote_dir) / target_name).as_posix()
        file_size = local_path.stat().st_size
        progress_callback = transfer_process(remote_path)

        if global_vars.is_transfer_stopped(remote_path):
            logger.info("【CloudDrive】上传已取消: %s", remote_path)
            return None

        if not self.get_folder(Path(remote_dir)):
            logger.error("【CloudDrive】上传失败，目标目录创建失败: %s", remote_dir)
            return None

        file_handle = 0
        try:
            file_handle = self.client.create_file(remote_dir, target_name)
            if file_handle <= 0:
                logger.error(
                    "【CloudDrive】上传失败，创建远端文件句柄失败: %s", remote_path
                )
                return None

            chunk_size = 3 * 1024 * 1024
            offset = 0
            with open(local_path, "rb") as f: 
                while True:
                    if global_vars.is_transfer_stopped(remote_path):
                        logger.info("【CloudDrive】上传已取消: %s", remote_path)
                        return None
                    data = f.read(chunk_size)
                    if not data:
                        break
                    bytes_written = self.client.write_to_file(file_handle, offset, data)
                    if bytes_written != len(data):
                        logger.error(
                            "【CloudDrive】上传失败，写入长度不匹配: %s, expect=%d, actual=%d",
                            remote_path,
                            len(data),
                            bytes_written,
                        )
                        return None
                    offset += bytes_written
                    if file_size:
                        progress_callback(offset * 100.0 / file_size)

            if not self.client.close_file(file_handle):
                logger.error("【CloudDrive】上传失败，关闭文件失败: %s", remote_path)
                return None

            file_handle = 0

            if not self._wait_upload_complete(remote_path):
                logger.error("【CloudDrive】等待云端上传完成失败: %s", remote_path)
                return None

            progress_callback(100)
            return self.get_item(Path(remote_path))
        except Exception as e:
            logger.error("【CloudDrive】直写上传失败 %s: %s", remote_path, e)
            return None
        finally:
            if file_handle > 0:
                try:
                    self.client.close_file(file_handle)
                except Exception:
                    pass

    def _upload_once(
        self,
        target_path: str,
        local_path: Path,
        file_size: int,
        hash_cache: Optional[RemoteHashCache] = None,
    ) -> Optional[FileItem]:
        """
        执行一次上传尝试

        :param target_path (str): 云端目标路径
        :param local_path (Path): 本地文件路径
        :param file_size (int): 文件大小
        :param hash_cache (RemoteHashCache): 当前源文件已按需算出的哈希，重试时复用

        :return FileItem: 上传成功返回 FileItem，失败返回 None
        """
        progress_callback = transfer_process(target_path)
        session = None
        replies = None
        reads = None
        hash_tasks = None
        upload_id = None
        upload_finished = False
        try:
            hash_cache = hash_cache or RemoteHashCache(local_path, file_size)
            session = self._remote_channel.start(
                target_path,
                lambda: self.client.start_remote_upload(
                    file_path=target_path,
                    file_size=file_size,
                    known_hashes=hash_cache.known_hashes(),
                    client_can_calculate_hashes=True,
                ),
            )
            upload_id = session.upload_id

            def _check_cancelled() -> None:
                if session.error is not None:
                    raise session.error
                if global_vars.is_transfer_stopped(target_path):
                    raise RemoteUploadCancelled(target_path)

            # Use the public gRPC future API so in-flight calls have deadlines
            # and can be cancelled when this upload is stopped or fails.
            metadata = []
            if self.client.jwt_token:
                metadata.append(("authorization", f"Bearer {self.client.jwt_token}"))

            def _send_data(offset, data, is_last, lazy_read):
                request = clouddrive_pb2.RemoteReadDataUpload(
                    upload_id=upload_id,
                    offset=offset,
                    length=len(data),
                    data=data,
                    is_last_chunk=is_last,
                    lazy_read=lazy_read,
                )
                return self.client.stub.RemoteReadData.future(
                    request, metadata=metadata, timeout=120
                )

            def _send_hash_progress(kind, bytes_hashed, value, blocks):
                request = clouddrive_pb2.RemoteHashProgressUpload(
                    upload_id=upload_id,
                    bytes_hashed=bytes_hashed,
                    total_bytes=file_size,
                    hash_type=int(kind),
                    block_hashes=list(blocks),
                )
                # hash_value is optional: progress must leave it absent.
                if value:
                    request.hash_value = value
                return self.client.stub.RemoteHashProgress.future(
                    request, metadata=metadata, timeout=30
                )

            def _on_hash_result(result: HashResult, cached: bool, seconds: float):
                logger.info(
                    "【CloudDrive】按需哈希完成: upload=%s path=%s type=%s "
                    "block_size=%d cached=%s seconds=%.3f",
                    upload_id,
                    target_path,
                    result.hash_type.name,
                    result.block_size,
                    cached,
                    seconds,
                )

            hash_tasks = RemoteHashDispatcher(
                hash_cache, _send_hash_progress, _check_cancelled, _on_hash_result
            )
            data_completed_logged = False

            def _on_read_result(result: ReadResult) -> None:
                nonlocal data_completed_logged
                if not data_completed_logged and reads.completed_bytes == file_size:
                    data_completed_logged = True
                    logger.info(
                        "【CloudDrive】源文件数据已回传，等待CD2完成: upload=%s path=%s bytes=%d",
                        upload_id, target_path, file_size,
                    )
                logger.debug(
                    "【CloudDrive】RemoteReadData: upload=%s offset=%d bytes=%d "
                    "read_ms=%.3f rpc_ms=%.3f",
                    upload_id,
                    result.offset,
                    result.length,
                    result.read_seconds * 1000,
                    result.rpc_seconds * 1000,
                )

            reads = RemoteReadDispatcher(
                local_path,
                file_size,
                _send_data,
                _check_cancelled,
                progress_callback,
                _on_read_result,
            )
            logger.info(
                "【CloudDrive】远程上传取数: upload=%s path=%s workers=%d buffer_bytes=%d",
                upload_id,
                target_path,
                reads.MAX_WORKERS,
                reads.MAX_BUFFER_BYTES,
            )
            last_report = monotonic()

            def _poll_upload() -> None:
                nonlocal last_report
                reads.poll()
                hash_tasks.poll()
                now = monotonic()
                if now - last_report >= 15:
                    last_report = now
                    logger.info(
                        "【CloudDrive】远程上传等待/进度: upload=%s path=%s "
                        "received=%d queued=%d completed=%d unique_bytes=%d/%d "
                        "pending=%d max_parallel_rpcs=%d hash_pending=%d last_message_ago=%.1fs",
                        upload_id,
                        target_path,
                        session.received,
                        session.queue.qsize(),
                        reads.completed_requests,
                        reads.completed_bytes,
                        file_size,
                        reads.pending_count,
                        reads.max_active_rpcs,
                        hash_tasks.pending_count,
                        now - session.last_received,
                    )

            replies = session.messages(
                _poll_upload,
                timeout=300,
                activity_time=lambda: hash_tasks.last_progress,
            )
            for reply in replies:
                _check_cancelled()
                which = reply.WhichOneof("request")
                if not which:
                    continue
                if which == "read_data":
                    req = reply.read_data
                    logger.debug(
                        "【CloudDrive】收到远程取数请求: upload=%s path=%s "
                        "offset=%d length=%d lazy_read=%s pending=%d",
                        upload_id,
                        target_path,
                        req.offset,
                        req.length,
                        getattr(req, "lazy_read", False),
                        reads.pending_count,
                    )
                    reads.submit(
                        req.offset, req.length, getattr(req, "lazy_read", False)
                    )
                elif which == "hash_data":
                    req = reply.hash_data
                    block_size = getattr(req, "block_size", 0) or 0
                    if hash_tasks.submit(req.hash_type, block_size):
                        logger.info(
                            "【CloudDrive】收到按需哈希请求: upload=%s path=%s "
                            "type=%s block_size=%d",
                            upload_id,
                            target_path,
                            req.hash_type,
                            block_size,
                        )
                elif which == "status_changed":
                    st = reply.status_changed.status
                    logger.info(
                        "【CloudDrive】远程上传状态: upload=%s path=%s status=%s",
                        upload_id, target_path, st,
                    )
                    if st in (UploadStatus.FINISH, UploadStatus.SKIPPED):
                        reads.finish()
                        hash_tasks.poll()
                        progress_callback(100)
                        upload_finished = True
                        logger.info(
                            "【CloudDrive】上传完成: upload=%s path=%s status=%s",
                            upload_id, target_path, st,
                        )
                        break
                    if st in (
                        UploadStatus.CANCELLED, UploadStatus.IGNORED,
                        UploadStatus.ERROR, UploadStatus.FATAL_ERROR,
                    ):
                        msg = reply.status_changed.error_message or "上传失败"
                        logger.error("【CloudDrive】上传状态错误: %s", msg)
                        return None
        except RemoteUploadCancelled:
            logger.info("【CloudDrive】上传已取消: %s", target_path)
            return None
        except TimeoutError as e:
            logger.warning(
                "【CloudDrive】上传流超时: upload=%s path=%s error=%s",
                upload_id, target_path, e,
            )
            return None
        except Exception as e:
            logger.error(
                "【CloudDrive】上传过程异常: upload=%s path=%s error=%s",
                upload_id, target_path, e,
            )
            return None
        finally:
            if hash_tasks is not None:
                hash_tasks.close()
            if reads is not None:
                reads.close()
                logger.info(
                    "【CloudDrive】远程取数结束: upload=%s path=%s requests=%d "
                    "unique_bytes=%d max_parallel_rpcs=%d",
                    upload_id,
                    target_path,
                    reads.completed_requests,
                    reads.completed_bytes,
                    reads.max_active_rpcs,
                )
            if replies is not None:
                replies.close()
            if session is not None:
                self._remote_channel.release(session)
            if upload_id and not upload_finished:
                self._cancel_upload(upload_id)
        if not upload_finished:
            return None
        logger.info(f"【CloudDrive】上传完成: {target_path}")
        return self.get_item(Path(target_path))

    def link(self, fileitem: FileItem, target_file: Path) -> bool:
        """
        硬链接文件
        云盘存储不支持硬链接操作

        :param fileitem (FileItem): 文件项
        :param target_file (Path): 目标文件路径

        :return bool: 始终返回False，表示不支持此操作
        """
        return False

    def softlink(self, fileitem: FileItem, target_file: Path) -> bool:
        """
        软链接文件
        云盘存储不支持软链接操作

        :param fileitem (FileItem): 文件项
        :param target_file (Path): 目标文件路径

        :return bool: 始终返回False，表示不支持此操作
        """
        return False

    def usage(self) -> Optional[StorageUsage]:
        """
        获取存储空间用量

        :return StorageUsage: 存储空间用量，如果获取失败则返回None
        """
        try:
            info = self.client.get_space_info("/")
            if not info:
                return None
            return StorageUsage(
                total=info.totalSpace or 0,
                available=info.freeSpace if info.freeSpace is not None else 0,
            )
        except Exception as e:
            logger.warning("【CloudDrive】GetSpaceInfo 失败: %s", e)
            return None

    def support_transtype(self) -> dict:
        """
        支持的整理方式

        :return dict: 支持的整理方式字典
        """
        return self.transtype

    def is_support_transtype(self, transtype: str) -> bool:
        """
        是否支持整理方式

        :param transtype (str): 整理方式 (move/copy)

        :return bool: 是否支持
        """
        return transtype in self.transtype
