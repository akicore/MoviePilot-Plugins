from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, cast

from clouddrive2_client import CloudDriveClient

from app.core.event import Event, eventmanager
from app.helper.storage import StorageHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import (
    FileItem,
    StorageOperSelectionEventData,
    StorageUsage,
)
from app.schemas.types import ChainEventType

from .clouddrive_api import CloudDriveApi
from .upload_balancer import UploadBalancer, parse_rules
from .version import VERSION


class CD2Storage(_PluginBase):
    """
    CD2Storage
    """

    plugin_name = "CD2Storage"
    plugin_desc = "CloudDrive2存储客户端"
    plugin_icon = "Cloudrive_A.png"
    plugin_version = VERSION
    plugin_author = "AkiCore"
    author_url = "https://github.com/akicore"
    plugin_config_prefix = "cd2storage_"
    plugin_order = 99
    auth_level = 1

    _enabled = False
    _client: Optional[CloudDriveClient] = None
    _disk_name = "CD2Storage"
    _clouddrive_api: Optional[CloudDriveApi] = None
    _host = ""
    _port = "19798"
    _token = ""
    UploadMode = Literal["remote_upload", "direct_write"]
    _upload_mode: UploadMode = "direct_write"
    _upload_balancer: Optional[UploadBalancer] = None

    def __init__(self) -> None:
        """
        初始化插件实例
        """
        super().__init__()

    def init_plugin(self, config: Optional[Dict] = None) -> None:
        """
        初始化插件

        :param config (Dict): 插件配置
        """
        if not config:
            return
        storage_helper = StorageHelper()
        storages = storage_helper.get_storagies()
        if not any(
            s.type == self._disk_name and s.name == self._disk_name for s in storages
        ):
            storage_helper.add_storage(
                storage=self._disk_name, name=self._disk_name, conf={}
            )
        self._enabled = config.get("enabled", False)
        self._host = (config.get("host") or "localhost").strip()
        self._port = str(config.get("port") or "19798").strip()
        self._token = (config.get("token") or "").strip()
        self._upload_mode = cast(
            self.UploadMode,
            (config.get("upload_mode") or "direct_write").strip() or "direct_write",
        )
        try:
            balance_rules = parse_rules(config.get("upload_balance_rules"))
        except (TypeError, ValueError) as e:
            logger.error("【CloudDrive】上传分流规则无效，已停用分流: %s", e)
            balance_rules = []
        self._upload_balancer = UploadBalancer(
            rules=balance_rules,
            records=self.get_data("upload_balance_records"),
            save_records=lambda records: self.save_data(
                "upload_balance_records", records
            ),
        )

        self._client = None
        self._clouddrive_api = None

        # Close the previous client before applying the new configuration.
        self.stop_service()

        if not self._enabled:
            return
        if not self._token:
            logger.warning("【CloudDrive】未配置 API 令牌，存储模块将不可用")
            return

        address = f"{self._host}:{self._port}"
        try:
            self._client = CloudDriveClient(
                address,
                options=[
                    ("grpc.keepalive_time_ms", 30000),
                    ("grpc.keepalive_timeout_ms", 10000),
                    ("grpc.keepalive_permit_without_calls", True),
                    ("grpc.http2.max_pings_without_data", 0),
                ],
            )
            # API tokens are already-issued bearer tokens; do not exchange credentials.
            self._client.jwt_token = self._token
            download_base = f"http://{self._host}:{self._port}"
            self._clouddrive_api = CloudDriveApi(
                self._client,
                disk_name=self._disk_name,
                download_base=download_base,
                upload_mode=self._upload_mode,
                upload_balancer=self._upload_balancer,
            )
        except Exception as e:
            logger.error("【CloudDrive】客户端创建失败: %s", e)
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    pass
                self._client = None
            return

    def get_state(self) -> bool:
        """
        返回插件是否已启用

        :return bool: 插件启用状态
        """
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 5},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "host",
                                            "label": "CloudDrive 地址",
                                            "hint": "如 localhost 或 192.168.1.100，不要带 http(s)",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "port",
                                            "label": "端口",
                                            "hint": "默认 19798",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "token",
                                            "label": "API 令牌",
                                            "type": "{{ 'password' }}",
                                            "hint": "CloudDrive2 的 API Token",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "upload_mode",
                                            "label": "上传模式",
                                            "items": [
                                                {"title": "直写上传", "value": "direct_write"},
                                                {"title": "远程上传", "value": "remote_upload"},
                                            ],
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "upload_balance_rules",
                                            "label": "上传路径分流规则（JSON）",
                                            "rows": 8,
                                            "auto-grow": True,
                                            "hint": '示例：[ {"prefix":"/abc/","branches":[{"path":"/abc1/","capacity_gb":20},{"path":"/abc2/","capacity_gb":30}]} ]。按规则顺序匹配；容量按十进制 GB 统计近 24 小时成功上传量。',
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "host": "localhost",
            "port": "19798",
            "token": "",
            "upload_mode": "direct_write",
            "upload_balance_rules": "",
        }

    def get_page(self) -> List[dict]:
        stats = (
            self._upload_balancer.get_usage_stats()
            if self._upload_balancer
            else {
                "rules": [],
                "total_capacity_bytes": 0,
                "total_used_bytes": 0,
                "total_remaining_bytes": 0,
            }
        )

        def format_size(size: int) -> str:
            return f"{size / 1_000_000_000:.2f} GB"

        def info_card(title: str, value: str, cols: int = 3) -> dict:
            return {
                "component": "VCol",
                "props": {"cols": 12, "sm": 6, "md": cols},
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal"},
                        "content": [
                            {
                                "component": "VCardText",
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {"class": "text-caption"},
                                        "text": title,
                                    },
                                    {
                                        "component": "div",
                                        "props": {"class": "text-h6"},
                                        "text": value,
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }

        page = [
            {
                "component": "VRow",
                "content": [
                    info_card("插件状态", "已启用" if self._enabled else "未启用"),
                    info_card("CloudDrive2 地址", f"{self._host}:{self._port}"),
                    info_card(
                        "上传模式",
                        "直写上传"
                        if self._upload_mode == "direct_write"
                        else "远程上传",
                    ),
                    info_card("令牌认证", "已配置" if self._token else "未配置"),
                ],
            },
            {
                "component": "VRow",
                "content": [
                    info_card("分流目录总额度", format_size(stats["total_capacity_bytes"])),
                    info_card("近 24 小时已用", format_size(stats["total_used_bytes"])),
                    info_card("分流目录剩余额度", format_size(stats["total_remaining_bytes"])),
                    info_card("分流规则数", str(len(stats["rules"]))),
                ],
            },
        ]

        for rule in stats["rules"]:
            branch_rows = []
            for branch in rule["branches"]:
                capacity = branch["capacity_bytes"]
                used = branch["used_bytes"]
                percent = min(100, int(used * 100 / capacity)) if capacity else 0
                branch_rows.append(
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"variant": "outlined"},
                                "content": [
                                    {
                                        "component": "VCardText",
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {"class": "text-subtitle-1"},
                                                "text": branch["path"],
                                            },
                                            {
                                                "component": "div",
                                                "props": {"class": "text-body-2"},
                                                "text": (
                                                    f"已用 {format_size(used)} / "
                                                    f"{format_size(capacity)} · 剩余 "
                                                    f"{format_size(branch['remaining_bytes'])}"
                                                ),
                                            },
                                            {
                                                "component": "VProgressLinear",
                                                "props": {
                                                    "model-value": percent,
                                                    "height": 6,
                                                    "rounded": True,
                                                },
                                            },
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                )
            page.append(
                {
                    "component": "VRow",
                    "content": [
                        {
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [
                                {
                                    "component": "div",
                                    "props": {"class": "text-h6 mt-4 mb-2"},
                                    "text": f"匹配前缀：{rule['prefix']}",
                                }
                            ],
                        },
                        *branch_rows,
                    ],
                }
            )
        if not stats["rules"]:
            page.append(
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "尚未配置上传分流规则。",
                    },
                }
            )
        return page

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_module(self) -> Dict[str, Any]:
        """
        返回储存模块能力映射

        :return Dict: 储存操作函数映射字典
        """
        return {
            "list_files": self.list_files,
            "any_files": self.any_files,
            "download_file": self.download_file,
            "upload_file": self.upload_file,
            "delete_file": self.delete_file,
            "rename_file": self.rename_file,
            "get_file_item": self.get_file_item,
            "get_parent_item": self.get_parent_item,
            "snapshot_storage": self.snapshot_storage,
            "storage_usage": self.storage_usage,
            "support_transtype": self.support_transtype,
            "create_folder": self.create_folder,
            "exists": self.exists,
            "get_item": self.get_item,
        }

    @eventmanager.register(ChainEventType.StorageOperSelection)
    def storage_oper_selection(self, event: Event) -> None:
        """
        监听储存选择事件，当所选储存为本插件时注入 storage_oper 为 CloudDriveApi

        :param event (Event): 事件对象，event.event_data 含 storage、storage_oper
        """
        if not self._enabled or not self._clouddrive_api:
            return
        event_data: StorageOperSelectionEventData = event.event_data
        if event_data.storage == self._disk_name:
            event_data.storage_oper = self._clouddrive_api  # noqa

    def list_files(
        self, fileitem: FileItem, recursion: bool = False
    ) -> Optional[List[FileItem]]:
        """
        列出目录下文件（及可选递归子目录）

        :param fileitem (FileItem): 目录或文件项，storage 需为本插件储存名
        :param recursion (bool): 是否递归列出子目录中的文件

        :return List: 文件项列表；非本储存或未就绪时返回空列表
        """
        if fileitem.storage != self._disk_name or not self._clouddrive_api:
            return []
        if recursion:
            result = self._clouddrive_api.iter_files(fileitem)
            if result is not None:
                return result
        result: List[FileItem] = []

        def __get_files(_item: FileItem, _r: bool = False) -> None:
            _items = self._clouddrive_api.list(_item)  # type: ignore[union-attr]
            if _items:
                if _r:
                    for t in _items:
                        if t.type == "dir":
                            __get_files(t, _r)
                        else:
                            result.append(t)
                else:
                    result.extend(_items)

        __get_files(fileitem, recursion)
        return result

    def any_files(
        self, fileitem: FileItem, extensions: Optional[list] = None
    ) -> Optional[bool]:
        """
        判断目录（含子目录）下是否存在文件；可限定扩展名

        :param fileitem (FileItem): 目录项
        :param extensions (list): 扩展名列表（如 [".mp4", ".mkv"]），None 表示任意文件

        :return bool: 存在返回 True，不存在返回 False；非本储存或未就绪返回 None
        """
        if fileitem.storage != self._disk_name or not self._clouddrive_api:
            return None

        def __any_file(_item: FileItem) -> bool:
            _items = self._clouddrive_api.list(_item)  # type: ignore[union-attr]
            if _items:
                if not extensions:
                    return True
                for t in _items:
                    if (
                        t.type == "file"
                        and t.extension
                        and f".{t.extension.lower()}" in extensions
                    ):
                        return True
                    if t.type == "dir" and __any_file(t):
                        return True
            return False

        return __any_file(fileitem)

    def create_folder(self, fileitem: FileItem, name: str) -> Optional[FileItem]:
        """
        在指定目录下创建文件夹

        :param fileitem (FileItem): 父目录项
        :param name (str): 新文件夹名称

        :return FileItem: 新目录的 FileItem；失败或非本储存时返回 None
        """
        if fileitem.storage != self._disk_name or not self._clouddrive_api:
            return None
        return self._clouddrive_api.create_folder(fileitem, name)

    def download_file(
        self, fileitem: FileItem, path: Optional[Path] = None
    ) -> Optional[Path]:
        """
        将云端文件下载到本地

        :param fileitem (FileItem): 要下载的文件项
        :param path (Path): 本地保存目录，None 时使用临时目录

        :return Path: 本地文件路径；失败或非本储存时返回 None
        """
        if fileitem.storage != self._disk_name or not self._clouddrive_api:
            return None
        return self._clouddrive_api.download(fileitem, path)

    def upload_file(
        self,
        fileitem: FileItem,
        path: Path,
        new_name: Optional[str] = None,
    ) -> Optional[FileItem]:
        """Upload callback; the shared CloudDriveApi entry point handles routing."""
        if fileitem.storage != self._disk_name or not self._clouddrive_api:
            return None
        logger.info(
            "【CloudDrive】经由插件 upload_file 回调上传：目标目录=%s，本地文件=%s",
            fileitem.path,
            path,
        )
        return self._clouddrive_api.upload(fileitem, path, new_name)

    def delete_file(self, fileitem: FileItem) -> Optional[bool]:
        """
        删除云端文件或目录

        :param fileitem (FileItem): 要删除的文件或目录项

        :return bool: 成功返回 True，失败返回 False；非本储存或未就绪返回 None
        """
        if fileitem.storage != self._disk_name or not self._clouddrive_api:
            return None
        return self._clouddrive_api.delete(fileitem)

    def rename_file(self, fileitem: FileItem, name: str) -> Optional[bool]:
        """
        重命名云端文件或目录

        :param fileitem (FileItem): 要重命名的项
        :param name (str): 新名称

        :return bool: 成功返回 True，失败返回 False；非本储存或未就绪返回 None
        """
        if fileitem.storage != self._disk_name or not self._clouddrive_api:
            return None
        return self._clouddrive_api.rename(fileitem, name)

    def exists(self, fileitem: FileItem) -> Optional[bool]:
        """
        判断指定路径在云端是否存在

        :param fileitem (FileItem): 文件或目录项（含 storage、path）

        :return bool: 存在返回 True，不存在返回 False；非本储存返回 None
        """
        if fileitem.storage != self._disk_name:
            return None
        return True if self.get_item(fileitem) else False

    def get_item(self, fileitem: FileItem) -> Optional[FileItem]:
        """
        按文件项获取对应的云端项（用于校验或取详情）

        :param fileitem (FileItem): 含 storage、path 的文件项

        :return FileItem: 云端 FileItem；不存在或非本储存时返回 None
        """
        if fileitem.storage != self._disk_name or not self._clouddrive_api:
            return None
        return self.get_file_item(storage=fileitem.storage, path=Path(fileitem.path))

    def get_file_item(self, storage: str, path: Path) -> Optional[FileItem]:
        """
        按储存名与路径获取云端文件或目录项

        :param storage (str): 储存名称，需为本插件储存名
        :param path (Path): 云端路径

        :return FileItem: FileItem；不存在或非本储存时返回 None
        """
        if storage != self._disk_name or not self._clouddrive_api:
            return None
        return self._clouddrive_api.get_item(path)

    def get_parent_item(self, fileitem: FileItem) -> Optional[FileItem]:
        """
        获取指定文件或目录的父目录项

        :param fileitem (FileItem): 当前项

        :return FileItem: 父目录 FileItem；非本储存或未就绪时返回 None
        """
        if fileitem.storage != self._disk_name or not self._clouddrive_api:
            return None
        return self._clouddrive_api.get_parent(fileitem)

    def snapshot_storage(
        self,
        storage: str,
        path: Path,
        last_snapshot_time: Optional[float] = None,
        max_depth: int = 5,
    ) -> Optional[Dict[str, Dict]]:
        """
        对指定目录做快照，收集路径下的文件信息（路径、大小、修改时间等）

        :param storage (str): 储存名称
        :param path (Path): 快照根路径
        :param last_snapshot_time (float): 仅收录修改时间大于此时间戳的文件，用于增量快照
        :param max_depth (int): 最大递归深度

        :return Dict: 路径到文件信息字典的映射；非本储存或未就绪时返回 None，根路径不存在时返回空字典
        """
        if storage != self._disk_name or not self._clouddrive_api:
            return None
        files_info: Dict[str, Dict] = {}

        def __snapshot_file(_fileitem: FileItem, current_depth: int = 0) -> None:
            try:
                if _fileitem.type == "dir":
                    if current_depth >= max_depth:
                        return
                    if (
                        last_snapshot_time
                        and _fileitem.modify_time
                        and _fileitem.modify_time <= last_snapshot_time
                    ):
                        return
                    sub_files = self._clouddrive_api.list(  # type: ignore[union-attr]
                        _fileitem
                    )
                    for sub_file in sub_files:
                        __snapshot_file(sub_file, current_depth + 1)
                else:
                    if (getattr(_fileitem, "modify_time", 0) or 0) > (
                        last_snapshot_time or 0
                    ):
                        files_info[_fileitem.path] = {
                            "size": _fileitem.size or 0,
                            "modify_time": getattr(_fileitem, "modify_time", 0),
                            "type": _fileitem.type,
                        }
            except Exception as e:
                logger.debug("Snapshot error for %s: %s", _fileitem.path, e)

        fileitem = self._clouddrive_api.get_item(path)
        if not fileitem:
            return {}
        __snapshot_file(fileitem)
        return files_info

    def storage_usage(self, storage: str) -> Optional[StorageUsage]:
        """
        获取储存空间用量（总空间、已用、可用）

        :param storage (str): 储存名称

        :return StorageUsage: StorageUsage；非本储存或未就绪时返回 None
        """
        if storage != self._disk_name or not self._clouddrive_api:
            return None
        return self._clouddrive_api.usage()

    def support_transtype(self, storage: str) -> Optional[Dict[str, str]]:
        """
        返回该储存支持的整理方式（如移动、复制）及展示名称

        :param storage (str): 储存名称

        :return Dict: 如 {"move": "移动", "copy": "复制"}；非本储存或未就绪时返回 None
        """
        if storage != self._disk_name or not self._clouddrive_api:
            return None
        return self._clouddrive_api.transtype

    def stop_service(self) -> None:
        """
        退出插件
        """
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.debug("【CloudDrive】关闭客户端: %s", e)
            self._client = None
        self._clouddrive_api = None
