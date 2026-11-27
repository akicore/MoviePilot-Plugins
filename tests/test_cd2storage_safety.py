"""CD2 directory and upload regressions; requires grpcio and clouddrive2-client."""

from concurrent.futures import ThreadPoolExecutor
import importlib
import importlib.util
import logging
from pathlib import Path, PurePosixPath
import sys
from tempfile import TemporaryDirectory
from threading import Barrier, Lock
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import grpc

PLUGIN = Path(__file__).resolve().parents[1] / "plugins.v2" / "cd2storage"
PACKAGE = "cd2_safety_test"
package = ModuleType(PACKAGE)
package.__path__ = [str(PLUGIN)]
sys.modules[PACKAGE] = package
local = importlib.import_module(f"{PACKAGE}.local_upload")
FolderManager = importlib.import_module(f"{PACKAGE}.folder_manager").FolderManager
pb = importlib.import_module("clouddrive2_client.proto.clouddrive_pb2")
LOGGER = logging.getLogger("cd2-safety-tests")
LOGGER.disabled = True


def load_api():
    modules = {name: ModuleType(name) for name in (
        "app", "app.core", "app.core.config", "app.log", "app.modules",
        "app.modules.filemanager", "app.modules.filemanager.storages",
        "app.schemas", "app.schemas.exception", "httpx",
    )}
    modules["app.core.config"].global_vars = SimpleNamespace(
        is_transfer_stopped=lambda path: False
    )
    modules["app.core.config"].settings = SimpleNamespace()
    modules["app.log"].logger = LOGGER
    modules["app.modules.filemanager.storages"].transfer_process = lambda path: lambda v: None
    modules["app.schemas"].FileItem = SimpleNamespace
    modules["app.schemas"].StorageUsage = SimpleNamespace
    modules["app.schemas.exception"].StorageQueryError = RuntimeError
    modules["httpx"].RequestError = RuntimeError
    modules["httpx"].stream = Mock()
    name = f"{PACKAGE}.clouddrive_api"
    spec = importlib.util.spec_from_file_location(name, PLUGIN / "clouddrive_api.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
        balance = importlib.import_module(f"{PACKAGE}.upload_balancer")
    return module, balance


api_module, balance_module = load_api()


def directory(path, id=None):
    return pb.CloudDriveFile(
        id=id or path, name=PurePosixPath(path).name,
        fullPathName=path, isDirectory=True,
    )


def cloud_file(path="/cloud/Series/E01.mkv", id="remote-id", size=100):
    return pb.CloudDriveFile(
        id=id, name=PurePosixPath(path).name, fullPathName=path,
        isCloudFile=True, size=size,
    )


def upload(path="/cloud/Series/E01.mkv", status=pb.UploadFileInfo.Finish, key="u"):
    return pb.UploadFileInfo(
        key=key, destPath=path, size=100, transferedBytes=100,
        statusEnum=status,
    )


class DirectoryClient:
    def __init__(self, lag=False):
        self.guard = Lock()
        self.files = {"/cloud": directory("/cloud")}
        self.creates = []
        self.lag = lag

    def get_sub_files(self, path, force_refresh=False):
        with self.guard:
            return [f for p, f in self.files.items()
                    if PurePosixPath(p).parent.as_posix() == path
                    and (not self.lag or p == "/cloud")]

    def create_folder(self, parent, name):
        with self.guard:
            path = (PurePosixPath(parent) / name).as_posix()
            self.creates.append(path)
            actual = path if path not in self.files else path + "(1)"
            self.files[actual] = directory(actual)
            return pb.CreateFolderResult(
                result=pb.FileOperationResult(success=True),
                folderCreated=self.files[actual],
            )


class AliasDirectoryClient:
    """Two CD2 mount paths backed by one provider directory tree."""
    def __init__(self, lag=False):
        self.guard = Lock()
        self.prefixes = ("/abc1", "/abc2")
        self.hidden_prefixes = set()
        self.files = {}
        self.creates = []
        self.lag = lag

    def split(self, path):
        for prefix in self.prefixes:
            if path == prefix or path.startswith(prefix + "/"):
                return prefix, path[len(prefix):]
        raise OSError(f"unmapped directory: {path}")

    def get_sub_files(self, path, force_refresh=False):
        with self.guard:
            if path == "/":
                return [directory(prefix, "mount:" + prefix) for prefix in self.prefixes]
            prefix, suffix = self.split(path)
            if suffix and suffix not in self.files:
                raise OSError(f"missing parent: {path}")
            return [directory(prefix + child, id)
                    for child, id in self.files.items()
                    if PurePosixPath(child).parent.as_posix() == (suffix or "/")
                    and not self.lag and prefix not in self.hidden_prefixes]

    def create_folder(self, parent, name):
        with self.guard:
            prefix, suffix = self.split(parent)
            child = suffix + "/" + name
            self.creates.append(prefix + child)
            actual = child if child not in self.files else child + "(1)"
            self.files[actual] = "remote:" + actual
            return pb.CreateFolderResult(
                result=pb.FileOperationResult(success=True),
                folderCreated=directory(prefix + actual, self.files[actual]),
            )


class MissingDirectory(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.NOT_FOUND


class IndexedAliasDirectoryClient(AliasDirectoryClient):
    """CD2 must refresh each ancestor before a peer's deep path resolves."""
    def __init__(self):
        super().__init__()
        self.indexed = set(self.prefixes) | {"/"}
        self.listings = []
        self.hidden_counts = {}

    def get_sub_files(self, path, force_refresh=False):
        with self.guard:
            self.listings.append((path, force_refresh))
            if path not in self.indexed:
                raise MissingDirectory(f"parent not indexed: {path}")
        entries = super().get_sub_files(path, force_refresh=force_refresh)
        with self.guard:
            remaining = self.hidden_counts.get(path, 0)
            if remaining:
                self.hidden_counts[path] -= 1
                return []
            if force_refresh:
                self.indexed.update(file.fullPathName for file in entries)
        return entries

    def create_folder(self, parent, name):
        result = super().create_folder(parent, name)
        with self.guard:
            self.indexed.add(result.folderCreated.fullPathName)
        return result


class DirectoryTests(unittest.TestCase):
    def test_twelve_parallel_episodes_create_each_component_once(self):
        client = DirectoryClient()
        api = api_module.CloudDriveApi(client, "test")
        start = Barrier(12)

        def get(_):
            start.wait(timeout=3)
            # Mix the direct MP callback with the recursive upload entry point.
            if _ % 2:
                parent = api.create_folder(SimpleNamespace(path="/cloud", fileid="/cloud"), "Series")
                return api.create_folder(parent, "Season 1")
            return api.get_folder(Path("/cloud/Series/Season 1"))

        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(get, range(12)))
        self.assertTrue(all(item.path == "/cloud/Series/Season 1/" for item in results))
        self.assertEqual(len({item.fileid for item in results}), 1)
        self.assertCountEqual(client.creates, ["/cloud/Series", "/cloud/Series/Season 1"])
        self.assertEqual(api._folders._locks, {})

    def test_refresh_lag_does_not_create_a_second_directory(self):
        client = DirectoryClient(lag=True)
        manager = FolderManager(client, LOGGER)
        start = Barrier(12)

        def get(_):
            start.wait(timeout=3)
            return manager.ensure("/cloud/", "Series")

        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(get, range(12)))
        self.assertEqual(client.creates, ["/cloud/Series"])
        self.assertEqual({f.id for f in results}, {"/cloud/Series"})

    def test_different_paths_can_create_in_parallel(self):
        barrier = Barrier(2)
        client = DirectoryClient()
        original = client.create_folder

        def create(parent, name):
            barrier.wait(timeout=3)
            return original(parent, name)

        client.create_folder = create
        manager = FolderManager(client, LOGGER)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda name: manager.ensure("/cloud", name), ["A", "B"]))
        self.assertEqual({f.name for f in results}, {"A", "B"})

    def test_failed_listing_does_not_create(self):
        client = DirectoryClient()
        client.get_sub_files = Mock(side_effect=OSError("unavailable"))
        manager = FolderManager(client, LOGGER)
        with self.assertRaises(OSError):
            manager.ensure("/cloud", "Series")
        self.assertEqual(client.creates, [])
        api = api_module.CloudDriveApi(client, "test")
        self.assertIsNone(api.get_folder(Path("/cloud/Series")))
        self.assertEqual(client.creates, [])

    def test_partially_failed_listing_does_not_create_or_reuse(self):
        client = DirectoryClient()

        def listing(*args, **kwargs):
            yield directory("/cloud/Series")
            raise OSError("stream failed")

        client.get_sub_files = listing
        with self.assertRaises(OSError):
            FolderManager(client, LOGGER).ensure("/cloud", "Series")
        self.assertEqual(client.creates, [])

    def test_renamed_or_wrong_directory_is_rejected(self):
        for bad in (directory("/cloud/Series(1)"), directory("/other/Series"),
                    cloud_file("/cloud/Series")):
            with self.subTest(actual=bad.fullPathName):
                client = SimpleNamespace(
                    get_sub_files=Mock(return_value=[]),
                    create_folder=Mock(return_value=pb.CreateFolderResult(
                        result=pb.FileOperationResult(success=True), folderCreated=bad,
                    )),
                )
                manager = FolderManager(client, LOGGER)
                with self.assertRaises((OSError, ValueError)):
                    manager.ensure("/cloud", "Series")
                with self.assertRaises(OSError):
                    manager.ensure("/cloud", "Series")
                client.create_folder.assert_called_once()

    def test_duplicate_existing_names_are_rejected(self):
        client = SimpleNamespace(
            get_sub_files=Mock(return_value=[directory("/cloud/Series", "a"), directory("/cloud/Series", "b")]),
            create_folder=Mock(),
        )
        with self.assertRaises(OSError):
            FolderManager(client, LOGGER).ensure("/cloud", "Series")
        client.create_folder.assert_not_called()

    def test_unknown_create_outcome_is_not_immediately_retried(self):
        client = SimpleNamespace(
            get_sub_files=Mock(return_value=[]),
            create_folder=Mock(side_effect=TimeoutError("unknown result")),
        )
        manager = FolderManager(client, LOGGER)
        with self.assertRaises(TimeoutError):
            manager.ensure("/cloud", "Series")
        with self.assertRaises(OSError):
            manager.ensure("/cloud", "Series")
        client.create_folder.assert_called_once()
        client.get_sub_files.return_value = [directory("/cloud/Series")]
        self.assertEqual(manager.ensure("/cloud", "Series").id, "/cloud/Series")
        client.create_folder.assert_called_once()


class SharedDirectoryTests(unittest.TestCase):
    def manager(self, client):
        return FolderManager(client, LOGGER, shared_groups=[("/abc1", "/abc2")])

    def test_parallel_first_creation_through_both_aliases(self):
        for fixture in (AliasDirectoryClient, IndexedAliasDirectoryClient):
            with self.subTest(fixture=fixture.__name__):
                client = fixture()
                manager = self.manager(client)
                ready = Barrier(12)

                def get(i):
                    ready.wait(timeout=3)
                    prefix = "/abc1" if i % 2 else "/abc2"
                    return manager.ensure(prefix, "Series")

                with ThreadPoolExecutor(max_workers=12) as pool:
                    results = list(pool.map(get, range(12)))
                self.assertEqual(len(client.creates), 1)
                self.assertEqual({file.id for file in results}, {"remote:/Series"})
                self.assertEqual({file.fullPathName for file in results},
                                 {"/abc1/Series", "/abc2/Series"})
                self.assertEqual(manager._locks, {})

    def test_peer_listing_prevents_duplicate_after_cache_expiry_or_restart(self):
        for reset in (False, True):
            with self.subTest(reset=reset):
                client = IndexedAliasDirectoryClient()
                manager = self.manager(client)
                manager.ensure("/abc1", "Series")
                if reset:
                    manager = self.manager(client)
                else:
                    manager._recent.clear()
                client.hidden_counts["/abc2"] = 1
                result = manager.ensure("/abc2", "Series")
                self.assertEqual(result.id, "remote:/Series")
                self.assertEqual(result.fullPathName, "/abc2/Series")
                self.assertEqual(client.creates, ["/abc1/Series"])

    def test_refreshed_alias_does_not_change_original_proto(self):
        client = IndexedAliasDirectoryClient()
        manager = self.manager(client)
        first = manager.ensure("/abc1", "Series")
        second = manager.ensure("/abc2", "Series")
        self.assertEqual(first.fullPathName, "/abc1/Series")
        self.assertEqual(second.fullPathName, "/abc2/Series")
        self.assertIsNot(first, second)

    def test_peer_ancestors_are_refreshed_before_listing_new_show(self):
        client = IndexedAliasDirectoryClient()
        manager = self.manager(client)
        manager.ensure("/abc1", "Cartoons")
        # The category is already indexed through both entries, as in the log.
        client.get_sub_files("/abc2", force_refresh=True)
        show = manager.ensure("/abc1/Cartoons", "Series")
        self.assertNotIn("/abc2/Cartoons/Series", client.indexed)
        before = len(client.listings)
        season = manager.ensure(show.fullPathName, "Season 1")
        self.assertEqual(season.id, "remote:/Cartoons/Series/Season 1")
        self.assertEqual(client.listings[before:], [
            ("/abc1/Cartoons/Series", True),
            ("/abc2", True), ("/abc2/Cartoons", True),
            ("/abc2/Cartoons/Series", True),
        ])
        self.assertEqual(client.creates, [
            "/abc1/Cartoons", "/abc1/Cartoons/Series",
            "/abc1/Cartoons/Series/Season 1",
        ])

    def test_deep_unindexed_target_is_refreshed_without_creating_duplicates(self):
        client = IndexedAliasDirectoryClient()
        manager = self.manager(client)
        path = "/abc1"
        for name in ("TV", "Year", "Series", "Season 1"):
            path = client.create_folder(path, name).folderCreated.fullPathName
        original_creates = list(client.creates)
        client.indexed = {p for p in client.indexed
                          if not p.startswith("/abc2/")}
        result = manager.ensure("/abc2/TV/Year/Series", "Season 1")
        self.assertEqual(result.fullPathName, "/abc2/TV/Year/Series/Season 1")
        self.assertEqual(result.id, "remote:/TV/Year/Series/Season 1")
        self.assertIn(result.fullPathName, client.indexed)
        self.assertEqual(client.creates, original_creates)

    def test_optional_missing_peer_parent_is_not_listed(self):
        client = IndexedAliasDirectoryClient()
        manager = self.manager(client)
        manager.ensure("/abc1", "Series")
        client.hidden_prefixes.add("/abc2")
        before = len(client.listings)
        result = manager.ensure("/abc1/Series", "Season 1")
        self.assertEqual(result.id, "remote:/Series/Season 1")
        self.assertEqual(client.listings[before:], [
            ("/abc1/Series", True), ("/abc2", True),
        ])
        self.assertEqual(len(client.creates), 2)

    def test_invisible_alias_is_not_fabricated_or_created_again(self):
        client = IndexedAliasDirectoryClient()
        manager = self.manager(client)
        first = manager.ensure("/abc1", "Series")
        client.hidden_prefixes.add("/abc2")
        module = sys.modules[f"{PACKAGE}.folder_manager"]
        with patch.object(module, "sleep") as pause:
            with self.assertRaises(OSError):
                manager.ensure("/abc2", "Series")
        self.assertEqual(pause.call_count, manager.VISIBILITY_ATTEMPTS - 1)
        self.assertEqual(client.creates, ["/abc1/Series"])
        self.assertNotIn("/abc2/Series", client.indexed)
        self.assertEqual(first.fullPathName, "/abc1/Series")
        self.assertEqual(manager._locks, {})
        client.hidden_prefixes.clear()
        self.assertEqual(manager.ensure("/abc2", "Series").id, first.id)
        self.assertEqual(client.creates, ["/abc1/Series"])

    def test_temporarily_invisible_alias_is_retried_with_force_refresh(self):
        client = IndexedAliasDirectoryClient()
        manager = self.manager(client)
        first = manager.ensure("/abc1", "Series")
        client.hidden_counts["/abc2"] = 2
        module = sys.modules[f"{PACKAGE}.folder_manager"]
        with patch.object(module, "sleep") as pause:
            result = manager.ensure("/abc2", "Series")
        pause.assert_called_once_with(manager.VISIBILITY_RETRY_SECONDS)
        self.assertEqual(result.id, first.id)
        self.assertIn(result.fullPathName, client.indexed)
        self.assertEqual(client.creates, ["/abc1/Series"])
        self.assertTrue(all(refresh for _, refresh in client.listings))

    def test_alias_identity_must_match_after_refresh(self):
        client = IndexedAliasDirectoryClient()
        manager = self.manager(client)
        manager.ensure("/abc1", "Series")
        original = client.get_sub_files
        client.hidden_counts["/abc2"] = 1

        def listing(path, force_refresh=False):
            entries = original(path, force_refresh=force_refresh)
            if path == "/abc2" and entries:
                return [directory("/abc2/Series", "unexpected-provider-id")]
            return entries

        client.get_sub_files = listing
        with self.assertRaises(OSError):
            manager.ensure("/abc2", "Series")
        self.assertEqual(client.creates, ["/abc1/Series"])

    def test_unavailable_target_parent_never_creates_children(self):
        client = IndexedAliasDirectoryClient()
        manager = self.manager(client)
        manager.ensure("/abc1", "Series")
        client.hidden_prefixes.add("/abc2")
        with self.assertRaises(OSError):
            manager.ensure("/abc2/Series", "Season 1")
        self.assertEqual(client.creates, ["/abc1/Series"])

    def test_not_found_mid_stream_does_not_retry_or_create(self):
        client = IndexedAliasDirectoryClient()

        def listing(*args, **kwargs):
            yield directory("/abc1/Series", "remote:/Series")
            raise MissingDirectory("stream failed")

        client.get_sub_files = Mock(side_effect=listing)
        with self.assertRaises(OSError):
            self.manager(client).ensure("/abc1", "Series")
        client.get_sub_files.assert_called_once_with("/abc1", force_refresh=True)
        self.assertEqual(client.creates, [])

    def test_permission_failure_does_not_trigger_parent_recovery(self):
        error = grpc.RpcError("permission denied")
        error.code = lambda: grpc.StatusCode.PERMISSION_DENIED
        client = IndexedAliasDirectoryClient()
        client.get_sub_files = Mock(side_effect=error)
        with self.assertRaises(grpc.RpcError):
            self.manager(client).ensure("/abc2/Series", "Season 1")
        client.get_sub_files.assert_called_once()
        self.assertEqual(client.creates, [])

    def test_unknown_create_outcome_blocks_the_other_alias(self):
        client = AliasDirectoryClient()
        client.create_folder = Mock(side_effect=TimeoutError("unknown"))
        manager = self.manager(client)
        with self.assertRaises(TimeoutError):
            manager.ensure("/abc1", "Series")
        with self.assertRaises(OSError):
            manager.ensure("/abc2", "Series")
        client.create_folder.assert_called_once()
        client.files["/Series"] = "remote:/Series"
        self.assertEqual(manager.ensure("/abc2", "Series").id, "remote:/Series")

    def test_slow_failed_create_renews_uncertainty_for_both_aliases(self):
        client = AliasDirectoryClient()
        manager = self.manager(client)
        clock = [0]

        def timeout(*args):
            clock[0] = 90
            raise TimeoutError("slow unknown outcome")

        client.create_folder = Mock(side_effect=timeout)
        module = sys.modules[f"{PACKAGE}.folder_manager"]
        with patch.object(module, "monotonic", lambda: clock[0]):
            with self.assertRaises(TimeoutError):
                manager.ensure("/abc1", "Series")
            with self.assertRaises(OSError):
                manager.ensure("/abc2", "Series")
        client.create_folder.assert_called_once()

    def test_alias_query_failure_never_creates_or_uses_cached_result(self):
        client = AliasDirectoryClient()
        manager = self.manager(client)
        manager.ensure("/abc1", "Series")
        client.get_sub_files = Mock(side_effect=OSError("unavailable"))
        with self.assertRaises(OSError):
            manager.ensure("/abc2", "Series")
        self.assertEqual(client.creates, ["/abc1/Series"])

    def test_aliases_with_different_existing_ids_are_rejected(self):
        client = AliasDirectoryClient()
        manager = self.manager(client)
        manager.ensure("/abc1", "Series")
        client.files["/Series"] = "different-folder"
        with self.assertRaises(OSError):
            manager.ensure("/abc2", "Series")
        self.assertEqual(client.creates, ["/abc1/Series"])

    def test_group_and_prefix_boundaries_do_not_collide(self):
        manager = FolderManager(None, LOGGER, shared_groups=[("/abc1", "/abc2"),
                                                            ("/other1", "/other2")])
        self.assertEqual(manager._key("/abc1/Series"), manager._key("/abc2/Series"))
        self.assertNotEqual(manager._key("/abc1/Series"), manager._key("/other1/Series"))
        self.assertNotEqual(manager._key("/abc1/Series"), manager._key("/abc12/Series"))
        self.assertEqual(manager._key("/abc2"), "/abc2")

    def test_shared_rule_configuration_and_validation(self):
        module = balance_module
        rule = {"prefix": "/abc", "shared_directories": True, "branches": [
            {"path": "/abc1", "capacity_gb": 20},
            {"path": "/abc2", "capacity_gb": 30},
        ]}
        parsed = module.parse_rules([rule])
        self.assertEqual(module.shared_directory_groups(parsed), [("/abc", "/abc1", "/abc2")])
        for invalid in ("true", 1, None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                module.parse_rules([{**rule, "shared_directories": invalid}])
        for invalid in ("/", "/abc1/sub"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                module.parse_rules([{**rule, "prefix": invalid}])
        independent = {**rule, "shared_directories": False}
        self.assertEqual(module.shared_directory_groups(module.parse_rules([independent])), [])


class BalancedUploadTests(unittest.TestCase):
    class SourceFile:
        # Model the Linux runtime path while stat() uses a real local fixture.
        def __init__(self, file):
            self.name = file.name
            self.stat = file.stat

        def __str__(self):
            return f"/PTTemp/{self.name}"

    def make_api(self, mode="direct_write", shared=False, indexed=False):
        balancer = api_module.UploadBalancer([
            {"prefix": "/abc1", "shared_directories": shared, "branches": [
                {"path": "/abc1", "capacity_bytes": 700},
                {"path": "/abc2", "capacity_bytes": 800},
            ]},
        ], [], lambda records: None)
        if shared:
            client = IndexedAliasDirectoryClient() if indexed else AliasDirectoryClient()
        else:
            client = DirectoryClient()
        if not shared:
            client.files = {path: directory(path) for path in ("/abc1", "/abc2")}
        api = api_module.CloudDriveApi(client, "test", upload_mode=mode,
                                       upload_balancer=balancer)
        return api, client, balancer

    def test_fifteen_parallel_episodes_share_input_but_not_routed_targets(self):
        for mode in ("direct_write", "remote_upload", "local_upload", "local_fallback"):
            with self.subTest(mode=mode), TemporaryDirectory() as temp:
                api, client, balancer = self.make_api(
                    "local_upload" if mode == "local_fallback" else mode
                )
                original = "/abc1/Series/Season 1/"
                shared = api.get_folder(Path(original))
                if mode == "local_upload":
                    api._local_upload_mappings = [local.LocalPathMapping("/", "/mapped")]
                files = []
                for i in range(15):
                    file = Path(temp) / f"E{i:02}.mkv"
                    file.write_bytes(b"x" * 100)
                    files.append(self.SourceFile(file))
                ready = Barrier(15)
                uploading = Barrier(15)
                seen = []
                guard = Lock()

                def transfer(target, file, *args):
                    # Keep every upload alive until all reservations are made.
                    uploading.wait(timeout=5)
                    with guard:
                        seen.append((target, target.path, target.fileid))
                    return SimpleNamespace(path=target.path + file.name)

                api._upload_direct_write = transfer
                api._upload_remote_upload = transfer
                api._upload_local_source = transfer

                def upload(file):
                    ready.wait(timeout=5)
                    return api.upload(shared, file)

                with ThreadPoolExecutor(max_workers=15) as pool:
                    results = list(pool.map(upload, files))
                paths = [path for _, path, _ in seen]
                self.assertEqual(paths.count(original), 7)
                self.assertEqual(paths.count("/abc2/Series/Season 1/"), 8)
                self.assertEqual(shared.path, original)
                for target, path, fileid in seen:
                    self.assertEqual(fileid, path.rstrip("/"))
                    if path.startswith("/abc2/"):
                        self.assertIsNot(target, shared)
                self.assertTrue(all(result is not None for result in results))
                self.assertCountEqual(client.creates, [
                    "/abc1/Series", "/abc1/Series/Season 1",
                    "/abc2/Series", "/abc2/Series/Season 1",
                ])
                self.assertEqual(balancer._reservations, {})
                self.assertEqual(sum(r["size"] for r in balancer._records
                                     if r["folder"] == "/abc1"), 700)
                self.assertEqual(sum(r["size"] for r in balancer._records
                                     if r["folder"] == "/abc2"), 800)

    def test_shared_branches_create_each_real_directory_once_in_all_modes(self):
        for indexed in (False, True):
            for mode in ("direct_write", "remote_upload", "local_upload", "local_fallback"):
                with self.subTest(indexed=indexed, mode=mode), TemporaryDirectory() as temp:
                    api, client, balancer = self.make_api(
                        "local_upload" if mode == "local_fallback" else mode,
                        shared=True, indexed=indexed,
                    )
                    shared = api.get_folder(Path("/abc1/Series/Season 1/"))
                    if mode == "local_upload":
                        api._local_upload_mappings = [local.LocalPathMapping("/", "/mapped")]
                    ready = Barrier(15)
                    seen = []
                    guard = Lock()

                    def transfer(target, file, *args):
                        ready.wait(timeout=5)
                        with guard:
                            seen.append((target.path, target.fileid))
                        return SimpleNamespace(path=target.path + file.name)

                    api._upload_direct_write = transfer
                    api._upload_remote_upload = transfer
                    api._upload_local_source = transfer
                    files = []
                    for i in range(15):
                        file = Path(temp) / f"E{i:02}.mkv"
                        file.write_bytes(b"x" * 100)
                        files.append(self.SourceFile(file))
                    with ThreadPoolExecutor(max_workers=15) as pool:
                        results = list(pool.map(lambda file: api.upload(shared, file), files))
                    self.assertTrue(all(result is not None for result in results))
                    self.assertEqual([p for p, _ in seen].count("/abc1/Series/Season 1/"), 7)
                    self.assertEqual([p for p, _ in seen].count("/abc2/Series/Season 1/"), 8)
                    self.assertEqual({id for _, id in seen}, {"remote:/Series/Season 1"})
                    self.assertEqual(client.creates, ["/abc1/Series", "/abc1/Series/Season 1"])
                    self.assertEqual(set(client.files), {"/Series", "/Series/Season 1"})
                    self.assertEqual(shared.path, "/abc1/Series/Season 1/")
                    self.assertEqual(balancer._reservations, {})
                    self.assertEqual(sum(r["size"] for r in balancer._records
                                         if r["folder"] == "/abc1"), 700)
                    self.assertEqual(sum(r["size"] for r in balancer._records
                                         if r["folder"] == "/abc2"), 800)

    def test_invisible_routed_alias_releases_quota_without_uploading(self):
        api, client, balancer = self.make_api(shared=True, indexed=True)
        _, first = balancer.reserve("/abc1/Series/Season 1/", 700)
        balancer.finish(first, success=True)
        shared = api.get_folder(Path("/abc1/Series/Season 1/"))
        client.hidden_prefixes.add("/abc2")
        api._upload_direct_write = Mock()
        module = sys.modules[f"{PACKAGE}.folder_manager"]
        with TemporaryDirectory() as temp, patch.object(module, "sleep"):
            file = Path(temp) / "E08.mkv"
            file.write_bytes(b"x" * 100)
            self.assertIsNone(api.upload(shared, file))
        api._upload_direct_write.assert_not_called()
        self.assertEqual(client.creates, ["/abc1/Series", "/abc1/Series/Season 1"])
        self.assertEqual(shared.path, "/abc1/Series/Season 1/")
        self.assertEqual(balancer._reservations, {})
        self.assertEqual(len(balancer._records), 1)

    def test_routed_directory_failure_releases_quota_and_does_not_upload(self):
        api, client, balancer = self.make_api()
        _, first = balancer.reserve("/abc1/Series/Season 1/", 700)
        balancer.finish(first, success=True)
        shared = api.get_folder(Path("/abc1/Series/Season 1/"))
        api.get_folder = Mock(return_value=None)
        api._upload_direct_write = Mock()
        with TemporaryDirectory() as temp:
            file = Path(temp) / "E08.mkv"
            file.write_bytes(b"x" * 100)
            self.assertIsNone(api.upload(shared, file))
        api.get_folder.assert_called_once_with(Path("/abc2/Series/Season 1/"))
        api._upload_direct_write.assert_not_called()
        self.assertEqual(shared.path, "/abc1/Series/Season 1/")
        self.assertEqual(balancer._reservations, {})
        self.assertEqual(len(balancer._records), 1)


class LocalUploadTests(unittest.TestCase):
    def make_worker(self, rpc, **kwargs):
        return local.LocalUpload(rpc, LOGGER, lambda: None, lambda v: None,
                                 timeout=0.03, interval=0, **kwargs)

    def test_upload_list_matches_full_file_across_pages(self):
        rpc = local.LocalUploadRpc(None)
        first = [upload(f"/other/{i}.mkv", key=str(i)) for i in range(100)]
        second = [upload(), upload("/cloud/Series/E02.mkv"),
                  upload("/cloud/Series"), upload("/cloud/Series-old/E01.mkv")]
        rpc.call = Mock(side_effect=[
            pb.GetUploadFileListResult(uploadFiles=first, totalCount=104),
            pb.GetUploadFileListResult(uploadFiles=second, totalCount=104),
        ])
        self.assertEqual([item.destPath for item in rpc.uploads("/cloud/Series/E01.mkv")],
                         ["/cloud/Series/E01.mkv"])
        self.assertEqual([call.args[1].pageNumber for call in rpc.call.call_args_list], [0, 1])
        self.assertTrue(all(not call.args[1].filter for call in rpc.call.call_args_list))

    def test_another_episode_error_does_not_affect_this_file(self):
        rpc = local.LocalUploadRpc(None)
        rpc.call = Mock(return_value=pb.GetUploadFileListResult(
            uploadFiles=[upload(), upload("/cloud/Series/E02.mkv", pb.UploadFileInfo.FatalError)],
            totalCount=2,
        ))
        rpc.copy_tasks = Mock(return_value=[])
        rpc.cloud_file = Mock(return_value=cloud_file())
        self.make_worker(rpc)._wait("/local/E01.mkv", "/cloud/Series", "/cloud/Series/E01.mkv", 100)
        rpc.cloud_file.assert_called_once()

    def test_current_episode_error_is_detected(self):
        rpc = local.LocalUploadRpc(None)
        rpc.call = Mock(return_value=pb.GetUploadFileListResult(
            uploadFiles=[upload(status=pb.UploadFileInfo.FatalError)], totalCount=1,
        ))
        rpc.copy_tasks = Mock(return_value=[])
        rpc.cloud_file = Mock()
        with self.assertRaises(OSError):
            self.make_worker(rpc)._wait("/local/E01.mkv", "/cloud/Series", "/cloud/Series/E01.mkv", 100)
        rpc.cloud_file.assert_not_called()

    def test_reused_upload_key_tracks_active_then_finished(self):
        done, active = upload(), upload(status=pb.UploadFileInfo.Transfer)
        file = cloud_file()
        rpc = SimpleNamespace(
            uploads=Mock(side_effect=[[active], [done]]),
            copy_tasks=Mock(return_value=[]), cloud_file=Mock(return_value=file),
        )
        worker = self.make_worker(rpc)
        worker._wait("/local/E01.mkv", "/cloud/Series", file.fullPathName, 100,
                     baseline_uploads={done.key: worker._upload_state(done)}, baseline_file=file)
        self.assertEqual(rpc.uploads.call_count, 2)
        rpc.cloud_file.assert_called_once()

    def test_old_finished_record_and_unchanged_file_do_not_finish(self):
        done, file = upload(), cloud_file()
        rpc = SimpleNamespace(uploads=Mock(return_value=[done]),
                              copy_tasks=Mock(return_value=[]), cloud_file=Mock(return_value=file))
        worker = self.make_worker(rpc)
        with self.assertRaises(TimeoutError):
            worker._wait("/local/E01.mkv", "/cloud/Series", file.fullPathName, 100,
                         baseline_uploads={done.key: worker._upload_state(done)}, baseline_file=file)

    def test_old_completed_copy_and_unchanged_file_do_not_finish(self):
        file = cloud_file()
        task = pb.CopyTask(sourcePath="/local/E01.mkv", destPath="/cloud/Series",
                           status=pb.CopyTask.Completed, uploadedFiles=1)
        rpc = SimpleNamespace(uploads=Mock(return_value=[]),
                              copy_tasks=Mock(return_value=[task]), cloud_file=Mock(return_value=file))
        with self.assertRaises(TimeoutError):
            self.make_worker(rpc)._wait(task.sourcePath, task.destPath, file.fullPathName, 100,
                                       baseline_copies={task.SerializeToString()}, baseline_file=file)

    def test_disappeared_fast_upload_needs_two_cloud_confirmations(self):
        file = cloud_file()
        rpc = SimpleNamespace(uploads=Mock(return_value=[]),
                              copy_tasks=Mock(return_value=[]), cloud_file=Mock(return_value=file))
        self.make_worker(rpc)._wait("/local/E01.mkv", "/cloud/Series", file.fullPathName, 100)
        self.assertEqual(rpc.cloud_file.call_count, 2)

    def test_cloud_file_rejects_redirect_and_duplicate_names(self):
        rpc = local.LocalUploadRpc(None)
        for files in ([cloud_file(), cloud_file(id="duplicate")],
                      [pb.CloudDriveFile(id="x", name="E01.mkv", fullPathName="/cloud/Series(1)/E01.mkv")]):
            with self.subTest(files=files):
                rpc.list_files = Mock(return_value=files)
                with self.assertRaises(OSError):
                    rpc.cloud_file("/cloud/Series/E01.mkv", 100)

    def test_direct_target_copy_waits_for_its_file(self):
        file = cloud_file()
        rpc = SimpleNamespace(
            uploads=Mock(side_effect=[[], [upload(status=pb.UploadFileInfo.Transfer)], [upload()]]),
            copy_tasks=Mock(return_value=[]), cloud_file=Mock(side_effect=[None, file]),
            copy=Mock(return_value=pb.FileOperationResult(success=True, resultFilePaths=[file.fullPathName])),
            cancel=Mock(), rename=Mock(),
        )
        self.assertEqual(self.make_worker(rpc).run("/local/E01.mkv", "/cloud/Series", "E01.mkv", 100),
                         file.fullPathName)
        rpc.copy.assert_called_once_with("/local/E01.mkv", "/cloud/Series")
        self.assertTrue(all(call.args == (file.fullPathName,) for call in rpc.uploads.call_args_list))
        rpc.rename.assert_not_called()
        rpc.cancel.assert_not_called()

    def test_copy_keeps_overwrite_policy(self):
        rpc = local.LocalUploadRpc(None)
        rpc.call = Mock(return_value=pb.FileOperationResult(success=True))
        rpc.copy("/local/E01.mkv", "/cloud/Series")
        request = rpc.call.call_args.args[1]
        self.assertEqual(request.destPath, "/cloud/Series")
        self.assertEqual(request.conflictPolicy, pb.CopyFileRequest.Overwrite)


if __name__ == "__main__":
    unittest.main()
