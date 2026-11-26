"""CD2 directory and upload regressions; requires grpcio and clouddrive2-client."""

from concurrent.futures import ThreadPoolExecutor
import importlib
import importlib.util
import logging
from pathlib import Path, PurePosixPath
import sys
from threading import Barrier, Lock
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

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
    return module


api_module = load_api()


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
