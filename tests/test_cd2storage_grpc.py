"""Optional loopback tests using the plugin's real gRPC/CD2 dependencies."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Empty, Queue
from threading import Condition, Event
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch

from test_cd2storage_remote_upload import api_module

try:
    import grpc
    from clouddrive2_client import CloudDriveClient
    from clouddrive2_client.proto import clouddrive_pb2 as pb
    from clouddrive2_client.proto import clouddrive_pb2_grpc as pb_grpc
except ImportError:
    grpc = None


if grpc is not None:

    class UploadServer(pb_grpc.CloudDriveFileSrvServicer):
        def __init__(self, source):
            self.source = source
            self.messages = Queue()
            self.release = Event()
            self.condition = Condition()
            self.requests = []
            self.acked = 0
            self.cancelled_rpcs = 0
            self.started = 0
            self.total_requests = 4

        def RemoteUploadChannel(self, request, context):
            context.send_initial_metadata(())
            while context.is_active():
                try:
                    yield self.messages.get(timeout=0.05)
                except Empty:
                    pass

        def StartRemoteUpload(self, request, context):
            self.started += 1
            for offset in (8192, 0, 12288, 4096)[: self.total_requests]:
                self.messages.put(
                    pb.RemoteUploadChannelReply(
                        upload_id="real-upload",
                        read_data=pb.RemoteReadDataRequest(offset=offset, length=4096),
                    )
                )
            return pb.RemoteUploadStarted(upload_id="real-upload")

        def RemoteReadData(self, request, context):
            with self.condition:
                self.requests.append((request, list(context.invocation_metadata())))
                self.condition.notify_all()
            while not self.release.wait(0.01):
                if not context.is_active():
                    with self.condition:
                        self.cancelled_rpcs += 1
                        self.condition.notify_all()
                    return pb.RemoteReadDataReply()
            expected = self.source[request.offset : request.offset + request.length]
            if request.data != expected:
                return pb.RemoteReadDataReply(
                    success=False, error_message="wrong range"
                )
            with self.condition:
                self.acked += 1
                if self.acked == self.total_requests:
                    self.messages.put(
                        pb.RemoteUploadChannelReply(
                            upload_id="real-upload",
                            status_changed=pb.RemoteUploadStatusChanged(
                                status=int(api_module.UploadStatus.FINISH)
                            ),
                        )
                    )
            return pb.RemoteReadDataReply(
                success=True, bytes_received=len(request.data)
            )

        def wait_requests(self, count):
            with self.condition:
                if not self.condition.wait_for(
                    lambda: len(self.requests) >= count, timeout=5
                ):
                    raise AssertionError(
                        f"Only {len(self.requests)}/{count} real RPCs arrived"
                    )

    class MultiUploadServer(UploadServer):
        def __init__(self, sources):
            super().__init__(next(iter(sources.values())))
            self.sources = sources
            self.uploads = {}
            self.devices = []
            self.channel_cancelled = Event()

        def RemoteUploadChannel(self, request, context):
            with self.condition:
                self.devices.append(request.device_id)
            try:
                yield from super().RemoteUploadChannel(request, context)
            finally:
                self.channel_cancelled.set()

        def StartRemoteUpload(self, request, context):
            uid = "upload:" + request.file_path
            with self.condition:
                self.started += 1
                self.uploads[uid] = {
                    "source": self.sources[request.file_path],
                    "release": Event(),
                    "acked": 0,
                }
            for offset in (8192, 0, 12288, 4096):
                self.messages.put(
                    pb.RemoteUploadChannelReply(
                        upload_id=uid,
                        read_data=pb.RemoteReadDataRequest(offset=offset, length=4096),
                    )
                )
            return pb.RemoteUploadStarted(upload_id=uid)

        def RemoteReadData(self, request, context):
            with self.condition:
                state = self.uploads[request.upload_id]
                self.requests.append((request, list(context.invocation_metadata())))
                self.condition.notify_all()
            while not state["release"].wait(0.01):
                if not context.is_active():
                    with self.condition:
                        self.cancelled_rpcs += 1
                        self.condition.notify_all()
                    return pb.RemoteReadDataReply()
            expected = state["source"][request.offset : request.offset + request.length]
            if request.data != expected:
                return pb.RemoteReadDataReply(success=False, error_message="wrong file")
            with self.condition:
                state["acked"] += 1
                if state["acked"] == 4:
                    self.messages.put(
                        pb.RemoteUploadChannelReply(
                            upload_id=request.upload_id,
                            status_changed=pb.RemoteUploadStatusChanged(
                                status=int(api_module.UploadStatus.FINISH),
                            ),
                        )
                    )
            return pb.RemoteReadDataReply(
                success=True, bytes_received=len(request.data)
            )

        def release_all(self):
            with self.condition:
                for state in self.uploads.values():
                    state["release"].set()


@unittest.skipIf(grpc is None, "Install cd2storage requirements for real gRPC tests")
class RealGrpcTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = bytes(range(251)) * 100
        self.path = Path(self.temp.name) / "source.bin"
        self.path.write_bytes(self.source)
        self.service = UploadServer(self.source)
        self.server = grpc.server(ThreadPoolExecutor(max_workers=8))
        pb_grpc.add_CloudDriveFileSrvServicer_to_server(self.service, self.server)
        port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.addCleanup(lambda: self.server.stop(0).wait(3))
        self.client = CloudDriveClient(f"127.0.0.1:{port}")
        self.client.jwt_token = "real-test-token"
        self.addCleanup(self.client.close)
        self.cancelled = Event()
        self.progress = []
        self.api = api_module.CloudDriveApi(
            self.client, "test", upload_mode="remote_upload"
        )
        self.addCleanup(self.api.close)
        self.api.get_item = Mock(return_value="uploaded")
        self.api._cancel_upload = Mock()
        for name, value in (
            ("clouddrive_pb2", pb),
            ("transfer_process", lambda path: self.progress.append),
            (
                "global_vars",
                SimpleNamespace(
                    is_transfer_stopped=lambda path: self.cancelled.is_set()
                ),
            ),
        ):
            context = patch.object(api_module, name, value)
            context.start()
            self.addCleanup(context.stop)

    def test_real_grpc_allows_four_overlapping_reads_with_auth(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            upload = executor.submit(
                self.api._upload_once, "/target", self.path, len(self.source), {}
            )
            try:
                # No RPC has been acknowledged: four arrivals prove overlap.
                self.service.wait_requests(4)
                self.assertFalse(upload.done())
                for request, metadata in self.service.requests:
                    self.assertIn(("authorization", "Bearer real-test-token"), metadata)
                    self.assertEqual(request.length, 4096)
                    self.assertEqual(
                        request.data,
                        self.source[request.offset : request.offset + 4096],
                    )
            finally:
                self.service.release.set()
            self.assertEqual(upload.result(timeout=5), "uploaded")
        self.assertEqual(self.progress[-1], 100)
        self.api._cancel_upload.assert_not_called()

    def test_real_grpc_cancellation_reaches_all_active_rpcs(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            upload = executor.submit(
                self.api._upload_once, "/target", self.path, len(self.source), {}
            )
            try:
                self.service.wait_requests(4)
                self.cancelled.set()
                self.assertIsNone(upload.result(timeout=5))
                with self.service.condition:
                    self.assertTrue(
                        self.service.condition.wait_for(
                            lambda: self.service.cancelled_rpcs == 4, timeout=3
                        )
                    )
            finally:
                self.service.release.set()
        self.api._cancel_upload.assert_called_once_with("real-upload")
        self.assertNotIn(100, self.progress)

    def test_real_grpc_single_request_upload_completes(self):
        self.service.total_requests = 1
        self.service.release.set()
        self.assertEqual(
            self.api._upload_once("/target", self.path, len(self.source), {}),
            "uploaded",
        )
        self.assertEqual(len(self.service.requests), 1)
        self.assertEqual(self.progress[-1], 100)


@unittest.skipIf(grpc is None, "Install cd2storage requirements for real gRPC tests")
class MultipleFileGrpcTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.sources = {"/a": b"a" * 16384, "/b": b"b" * 16384}
        self.paths = {}
        for name, data in self.sources.items():
            path = Path(self.temp.name) / name.lstrip("/")
            path.write_bytes(data)
            self.paths[name] = path
        self.service = MultiUploadServer(self.sources)
        self.server = grpc.server(ThreadPoolExecutor(max_workers=12))
        pb_grpc.add_CloudDriveFileSrvServicer_to_server(self.service, self.server)
        port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.addCleanup(lambda: self.server.stop(0).wait(3))
        self.client = CloudDriveClient(f"127.0.0.1:{port}")
        self.client.jwt_token = "multi-token"
        self.addCleanup(self.client.close)
        self.api = api_module.CloudDriveApi(
            self.client,
            "test",
            remote_device_id="persisted-test-device",
        )
        self.addCleanup(self.api.close)
        self.api.get_item = Mock(side_effect=lambda path: path.as_posix())
        self.api._cancel_upload = Mock()
        self.cancel_a = Event()
        self.progress = {"/a": [], "/b": []}
        for name, value in (
            ("clouddrive_pb2", pb),
            ("transfer_process", lambda path: self.progress[path].append),
            (
                "global_vars",
                SimpleNamespace(
                    is_transfer_stopped=lambda path: path == "/a"
                    and self.cancel_a.is_set(),
                ),
            ),
        ):
            context = patch.object(api_module, name, value)
            context.start()
            self.addCleanup(context.stop)

    def submit(self, executor, name):
        return executor.submit(
            self.api._upload_once,
            name,
            self.paths[name],
            len(self.sources[name]),
            {},
        )

    def test_two_files_transfer_concurrently_and_first_finish_keeps_channel_open(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            first, second = self.submit(executor, "/a"), self.submit(executor, "/b")
            try:
                self.service.wait_requests(8)
                self.assertEqual(self.service.devices, ["persisted-test-device"])
                self.assertEqual(
                    {request.upload_id for request, _ in self.service.requests},
                    {"upload:/a", "upload:/b"},
                )
                self.service.uploads["upload:/a"]["release"].set()
                self.assertEqual(first.result(timeout=5), "/a")
                self.assertFalse(second.done())
                self.assertFalse(self.service.channel_cancelled.is_set())
                self.service.uploads["upload:/b"]["release"].set()
                self.assertEqual(second.result(timeout=5), "/b")
            finally:
                self.service.release_all()
        self.assertEqual(self.service.started, 2)  # No hidden restart/retry.
        self.assertEqual(self.progress["/a"][-1], 100)
        self.assertEqual(self.progress["/b"][-1], 100)
        self.api._cancel_upload.assert_not_called()

    def test_cancelling_one_file_does_not_cancel_other_files_channel_or_rpcs(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            first, second = self.submit(executor, "/a"), self.submit(executor, "/b")
            try:
                self.service.wait_requests(8)
                self.cancel_a.set()
                self.assertIsNone(first.result(timeout=5))
                with self.service.condition:
                    self.assertTrue(
                        self.service.condition.wait_for(
                            lambda: self.service.cancelled_rpcs == 4,
                            timeout=3,
                        )
                    )
                self.assertFalse(second.done())
                self.assertFalse(self.service.channel_cancelled.is_set())
                self.service.uploads["upload:/b"]["release"].set()
                self.assertEqual(second.result(timeout=5), "/b")
            finally:
                self.service.release_all()
        self.api._cancel_upload.assert_called_once_with("upload:/a")
        self.assertNotIn(100, self.progress["/a"])
        self.assertEqual(self.progress["/b"][-1], 100)


if __name__ == "__main__":
    unittest.main()
