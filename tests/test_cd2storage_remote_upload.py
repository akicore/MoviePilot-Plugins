"""Run with: python -m unittest discover -s tests -v (no MoviePilot needed)."""

import importlib.util
import logging
import os
from pathlib import Path
from queue import Queue
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Condition, Event, enumerate as list_threads
from types import ModuleType, SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch


PLUGIN = Path(__file__).resolve().parents[1] / "plugins.v2" / "cd2storage"


def load_api():
    """Load real plugin code with only its external host dependencies stubbed."""
    package = ModuleType("cd2storage_under_test")
    package.__path__ = [str(PLUGIN)]
    sys.modules[package.__name__] = package
    modules = {}
    for name in (
        "app",
        "app.core",
        "app.core.config",
        "app.log",
        "app.modules",
        "app.modules.filemanager",
        "app.modules.filemanager.storages",
        "app.schemas",
        "app.schemas.exception",
        "cryptography",
        "cryptography.hazmat",
        "cryptography.hazmat.primitives",
        "grpc",
        "httpx",
        "clouddrive2_client",
        "clouddrive2_client.proto",
    ):
        modules[name] = ModuleType(name)
    modules["app.core.config"].global_vars = SimpleNamespace(
        is_transfer_stopped=lambda path: False
    )
    modules["app.core.config"].settings = SimpleNamespace()
    modules["app.log"].logger = logging.getLogger("cd2storage-tests")
    modules["app.log"].logger.disabled = True
    modules["app.modules.filemanager.storages"].transfer_process = (
        lambda path: lambda p: None
    )
    modules["app.schemas"].FileItem = SimpleNamespace
    modules["app.schemas"].StorageUsage = SimpleNamespace
    modules["app.schemas.exception"].StorageQueryError = RuntimeError
    modules["cryptography.hazmat.primitives"].hashes = SimpleNamespace()
    modules["grpc"].Call = object
    modules["grpc"].RpcError = RuntimeError
    modules["grpc"].StatusCode = SimpleNamespace()
    modules["httpx"].RequestError = RuntimeError
    modules["httpx"].stream = Mock()
    modules["clouddrive2_client"].CloudDriveClient = object
    modules["clouddrive2_client.proto"].clouddrive_pb2 = SimpleNamespace(
        RemoteReadDataUpload=lambda **values: SimpleNamespace(**values)
    )
    name = f"{package.__name__}.clouddrive_api"
    spec = importlib.util.spec_from_file_location(name, PLUGIN / "clouddrive_api.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


api_module = load_api()
RemoteReadDispatcher = api_module.RemoteReadDispatcher
RemoteUploadCancelled = api_module.RemoteUploadCancelled


class Transport:
    def __init__(self):
        self.condition = Condition()
        self.calls = []

    def send(self, offset, data, is_last, lazy_read):
        future = Future()
        with self.condition:
            self.calls.append((offset, bytes(data), is_last, lazy_read, future))
            self.condition.notify_all()
        return future

    def wait_calls(self, count):
        with self.condition:
            if not self.condition.wait_for(lambda: len(self.calls) >= count, timeout=3):
                raise AssertionError(f"Only {len(self.calls)}/{count} RPCs started")
            return list(self.calls)

    def complete(self, index, success=True, bytes_received=None):
        call = self.calls[index]
        call[4].set_result(
            SimpleNamespace(
                success=success,
                error_message="injected failure" if not success else "",
                bytes_received=len(call[1])
                if bytes_received is None
                else bytes_received,
            )
        )


class RemoteReadFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "source.bin"
        self.data = bytes(range(251)) * 100
        self.path.write_bytes(self.data)
        self.transport = Transport()
        self.progress = []
        self.results = []
        self.cancelled = Event()

    def dispatcher(self):
        def check():
            if self.cancelled.is_set():
                raise RemoteUploadCancelled("test cancellation")

        reads = RemoteReadDispatcher(
            self.path,
            len(self.data),
            self.transport.send,
            check,
            self.progress.append,
            self.results.append,
        )
        self.addCleanup(reads.close)
        return reads


class RemoteReadTests(RemoteReadFixture):
    def test_four_requests_are_in_flight_and_ranges_are_correct(self):
        reads = self.dispatcher()
        offsets = (5000, 0, 12000, 2000)
        for index, offset in enumerate(offsets):
            reads.submit(offset, 2000, bool(index % 2))
        calls = self.transport.wait_calls(4)
        self.assertEqual(reads.max_active_rpcs, 4)
        for offset, data, is_last, lazy_read, _ in calls:
            self.assertEqual(data, self.data[offset : offset + 2000])
            self.assertFalse(is_last)
            self.assertEqual(lazy_read, bool(offsets.index(offset) % 2))
        for index in (3, 0, 2, 1):
            self.transport.complete(index)
        reads.finish()
        self.assertEqual(reads.completed_requests, 4)
        self.assertEqual(reads.completed_bytes, 8000)

    def test_single_request_needs_no_following_request(self):
        reads = self.dispatcher()
        reads.submit(0, len(self.data), False)
        self.transport.wait_calls(1)
        self.transport.complete(0)
        reads.finish()
        self.assertEqual(reads.completed_bytes, len(self.data))
        self.assertEqual(self.progress, [99.0])

    def test_duplicate_and_overlapping_ranges_do_not_inflate_progress(self):
        reads = self.dispatcher()
        for offset, length in ((100, 100), (0, 50), (100, 100), (25, 150)):
            reads.submit(offset, length, False)
        self.transport.wait_calls(4)
        for index in (2, 0, 3, 1):
            self.transport.complete(index)
            reads.poll()
        reads.finish()
        self.assertEqual(reads.completed_bytes, 200)
        self.assertEqual(self.progress, sorted(self.progress))
        self.assertLess(max(self.progress), 100)

    def test_worker_limit_applies_before_starting_fifth_rpc(self):
        reads = self.dispatcher()
        for offset in range(4):
            reads.submit(offset, 1, False)
        self.transport.wait_calls(4)
        attempted = Event()
        with ThreadPoolExecutor(max_workers=1) as executor:

            def fifth():
                attempted.set()
                reads.submit(4, 1, False)

            pending = executor.submit(fifth)
            self.assertTrue(attempted.wait(1))
            self.assertEqual(len(self.transport.calls), 4)
            self.transport.complete(0)
            pending.result(timeout=3)
        self.transport.wait_calls(5)
        for index in range(1, 5):
            self.transport.complete(index)
        reads.finish()
        self.assertLessEqual(reads.max_active_rpcs, 4)

    def test_data_budget_applies_before_reading_next_payload(self):
        reads = self.dispatcher()
        reads.MAX_BUFFER_BYTES = 10
        reads.submit(0, 6, False)
        self.transport.wait_calls(1)
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(reads.submit, 6, 6, False)
            self.assertEqual(len(self.transport.calls), 1)
            self.transport.complete(0)
            pending.result(timeout=3)
        self.transport.wait_calls(2)
        self.transport.complete(1)
        reads.finish()
        with self.assertRaisesRegex(ValueError, "data budget"):
            reads.submit(0, 11, False)

    def test_failure_is_propagated_and_other_rpcs_are_cancelled(self):
        reads = self.dispatcher()
        reads.submit(0, 100, False)
        reads.submit(100, 100, False)
        self.transport.wait_calls(2)
        self.transport.complete(0, success=False)
        with self.assertRaisesRegex(OSError, "injected failure"):
            reads.finish()
        reads.close()
        self.assertTrue(self.transport.calls[1][4].cancelled())

    def test_transport_exception_is_propagated(self):
        reads = self.dispatcher()
        reads.submit(0, 100, False)
        self.transport.wait_calls(1)[0][4].set_exception(TimeoutError("RPC deadline"))
        with self.assertRaisesRegex(TimeoutError, "RPC deadline"):
            reads.finish()

    def test_partial_acknowledgement_is_rejected(self):
        reads = self.dispatcher()
        reads.submit(0, 100, False)
        self.transport.wait_calls(1)
        self.transport.complete(0, bytes_received=99)
        with self.assertRaisesRegex(OSError, "99/100"):
            reads.finish()

    def test_cancellation_prevents_new_requests_and_cancels_rpcs(self):
        reads = self.dispatcher()
        for offset in range(4):
            reads.submit(offset, 1, False)
        self.transport.wait_calls(4)
        self.cancelled.set()
        with self.assertRaises(RemoteUploadCancelled):
            reads.submit(4, 1, False)
        reads.close()
        self.assertTrue(all(call[4].cancelled() for call in self.transport.calls))
        self.assertFalse(
            any(thread.name.startswith("cd2-remote-read") for thread in list_threads())
        )

    def test_source_change_is_rejected(self):
        reads = self.dispatcher()
        self.path.write_bytes(b"changed")
        reads.submit(0, 5, False)
        with self.assertRaisesRegex(OSError, "Source file changed"):
            reads.finish()
        self.assertFalse(self.transport.calls)

    def test_eof_range_is_clamped_and_invalid_ranges_are_rejected(self):
        reads = self.dispatcher()
        reads.submit(len(self.data) - 3, 100, True)
        call = self.transport.wait_calls(1)[0]
        self.assertEqual(call[1], self.data[-3:])
        self.assertTrue(call[2])
        self.transport.complete(0)
        reads.finish()
        for offset, length in ((-1, 1), (len(self.data) + 1, 1), (0, -1), (0, 0)):
            with self.assertRaises(ValueError):
                reads.submit(offset, length, False)

    def test_empty_file_read_is_supported(self):
        self.path.write_bytes(b"")
        self.data = b""
        reads = self.dispatcher()
        reads.submit(0, 0, False)
        call = self.transport.wait_calls(1)[0]
        self.assertEqual(call[1], b"")
        self.assertTrue(call[2])
        self.transport.complete(0)
        reads.finish()
        self.assertFalse(self.progress)

    def test_same_size_source_mutation_is_rejected(self):
        reads = self.dispatcher()
        previous = self.path.stat()
        self.path.write_bytes(b"x" * len(self.data))
        os.utime(
            self.path, ns=(previous.st_atime_ns, previous.st_mtime_ns + 1_000_000_000)
        )
        reads.submit(0, 100, False)
        with self.assertRaisesRegex(OSError, "Source file changed"):
            reads.finish()

    def test_cancellation_interrupts_capacity_wait(self):
        reads = self.dispatcher()
        for offset in range(4):
            reads.submit(offset, 1, False)
        self.transport.wait_calls(4)
        attempted = Event()
        with ThreadPoolExecutor(max_workers=1) as executor:

            def fifth():
                attempted.set()
                reads.submit(4, 1, False)

            pending = executor.submit(fifth)
            self.assertTrue(attempted.wait(1))
            self.cancelled.set()
            with self.assertRaises(RemoteUploadCancelled):
                pending.result(timeout=3)
        reads.close()
        self.assertTrue(all(call[4].cancelled() for call in self.transport.calls))

    def test_range_accounting_matches_byte_coverage(self):
        import random

        reads = self.dispatcher()
        expected = set()
        randomizer = random.Random(17)
        for batch in range(20):
            for _ in range(4):
                offset = randomizer.randrange(2000)
                length = randomizer.randrange(1, 400)
                expected.update(range(offset, offset + length))
                reads.submit(offset, length, False)
            self.transport.wait_calls((batch + 1) * 4)
            indices = list(range(batch * 4, (batch + 1) * 4))
            randomizer.shuffle(indices)
            for index in indices:
                self.transport.complete(index)
            reads.finish()
            self.assertEqual(reads.completed_bytes, len(expected))


class Stream:
    def __init__(self):
        self.messages = Queue()
        self.cancelled = Event()

    def initial_metadata(self):
        return ()

    def __iter__(self):
        return self

    def __next__(self):
        item = self.messages.get(timeout=5)
        if item is None:
            raise StopIteration
        return item

    def cancel(self):
        self.cancelled.set()
        self.messages.put(None)

    def read(self, offset, length):
        self.messages.put(
            SimpleNamespace(
                upload_id="upload-test",
                WhichOneof=lambda name: "read_data",
                read_data=SimpleNamespace(
                    offset=offset, length=length, lazy_read=False
                ),
            )
        )

    def finish(self):
        self.messages.put(
            SimpleNamespace(
                upload_id="upload-test",
                WhichOneof=lambda name: "status_changed",
                status_changed=SimpleNamespace(status=api_module.UploadStatus.FINISH),
            )
        )


class UploadIntegrationTests(RemoteReadFixture):
    def make_api(self):
        stream = Stream()

        def future(request, metadata, timeout):
            self.assertEqual(metadata, [("authorization", "Bearer test-token")])
            self.assertEqual(timeout, 120)
            return self.transport.send(
                request.offset, request.data, request.is_last_chunk, request.lazy_read
            )

        client = SimpleNamespace(
            jwt_token="test-token",
            stub=SimpleNamespace(RemoteReadData=SimpleNamespace(future=future)),
            remote_upload_channel=lambda **kw: stream,
            start_remote_upload=lambda **kw: SimpleNamespace(upload_id="upload-test"),
            remote_upload_control_cancel=Mock(),
        )
        api = api_module.CloudDriveApi(client, "test", upload_mode="remote_upload")
        self.addCleanup(api.close)
        api.get_item = Mock(return_value="uploaded-item")
        api_module.transfer_process = lambda path: self.progress.append
        api_module.global_vars = SimpleNamespace(
            is_transfer_stopped=lambda path: self.cancelled.is_set()
        )
        return api, stream, client

    def test_finish_waits_for_all_out_of_order_acks(self):
        api, stream, client = self.make_api()
        for offset in (5000, 0, 10000, 2000):
            stream.read(offset, 1000)
        stream.finish()
        with ThreadPoolExecutor(max_workers=1) as executor:
            upload = executor.submit(
                api._upload_once, "/target", self.path, len(self.data), {}
            )
            self.transport.wait_calls(4)
            self.assertFalse(upload.done())
            self.assertNotIn(100, self.progress)
            for index in (3, 1, 2, 0):
                self.transport.complete(index)
            self.assertEqual(upload.result(timeout=3), "uploaded-item")
        self.assertEqual(self.progress[-1], 100)
        client.remote_upload_control_cancel.assert_not_called()
        self.assertFalse(stream.cancelled.is_set())
        api.close()
        self.assertTrue(stream.cancelled.is_set())
        self.assertFalse(
            any(thread.name.startswith("cd2-") for thread in list_threads())
        )

    def test_failure_is_detected_while_stream_is_idle(self):
        api, stream, client = self.make_api()
        stream.read(0, 1000)
        stream.read(1000, 1000)
        with ThreadPoolExecutor(max_workers=1) as executor:
            upload = executor.submit(
                api._upload_once, "/target", self.path, len(self.data), {}
            )
            self.transport.wait_calls(2)
            self.transport.complete(0, success=False)
            self.assertIsNone(upload.result(timeout=3))
        client.remote_upload_control_cancel.assert_called_once_with("upload-test")
        self.assertTrue(self.transport.calls[1][4].cancelled())
        self.assertNotIn(100, self.progress)
        self.assertFalse(stream.cancelled.is_set())

    def test_cancellation_is_detected_while_stream_is_idle(self):
        api, stream, client = self.make_api()
        stream.read(0, 1000)
        with ThreadPoolExecutor(max_workers=1) as executor:
            upload = executor.submit(
                api._upload_once, "/target", self.path, len(self.data), {}
            )
            self.transport.wait_calls(1)
            self.cancelled.set()
            self.assertIsNone(upload.result(timeout=3))
        self.assertTrue(self.transport.calls[0][4].cancelled())
        client.remote_upload_control_cancel.assert_called_once_with("upload-test")
        self.assertNotIn(100, self.progress)


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    unittest.main()
