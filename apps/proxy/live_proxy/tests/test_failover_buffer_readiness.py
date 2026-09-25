"""A replacement must publish output before its startup deadline is cleared."""
import time
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from apps.proxy.live_proxy.input.buffer import StreamBuffer
from apps.proxy.live_proxy.input.manager import StreamManager


class FailoverBufferReadinessTests(SimpleTestCase):
    def _manager(self, initial_index=0):
        redis = MagicMock()
        redis.get.return_value = initial_index
        redis.incr.side_effect = range(initial_index + 1, initial_index + 10)
        with patch("apps.proxy.config.TSConfig.get_proxy_settings", return_value={}):
            buffer = StreamBuffer(channel_id="test-channel", redis_client=redis)
            # Two packets keep the tests small; production's configured chunk
            # size and the actual add_chunk()/fetch_chunk() paths are unchanged.
            buffer.target_chunk_size = 188 * 2
            manager = StreamManager(
                "test-channel", "http://example.com/live.ts", buffer,
                stream_id=100, channel_name="Test Channel",
            )
        manager.connected = True
        manager._fail_fast_candidate = True
        manager.failover_started_at = time.monotonic()
        manager.socket = MagicMock()
        manager._upstream_may_continue = MagicMock(return_value=True)
        manager._update_bytes_processed = MagicMock()
        return manager

    def test_reader_index_movement_does_not_establish_replacement(self):
        manager = self._manager(initial_index=12)
        packet = b"\x47" + b"\0" * 187
        redis = manager.buffer.redis_client
        redis.pipeline.return_value.execute.return_value = [packet] * 3
        manager.pending_buffering_failover_duration = 8.0

        # Twelve chunks already exist. Reading old chunks 5-7 moves the
        # generic index back before the replacement captures its baseline.
        self.assertEqual(manager.buffer.get_chunks(start_index=4), [packet] * 3)
        self.assertEqual(manager.buffer.index, 7)
        with patch("apps.proxy.live_proxy.input.manager.log_system_event"):
            self.assertTrue(manager.update_url("http://example.com/replacement.ts"))
        self.assertEqual(manager._source_published_index, 12)
        self.assertEqual(manager.pending_buffering_failover_duration, 8.0)
        self.assertFalse(manager._current_source_has_media)
        self.assertIsNone(manager.failover_started_at)

        # Reading old chunks 10-12 advances that index without a new write.
        self.assertEqual(
            manager.buffer.get_chunks_exact(start_index=9, count=3),
            [packet] * 3,
        )
        manager.connected = True
        manager._fail_fast_candidate = True
        manager.failover_started_at = time.monotonic()
        manager.socket = MagicMock()
        manager.socket.recv.side_effect = [packet, b""]
        manager._process_stream_data()

        # One packet cannot fill the two-packet test publication size.
        redis.incr.assert_not_called()
        self.assertEqual(manager.buffer.index, 12)
        self.assertEqual(manager.buffer.published_index, 12)
        self.assertFalse(manager._current_source_has_media)
        self.assertTrue(manager._fail_fast_candidate)
        self.assertIsNotNone(manager.failover_started_at)

    def test_new_published_chunk_establishes_replacement(self):
        manager = self._manager(initial_index=7)
        manager.socket.recv.side_effect = [(b"\x47" + b"\0" * 187) * 2, b""]
        manager._process_stream_data()
        self.assertEqual(manager.buffer.index, 8)
        self.assertEqual(manager.buffer.published_index, 8)
        self.assertTrue(manager._current_source_has_media)
        self.assertFalse(manager._fail_fast_candidate)
        self.assertIsNone(manager.failover_started_at)

    def test_partial_data_cannot_extend_failover_deadline(self):
        manager = self._manager(initial_index=7)
        manager.failover_started_at = 100.0
        clock = [100.0]

        def receive_partial_data():
            clock[0] += 10
            return manager.buffer.add_chunk(b"\x47")

        manager.fetch_chunk = MagicMock(side_effect=receive_partial_data)
        with patch("apps.proxy.live_proxy.input.manager.time.monotonic", side_effect=lambda: clock[0]):
            manager._process_stream_data()
        self.assertEqual(manager.fetch_chunk.call_count, 3)
        self.assertTrue(manager.needs_stream_switch)
        self.assertFalse(manager._current_source_has_media)
