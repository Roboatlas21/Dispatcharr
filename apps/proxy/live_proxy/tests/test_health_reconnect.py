"""Tests for health-monitor reconnect handling in the live stream manager.

The health monitor flags a previously established stream that stopped producing
data by setting needs_reconnect. The already-playing connection itself does
not consume one of the configured recovery attempts; after it drops, the normal
same-URL retry budget starts. Repeated recovery failures eventually fail over.
"""
from unittest.mock import patch

from django.test import TestCase

from apps.proxy.live_proxy.input.manager import StreamManager


class _Buffer:
    """Buffer stand-in with no redis_client, so run() skips its Redis teardown."""


def _make_manager(**overrides):
    sm = StreamManager.__new__(StreamManager)
    sm.channel_id = "test-channel"
    sm.channel_name = "Test Channel"
    sm.url = "http://example.com/stream.ts"
    sm.running = True
    sm.connected = True
    sm.stop_requested = False
    sm.needs_reconnect = False
    sm.needs_stream_switch = False
    sm.url_switching = False
    sm.url_switch_start_time = 0
    sm.url_switch_timeout = 10
    sm.transcode = False
    sm.retry_count = 0
    sm.max_retries = 3
    sm._retry_window_seconds = 1800
    sm._last_failure_time = None
    sm._stable_connection_threshold = 30
    sm.current_stream_id = 100
    sm.tried_stream_ids = {100}
    sm._failover_rotation_passes = 0
    sm._rotation_started_at = None
    sm._rotation_generation = 0
    sm._current_source_has_media = True
    sm._fail_fast_candidate = False
    sm._recovering_established_source = False
    sm._attempt_media_started_at = None
    sm._attempt_last_media_at = None
    sm._attempt_was_stable = False
    sm.failover_started_at = None
    sm.pending_buffering_failover_duration = None
    sm.failover_init_grace_period = 30
    sm.buffering = False
    sm.buffering_start_time = None
    sm.last_data_time = 0.0
    sm._buffer_check_timers = []
    sm.transcode_process_active = False
    sm.buffer = _Buffer()
    for key, value in overrides.items():
        setattr(sm, key, value)
    return sm


class ProcessStreamDataExitTests(TestCase):
    """The chunk loop must yield when the health monitor asks for a reconnect."""

    def test_exits_when_reconnect_is_requested(self):
        sm = _make_manager()
        chunk_calls = []

        def fake_fetch_chunk():
            chunk_calls.append(1)
            if len(chunk_calls) == 3:
                sm.needs_reconnect = True
            if len(chunk_calls) >= 50:
                sm.running = False
            return True

        sm.fetch_chunk = fake_fetch_chunk
        sm._process_stream_data()

        self.assertEqual(len(chunk_calls), 3)
        self.assertFalse(sm.connected)

    def test_keeps_reading_while_no_recovery_is_requested(self):
        sm = _make_manager()
        chunk_calls = []

        def fake_fetch_chunk():
            chunk_calls.append(1)
            if len(chunk_calls) >= 5:
                sm.running = False
            return True

        sm.fetch_chunk = fake_fetch_chunk
        sm._process_stream_data()

        self.assertEqual(len(chunk_calls), 5)


class HealthReconnectRetryLoopTests(TestCase):
    """Health reconnects use the established-source recovery budget."""

    def test_reconnect_closes_and_reestablishes_same_url(self):
        sm = _make_manager()
        events = []

        def fake_establish():
            events.append("establish")
            sm.connected = True
            return True

        def fake_process():
            events.append("process")
            if events.count("process") == 1:
                sm.needs_reconnect = True
            else:
                sm.running = False

        def fake_close_socket():
            events.append("close")
            sm.connected = False

        with patch.object(StreamManager, "_monitor_health"), \
                patch.object(StreamManager, "_ensure_owner_or_stop", return_value=True), \
                patch.object(StreamManager, "_close_all_connections"), \
                patch.object(StreamManager, "_try_next_stream_with_rotation_interval", return_value=False) as try_next, \
                patch("apps.proxy.live_proxy.input.manager.close_old_connections"), \
                patch.object(sm, "_establish_http_connection", side_effect=fake_establish), \
                patch.object(sm, "_process_stream_data", side_effect=fake_process), \
                patch.object(sm, "_close_socket", side_effect=fake_close_socket), \
                patch("apps.proxy.live_proxy.input.manager.gevent.sleep"):
            sm.run()

        self.assertEqual(
            events, ["establish", "process", "close", "establish", "process"]
        )
        self.assertEqual(sm.retry_count, 0)
        self.assertTrue(sm._recovering_established_source)
        self.assertFalse(sm.needs_reconnect)
        try_next.assert_not_called()

    def test_repeated_reconnects_exhaust_retry_budget_and_failover(self):
        sm = _make_manager(max_retries=3)
        events = []

        def fake_establish():
            events.append("establish")
            sm.connected = True
            return True

        def fake_process():
            events.append("process")
            sm.needs_reconnect = True

        def fake_close_socket():
            events.append("close")
            sm.connected = False

        def fake_try_next():
            events.append("try_next")
            sm.running = False
            return False

        with patch.object(StreamManager, "_monitor_health"), \
                patch.object(StreamManager, "_ensure_owner_or_stop", return_value=True), \
                patch.object(StreamManager, "_close_all_connections"), \
                patch.object(StreamManager, "_try_next_stream_with_rotation_interval", side_effect=fake_try_next), \
                patch("apps.proxy.live_proxy.input.manager.close_old_connections"), \
                patch.object(sm, "_establish_http_connection", side_effect=fake_establish), \
                patch.object(sm, "_process_stream_data", side_effect=fake_process), \
                patch.object(sm, "_close_socket", side_effect=fake_close_socket), \
                patch("apps.proxy.live_proxy.input.manager.gevent.sleep"), \
                patch("apps.proxy.live_proxy.input.manager.log_system_event"):
            sm.run()

        self.assertEqual(events.count("close"), 4)
        self.assertEqual(events.count("establish"), 4)
        self.assertEqual(sm.retry_count, 3)
        self.assertIn("try_next", events)
        self.assertFalse(sm.needs_reconnect)

    def test_stream_switch_request_still_reaches_failover(self):
        sm = _make_manager()
        events = []

        def fake_establish():
            events.append("establish")
            sm.connected = True
            return True

        def fake_process():
            events.append("process")
            sm.needs_stream_switch = True

        def fake_try_next():
            events.append("try_next")
            sm.running = False
            return False

        with patch.object(StreamManager, "_monitor_health"), \
                patch.object(StreamManager, "_ensure_owner_or_stop", return_value=True), \
                patch.object(StreamManager, "_close_all_connections"), \
                patch.object(StreamManager, "_try_next_stream_with_rotation_interval", side_effect=fake_try_next), \
                patch("apps.proxy.live_proxy.input.manager.close_old_connections"), \
                patch.object(sm, "_establish_http_connection", side_effect=fake_establish), \
                patch.object(sm, "_process_stream_data", side_effect=fake_process), \
                patch.object(sm, "_close_socket"):
            sm.run()

        self.assertEqual(events, ["establish", "process", "try_next"])
        self.assertEqual(sm.retry_count, 0)
