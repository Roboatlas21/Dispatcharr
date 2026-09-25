"""Tests for connection retry idle reset and stable-playback failover reset."""
import time
from unittest.mock import MagicMock, call, patch

from django.test import SimpleTestCase, TestCase

from apps.proxy.live_proxy.config_helper import ConfigHelper
from apps.proxy.live_proxy.input.manager import StreamManager


def _make_manager(**overrides):
    sm = StreamManager.__new__(StreamManager)
    sm.channel_id = "test-channel"
    sm.max_retries = 3
    sm._retry_window_seconds = 1800
    sm._stable_connection_threshold = 30
    sm._last_failure_time = None
    sm.retry_count = 0
    sm.current_stream_id = 100
    sm.tried_stream_ids = {100, 200, 300}
    sm._rotation_started_at = 100.0
    sm._rotation_generation = 0
    sm.min_failover_rotation_interval = 10

    sm.running = True
    sm.stop_requested = False
    sm.url_switching = False
    sm._ensure_owner_or_stop = lambda: True
    for key, value in overrides.items():
        setattr(sm, key, value)
    return sm


class RetryIdleResetTests(TestCase):
    def test_counter_resets_after_idle_period(self):
        sm = _make_manager(_retry_window_seconds=60)
        sm._last_failure_time = time.time() - 120
        sm.retry_count = 2

        count = sm._record_connection_failure()

        self.assertEqual(count, 1)

    def test_counter_accumulates_within_idle_period(self):
        sm = _make_manager(_retry_window_seconds=1800)
        self.assertEqual(sm._record_connection_failure(), 1)
        self.assertEqual(sm._record_connection_failure(), 2)
        self.assertEqual(sm._record_connection_failure(), 3)
        self.assertFalse(sm.should_retry())

    def test_stable_connection_resets_retry_and_rotation_budget(self):
        sm = _make_manager(
            tried_stream_ids={100, 200, 300},
            _recovering_established_source=True,
        )
        sm._record_connection_failure()
        sm._record_connection_failure()

        sm._note_stable_connection()

        self.assertEqual(sm.retry_count, 0)
        self.assertIsNone(sm._last_failure_time)
        self.assertEqual(sm.tried_stream_ids, {100})
        self.assertIsNone(sm._rotation_started_at)
        self.assertFalse(sm._recovering_established_source)

    def test_clear_connection_failure_history(self):
        sm = _make_manager()
        sm._record_connection_failure()
        sm._record_connection_failure()
        sm._clear_connection_failure_history()
        self.assertEqual(sm.retry_count, 0)
        self.assertIsNone(sm._last_failure_time)

    def test_manual_reset_clears_retry_and_source_state(self):
        buffer = MagicMock(published_index=7, redis_client=None)
        with patch("apps.proxy.config.TSConfig.get_proxy_settings", return_value={}):
            sm = StreamManager("test-channel", "http://old", buffer, channel_name="Test")
        sm.tried_stream_ids = {100, 200}
        sm._current_source_has_media = sm._fail_fast_candidate = True
        sm._attempt_media_started_at = sm._attempt_last_media_at = 1.0
        sm._attempt_was_stable = sm._recovering_established_source = True
        sm.failover_started_at = sm.pending_buffering_failover_duration = 3.0
        sm._record_connection_failure()
        with patch.object(sm, "_ensure_owner_or_stop", return_value=True), \
             patch("apps.proxy.live_proxy.input.manager.log_system_event"):
            self.assertTrue(sm.update_url("http://new", manual=True))
        self.assertEqual(sm._rotation_generation, 1)
        self.assertEqual(sm._source_published_index, 7)
        for field in ("_current_source_has_media", "_fail_fast_candidate", "_recovering_established_source",
                      "_attempt_was_stable", "retry_count", "tried_stream_ids"):
            self.assertFalse(getattr(sm, field), field)
        for field in ("_rotation_started_at", "_attempt_media_started_at", "_attempt_last_media_at",
                      "failover_started_at", "pending_buffering_failover_duration", "_last_failure_time"):
            self.assertIsNone(getattr(sm, field), field)


class FailoverRotationIntervalTests(TestCase):
    def test_remaining_interval(self):
        for start, now, expected in ((100.0, 103.0, 7.0), (100.0, 147.0, 0.0), (None, 103.0, 0.0)):
            with self.subTest(start=start, now=now), patch(
                "apps.proxy.live_proxy.input.manager.time.monotonic", return_value=now,
            ):
                self.assertEqual(_make_manager(_rotation_started_at=start)._rotation_interval_remaining(), expected)

    @patch.object(StreamManager, "_try_next_stream", return_value=True)
    @patch.object(StreamManager, "_sleep_interruptible")
    def test_selected_alternate_does_not_start_new_rotation(self, mock_sleep, mock_try):
        sm = _make_manager()
        self.assertTrue(sm._try_next_stream_with_rotation_interval())
        mock_try.assert_called_once_with()
        mock_sleep.assert_not_called()
        self.assertEqual(sm._rotation_started_at, 100.0)

    def test_exhausted_pass_waits_only_for_remaining_interval_then_wraps(self):
        for now, wait, next_start in ((103.0, 7.0, 110.0), (147.0, 0.0, 147.0)):
            with self.subTest(now=now), \
                 patch("apps.proxy.live_proxy.input.manager.get_alternate_streams", return_value=[]) as select, \
                 patch.object(StreamManager, "_sleep_interruptible", return_value=True) as sleep, \
                 patch("apps.proxy.live_proxy.input.manager.time.monotonic", side_effect=[now, next_start]):
                sm = _make_manager()
                self.assertFalse(sm._try_next_stream_with_rotation_interval())
                sleep.assert_called_once_with(wait, 0)
                self.assertEqual(select.call_args_list, [call(sm.channel_id, 100), call(sm.channel_id, None)])
                self.assertEqual(sm.tried_stream_ids, set())
                self.assertEqual(sm._rotation_started_at, next_start)

    def test_interrupted_interval_does_not_wrap(self):
        for field, value, expected in (("_rotation_generation", 1, None), ("running", False, False)):
            with self.subTest(field=field), \
                 patch.object(StreamManager, "_try_next_stream", return_value=False) as select, \
                 patch("apps.proxy.live_proxy.input.manager.time.monotonic", return_value=103.0):
                sm = _make_manager()
                def interrupt(_seconds, _generation):
                    setattr(sm, field, value)
                    return False
                sm._sleep_interruptible = interrupt
                self.assertIs(sm._try_next_stream_with_rotation_interval(), expected)
                select.assert_called_once_with()
                self.assertEqual(sm._rotation_started_at, 100.0)
                self.assertEqual(sm.tried_stream_ids, {100, 200, 300})


class FailoverConfigDefaultsTests(SimpleTestCase):
    def test_retry_window_default(self):
        self.assertEqual(ConfigHelper.retry_window_seconds(), 1800)

    def test_stable_connection_threshold_default(self):
        self.assertEqual(ConfigHelper.stable_connection_threshold(), 30)
