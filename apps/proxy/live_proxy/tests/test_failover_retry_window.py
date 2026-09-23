"""Tests for connection retry budgets and failover rotation intervals."""
import time
from unittest.mock import call, patch

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
    sm._failover_rotation_passes = 0
    sm._rotation_started_at = 100.0
    sm._rotation_generation = 0
    sm.min_failover_rotation_interval = 10

    sm._current_source_has_media = True
    sm._fail_fast_candidate = False
    sm._recovering_established_source = False
    sm._attempt_media_started_at = None
    sm._attempt_last_media_at = None
    sm._attempt_was_stable = False
    sm.failover_started_at = None
    sm.pending_buffering_failover_duration = None

    sm.running = True
    sm.stop_requested = False
    sm.url_switching = False
    sm._ensure_owner_or_stop = lambda: True

    for key, value in overrides.items():
        setattr(sm, key, value)
    return sm


class RetryBudgetTests(TestCase):
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
            _failover_rotation_passes=4,
            _recovering_established_source=True,
        )
        sm._record_connection_failure()
        sm._record_connection_failure()

        sm._note_stable_connection()

        self.assertEqual(sm.retry_count, 0)
        self.assertIsNone(sm._last_failure_time)
        self.assertEqual(sm.tried_stream_ids, {100})
        self.assertEqual(sm._failover_rotation_passes, 0)
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
        sm = _make_manager(
            tried_stream_ids={100, 200, 300},
            _failover_rotation_passes=7,
            _rotation_generation=5,
            _current_source_has_media=True,
            _fail_fast_candidate=True,
            _recovering_established_source=True,
            _attempt_media_started_at=1.0,
            _attempt_last_media_at=2.0,
            _attempt_was_stable=True,
            failover_started_at=3.0,
            pending_buffering_failover_duration=4.0,
        )
        sm._record_connection_failure()

        sm.reset_failover_rotation_state()

        self.assertEqual(sm.tried_stream_ids, set())
        self.assertEqual(sm._failover_rotation_passes, 0)
        self.assertIsNone(sm._rotation_started_at)
        self.assertEqual(sm._rotation_generation, 6)
        self.assertFalse(sm._current_source_has_media)
        self.assertFalse(sm._fail_fast_candidate)
        self.assertFalse(sm._recovering_established_source)
        self.assertIsNone(sm._attempt_media_started_at)
        self.assertIsNone(sm._attempt_last_media_at)
        self.assertFalse(sm._attempt_was_stable)
        self.assertIsNone(sm.failover_started_at)
        self.assertIsNone(sm.pending_buffering_failover_duration)
        self.assertEqual(sm.retry_count, 0)
        self.assertIsNone(sm._last_failure_time)


class FailoverRotationIntervalTests(TestCase):
    def test_remaining_interval_counts_time_spent_in_current_pass(self):
        sm = _make_manager(
            _rotation_started_at=100.0,
            min_failover_rotation_interval=10,
        )

        with patch(
            "apps.proxy.live_proxy.input.manager.time.monotonic",
            return_value=103.0,
        ):
            self.assertEqual(sm._rotation_interval_remaining(), 7.0)

    def test_remaining_interval_never_goes_negative(self):
        sm = _make_manager(
            _rotation_started_at=100.0,
            min_failover_rotation_interval=10,
        )

        with patch(
            "apps.proxy.live_proxy.input.manager.time.monotonic",
            return_value=147.0,
        ):
            self.assertEqual(sm._rotation_interval_remaining(), 0.0)

    def test_no_rotation_start_means_no_wait(self):
        sm = _make_manager(_rotation_started_at=None)

        self.assertEqual(sm._rotation_interval_remaining(), 0.0)

    @patch.object(StreamManager, "_try_next_stream", return_value=True)
    @patch.object(StreamManager, "_sleep_interruptible")
    def test_selected_alternate_does_not_start_new_rotation(
        self, mock_sleep, mock_try
    ):
        sm = _make_manager(_failover_rotation_passes=2)

        self.assertTrue(sm._try_next_stream_with_rotation_interval())

        mock_try.assert_called_once_with()
        mock_sleep.assert_not_called()
        self.assertEqual(sm._failover_rotation_passes, 2)

    @patch.object(StreamManager, "_try_next_stream", side_effect=[False, True])
    @patch.object(StreamManager, "_sleep_interruptible", return_value=True)
    def test_exhausted_pass_waits_only_for_remaining_interval_then_wraps(
        self, mock_sleep, mock_try
    ):
        sm = _make_manager(
            tried_stream_ids={100, 200},
            _rotation_started_at=100.0,
            min_failover_rotation_interval=10,
        )

        with patch(
            "apps.proxy.live_proxy.input.manager.time.monotonic",
            side_effect=[103.0, 110.0],
        ):
            self.assertTrue(sm._try_next_stream_with_rotation_interval())

        mock_sleep.assert_called_once_with(7.0, 0)
        self.assertEqual(
            mock_try.call_args_list,
            [call(), call(new_rotation=True)],
        )
        self.assertEqual(sm.tried_stream_ids, set())
        self.assertEqual(sm._failover_rotation_passes, 1)
        self.assertEqual(sm._rotation_started_at, 110.0)

    @patch.object(StreamManager, "_try_next_stream", side_effect=[False, True])
    @patch.object(StreamManager, "_sleep_interruptible", return_value=True)
    def test_long_pass_wraps_without_additional_delay(
        self, mock_sleep, mock_try
    ):
        sm = _make_manager(
            _rotation_started_at=100.0,
            min_failover_rotation_interval=10,
        )

        with patch(
            "apps.proxy.live_proxy.input.manager.time.monotonic",
            side_effect=[147.0, 147.0],
        ):
            self.assertTrue(sm._try_next_stream_with_rotation_interval())

        mock_sleep.assert_called_once_with(0.0, 0)
        self.assertEqual(
            mock_try.call_args_list,
            [call(), call(new_rotation=True)],
        )

    @patch.object(StreamManager, "_try_next_stream", return_value=False)
    def test_superseded_during_interval_does_not_wrap(self, mock_try):
        sm = _make_manager(
            _rotation_started_at=100.0,
            min_failover_rotation_interval=10,
        )

        def supersede(_seconds, _generation):
            sm._rotation_generation += 1
            return False

        sm._sleep_interruptible = supersede

        with patch(
            "apps.proxy.live_proxy.input.manager.time.monotonic",
            return_value=103.0,
        ):
            self.assertIsNone(sm._try_next_stream_with_rotation_interval())

        mock_try.assert_called_once_with()
        self.assertEqual(sm._failover_rotation_passes, 0)

    @patch.object(StreamManager, "_try_next_stream", return_value=False)
    def test_stop_during_interval_does_not_wrap(self, mock_try):
        sm = _make_manager(
            _rotation_started_at=100.0,
            min_failover_rotation_interval=10,
        )

        def stop(_seconds, _generation):
            sm.running = False
            return False

        sm._sleep_interruptible = stop

        with patch(
            "apps.proxy.live_proxy.input.manager.time.monotonic",
            return_value=103.0,
        ):
            self.assertFalse(sm._try_next_stream_with_rotation_interval())

        mock_try.assert_called_once_with()
        self.assertEqual(sm._failover_rotation_passes, 0)


class FailoverConfigDefaultsTests(SimpleTestCase):
    def test_retry_window_default(self):
        self.assertEqual(ConfigHelper.retry_window_seconds(), 1800)

    def test_stable_connection_threshold_default(self):
        self.assertEqual(ConfigHelper.stable_connection_threshold(), 30)

    @patch("apps.proxy.config.TSConfig.get_proxy_settings", return_value={})
    def test_connection_attempts_default(self, _mock_settings):
        self.assertEqual(ConfigHelper.max_retries(), 3)

    @patch(
        "apps.proxy.config.TSConfig.get_proxy_settings",
        return_value={"stream_connection_attempts": 5},
    )
    def test_connection_attempts_uses_configured_value(self, _mock_settings):
        self.assertEqual(ConfigHelper.max_retries(), 5)

    @patch("apps.proxy.config.TSConfig.get_proxy_settings", return_value={})
    def test_min_failover_rotation_interval_default(self, _mock_settings):
        self.assertEqual(ConfigHelper.min_failover_rotation_interval(), 10)

    @patch(
        "apps.proxy.config.TSConfig.get_proxy_settings",
        return_value={"min_failover_rotation_interval": 0},
    )
    def test_min_failover_rotation_interval_allows_immediate_wrap(
        self, _mock_settings
    ):
        self.assertEqual(ConfigHelper.min_failover_rotation_interval(), 0)
