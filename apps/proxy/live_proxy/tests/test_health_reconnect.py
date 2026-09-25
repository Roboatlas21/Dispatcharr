"""Tests for health-monitor reconnect handling in the live stream manager.

The health monitor flags a previously stable stream that stopped producing data
by setting ``needs_reconnect``. Three things have to happen for that flag to
mean anything:

1. The chunk-reading loop must notice it and yield, instead of staying parked on
   a dead connection.
2. The per-URL retry loop must clear the flag and tear the old socket down before
   opening a new one, so the next ``_process_stream_data`` call does not exit
   immediately and the HTTP reader thread is not orphaned.
3. Recovery failures count toward ``max_retries`` / the retry window; the
   connection that was already playing does not consume that recovery budget.
   A URL that keeps dying eventually fails over instead of reconnecting forever.
"""
from threading import Lock
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, TestCase

from apps.proxy.live_proxy.input.manager import StreamManager


class _Buffer:
    """Buffer stand-in with no redis_client, so run() skips its Redis teardown."""

    index = 0
    published_index = 0


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
    sm._switch_lock = Lock()
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
    sm._rotation_started_at = None
    sm._rotation_generation = 0
    sm.pending_buffering_failover_duration = None
    sm.failover_init_grace_period = 30
    sm.buffering = False
    sm.buffering_start_time = None
    sm.last_data_time = 0.0
    sm._buffer_check_timers = []
    sm.transcode_process_active = False
    sm.buffer = _Buffer()
    sm._reset_source_state()
    sm._current_source_has_media = True
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
                # Safety valve so a loop that ignores the flag still terminates
                # rather than hanging the test run.
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


class ConnectionFailureRetryLoopTests(SimpleTestCase):
    """Ordinary failures and exceptions share the same retry/cancellation rules."""

    def _run_failures(self, sm, outcomes, select_next=None):
        outcomes = iter(outcomes)

        def establish():
            try:
                outcome = next(outcomes)
            except StopIteration:
                sm.running = False
                return False
            if callable(outcome):
                return outcome()
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        def exhausted():
            sm.running = False
            return False

        with patch("apps.proxy.live_proxy.input.manager.threading.Thread"), \
             patch("apps.proxy.live_proxy.input.manager.close_old_connections"), \
             patch("apps.proxy.live_proxy.input.manager.gevent.sleep"), \
             patch("apps.proxy.live_proxy.server.ProxyServer.get_instance"), \
             patch.object(sm, "_ensure_owner_or_stop", return_value=True), \
             patch.object(sm, "_close_all_connections"), \
             patch.object(sm, "_process_stream_data"), \
             patch.object(sm, "_establish_http_connection", side_effect=establish) as attempt, \
             patch.object(sm, "_try_next_stream_with_rotation_interval",
                          side_effect=select_next or exhausted) as select, \
             patch("apps.proxy.live_proxy.input.manager.log_system_event") as event:
            sm.run()
        return attempt, select, event

    def test_primary_uses_configured_budget_for_all_failure_paths(self):
        for budget in (1, 3, 5):
            for mode in ("ordinary", "exception", "mixed"):
                with self.subTest(budget=budget, mode=mode):
                    sm = _make_manager(max_retries=budget, _current_source_has_media=False)
                    outcomes = [
                        OSError("upstream failed") if mode == "exception" or (mode == "mixed" and i % 2 == 0) else False
                        for i in range(budget)
                    ]
                    attempt, select, event = self._run_failures(sm, outcomes)
                    self.assertEqual(attempt.call_count, budget)
                    self.assertEqual(sm.retry_count, budget)
                    select.assert_called_once_with()
                    event.assert_called_once()
                    self.assertEqual(event.call_args.args, ("channel_error",))
                    self.assertEqual(event.call_args.kwargs["attempts"], budget)
                    self.assertEqual(event.call_args.kwargs["error_type"],
                                     "connection_failed" if mode == "ordinary" else "connection_exception")

    def test_unproven_backup_gets_one_attempt(self):
        for outcome in (False, OSError("upstream failed")):
            with self.subTest(outcome=outcome):
                sm = _make_manager(max_retries=5, _current_source_has_media=False, _fail_fast_candidate=True)
                attempt, select, _ = self._run_failures(sm, [outcome] * 5)
                self.assertEqual(attempt.call_count, 1)
                select.assert_called_once_with()

    def test_superseded_exception_does_not_charge_replacement(self):
        with patch("apps.proxy.config.TSConfig.get_proxy_settings", return_value={}):
            sm = StreamManager("test-channel", "http://old", MagicMock(redis_client=None, published_index=0),
                               channel_name="Test")

        def supersede():
            self.assertTrue(sm.update_url("http://example.com/manual.ts", manual=True))
            raise OSError("old connection failed")

        attempt, select, _ = self._run_failures(sm, [supersede])
        self.assertEqual(attempt.call_count, 2)  # Replacement starts a fresh attempt.
        self.assertEqual(sm.retry_count, 0)
        select.assert_not_called()

    def test_stop_during_exception_does_not_consume_attempt(self):
        sm = _make_manager(_current_source_has_media=False)

        def stop():
            sm.stop_requested = True
            raise OSError("closed during shutdown")

        attempt, select, _ = self._run_failures(sm, [stop])
        self.assertEqual(attempt.call_count, 1)
        self.assertEqual(sm.retry_count, 0)
        select.assert_not_called()

    def test_stream_switch_limit_still_stops_rotation(self):
        sm = _make_manager(max_retries=1, _current_source_has_media=False)

        def select_next():
            sm._rotation_generation += 1
            sm.current_stream_id += 1
            sm.url = f"http://example.com/{sm.current_stream_id}.ts"
            sm._reset_source_state()
            sm._fail_fast_candidate = True
            sm._clear_connection_failure_history()
            return True

        with patch("apps.proxy.live_proxy.input.manager.ConfigHelper.max_stream_switches", return_value=2):
            attempt, select, _ = self._run_failures(sm, [False] * 5, select_next)
        self.assertEqual(select.call_count, 2)
        self.assertEqual(attempt.call_count, 3)
