"""Tests for Redis channel state after buffering-timeout failover.

After replacement media arrives, Redis must not stay latched at buffering
once the in-memory buffering flag is cleared. The media path writes ACTIVE
(same as normal buffering recovery) so mid-session clients are not forced
through the connecting init-wait path.
"""
from unittest.mock import MagicMock, patch

from django.test import TestCase

from apps.proxy.live_proxy.constants import ChannelMetadataField, ChannelState
from apps.proxy.live_proxy.input.manager import StreamManager
from apps.proxy.live_proxy.redis_keys import RedisKeys


CHANNEL_ID = "00000000-0000-0000-0000-000000000149"


class _DictRedis:
    """Minimal Redis stand-in that records hash field writes."""

    def __init__(self):
        self.hashes = {}

    def hset(self, key, field=None, value=None, mapping=None):
        bucket = self.hashes.setdefault(key, {})
        if mapping:
            bucket.update(mapping)
        elif field is not None:
            bucket[field] = value


def _make_stream_manager(redis_client, buffering_timeout=1.0, buffering_speed=1.0):
    sm = StreamManager.__new__(StreamManager)
    sm.channel_id = CHANNEL_ID
    sm.channel_name = "BBC News"
    sm.buffering = True
    sm.buffering_timeout = buffering_timeout
    sm.buffering_speed = buffering_speed
    sm.buffering_start_time = 0.0
    sm.needs_reconnect = False
    sm.needs_stream_switch = False
    sm.pending_buffering_failover_duration = None
    sm.failover_init_grace_period = 30
    sm.running = True
    sm.connected = True
    sm.stop_requested = False
    sm.url_switching = False
    sm._rotation_generation = 0
    sm._stable_connection_threshold = 30
    sm.last_data_time = 0.0

    # Avoid the bitrate-to-DB flush path; these tests only care about state.
    sm.current_stream_id = None
    sm._bitrate_warmup_samples = 10
    sm._smoothed_output_bitrate = None
    sm._last_bitrate_db_save_time = 0
    sm._bitrate_db_save_interval = 60

    buffer = MagicMock()
    buffer.redis_client = redis_client
    buffer.channel_id = CHANNEL_ID
    buffer.index = 0
    buffer.published_index = 0
    sm.buffer = buffer
    sm._reset_source_state()
    sm._current_source_has_media = True
    return sm


class BufferingTimeoutFailoverStateTests(TestCase):
    @patch.object(StreamManager, "_update_ffmpeg_stats_in_redis")
    def test_buffering_timeout_requests_switch_but_keeps_buffering(
        self, _update_stats
    ):
        redis = _DictRedis()
        metadata_key = RedisKeys.channel_metadata(CHANNEL_ID)
        redis.hashes[metadata_key] = {
            ChannelMetadataField.STATE: ChannelState.BUFFERING,
        }
        sm = _make_stream_manager(redis)

        with patch("apps.proxy.live_proxy.input.manager.time") as mock_time:
            mock_time.time.return_value = 10.0
            with patch("apps.proxy.live_proxy.input.manager.log_system_event"):
                sm._parse_ffmpeg_stats(
                    "frame=100 fps=30 q=28.0 size=1024kB time=00:00:03.00 "
                    "bitrate=500.0kbits/s speed=0.5x"
                )

        self.assertTrue(sm.buffering)
        self.assertTrue(sm.needs_stream_switch)
        self.assertFalse(sm.needs_reconnect)
        self.assertEqual(sm.pending_buffering_failover_duration, 10.0)
        self.assertEqual(
            redis.hashes[metadata_key][ChannelMetadataField.STATE],
            ChannelState.BUFFERING,
        )

    @patch.object(StreamManager, "_update_ffmpeg_stats_in_redis")
    def test_failover_progress_does_not_clear_buffering_before_media(
        self, _update_stats
    ):
        redis = _DictRedis()
        metadata_key = RedisKeys.channel_metadata(CHANNEL_ID)
        redis.hashes[metadata_key] = {
            ChannelMetadataField.STATE: ChannelState.BUFFERING,
        }
        sm = _make_stream_manager(redis)
        sm.failover_started_at = 1.0
        sm.pending_buffering_failover_duration = 5.0

        sm._parse_ffmpeg_stats(
            "frame=200 fps=30 q=28.0 size=2048kB time=00:00:06.00 "
            "bitrate=700.0kbits/s speed=1.05x"
        )

        self.assertTrue(sm.buffering)
        self.assertEqual(sm.pending_buffering_failover_duration, 5.0)
        self.assertEqual(
            redis.hashes[metadata_key][ChannelMetadataField.STATE],
            ChannelState.BUFFERING,
        )

    def test_first_accepted_failover_data_clears_buffering(self):
        redis = _DictRedis()
        metadata_key = RedisKeys.channel_metadata(CHANNEL_ID)
        redis.hashes[metadata_key] = {
            ChannelMetadataField.STATE: ChannelState.BUFFERING,
        }
        sm = _make_stream_manager(redis)
        sm.failover_started_at = 1.0
        sm.pending_buffering_failover_duration = 5.0
        sm._current_source_has_media = False
        sm._fail_fast_candidate = True

        calls = 0

        def fake_fetch_chunk():
            nonlocal calls
            calls += 1
            if calls == 1:
                sm.buffer.index = 1
                sm.buffer.published_index = 1
                return True
            sm.needs_reconnect = True
            return False

        sm.fetch_chunk = fake_fetch_chunk

        with patch("apps.proxy.live_proxy.input.manager.log_system_event"):
            sm._process_stream_data()

        self.assertFalse(sm.buffering)
        self.assertIsNone(sm.buffering_start_time)
        self.assertIsNone(sm.failover_started_at)
        self.assertIsNone(sm.pending_buffering_failover_duration)
        self.assertTrue(sm._current_source_has_media)
        self.assertFalse(sm._fail_fast_candidate)
        self.assertEqual(
            redis.hashes[metadata_key][ChannelMetadataField.STATE],
            ChannelState.ACTIVE,
        )

    def test_failover_candidate_without_data_keeps_buffering(self):
        redis = _DictRedis()
        metadata_key = RedisKeys.channel_metadata(CHANNEL_ID)
        redis.hashes[metadata_key] = {
            ChannelMetadataField.STATE: ChannelState.BUFFERING,
        }
        sm = _make_stream_manager(redis)
        sm.failover_started_at = 1.0
        sm.pending_buffering_failover_duration = 5.0
        sm.failover_init_grace_period = 1.0
        sm.fetch_chunk = MagicMock(return_value=False)

        with patch(
            "apps.proxy.live_proxy.input.manager.time.monotonic",
            return_value=3.0,
        ):
            sm._process_stream_data()

        self.assertTrue(sm.buffering)
        self.assertTrue(sm.needs_stream_switch)
        self.assertEqual(sm.pending_buffering_failover_duration, 5.0)
        self.assertEqual(
            redis.hashes[metadata_key][ChannelMetadataField.STATE],
            ChannelState.BUFFERING,
        )
