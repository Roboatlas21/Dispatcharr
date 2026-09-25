"""Stream connection management for TS proxy"""

import threading
import time
import socket
import requests
import subprocess
import gevent
import re
from django.db import connection, close_old_connections
from apps.proxy.config import TSConfig as Config
from apps.channels.models import Channel, Stream
from core.utils import log_system_event
from .buffer import StreamBuffer
from ..utils import detect_stream_type, get_logger
from ..redis_keys import RedisKeys
from ..constants import ChannelState, EventType, StreamType, ChannelMetadataField, TS_PACKET_SIZE
from ..config_helper import ConfigHelper
from ..url_utils import get_alternate_streams, get_stream_info_for_switch, get_stream_object
from ..utils import resolve_channel_display_name

logger = get_logger()

class StreamManager:
    """Manages a connection to a TS stream without using raw sockets"""

    def __init__(
        self,
        channel_id,
        url,
        buffer,
        user_agent=None,
        transcode=False,
        stream_id=None,
        worker_id=None,
        channel_name=None,
        source_authority=None, m3u_profile_id=None, stream_name=None, stream_profile=None,
    ):
        # Basic properties
        self.channel_id = channel_id
        # Prefer caller/Redis name so construction never checks out a geventpool slot.
        redis_client = getattr(buffer, "redis_client", None)
        self.channel_name = resolve_channel_display_name(
            channel_id, channel_name=channel_name, redis_client=redis_client
        )
        self.url = url
        self.buffer = buffer
        self.running = True
        self.connected = False
        self.retry_count = 0
        self.max_retries = ConfigHelper.max_retries()
        self._retry_window_seconds = ConfigHelper.retry_window_seconds()
        self._last_failure_time = None
        self._stable_connection_threshold = ConfigHelper.stable_connection_threshold()
        self.current_response = None
        self.current_session = None
        self.url_switching = False
        self.url_switch_start_time = 0
        self.url_switch_timeout = ConfigHelper.url_switch_timeout()
        self.buffering = False
        self.buffering_timeout = ConfigHelper.buffering_timeout()
        self.failover_init_grace_period = ConfigHelper.failover_init_grace_period()
        self.upstream_read_timeout = ConfigHelper.upstream_read_timeout()
        self.min_failover_rotation_interval = ConfigHelper.min_failover_rotation_interval()
        self.buffering_speed = ConfigHelper.buffering_speed()
        self.buffering_start_time = None
        self.pending_buffering_failover_duration = None
        # Store worker_id for ownership checks
        self.worker_id = worker_id
        self.source_authority = source_authority
        self._switch_lock = threading.Lock()
        self.metadata_updated = False
        self.m3u_profile_id = int(m3u_profile_id) if m3u_profile_id is not None else None
        self.stream_name = stream_name
        self.stream_profile = stream_profile

        # Sockets used for transcode jobs
        self.socket = None
        self.transcode = transcode
        self.transcode_process = None

        # User agent for connection
        self.user_agent = Config.DEFAULT_USER_AGENT if user_agent is None else user_agent

        # Stream health monitoring
        self.last_data_time = time.time()
        self.healthy = True
        self.health_check_interval = ConfigHelper.get('HEALTH_CHECK_INTERVAL', 5)
        self.chunk_size = ConfigHelper.chunk_size()

        # Recovery flags the health monitor raises for the main loop.
        self.needs_reconnect = False
        self.needs_stream_switch = False
        self.last_health_action_time = 0

        # Add to your __init__ method
        self._buffer_check_timers = []
        self.stopping = False
        self.stop_requested = False

        # Add tracking for tried streams and current stream
        self.current_stream_id = int(stream_id) if stream_id is not None else None
        self.tried_stream_ids = set()

        # Full-list exhaustion wraps (capped by MAX_STREAM_SWITCHES).
        self._rotation_started_at = time.monotonic()
        self._rotation_generation = 0
        self._reset_source_state()

        if stream_id:
            self.tried_stream_ids.add(self.current_stream_id)
            logger.info(f"Initialized stream manager for channel {buffer.channel_id} with stream ID {stream_id}")
        else:
            # Try to get stream ID from Redis metadata if available
            if hasattr(buffer, 'redis_client') and buffer.redis_client:
                try:
                    metadata_key = RedisKeys.channel_metadata(channel_id)

                    # Log all metadata for debugging purposes
                    metadata = buffer.redis_client.hgetall(metadata_key)
                    if metadata:
                        logger.debug(f"Redis metadata for channel {channel_id}: {metadata}")

                    # Try to get stream_id specifically
                    stream_id_bytes = buffer.redis_client.hget(metadata_key, "stream_id")
                    if stream_id_bytes:
                        self.current_stream_id = int(stream_id_bytes)
                        self.tried_stream_ids.add(self.current_stream_id)
                        logger.info(f"Loaded stream ID {self.current_stream_id} from Redis for channel {buffer.channel_id}")
                    else:
                        logger.warning(f"No stream_id found in Redis for channel {channel_id}. "
                                     f"Stream switching will rely on URL comparison to avoid selecting the same stream.")
                except Exception as e:
                    logger.warning(f"Error loading stream ID from Redis: {e}")
            else:
                logger.warning(f"Unable to get stream ID for channel {channel_id}. "
                             f"Stream switching will rely on URL comparison to avoid selecting the same stream.")

        logger.info(f"Initialized stream manager for channel {buffer.channel_id}")

        self.transcode_process_active = False

        # Track stream command for efficient log parser routing
        self.stream_command = None
        self.parser_type = None  # Will be set when transcode process starts

        # Add tracking for data throughput
        self.bytes_processed = 0
        self.last_bytes_update = time.time()

        # Cached result of the pipelined Redis ownership audit (hot read path).
        self._ownership_cache_valid_until = 0.0
        self._ownership_cached = True
        self._OWNERSHIP_CHECK_INTERVAL = 1.0
        self.bytes_update_interval = 5  # Update Redis every 5 seconds

        # Add stderr reader thread property
        self.stderr_reader_thread = None
        self.ffmpeg_input_phase = True  # Track if we're still reading input info

        # Add HTTP reader thread property
        self.http_reader = None

        # Output bitrate smoothing / throttled DB persistence
        self._smoothed_output_bitrate = None
        self._last_bitrate_db_save_time = 0
        self._bitrate_db_save_interval = 30  # seconds between DB writes
        self._bitrate_warmup_samples = 10   # discard first N samples while EMA stabilizes (~5s)

    def _create_session(self):
        """Create and configure requests session with optimal settings"""
        session = requests.Session()

        # Configure session headers
        session.headers.update({
            'User-Agent': self.user_agent,
            'Connection': 'keep-alive'
        })

        # Set up connection pooling for better performance
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=1,     # Single connection for this stream
            pool_maxsize=1,         # Max size of connection pool
            max_retries=3,          # Auto-retry for failed requests
            pool_block=False        # Don't block when pool is full
        )

        # Apply adapter to both HTTP and HTTPS
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        return session

    def _record_connection_failure(self):
        """Record a failure; reset the counter if the last one was long ago."""
        now = time.time()
        if (
            self._last_failure_time is not None
            and (now - self._last_failure_time) > self._retry_window_seconds
        ):
            self.retry_count = 0
        self._last_failure_time = now
        self.retry_count += 1
        return self.retry_count

    def _clear_connection_failure_history(self):
        self.retry_count = 0
        self._last_failure_time = None

    def _reset_source_state(self):
        """Reset media readiness without changing rotation or event history."""
        self._current_source_has_media = False
        self._source_published_index = self.buffer.published_index
        self._fail_fast_candidate = False
        self._recovering_established_source = False
        self._attempt_media_started_at = None
        self._attempt_last_media_at = None
        self._attempt_was_stable = False
        self.failover_started_at = None

    def _failover_startup_expired(self):
        if (self.failover_started_at is None
                or time.monotonic() - self.failover_started_at < self.failover_init_grace_period):
            return False
        logger.warning(
            "Failover stream did not publish a buffer chunk within %ss for channel %s; trying next stream",
            self.failover_init_grace_period, self.channel_id,
        )
        self.needs_stream_switch = True
        return True

    def _note_stable_connection(self):
        """Reset stream-switch bookkeeping after sustained successful playback."""
        if self.current_stream_id:
            self.tried_stream_ids = {self.current_stream_id}
        else:
            self.tried_stream_ids.clear()
        self._rotation_started_at = None
        self._recovering_established_source = False
        self._clear_connection_failure_history()

    def _selection_status(self, generation):
        """True: proceed; None: manual change superseded us; False: stopped."""
        if not self.running or self.stop_requested or not self._ensure_owner_or_stop():
            return False
        # Ownership checks can yield; inspect the generation afterward as well.
        if generation != self._rotation_generation or self.url_switching:
            return None
        return True

    def _sleep_interruptible(self, seconds, generation):
        """Use monotonic time and validate cancellation even for a zero wait."""
        deadline = time.monotonic() + seconds
        while self._selection_status(generation) is True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            gevent.sleep(min(0.25, remaining))
        return False

    def _rotation_interval_remaining(self):
        if self._rotation_started_at is None:
            return 0.0
        elapsed = time.monotonic() - self._rotation_started_at
        return max(0.0, self.min_failover_rotation_interval - elapsed)

    def _try_next_stream_with_rotation_interval(self):
        """Return True on selection, None on supersession, False on exhaustion/stop."""
        generation = self._rotation_generation
        status = self._selection_status(generation)
        if status is not True:
            return status
        if self._rotation_started_at is None:
            self._rotation_started_at = time.monotonic()

        result = self._try_next_stream()
        if result is not False:
            return result
        remaining = self._rotation_interval_remaining()
        if remaining > 0:
            logger.info(f"Waiting {remaining:.1f}s for rotation interval on {self.channel_id}")
        if not self._sleep_interruptible(remaining, generation):
            return self._selection_status(generation)

        self._rotation_started_at = time.monotonic()
        return self._try_next_stream(new_rotation=True)

    def _wait_for_existing_processes_to_close(self, timeout=5.0):
        """Wait for existing processes/connections to fully close before establishing new ones"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            # Check if transcode process is still running
            if self.transcode_process and self.transcode_process.poll() is None:
                logger.debug(f"Waiting for existing transcode process to terminate for channel {self.channel_id}")
                gevent.sleep(0.1)
                continue

            # Check if HTTP connections are still active
            if self.current_response or self.current_session:
                logger.debug(f"Waiting for existing HTTP connections to close for channel {self.channel_id}")
                gevent.sleep(0.1)
                continue

            # Check if socket is still active
            if self.socket:
                logger.debug(f"Waiting for existing socket to close for channel {self.channel_id}")
                gevent.sleep(0.1)
                continue

            # All processes/connections are closed
            logger.debug(f"All existing processes closed for channel {self.channel_id}")
            return True

        # Timeout reached
        logger.warning(f"Timeout waiting for existing processes to close for channel {self.channel_id} after {timeout}s")
        return False

    def _invalidate_ownership_cache(self):
        self._ownership_cache_valid_until = 0.0

    @staticmethod
    def _decode_redis_value(value):
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode()
        return value

    def _disconnect_shutdown_ready(self, disconnect_value):
        """True when last-client disconnect has passed the configured shutdown delay."""
        if not disconnect_value:
            return False

        shutdown_delay = ConfigHelper.channel_shutdown_delay()
        if shutdown_delay <= 0:
            return True

        disconnect_value = self._decode_redis_value(disconnect_value)
        try:
            disconnect_time = float(disconnect_value)
        except (ValueError, TypeError):
            return False
        return (time.time() - disconnect_time) >= shutdown_delay

    def _evaluate_ownership_from_redis(self, redis_client):
        """Single pipelined Redis round-trip for the full ownership audit."""
        stop_key = RedisKeys.channel_stopping(self.channel_id)
        metadata_key = RedisKeys.channel_metadata(self.channel_id)
        clients_key = RedisKeys.clients(self.channel_id)
        owner_key = RedisKeys.channel_owner(self.channel_id)
        disconnect_key = RedisKeys.last_client_disconnect(self.channel_id)

        pipe = redis_client.pipeline(transaction=False)
        pipe.exists(stop_key)
        pipe.exists(metadata_key)
        pipe.scard(clients_key)
        pipe.get(owner_key)
        pipe.get(disconnect_key)
        pipe.hget(metadata_key, ChannelMetadataField.STATE)
        (
            stop_exists,
            metadata_exists,
            client_count,
            current_owner,
            disconnect_value,
            state_raw,
        ) = pipe.execute()

        if stop_exists:
            return False

        if not metadata_exists:
            logger.warning(
                f"Channel {self.channel_id} metadata removed from Redis - stopping upstream"
            )
            return False

        client_count = client_count or 0
        state = self._decode_redis_value(state_raw)
        current_owner = self._decode_redis_value(current_owner)

        if current_owner and current_owner != self.worker_id:
            return False

        if not current_owner:
            if client_count == 0 and state not in ChannelState.PRE_ACTIVE:
                logger.warning(
                    f"Channel {self.channel_id} has no owner and no clients - stopping upstream"
                )
                return False
            return True

        if client_count == 0 and self._disconnect_shutdown_ready(disconnect_value):
            logger.info(
                f"Channel {self.channel_id} disconnect shutdown ready - stopping upstream"
            )
            return False

        return True

    def _still_owner(self, *, force=False):
        """Return True while this worker should keep the upstream connection open."""
        if self.stopping or self.stop_requested:
            return False

        if not self.worker_id:
            return True

        redis_client = getattr(self.buffer, 'redis_client', None)
        if not redis_client:
            return True

        now = time.time()
        if not force and now < self._ownership_cache_valid_until:
            return self._ownership_cached

        try:
            result = self._evaluate_ownership_from_redis(redis_client)
            self._ownership_cached = result
            self._ownership_cache_valid_until = now + self._OWNERSHIP_CHECK_INTERVAL
            return result
        except Exception as e:
            logger.debug(f"Ownership check failed for channel {self.channel_id}: {e}")
            return True

    def _upstream_may_continue(self):
        """
        Per-chunk gate for the hot read path.

        Local stop flags are checked every chunk. The coordinated-teardown Redis
        flag is checked every chunk (one EXISTS). The full ownership audit is
        pipelined and cached for ~1s — enough for loop boundaries while avoiding
        6+ Redis round-trips per chunk during steady streaming.
        """
        if self.stopping or self.stop_requested or not self.running:
            return False
        if self.buffer is not None and self.buffer.stopping:
            return False

        redis_client = getattr(self.buffer, 'redis_client', None)
        if redis_client and self.worker_id:
            try:
                if redis_client.exists(RedisKeys.channel_stopping(self.channel_id)):
                    self._ownership_cached = False
                    self._ownership_cache_valid_until = 0.0
                    return False
            except Exception as e:
                logger.debug(
                    f"Channel stopping check failed for {self.channel_id}: {e}"
                )

        return self._still_owner()

    def _ensure_owner_or_stop(self):
        if self._still_owner(force=True):
            return True

        logger.warning(
            f"Stream manager for channel {self.channel_id} lost ownership "
            f"(worker {self.worker_id}) - stopping upstream"
        )
        self.stop()
        return False

    def run(self):
        """Main execution loop using HTTP streaming with improved connection handling and stream switching"""
        # Add a stop flag to the class properties
        self.stop_requested = False
        # Add tracking for stream switching attempts
        stream_switch_attempts = 0
        # Get max stream switches from config using the helper method
        max_stream_switches = ConfigHelper.max_stream_switches()  # Prevent infinite switching loops

        try:


            # Start health monitor thread
            health_thread = threading.Thread(target=self._monitor_health, daemon=True)
            health_thread.start()

            logger.info(f"Starting stream for URL: {self.url} for channel {self.channel_id}")

            # Main stream switching loop - we'll try different streams if needed
            while self.running:
                close_old_connections()
                if not self._ensure_owner_or_stop():
                    break
                if self._switch_lock.locked():
                    gevent.sleep(0.1)
                    continue  # Existing reads continue; new connections wait for preparation.
                # Check for stuck switching state
                if (self.url_switching and not self._switch_lock.locked()
                        and time.time() - self.url_switch_start_time > self.url_switch_timeout):
                    logger.warning(f"URL switching state appears stuck for channel {self.channel_id} "
                                 f"({time.time() - self.url_switch_start_time:.1f}s > {self.url_switch_timeout}s timeout). "
                                 f"Resetting switching state.")
                    self._reset_url_switching_state()

                # NEW: Check for health monitor recovery requests
                if hasattr(self, 'needs_reconnect') and self.needs_reconnect and not self.url_switching:
                    logger.info(f"Health monitor requested reconnect for channel {self.channel_id}")
                    self.needs_reconnect = False
                    if self._current_source_has_media and not self._recovering_established_source:
                        self._recovering_established_source = True
                        self._clear_connection_failure_history()
                    self._close_socket()

                if hasattr(self, 'needs_stream_switch') and self.needs_stream_switch and not self.url_switching:
                    logger.info(f"Stream switch requested for channel {self.channel_id}")
                    self.needs_stream_switch = False

                    if stream_switch_attempts >= max_stream_switches:
                        logger.error(f"Maximum stream switches ({max_stream_switches}) reached for channel {self.channel_id}")
                        break

                    switch_result = self._try_next_stream_with_rotation_interval()
                    if switch_result is None:
                        stream_switch_attempts = 0
                        continue
                    if switch_result:
                        logger.info(f"Requested stream switch successful for channel {self.channel_id}")
                        stream_switch_attempts += 1
                        self._clear_connection_failure_history()
                        continue  # Go back to main loop with new stream
                    else:
                        logger.error(f"Requested stream switch failed for channel {self.channel_id}")
                        break

                # Check stream type before connecting
                self.stream_type = detect_stream_type(self.url)
                if self.transcode == False and self.stream_type in (StreamType.HLS, StreamType.RTSP, StreamType.UDP):
                    stream_type_name = "HLS" if self.stream_type == StreamType.HLS else ("RTSP/RTP" if self.stream_type == StreamType.RTSP else "UDP")
                    logger.info(f"Detected {stream_type_name} stream: {self.url} for channel {self.channel_id}")
                    logger.info(f"{stream_type_name} streams require FFmpeg for channel {self.channel_id}")
                    # Enable transcoding for HLS, RTSP/RTP, and UDP streams
                    self.transcode = True
                    # We'll override the stream profile selection with ffmpeg in the transcoding section
                    self.force_ffmpeg = True
                url_failed = False
                if self.url_switching:
                    logger.debug(f"Skipping connection attempt during URL switch for channel {self.channel_id}")
                    gevent.sleep(0.1)
                    continue
                # A manual change must not let the old attempt charge the new source.
                source_generation = self._rotation_generation
                # Connection retry loop for current URL
                while (
                    self.running
                    and source_generation == self._rotation_generation
                    and not self.url_switching
                    and self.retry_count < self.max_retries
                    and not url_failed
                    and not self.needs_stream_switch
                ):
                    if not self._ensure_owner_or_stop():
                        break
                    if source_generation != self._rotation_generation or self.url_switching:
                        break

                    if self._failover_startup_expired():
                        break

                    if self._rotation_started_at is None:
                        self._rotation_started_at = time.monotonic()
                    attempt = self.retry_count + 1
                    attempt_limit = 1 if self._fail_fast_candidate and not self._current_source_has_media else self.max_retries
                    self._attempt_media_started_at = None
                    self._attempt_last_media_at = None
                    self._attempt_was_stable = False
                    logger.info(
                        f"Connection attempt {attempt}/{attempt_limit} "
                        f"for URL: {self.url} for channel {self.channel_id}"
                    )

                    # Handle connection based on whether we transcode or not
                    connection_result = False
                    connection_error = None
                    try:
                        if self.transcode:
                            connection_result = self._establish_transcode_connection()
                        else:
                            connection_result = self._establish_http_connection()

                        if source_generation != self._rotation_generation or self.url_switching:
                            break
                        if connection_result:
                            # Store connection start time to measure success duration
                            connection_start_time = time.time()

                            # Log reconnection event if this is a retry (not first attempt)
                            if self.retry_count > 0:
                                try:
                                    log_system_event(
                                        'channel_reconnect',
                                        channel_id=self.channel_id,
                                        channel_name=self.channel_name,
                                        attempt=attempt,
                                        max_attempts=self.max_retries
                                    )
                                except Exception as e:
                                    logger.error(f"Could not log reconnection event: {e}")

                            # Successfully connected - read stream data until disconnect/error
                            self._process_stream_data()
                            # If we get here, the connection was closed/failed
                            if source_generation != self._rotation_generation or self.url_switching:
                                break

                            connection_duration = time.time() - connection_start_time
                            if self._attempt_was_stable:
                                logger.info(
                                    f"Stream reached the stable-media threshold, "
                                    f"resetting switch rotation state for channel: {self.channel_id}"
                                )
                                self._note_stable_connection()
                                stream_switch_attempts = 0

                            if self.needs_stream_switch:
                                logger.info(f"Stream needs to switch after {connection_duration:.1f} seconds for channel: {self.channel_id}")
                                break  # Exit to switch streams


                        # Connection failed or ended - decide what to do next
                        if self.stop_requested or not self.running:
                            # Normal shutdown requested
                            return

                        if self.needs_reconnect:
                            # Health monitor asked for a same-URL reconnect. Clear the
                            # flag and tear the old socket down so the next establish
                            # does not orphan the reader thread, then fall through to
                            # the normal failure accounting. Repeated health reconnects
                            # count toward max_retries like any other URL failure.
                            self.needs_reconnect = False
                            logger.info(
                                f"Health monitor requested reconnect for channel: {self.channel_id}"
                            )
                            self._close_socket()

                    except Exception as e:
                        logger.error(f"Connection error on channel: {self.channel_id}: {e}", exc_info=True)
                        connection_error = str(e)[:200]

                    if source_generation != self._rotation_generation or self.url_switching:
                        break
                    if self.stop_requested or not self.running:
                        return
                    if self.needs_stream_switch:
                        break
                    self.connected = False

                    # A newly selected alternate gets one attempt until it
                    # contributes accepted media to the shared buffer.
                    if self._fail_fast_candidate and not self._current_source_has_media:
                        self.needs_stream_switch = True
                        break

                    # The connection that was already playing is not one of
                    # the configured recovery attempts. Start a fresh budget.
                    # Short-lived recovery attempts do not reset it again.
                    if self._current_source_has_media and not self._recovering_established_source:
                        self._recovering_established_source = True
                        if self._rotation_started_at is None:
                            self._rotation_started_at = time.monotonic()
                        self._clear_connection_failure_history()
                        gevent.sleep(0.25)
                        continue

                    failures = self._record_connection_failure()

                    if failures >= self.max_retries:
                        url_failed = True
                        logger.warning(
                            f"Maximum retry attempts ({self.max_retries}) reached for URL: {self.url} "
                            f"for channel: {self.channel_id}"
                        )

                        error_details = {"error_type": "connection_failed"}
                        if connection_error is not None:
                            error_details.update(error_type="connection_exception", error_message=connection_error)
                        try:
                            log_system_event(
                                'channel_error',
                                channel_id=self.channel_id,
                                channel_name=self.channel_name,
                                url=self.url[:100] if self.url else None,
                                attempts=self.max_retries,
                                **error_details,
                            )
                        except Exception as e:
                            logger.error(f"Could not log connection error event: {e}")
                    else:
                        # Wait with exponential backoff before retrying
                        timeout = min(.25 * failures, 3)  # Cap at 3 seconds
                        logger.info(
                            f"Reconnecting in {timeout} seconds... "
                            f"(attempt {failures}/{self.max_retries}) "
                            f"for channel: {self.channel_id}"
                        )
                        gevent.sleep(timeout)

                if source_generation != self._rotation_generation or self.url_switching:
                    stream_switch_attempts = 0
                    continue
                # If URL failed and we're still running, try switching to another stream
                if url_failed and self.running:
                    logger.info(f"URL {self.url} failed after {self.retry_count} attempts, trying next stream for channel: {self.channel_id}")

                    if stream_switch_attempts >= max_stream_switches:
                        logger.error(f"Maximum stream switches ({max_stream_switches}) reached for channel {self.channel_id}")
                        break

                    switch_result = self._try_next_stream_with_rotation_interval()
                    if switch_result is None:
                        stream_switch_attempts = 0
                        continue
                    if switch_result:
                        # Successfully switched to a new stream, continue with the new URL
                        stream_switch_attempts += 1
                        logger.info(f"Successfully switched to new URL: {self.url} (switch attempt {stream_switch_attempts}/{max_stream_switches}) for channel: {self.channel_id}")
                        self._clear_connection_failure_history()
                        # Continue outer loop with new URL - DON'T add a break statement here
                    else:
                        # No more streams to try
                        logger.error(f"Failed to find alternative streams after {stream_switch_attempts} attempts for channel: {self.channel_id}")
                        break
                elif not self.running:
                    # Normal shutdown was requested
                    break

        except Exception as e:
            logger.error(f"Stream error: {e}", exc_info=True)
        finally:
            try:
                from ..server import ProxyServer
                ProxyServer.get_instance()._live_stream_managers.pop(self.channel_id, None)
            except Exception:
                pass

            # Enhanced cleanup in the finally block
            self.connected = False

            # Explicitly cancel all timers
            for timer in list(self._buffer_check_timers):
                try:
                    if timer and timer.is_alive():
                        timer.cancel()
                except Exception:
                    pass

            self._buffer_check_timers.clear()

            # Make sure transcode process is terminated
            if self.transcode_process_active:
                logger.info(f"Ensuring transcode process is terminated in finally block for channel: {self.channel_id}")
                self._close_socket()

            # Close all connections
            self._close_all_connections()

            # Transition to ERROR so clients stop waiting. Ownership may have
            # expired during retries, so fall back to a state guard when no
            # owner exists — but never clobber a new owner's active stream.
            if hasattr(self.buffer, 'redis_client') and self.buffer.redis_client:
                try:
                    metadata_key = RedisKeys.channel_metadata(self.channel_id)
                    owner_key = RedisKeys.channel_owner(self.channel_id)
                    current_owner = self._decode_redis_value(
                        self.buffer.redis_client.get(owner_key)
                    )

                    is_owner = (
                        current_owner
                        and self.worker_id
                        and current_owner == self.worker_id
                    )
                    no_owner = current_owner is None

                    should_update = is_owner
                    if not should_update and no_owner:
                        current_state = self._decode_redis_value(
                            self.buffer.redis_client.hget(
                                metadata_key, ChannelMetadataField.STATE
                            )
                        )
                        should_update = current_state in ChannelState.PRE_ACTIVE
                        if not should_update and current_state:
                            logger.info(
                                f"Channel {self.channel_id} has no owner but "
                                f"state is {current_state} — skipping ERROR update"
                            )

                    if should_update:
                        if self.tried_stream_ids and len(self.tried_stream_ids) > 0:
                            error_message = f"All {len(self.tried_stream_ids)} stream options failed"
                        else:
                            error_message = f"Connection failed after {self.max_retries} attempts"

                        update_data = {
                            ChannelMetadataField.STATE: ChannelState.ERROR,
                            ChannelMetadataField.STATE_CHANGED_AT: str(time.time()),
                            ChannelMetadataField.ERROR_MESSAGE: error_message,
                            ChannelMetadataField.ERROR_TIME: str(time.time())
                        }
                        self.buffer.redis_client.hset(metadata_key, mapping=update_data)
                        logger.info(
                            f"Updated channel {self.channel_id} state to ERROR "
                            f"in Redis after stream failure "
                            f"(owner={'self' if is_owner else 'expired'})"
                        )

                        # Signal clients to disconnect
                        stop_key = RedisKeys.channel_stopping(self.channel_id)
                        self.buffer.redis_client.setex(stop_key, 60, "true")
                except Exception as e:
                    logger.error(f"Failed to update channel state in Redis: {e} for channel {self.channel_id}", exc_info=True)

            # Close database connection for this thread
            try:
                connection.close()
            except Exception:
                pass

            logger.info(f"Stream manager stopped for channel {self.channel_id}")

    def _establish_transcode_connection(self):
        """Establish a connection using transcoding"""
        try:
            logger.debug(f"Building transcode command for channel {self.channel_id}")

            # Check if we already have a running transcode process
            if self.transcode_process and self.transcode_process.poll() is None:
                logger.info(f"Existing transcode process found for channel {self.channel_id}, closing before establishing new connection")
                self._close_socket()

                # Wait for the process to fully terminate
                if not self._wait_for_existing_processes_to_close():
                    logger.error(f"Failed to close existing transcode process for channel {self.channel_id}")
                    return False

            # Also check for any lingering HTTP connections
            if self.current_response or self.current_session:
                logger.debug(f"Closing existing HTTP connections before establishing transcode connection for channel {self.channel_id}")
                self._close_connection()

            try:
                channel = get_stream_object(self.channel_id)

                # Use FFmpeg specifically for HLS streams
                if hasattr(self, 'force_ffmpeg') and self.force_ffmpeg:
                    from core.models import StreamProfile
                    try:
                        stream_profile = StreamProfile.objects.get(name='ffmpeg', locked=True)
                        logger.info("Using FFmpeg stream profile for unsupported proxy content (HLS/RTSP/UDP)")
                    except StreamProfile.DoesNotExist:
                        # Fall back to channel's profile if FFmpeg not found
                        stream_profile = channel.get_stream_profile()
                        logger.warning(f"FFmpeg profile not found, using channel default profile for channel: {self.channel_id}")
                else:
                    stream_profile = channel.get_stream_profile()

                # Build and start transcode command
                self.transcode_cmd = stream_profile.build_command(self.url, self.user_agent, channel.id)

                # Store stream command for efficient log parser routing
                self.stream_command = stream_profile.command
                # Map actual commands to parser types for direct routing
                command_to_parser = {
                    'ffmpeg': 'ffmpeg',
                    'cvlc': 'vlc',
                    'vlc': 'vlc',
                    'streamlink': 'streamlink'
                }
                self.parser_type = command_to_parser.get(self.stream_command.lower())
                if self.parser_type:
                    logger.debug(f"Using {self.parser_type} parser for log parsing (command: {self.stream_command})")
                else:
                    logger.debug(f"Unknown stream command '{self.stream_command}', will use auto-detection for log parsing")

                # FFmpeg's network read deadline is separate from output readiness.
                if (self.upstream_read_timeout > 0 and self.stream_command.lower() == 'ffmpeg'
                        and self.url.lower().startswith(('http://', 'https://'))
                        and '-rw_timeout' not in self.transcode_cmd and '-i' in self.transcode_cmd):
                    input_index = self.transcode_cmd.index('-i')
                    self.transcode_cmd[input_index:input_index] = [
                        '-rw_timeout', str(int(self.upstream_read_timeout * 1_000_000)),
                    ]

                # For UDP streams, remove any user_agent parameters from the command
                if hasattr(self, 'stream_type') and self.stream_type == StreamType.UDP:
                    # Options and their values are separate tokens, even for an empty UA.
                    command = iter(self.transcode_cmd)
                    self.transcode_cmd = []
                    for arg in command:
                        option = arg.lower().split('=', 1)[0]
                        if option in ('-user_agent', '-user-agent', '--user-agent', '--http-user-agent'):
                            if '=' not in arg:
                                next(command, None)
                        elif option in ('-headers', '--http-header'):
                            value = arg.split('=', 1)[1] if '=' in arg else next(command, '')
                            headers = [line for line in value.splitlines()
                                       if not line.lstrip().lower().startswith(('user-agent:', 'user-agent='))]
                            if headers:
                                value = '\r\n'.join(headers) + '\r\n' if option == '-headers' else '\n'.join(headers)
                                self.transcode_cmd.extend([option, value])
                        else:
                            self.transcode_cmd.append(arg)
                    logger.debug(f"Removed user_agent parameters from UDP stream command for channel: {self.channel_id}")
            finally:
                # Release the pool slot before posix_spawn or before returning on profile errors.
                close_old_connections()

            logger.debug(f"Starting transcode process: {self.transcode_cmd} for channel: {self.channel_id}")

            import os as _os
            import shutil as _shutil
            import signal as _signal
            import time as _time

            relay_read, relay_write = _os.pipe()
            self.socket = _os.fdopen(relay_read, 'rb', buffering=0)
            stderr_read, stderr_write = _os.pipe()
            _stderr_read_transferred = False
            try:
                _t0 = _time.monotonic()

                # os.posix_spawn does not call pthread_atfork handlers, making
                # it safe to call directly from the hub's greenlet.  All
                # fork()-based approaches (subprocess.Popen, whether called
                # from the greenlet or a threadpool thread) hang in gevent's
                # _before_fork atfork handler indefinitely under gevent+uWSGI.
                _executable = _shutil.which(self.transcode_cmd[0]) or self.transcode_cmd[0]
                _pid = _os.posix_spawn(
                    _executable,
                    self.transcode_cmd,
                    _os.environ,
                    file_actions=[
                        (_os.POSIX_SPAWN_OPEN, 0, '/dev/null', _os.O_RDONLY, 0),
                        (_os.POSIX_SPAWN_DUP2, relay_write, 1),
                        (_os.POSIX_SPAWN_DUP2, stderr_write, 2),
                        (_os.POSIX_SPAWN_CLOSE, relay_write),
                        (_os.POSIX_SPAWN_CLOSE, stderr_write),
                    ],
                )
                logger.debug(
                    f"posix_spawn completed in {_time.monotonic() - _t0:.3f}s "
                    f"pid={_pid} for channel {self.channel_id}"
                )

                _stderr_file = _os.fdopen(stderr_read, 'rb', buffering=0)
                _stderr_read_transferred = True

                class _SpawnedProcess:
                    """Minimal Popen-compatible wrapper for a posix_spawn'd process."""
                    stdin = None
                    stdout = None

                    def __init__(self):
                        self.pid = _pid
                        self.returncode = None
                        self.stderr = _stderr_file

                    def _reap(self, status):
                        if _os.WIFEXITED(status):
                            self.returncode = _os.WEXITSTATUS(status)
                        elif _os.WIFSIGNALED(status):
                            self.returncode = -_os.WTERMSIG(status)
                        else:
                            self.returncode = -1

                    def poll(self):
                        if self.returncode is not None:
                            return self.returncode
                        try:
                            rpid, status = _os.waitpid(self.pid, _os.WNOHANG)
                            if rpid:
                                self._reap(status)
                        except ChildProcessError:
                            self.returncode = -1
                        return self.returncode

                    def wait(self, timeout=None):
                        if self.returncode is not None:
                            return self.returncode
                        import gevent as _gevent
                        deadline = _time.monotonic() + timeout if timeout is not None else None
                        while True:
                            try:
                                rpid, status = _os.waitpid(self.pid, _os.WNOHANG)
                            except ChildProcessError:
                                self.returncode = -1
                                return self.returncode
                            if rpid:
                                self._reap(status)
                                return self.returncode
                            if deadline is not None and _time.monotonic() >= deadline:
                                raise subprocess.TimeoutExpired(self.pid, timeout)
                            _gevent.sleep(0.01)

                    def kill(self):
                        try:
                            _os.kill(self.pid, _signal.SIGKILL)
                        except ProcessLookupError:
                            pass

                    def terminate(self):
                        try:
                            _os.kill(self.pid, _signal.SIGTERM)
                        except ProcessLookupError:
                            pass

                self.transcode_process = _SpawnedProcess()
            except Exception:
                if not _stderr_read_transferred:
                    _os.close(stderr_read)
                raise
            finally:
                _os.close(relay_write)
                _os.close(stderr_write)

            # Start a thread to read stderr
            self._start_stderr_reader()

            # Set flag that transcoding process is active
            self.transcode_process_active = True

            self.connected = True

            # Set connection start time for stability tracking
            self.connection_start_time = time.time()

            # Set channel state to waiting for clients
            self._set_waiting_for_clients()

            return True
        except Exception as e:
            logger.error(f"Error establishing transcode connection for channel: {self.channel_id}: {e}", exc_info=True)
            self._close_socket()
            return False

    def _start_stderr_reader(self):
        """Start a thread to read stderr from the transcode process"""
        if self.transcode_process and self.transcode_process.stderr:
            self.stderr_reader_thread = threading.Thread(
                target=self._read_stderr,
                daemon=True  # Use daemon thread so it doesn't block program exit
            )
            self.stderr_reader_thread.start()
            logger.debug(f"Started stderr reader thread for channel {self.channel_id}")

    def _read_stderr(self):
        """Read and log ffmpeg stderr output with real-time stats parsing"""
        import os as _os
        import select as _select
        import gevent

        try:
            stderr = self.transcode_process.stderr
            if not stderr:
                return
            stderr_fd = stderr.fileno()
            buf = b""

            while self.running and self.transcode_process and self.transcode_process.stderr:
                try:
                    ready, _, _ = _select.select([stderr_fd], [], [], 1.0)
                    if not ready:
                        if not self.running or not self.transcode_process:
                            break
                        continue

                    chunk = _os.read(stderr_fd, 4096)
                    if not chunk:
                        break

                    # Yield to the hub after each read so fetch_chunk and other
                    # greenlets can run. Without this, the byte-at-a-time loop
                    # monopolises the event loop during ffmpeg startup output,
                    # starving the data reader and preventing the buffer from filling.
                    gevent.sleep(0)

                    buf += chunk

                    while True:
                        cr = buf.find(b'\r')
                        nl = buf.find(b'\n')
                        if cr == -1 and nl == -1:
                            if len(buf) > 1024 and b"frame=" not in buf:
                                line_text = buf.decode('utf-8', errors='ignore').strip()
                                if line_text:
                                    self._log_stderr_content(line_text)
                                buf = b""
                            break
                        if cr != -1 and (nl == -1 or cr < nl):
                            line, buf = buf[:cr], buf[cr + 1:]
                        else:
                            line, buf = buf[:nl], buf[nl + 1:]
                        line_text = line.decode('utf-8', errors='ignore').strip()
                        if not line_text:
                            continue
                        if "frame=" in line_text:
                            self._parse_ffmpeg_stats(line_text)
                        self._log_stderr_content(line_text)

                except Exception as e:
                    logger.error(f"Error reading stderr for channel {self.channel_id}: {e}")
                    break

            if buf.strip():
                try:
                    remaining_text = buf.decode('utf-8', errors='ignore').strip()
                    if remaining_text:
                        if "frame=" in remaining_text:
                            self._parse_ffmpeg_stats(remaining_text)
                        self._log_stderr_content(remaining_text)
                except Exception as e:
                    logger.debug(f"Error processing remaining stderr buffer: {e}")

        except Exception as e:
            try:
                logger.error(f"Error in stderr reader thread for channel {self.channel_id}: {e}")
            except:
                pass
        finally:
            close_old_connections()

    def _log_stderr_content(self, content):
        """Log stderr content from FFmpeg with appropriate log levels"""
        try:
            content = content.strip()
            if not content:
                return

            # Convert to lowercase for easier matching
            content_lower = content.lower()
            # Check if we are still in the input phase
            if content_lower.startswith('input #') or 'decoder' in content_lower:
                self.ffmpeg_input_phase = True
            # Track FFmpeg phases - once we see output info, we're past input phase
            if content_lower.startswith('output #') or 'encoder' in content_lower:
                self.ffmpeg_input_phase = False

            # Route to appropriate parser based on known command type
            from ..services.log_parsers import LogParserFactory
            from ..services.channel_service import ChannelService

            parse_result = None

            # If we know the parser type, use direct routing for efficiency
            if self.parser_type:
                # Get the appropriate parser and check what it can parse
                parser = LogParserFactory._parsers.get(self.parser_type)
                if parser:
                    stream_type = parser.can_parse(content)
                    if stream_type == 'vlc_input_failed':
                        logger.warning(
                            f"VLC could not open input for channel {self.channel_id}: {content}"
                        )
                        self.connected = False
                        self._close_socket()
                    elif stream_type:
                        # Parser can handle this line, parse it directly
                        parsed_data = LogParserFactory.parse(stream_type, content)
                        if parsed_data:
                            parse_result = (stream_type, parsed_data)
            else:
                # Unknown command type - use auto-detection as fallback
                parse_result = LogParserFactory.auto_parse(content)

            if parse_result:
                stream_type, parsed_data = parse_result
                # For FFmpeg, only parse during input phase
                if stream_type in ['video', 'audio', 'input']:
                    if self.ffmpeg_input_phase:
                        ChannelService.parse_and_store_stream_info(self.channel_id, content, stream_type, self.current_stream_id)
                else:
                    # VLC and Streamlink can be parsed anytime
                    ChannelService.parse_and_store_stream_info(self.channel_id, content, stream_type, self.current_stream_id)

            # Determine log level based on content
            if any(keyword in content_lower for keyword in ['error', 'failed', 'cannot', 'invalid', 'corrupt']):
                logger.error(f"Stream process error for channel {self.channel_id}: {content}")
            elif any(keyword in content_lower for keyword in ['warning', 'deprecated', 'ignoring']):
                logger.warning(f"Stream process warning for channel {self.channel_id}: {content}")
            elif content.startswith('frame=') or 'fps=' in content or 'speed=' in content:
                # Stats lines - log at trace level to avoid spam
                logger.trace(f"Stream stats for channel {self.channel_id}: {content}")
            elif any(keyword in content_lower for keyword in ['input', 'output', 'stream', 'video', 'audio']):
                # Stream info - log at info level
                logger.info(f"Stream info for channel {self.channel_id}: {content}")
            else:
                # Everything else at debug level
                logger.debug(f"Stream process output for channel {self.channel_id}: {content}")

        except Exception as e:
            logger.error(f"Error logging stderr content for channel {self.channel_id}: {e}")

    def _parse_ffmpeg_stats(self, stats_line):
        """Parse FFmpeg stats line and extract speed, fps, and bitrate"""
        try:
            # Example FFmpeg stats line:
            # frame= 1234 fps= 30 q=28.0 size=    2048kB time=00:00:41.33 bitrate= 406.1kbits/s speed=1.02x

            # Extract speed (e.g., "speed=1.02x")
            speed_match = re.search(r'speed=\s*([0-9.]+)x?', stats_line)
            ffmpeg_speed = float(speed_match.group(1)) if speed_match else None

            # Extract fps (e.g., "fps= 30")
            fps_match = re.search(r'fps=\s*([0-9.]+)', stats_line)
            ffmpeg_fps = float(fps_match.group(1)) if fps_match else None

            # Extract bitrate (e.g., "bitrate= 406.1kbits/s")
            bitrate_match = re.search(r'bitrate=\s*([0-9.]+(?:\.[0-9]+)?)\s*([kmg]?)bits/s', stats_line, re.IGNORECASE)
            ffmpeg_output_bitrate = None
            if bitrate_match:
                bitrate_value = float(bitrate_match.group(1))
                unit = bitrate_match.group(2).lower()
                # Convert to kbps
                if unit == 'm':
                    bitrate_value *= 1000
                elif unit == 'g':
                    bitrate_value *= 1000000
                # If no unit or 'k', it's already in kbps
                ffmpeg_output_bitrate = bitrate_value

            # Calculate actual FPS
            actual_fps = None
            if ffmpeg_fps is not None and ffmpeg_speed is not None and ffmpeg_speed > 0:
                actual_fps = ffmpeg_fps / ffmpeg_speed
            # Store in Redis if we have valid data
            if any(x is not None for x in [ffmpeg_speed, ffmpeg_fps, actual_fps, ffmpeg_output_bitrate]):
                self._update_ffmpeg_stats_in_redis(ffmpeg_speed, ffmpeg_fps, actual_fps, ffmpeg_output_bitrate)

                # Update local EMA and periodically flush to database
                if ffmpeg_output_bitrate is not None and self.current_stream_id:
                    if self._bitrate_warmup_samples > 0:
                        # Discard early samples from the EMA
                        self._bitrate_warmup_samples -= 1
                    else:
                        if self._smoothed_output_bitrate is None:
                            self._smoothed_output_bitrate = ffmpeg_output_bitrate
                        else:
                            self._smoothed_output_bitrate = 0.9 * self._smoothed_output_bitrate + 0.1 * ffmpeg_output_bitrate

                        now = time.time()
                        if now - self._last_bitrate_db_save_time >= self._bitrate_db_save_interval:
                            from ..services.channel_service import ChannelService
                            ChannelService._update_stream_stats_in_db(
                                self.current_stream_id,
                                ffmpeg_output_bitrate=round(self._smoothed_output_bitrate, 1)
                            )
                            self._last_bitrate_db_save_time = now

            # Fix the f-string formatting
            actual_fps_str = f"{actual_fps:.1f}" if actual_fps is not None else "N/A"
            ffmpeg_output_bitrate_str = f"{ffmpeg_output_bitrate:.1f}" if ffmpeg_output_bitrate is not None else "N/A"
            # Log the stats
            logger.debug(f"FFmpeg stats for channel {self.channel_id}: - Speed: {ffmpeg_speed}x, FFmpeg FPS: {ffmpeg_fps}, "
                        f"Actual FPS: {actual_fps_str}, "
                        f"Output Bitrate: {ffmpeg_output_bitrate_str} kbps")
            # A failover candidate is judged by whether it produces data, not
            # by FFmpeg progress from the old/new process.
            if (
                self.url_switching
                or self.failover_started_at is not None
                or self.pending_buffering_failover_duration is not None
            ):
                return

            # If we have a valid speed, check for buffering
            if ffmpeg_speed is not None and ffmpeg_speed < self.buffering_speed:
                if self.buffering:
                    # Buffering is still ongoing, check for how long
                    if self.buffering_start_time is None:
                        self.buffering_start_time = time.time()
                    else:
                        buffering_duration = time.time() - self.buffering_start_time
                        if buffering_duration > self.buffering_timeout:
                            logger.error(f"Buffering timeout reached for channel {self.channel_id} after {buffering_duration:.1f} seconds")
                            self.pending_buffering_failover_duration = buffering_duration
                            self.needs_reconnect = False
                            self.needs_stream_switch = True
                else:
                    # Buffering just started, set the flag and start timer
                    self.buffering = True
                    self._attempt_media_started_at = None
                    self.buffering_start_time = time.time()
                    logger.warning(f"Buffering started for channel {self.channel_id} - speed: {ffmpeg_speed}x")

                    # Log system event for buffering
                    try:
                        log_system_event(
                            'channel_buffering',
                            channel_id=self.channel_id,
                            channel_name=self.channel_name,
                            speed=ffmpeg_speed
                        )
                    except Exception as e:
                        logger.error(f"Could not log buffering event: {e}")

                # Log buffering warning
                logger.debug(f"FFmpeg speed on channel {self.channel_id} is below {self.buffering_speed} ({ffmpeg_speed}x) - buffering detected")
                # Set channel state to buffering
                if hasattr(self.buffer, 'redis_client') and self.buffer.redis_client:
                    metadata_key = RedisKeys.channel_metadata(self.channel_id)
                    self.buffer.redis_client.hset(metadata_key, ChannelMetadataField.STATE, ChannelState.BUFFERING)
            elif ffmpeg_speed is not None and ffmpeg_speed >= self.buffering_speed:
                # Speed is good, check if we were buffering
                if self.buffering:
                    # Reset buffering state
                    logger.info(f"Buffering ended for channel {self.channel_id} - speed: {ffmpeg_speed}x")
                    self.buffering = False
                    self.buffering_start_time = None
                    # Set channel state to active if speed is good
                    if hasattr(self.buffer, 'redis_client') and self.buffer.redis_client:
                        metadata_key = RedisKeys.channel_metadata(self.channel_id)
                        self.buffer.redis_client.hset(metadata_key, ChannelMetadataField.STATE, ChannelState.ACTIVE)

        except Exception as e:
            logger.debug(f"Error parsing FFmpeg stats: {e}")

    def _update_ffmpeg_stats_in_redis(self, speed, fps, actual_fps, output_bitrate):
        """Update FFmpeg performance stats in Redis metadata"""
        try:
            if hasattr(self.buffer, 'redis_client') and self.buffer.redis_client:
                metadata_key = RedisKeys.channel_metadata(self.channel_id)
                update_data = {
                    ChannelMetadataField.FFMPEG_STATS_UPDATED: str(time.time())
                }

                if speed is not None:
                    update_data[ChannelMetadataField.FFMPEG_SPEED] = str(round(speed, 3))

                if fps is not None:
                    update_data[ChannelMetadataField.FFMPEG_FPS] = str(round(fps, 1))

                if actual_fps is not None:
                    update_data[ChannelMetadataField.ACTUAL_FPS] = str(round(actual_fps, 1))

                if output_bitrate is not None:
                    update_data[ChannelMetadataField.FFMPEG_OUTPUT_BITRATE] = str(round(output_bitrate, 1))

                self.buffer.redis_client.hset(metadata_key, mapping=update_data)

        except Exception as e:
            logger.error(f"Error updating FFmpeg stats in Redis: {e}")


    def _establish_http_connection(self):
        """Establish HTTP connection using thread-based reader (same as transcode path)"""
        try:
            logger.debug(f"Using HTTP streamer thread to connect to stream: {self.url}")

            # Check if we already have active HTTP connections
            if self.current_response or self.current_session:
                logger.info(f"Existing HTTP connection found for channel {self.channel_id}, closing before establishing new connection")
                self._close_connection()

                # Wait for connections to fully close
                if not self._wait_for_existing_processes_to_close():
                    logger.error(f"Failed to close existing HTTP connections for channel {self.channel_id}")
                    return False

            # Also check for any lingering transcode processes
            if self.transcode_process and self.transcode_process.poll() is None:
                logger.debug(f"Closing existing transcode process before establishing HTTP connection for channel {self.channel_id}")
                self._close_socket()

            # Use HTTPStreamReader to fetch stream and pipe to a readable file descriptor
            # This allows us to use the same fetch_chunk() path as transcode
            from .http_streamer import HTTPStreamReader

            # Create and start the HTTP stream reader
            self.http_reader = HTTPStreamReader(
                url=self.url,
                user_agent=self.user_agent,
                chunk_size=self.chunk_size
            )

            # Start the reader thread and get the read end of the pipe
            pipe_fd = self.http_reader.start()

            # Wrap the file descriptor in a file object (same as transcode stdout)
            import os
            self.socket = os.fdopen(pipe_fd, 'rb', buffering=0)
            self.connected = True
            self.healthy = True

            logger.info(f"Successfully started HTTP streamer thread for channel {self.channel_id}")

            # Store connection start time for stability tracking
            self.connection_start_time = time.time()

            # Set channel state to waiting for clients
            self._set_waiting_for_clients()

            return True

        except Exception as e:
            logger.error(f"Error establishing HTTP connection for channel {self.channel_id}: {e}", exc_info=True)
            self._close_socket()
            return False

    def _update_bytes_processed(self, chunk_size):
        """Update the total bytes processed in Redis metadata"""
        if not self._upstream_may_continue():
            return

        try:
            # Update local counter
            self.bytes_processed += chunk_size

            # Only update Redis periodically to reduce overhead
            now = time.time()
            if now - self.last_bytes_update >= self.bytes_update_interval:
                if hasattr(self.buffer, 'redis_client') and self.buffer.redis_client:
                    # Update channel metadata with total bytes
                    metadata_key = RedisKeys.channel_metadata(self.channel_id)

                    # Use hincrby to atomically increment the total_bytes field
                    self.buffer.redis_client.hincrby(metadata_key, ChannelMetadataField.TOTAL_BYTES, self.bytes_processed)

                    # Reset local counter after updating Redis
                    self.bytes_processed = 0
                    self.last_bytes_update = now

                    logger.debug(f"Updated {ChannelMetadataField.TOTAL_BYTES} in Redis for channel {self.channel_id}")
        except Exception as e:
            logger.error(f"Error updating bytes processed: {e}")

    def _process_stream_data(self):
        """Process stream data until disconnect or error - unified path for both transcode and HTTP"""
        generation = self._rotation_generation
        try:
            # Both transcode and HTTP now use the same subprocess/socket approach
            # This gives us perfect control: check flags between chunks, timeout just returns False
            while (self.running and self.connected and not self.stop_requested
                   and not self.needs_stream_switch and not self.needs_reconnect):
                success = self.fetch_chunk()
                if generation != self._rotation_generation or self.url_switching:
                    return  # Do not change the replacement's connected/media state.
                if self.stop_requested or not self.running or self.needs_stream_switch or self.needs_reconnect:
                    break
                if success:
                    self.last_data_time = time.time()
                # Partial input is activity, but only a new published chunk
                # establishes the source. Old buffer chunks cannot do so.
                if success and (self._current_source_has_media
                                or self.buffer.published_index > self._source_published_index):
                    now = time.monotonic()
                    if self.buffering:
                        self._attempt_media_started_at = None
                    elif (self._attempt_media_started_at is None
                          or (self._attempt_last_media_at is not None
                              and now - self._attempt_last_media_at > Config.CONNECTION_TIMEOUT)):
                        self._attempt_media_started_at = now
                    self._attempt_last_media_at = now
                    if (self._attempt_media_started_at is not None
                            and now - self._attempt_media_started_at >= self._stable_connection_threshold):
                        self._attempt_was_stable = True

                    if not self._current_source_has_media:
                        self._current_source_has_media = True
                        self._fail_fast_candidate = False

                    if self.failover_started_at is not None:
                        self.failover_started_at = None
                        duration = self.pending_buffering_failover_duration
                        self.pending_buffering_failover_duration = None

                        if duration is not None:
                            self.buffering = False
                            self.buffering_start_time = None

                            if hasattr(self.buffer, 'redis_client') and self.buffer.redis_client:
                                metadata_key = RedisKeys.channel_metadata(self.channel_id)
                                self.buffer.redis_client.hset(
                                    metadata_key, ChannelMetadataField.STATE, ChannelState.ACTIVE,
                                )

                            try:
                                log_system_event(
                                    'channel_failover',
                                    channel_id=self.channel_id,
                                    channel_name=self.channel_name,
                                    reason='buffering_timeout',
                                    duration=duration,
                                )
                            except Exception as e:
                                logger.error(f"Could not log failover event: {e}")
                else:
                    # No published output yet, or a timeout/read error.
                    if not self.running:
                        break

                    if self._failover_startup_expired():
                        break

                    # Do not slow down partial input while filling a chunk.
                    if not success:
                        gevent.sleep(0.1)
        except Exception as e:
            logger.error(f"Error processing stream data for channel {self.channel_id}: {e}", exc_info=True)

        # If we exit the loop, connection is closed or failed
        if generation == self._rotation_generation and not self.url_switching:
            self.connected = False

    def _close_all_connections(self):
        """Close all connection resources"""
        if self.socket or self.transcode_process:
            try:
                self._close_socket()
            except Exception as e:
                logger.debug(f"Error closing socket for channel {self.channel_id}: {e}")

        if self.current_response:
            try:
                self.current_response.close()
            except Exception as e:
                logger.debug(f"Error closing response for channel {self.channel_id}: {e}")

        if self.current_session:
            try:
                self.current_session.close()
            except Exception as e:
                logger.debug(f"Error closing session for channel {self.channel_id}: {e}")

        # Clear references
        self.socket = None
        self.current_response = None
        self.current_session = None
        self.transcode_process = None

    def stop(self):
        """Stop the stream manager and cancel all timers"""
        logger.info(f"Stopping stream manager for channel {self.channel_id}")

        self.stopping = True
        self._invalidate_ownership_cache()
        if self.buffer is not None:
            self.buffer.stopping = True

        # Cancel all buffer check timers
        for timer in list(self._buffer_check_timers):
            try:
                if timer and timer.is_alive():
                    timer.cancel()
            except Exception as e:
                logger.error(f"Error canceling buffer check timer for channel {self.channel_id}: {e}")

        self._buffer_check_timers.clear()

        # Set the flag first
        self.stop_requested = True

        # Close any active response connection
        if hasattr(self, 'current_response') and self.current_response:  # CORRECT NAME
            try:
                self.current_response.close()  # CORRECT NAME
            except Exception:
                pass

        # Also close the session
        if hasattr(self, 'current_session') and self.current_session:
            try:
                self.current_session.close()
            except Exception:
                pass

        # Explicitly close socket/transcode resources
        self._close_socket()

        # Set running to false to ensure thread exits
        self.running = False

        # Flush the final bitrate to DB on stop only if warmup completed and we have
        # a meaningful EMA. Short previews / channel hops that die during warmup do NOT
        # write anything, preserving any previously correct value in the database.
        if self._smoothed_output_bitrate is not None and self.current_stream_id:
            final_bitrate = self._smoothed_output_bitrate
            try:
                from ..services.channel_service import ChannelService
                ChannelService._update_stream_stats_in_db(
                    self.current_stream_id,
                    ffmpeg_output_bitrate=round(final_bitrate, 1)
                )
            except Exception as e:
                logger.debug(f"Error flushing final bitrate to DB for channel {self.channel_id}: {e}")

    def _publish_source_metadata(self):
        """Publish the committed identity while the switch lock is held."""
        self.metadata_updated = False
        if self.source_authority is None:
            return
        try:
            from ..services.channel_service import ChannelService
            self.metadata_updated = ChannelService._update_channel_metadata(
                self.channel_id, self.url, self.user_agent, self.current_stream_id,
                self.m3u_profile_id, self.stream_name, authority=self.source_authority,
                stream_profile=self.stream_profile,
                switch_time=getattr(self, '_source_switch_time', None),
                switch_reason=getattr(self, '_source_switch_reason', None),
            )
        except Exception:
            logger.exception("Source metadata publication failed for %s", self.channel_id)

    def update_url(self, new_url, stream_id=None, m3u_profile_id=None, force=False,
                   expected_generation=None, user_agent=None, stream_name=None,
                   stream_profile=None, transcode=None, manual=False, fail_fast=None):
        """Serialize preparation, handoff and publication without cancelling rejected readers."""
        if not self._switch_lock.acquire(blocking=False):
            return None
        generation = self._rotation_generation
        channel = None
        previous_assignment = None
        moved = claimed = committed = False
        original_identity = (self.url, self.current_stream_id, self.m3u_profile_id, self.user_agent)

        def current():
            return (self.running and not self.stop_requested
                    and generation == self._rotation_generation)

        def assignment():
            redis_client = RedisClient.get_client()
            assigned = redis_client.get(f"channel_stream:{channel.id}")
            profile = redis_client.get(f"stream_profile:{int(assigned)}") if assigned else None
            return (int(assigned) if assigned else None, int(profile) if profile else None)

        from core.utils import RedisClient
        try:
            self.metadata_updated = False
            if not current() or (expected_generation is not None and generation != expected_generation):
                return None
            if not new_url:
                return False
            target_id = self.current_stream_id if stream_id is None else int(stream_id)
            target_ua = self.user_agent if user_agent is None else user_agent
            target_profile = self.m3u_profile_id if m3u_profile_id is None else int(m3u_profile_id)
            same_source = (new_url, target_id, target_profile, target_ua) == original_identity
            if same_source and not force:
                self._publish_source_metadata()
                return current() and (self.source_authority is None or self.metadata_updated)
            if stream_name is None and target_id != self.current_stream_id:
                stream_name = Stream.objects.filter(id=target_id).values_list('name', flat=True).first()
            if not current():
                return None
            if self.source_authority is not None:
                if not self.source_authority.claim():
                    return False
                claimed = True
            if not self._ensure_owner_or_stop() or not current():
                return None
            if self.current_stream_id and stream_id and m3u_profile_id:
                channel = Channel.objects.get(uuid=self.channel_id)
                previous_assignment = assignment()
                if not current():
                    return None
                if not channel.update_stream_profile(target_profile, new_stream_id=target_id):
                    return False
                moved = all(value is not None for value in previous_assignment)
            if self.source_authority is not None and not self.source_authority.current():
                return None
            if not self._ensure_owner_or_stop() or not current():
                return None

            # Only accepted handoffs fence readers; preparation leaves playback running.
            self._rotation_generation += 1
            generation = self._rotation_generation
            self.url_switching = True
            self.url_switch_start_time = time.time()
            # Check which type of connection we're using and close it properly
            if self.transcode or self.socket or getattr(self, 'http_reader', None):
                logger.debug(f"Closing transcode process before URL change for channel {self.channel_id}")
                self._close_socket()
            else:
                logger.debug(f"Closing HTTP connection before URL change for channel {self.channel_id}")
                self._close_connection()
            if self.source_authority is not None and not self.source_authority.current():
                return None
            if not current():
                return None
            self.url = new_url
            self.user_agent = target_ua
            self.m3u_profile_id = target_profile
            if stream_name is not None or original_identity[1] != target_id:
                self.stream_name = stream_name
            if stream_profile is not None or original_identity[1] != target_id:
                self.stream_profile = stream_profile
            if transcode is not None:
                self.transcode = transcode
            moved = False
            committed = True
            self.connected = False
            self.last_data_time = time.time()
            self.healthy = True
            self.needs_reconnect = self.needs_stream_switch = False
            self.last_health_action_time = 0
            self.buffering = False
            self.buffering_start_time = None
            self._reset_source_state()
            self.force_ffmpeg = False
            self.ffmpeg_input_phase = True
            # Reset bitrate EMA on every URL change so stale data never carries over
            self._smoothed_output_bitrate = None
            self._last_bitrate_db_save_time = 0
            self._bitrate_warmup_samples = 10

            # Update stream ID if provided
            if stream_id:
                old_stream_id = self.current_stream_id
                self.current_stream_id = target_id
                # Add stream ID to tried streams for proper tracking
                self.tried_stream_ids.add(target_id)
                logger.info(f"Updated stream ID from {old_stream_id} to {stream_id} for channel {self.channel_id}")

            # Reset retry counter to allow immediate reconnect
            self._clear_connection_failure_history()
            if manual:
                self.reset_failover_rotation_state()
                self.pending_buffering_failover_duration = None
            if fail_fast is not None:
                self.failover_started_at = time.monotonic()
                self._fail_fast_candidate = fail_fast
            self._source_switch_time = time.time()
            self._source_switch_reason = 'manual' if manual else 'max_retries_exceeded'
            # Also reset buffer position to prevent stale data after URL change
            if hasattr(self.buffer, 'reset_buffer_position'):
                try:
                    self.buffer.reset_buffer_position()
                    logger.debug("Reset buffer position for clean URL switch")
                except Exception as e:
                    logger.warning(f"Failed to reset buffer position: {e}")

            # Log stream switch event
            try:
                log_system_event(
                    'stream_switch',
                    channel_id=self.channel_id,
                    channel_name=self.channel_name,
                    new_url=new_url[:100] if new_url else None,
                    stream_id=stream_id
                )
            except Exception as e:
                logger.error(f"Could not log stream switch event: {e}")

            self._publish_source_metadata()
            return True if current() else None
        except Exception as e:
            logger.error(f"Error during URL update for channel {self.channel_id}: {e}", exc_info=True)
            return committed if current() else None
        finally:
            try:
                if (moved and (self.url, self.current_stream_id, self.m3u_profile_id, self.user_agent) == original_identity
                        and assignment() == (int(stream_id), int(m3u_profile_id))):
                    if self.source_authority is None or self.source_authority.current():
                        if not channel.update_stream_profile(previous_assignment[1], new_stream_id=previous_assignment[0]):
                            # The old capacity may be gone. Teardown releases the active target.
                            self.stop()
                if claimed and not committed and current():
                    self._publish_source_metadata()
            except Exception:
                logger.exception("Source rollback failed for %s", self.channel_id)
                if (self.url, self.current_stream_id, self.m3u_profile_id, self.user_agent) == original_identity:
                    self.stop()
            finally:
                # Always reset the URL switching flag when done, whether successful or not
                if generation == self._rotation_generation:
                    self.url_switching = False
                try:
                    close_old_connections()
                finally:
                    self._switch_lock.release()

    def should_retry(self) -> bool:
        """Check if connection retry is allowed"""
        return self.retry_count < self.max_retries

    def _health_inactivity_threshold(self):
        """How long without data before marking the stream unhealthy."""
        if self.connected and getattr(self.buffer, 'index', 0) == 0:
            return ConfigHelper.channel_init_grace_period()
        return getattr(Config, 'CONNECTION_TIMEOUT', 10)

    def _monitor_health(self):
        """Monitor stream health and set flags for the main loop to handle recovery"""
        consecutive_unhealthy_checks = 0
        max_unhealthy_checks = 3
        action_cooldown = 30  # Prevent rapid recovery attempts

        while self.running:
            try:
                if (
                    self.url_switching
                    or self.failover_started_at is not None
                    or self.pending_buffering_failover_duration is not None
                ):
                    consecutive_unhealthy_checks = 0
                    gevent.sleep(self.health_check_interval)
                    continue

                now = time.time()
                inactivity_duration = now - self.last_data_time
                timeout_threshold = self._health_inactivity_threshold()

                if inactivity_duration > timeout_threshold and self.connected:
                    if self.healthy:
                        logger.warning(f"Stream unhealthy for channel {self.channel_id} - no data for {inactivity_duration:.1f}s")
                        self.healthy = False

                    consecutive_unhealthy_checks += 1

                    # Only set flags if enough time has passed since last action
                    if (consecutive_unhealthy_checks >= max_unhealthy_checks and
                        now - self.last_health_action_time > action_cooldown):

                        # Calculate stability to decide on action type
                        connection_start_time = getattr(self, 'connection_start_time', 0)
                        stable_time = self.last_data_time - connection_start_time if connection_start_time > 0 else 0

                        if stable_time >= 30:  # Stream was stable, try reconnect first
                            if not self.needs_reconnect:
                                logger.info(f"Setting reconnect flag for stable stream (stable for {stable_time:.1f}s) for channel {self.channel_id}")
                                self.needs_reconnect = True
                                self.last_health_action_time = now
                        else:
                            # Stream wasn't stable, suggest stream switch
                            if not self.needs_stream_switch:
                                logger.info(f"Setting stream switch flag for unstable stream (stable for {stable_time:.1f}s) for channel {self.channel_id}")
                                self.needs_stream_switch = True
                                self.last_health_action_time = now

                        consecutive_unhealthy_checks = 0 # Reset after setting flag

                elif self.connected and not self.healthy:
                    # Auto-recover health when data resumes
                    logger.info(f"Stream health restored for channel {self.channel_id} - data resumed after {inactivity_duration:.1f}s")
                    self.healthy = True
                    consecutive_unhealthy_checks = 0
                    # Clear recovery flags when healthy again
                    self.needs_reconnect = False
                    self.needs_stream_switch = False

                if self.healthy:
                    consecutive_unhealthy_checks = 0

            except Exception as e:
                logger.error(f"Error in health monitor: {e}")

            gevent.sleep(self.health_check_interval)

    def _attempt_reconnect(self):
        """Attempt to reconnect to the current stream"""
        try:
            logger.info(f"Attempting reconnect to current stream for channel {self.channel_id}")

            # Don't try to reconnect if we're already switching URLs
            if self.url_switching:
                logger.info(f"URL switching already in progress, skipping reconnect for channel {self.channel_id}")
                return False

            # Set a flag to prevent concurrent operations
            if hasattr(self, 'reconnecting') and self.reconnecting:
                logger.info(f"Reconnect already in progress, skipping for channel {self.channel_id}")
                return False

            self.reconnecting = True

            try:
                # Close existing connection and wait for it to fully terminate
                if self.transcode or self.socket:
                    logger.debug(f"Closing transcode process before reconnect for channel {self.channel_id}")
                    self._close_socket()
                else:
                    logger.debug(f"Closing HTTP connection before reconnect for channel {self.channel_id}")
                    self._close_connection()

                # Wait for all processes to fully close before attempting reconnect
                if not self._wait_for_existing_processes_to_close():
                    logger.warning(f"Some processes may still be running during reconnect for channel {self.channel_id}")

                self.connected = False

                # Attempt to establish a new connection using the same URL
                connection_result = False
                if self.transcode:
                    connection_result = self._establish_transcode_connection()
                else:
                    connection_result = self._establish_http_connection()

                if connection_result:
                    self.connection_start_time = time.time()
                    logger.info(f"Reconnect successful for channel {self.channel_id}")

                    # Log reconnection event
                    try:
                        log_system_event(
                            'channel_reconnect',
                            channel_id=self.channel_id,
                            channel_name=self.channel_name,
                            reason='health_monitor'
                        )
                    except Exception as e:
                        logger.error(f"Could not log reconnection event: {e}")

                    return True
                else:
                    logger.warning(f"Reconnect failed for channel {self.channel_id}")
                    return False

            finally:
                self.reconnecting = False

        except Exception as e:
            logger.error(f"Error in reconnect attempt for channel {self.channel_id}: {e}", exc_info=True)
            self.reconnecting = False
            return False

    def reset_failover_rotation_state(self):
        """Clear tried-stream / wrap bookkeeping after a manual stream change."""
        self.tried_stream_ids = set()
        self._rotation_started_at = None

    def _close_connection(self):
        """Close HTTP connection resources"""
        # Close response if it exists
        if hasattr(self, 'current_response') and self.current_response:
            try:
                self.current_response.close()
            except Exception as e:
                logger.debug(f"Error closing response for channel {self.channel_id}: {e}")
            self.current_response = None

        # Close session if it exists
        if hasattr(self, 'current_session') and self.current_session:
            try:
                self.current_session.close()
            except Exception as e:
                logger.debug(f"Error closing session for channel {self.channel_id}: {e}")
            self.current_session = None

    def _close_socket(self):
        """Close socket and transcode resources as needed"""
        # First try to use _close_connection for HTTP resources
        if self.current_response or self.current_session:
            self._close_connection()

        # Stop HTTP reader thread if it exists
        if hasattr(self, 'http_reader') and self.http_reader:
            try:
                logger.debug(f"Stopping HTTP reader thread for channel {self.channel_id}")
                self.http_reader.stop()
                self.http_reader = None
            except Exception as e:
                logger.debug(f"Error stopping HTTP reader for channel {self.channel_id}: {e}")

        # Kill proc before closing self.socket. Closing relay_read while the stream
        # OS thread is blocked in select() on it does not reliably wake that select()
        # on Linux. Killing ffmpeg closes its relay_write, sending EOF to the stream
        # thread naturally. We close self.socket afterward as cleanup only.
        proc = self.transcode_process
        self.transcode_process = None  # claim early so concurrent greenlets skip this block
        if proc:
            try:
                logger.debug(f"Killing transcode process for channel {self.channel_id}")
                proc.kill()

                # Give it a very short time to die
                try:
                    proc.wait(timeout=0.5)
                except subprocess.TimeoutExpired:
                    logger.error(f"Failed to kill transcode process even with force for channel {self.channel_id}")
            except Exception as e:
                logger.debug(f"Error terminating transcode process for channel {self.channel_id}: {e}")

                # Final attempt: try to kill directly
                try:
                    proc.kill()
                except Exception as e:
                    logger.error(f"Final kill attempt failed for channel {self.channel_id}: {e}")

        # Close relay socket after proc death; stream thread has already unblocked via EOF.
        if self.socket:
            try:
                self.socket.close()
            except Exception as e:
                logger.debug(f"Error closing socket for channel {self.channel_id}: {e}")

        if proc:
            # Explicitly close all subprocess pipes to prevent file descriptor leaks
            try:
                if proc.stdin:
                    proc.stdin.close()
                if proc.stdout:
                    proc.stdout.close()
                if proc.stderr:
                    proc.stderr.close()
                logger.debug(f"Closed all subprocess pipes for channel {self.channel_id}")
            except Exception as e:
                logger.debug(f"Error closing subprocess pipes for channel {self.channel_id}: {e}")

            # Join stderr reader thread to ensure it's fully terminated
            if hasattr(self, 'stderr_reader_thread') and self.stderr_reader_thread and self.stderr_reader_thread.is_alive():
                try:
                    logger.debug(f"Waiting for stderr reader thread to terminate for channel {self.channel_id}")
                    stderr_join_timeout = 0.25 if self.stopping else 2.0
                    self.stderr_reader_thread.join(timeout=stderr_join_timeout)
                    if self.stderr_reader_thread.is_alive():
                        logger.warning(f"Stderr reader thread did not terminate within timeout for channel {self.channel_id}")
                except Exception as e:
                    logger.debug(f"Error joining stderr reader thread for channel {self.channel_id}: {e}")
                finally:
                    self.stderr_reader_thread = None

            self.transcode_process_active = False

            # Clear transcode active key in Redis if available
            if hasattr(self.buffer, 'redis_client') and self.buffer.redis_client:
                try:
                    transcode_key = RedisKeys.transcode_active(self.channel_id)
                    self.buffer.redis_client.delete(transcode_key)
                    logger.debug(f"Cleared transcode active flag for channel {self.channel_id}")
                except Exception as e:
                    logger.debug(f"Error clearing transcode flag for channel {self.channel_id}: {e}")
        self.socket = None
        self.connected = False
        # Cancel any remaining buffer check timers
        for timer in list(self._buffer_check_timers):
            try:
                if timer and timer.is_alive():
                    timer.cancel()
                    logger.debug(f"Cancelled buffer check timer during socket close for channel {self.channel_id}")
            except Exception as e:
                logger.debug(f"Error canceling timer during socket close for channel {self.channel_id}: {e}")

        self._buffer_check_timers = []

    def fetch_chunk(self):
        """Fetch data from socket with timeout handling"""
        if not self.connected or not self.socket:
            return False

        generation = self._rotation_generation
        source_socket = self.socket
        try:
            # Set timeout for chunk reads
            chunk_timeout = ConfigHelper.chunk_timeout()  # Use centralized timeout configuration

            if self.failover_started_at is not None:
                remaining = self.failover_init_grace_period - (
                    time.monotonic() - self.failover_started_at
                )
                if remaining <= 0:
                    return False
                chunk_timeout = min(chunk_timeout, remaining)

            try:
                # Handle different socket types with timeout
                if hasattr(source_socket, 'recv'):
                    # Standard socket - set timeout
                    original_timeout = source_socket.gettimeout()
                    source_socket.settimeout(chunk_timeout)
                    chunk = source_socket.recv(Config.CHUNK_SIZE)
                    source_socket.settimeout(original_timeout)  # Restore original timeout
                else:
                    # Non-socket file object (io.FileIO from os.fdopen) - use raw
                    # fd + os.read to stay cooperative under gevent.
                    import select as _select
                    import os as _os

                    try:
                        fd = source_socket.fileno()
                        deadline = time.monotonic() + chunk_timeout
                        while True:
                            if (generation != self._rotation_generation or self.url_switching
                                    or not self.running or self.stop_requested
                                    or self.needs_stream_switch or self.needs_reconnect):
                                return False
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                logger.debug(f"Chunk read timeout ({chunk_timeout}s) for channel {self.channel_id}")
                                return False
                            ready, _, _ = _select.select([fd], [], [], min(0.25, remaining))
                            if ready:
                                break
                        chunk = _os.read(fd, Config.CHUNK_SIZE)
                    except (ValueError, OSError) as e:
                        if generation != self._rotation_generation or self.url_switching:
                            return False
                        self.connected = False
                        if self.running and not self.stop_requested:
                            logger.debug(f"Read error for channel {self.channel_id}: {e}")
                        return False

            except socket.timeout:
                # Socket timeout occurred
                logger.debug(f"Socket timeout ({chunk_timeout}s) for channel {self.channel_id}")
                return False

            if (generation != self._rotation_generation or self.url_switching
                    or self.stop_requested or not self.running
                    or self.needs_stream_switch or self.needs_reconnect):
                return False
            if not chunk:
                # Connection closed by server
                logger.warning(f"Server closed connection for channel {self.channel_id}")

                if generation != self._rotation_generation or self.url_switching:
                    return False
                self._close_socket()
                if generation != self._rotation_generation or self.url_switching:
                    return False
                self.connected = False
                return False

            if not self._upstream_may_continue():
                self.stop()
                return False

            # Track chunk size before adding to buffer
            chunk_size = len(chunk)
            self._update_bytes_processed(chunk_size)

            if generation != self._rotation_generation or self.url_switching:
                return False
            # Add directly to buffer without TS-specific processing
            success = self.buffer.add_chunk(chunk)

            if success and hasattr(self.buffer, 'redis_client') and self.buffer.redis_client:
                last_data_key = RedisKeys.last_data(self.buffer.channel_id)
                self.buffer.redis_client.set(last_data_key, str(time.time()), ex=60)

            return success

        except (socket.timeout, socket.error) as e:
            # Socket error
            logger.error(f"Socket error: {e}")
            if generation != self._rotation_generation or self.url_switching:
                return False
            self._close_socket()
            if generation != self._rotation_generation or self.url_switching:
                return False
            self.connected = False
            return False

        except Exception as e:
            logger.error(f"Error in fetch_chunk: {e}")
            return False

    def _set_waiting_for_clients(self):
        """Set channel state to waiting for clients AFTER buffer has enough chunks"""
        try:
            if hasattr(self.buffer, 'channel_id') and hasattr(self.buffer, 'redis_client'):
                channel_id = self.buffer.channel_id
                redis_client = self.buffer.redis_client

                if channel_id and redis_client:
                    current_time = str(time.time())
                    metadata_key = RedisKeys.channel_metadata(channel_id)

                    # Check current state first
                    current_state = None
                    try:
                        metadata = redis_client.hgetall(metadata_key)
                        state_field = ChannelMetadataField.STATE
                        if metadata and state_field in metadata:
                            current_state = metadata[state_field]
                    except Exception as e:
                        logger.error(f"Error checking current state: {e}")

                    # Only update if not already past connecting
                    if not current_state or current_state in [ChannelState.INITIALIZING, ChannelState.CONNECTING]:
                        # NEW CODE: Check if buffer has enough chunks
                        # IMPORTANT: Read from Redis, not local buffer.index, because in multi-worker setup
                        # each worker has its own StreamBuffer instance with potentially stale local index
                        buffer_index_key = RedisKeys.buffer_index(channel_id)
                        current_buffer_index = 0
                        try:
                            redis_index = redis_client.get(buffer_index_key)
                            if redis_index:
                                current_buffer_index = int(redis_index)
                        except Exception as e:
                            logger.error(f"Error reading buffer index from Redis: {e}")

                        initial_chunks_needed = ConfigHelper.initial_behind_chunks()

                        if current_buffer_index < initial_chunks_needed:
                            # Not enough buffer yet - set to connecting state if not already
                            if current_state != ChannelState.CONNECTING:
                                update_data = {
                                    ChannelMetadataField.STATE: ChannelState.CONNECTING,
                                    ChannelMetadataField.STATE_CHANGED_AT: current_time
                                }
                                redis_client.hset(metadata_key, mapping=update_data)
                                logger.info(f"Channel {channel_id} connected but waiting for buffer to fill: {current_buffer_index}/{initial_chunks_needed} chunks")

                            # Schedule a retry to check buffer status again
                            timer = threading.Timer(0.5, self._check_buffer_and_set_state)
                            timer.daemon = True
                            timer.start()
                            return False

                        from ..services.channel_service import ChannelService

                        ChannelService.promote_channel_when_buffer_ready(channel_id)
                    else:
                        logger.debug(f"Not changing state: channel {channel_id} already in {current_state} state")
        except Exception as e:
            logger.error(f"Error setting waiting for clients state for channel {channel_id}: {e}")

    def _check_buffer_and_set_state(self):
        """Check buffer size and set state to waiting_for_clients when ready"""
        try:
            # Enhanced stop detection with short-circuit return
            if not self.running or getattr(self, 'stopping', False) or getattr(self, 'reconnecting', False):
                logger.debug(f"Buffer check aborted - channel {self.buffer.channel_id} is stopping or reconnecting")
                return False  # Return value to indicate check was aborted

            # Clean up completed timers
            self._buffer_check_timers = [t for t in self._buffer_check_timers if t.is_alive()]

            if hasattr(self.buffer, 'channel_id') and hasattr(self.buffer, 'redis_client'):
                channel_id = self.buffer.channel_id
                redis_client = self.buffer.redis_client

                # IMPORTANT: Read from Redis, not local buffer.index
                buffer_index_key = RedisKeys.buffer_index(channel_id)
                current_buffer_index = 0
                try:
                    redis_index = redis_client.get(buffer_index_key)
                    if redis_index:
                        current_buffer_index = int(redis_index)
                except Exception as e:
                    logger.error(f"Error reading buffer index from Redis: {e}")

                initial_chunks_needed = ConfigHelper.initial_behind_chunks()  # Use ConfigHelper for consistency

                if current_buffer_index >= initial_chunks_needed:
                    # We now have enough buffer, call _set_waiting_for_clients again
                    logger.info(f"Buffer threshold reached for channel {channel_id}: {current_buffer_index}/{initial_chunks_needed} chunks")
                    self._set_waiting_for_clients()
                else:
                    # Still waiting, log progress and schedule another check
                    logger.debug(f"Buffer filling for channel {channel_id}: {current_buffer_index}/{initial_chunks_needed} chunks")

                    # Schedule another check - NOW WITH STOPPING CHECK
                    if self.running and not getattr(self, 'stopping', False):
                        timer = threading.Timer(0.5, self._check_buffer_and_set_state)
                        timer.daemon = True
                        timer.start()
                        self._buffer_check_timers.append(timer)

            return True  # Return value to indicate check was successful
        except Exception as e:
            logger.error(f"Error in buffer check for channel {self.channel_id}: {e}")
            return False

    def _try_next_stream(self, new_rotation=False):
        """
        Try to switch to the next available stream for this channel.
        Will iterate through eligible alternate streams until a handoff succeeds.

        Returns:
            True if selected, False if exhausted/stopped, None if superseded.
        """
        generation = self._rotation_generation
        try:
            status = self._selection_status(generation)
            if status is not True:
                return status
            logger.info(f"Trying to find alternative stream for channel {self.channel_id}, current stream ID: {self.current_stream_id}")

            # A fresh rotation re-reads every eligible source in configured
            # priority order, including the current source.
            alternate_streams = get_alternate_streams(self.channel_id, None if new_rotation else self.current_stream_id)
            status = self._selection_status(generation)
            if status is not True:
                return status
            if new_rotation:
                self.tried_stream_ids.clear()
            logger.info(f"Found {len(alternate_streams)} potential alternate streams for channel {self.channel_id}")

            # Filter out streams we've already tried
            untried_streams = [s for s in alternate_streams if s['stream_id'] not in self.tried_stream_ids]
            if untried_streams:
                ids_to_try = ', '.join([str(s['stream_id']) for s in untried_streams])
                logger.info(f"Found {len(untried_streams)} untried streams for channel {self.channel_id}: [{ids_to_try}]")
            else:
                logger.warning(f"No untried streams available for channel {self.channel_id}, tried: {self.tried_stream_ids}")

            if not untried_streams:
                return False

            for next_stream in untried_streams:
                stream_id = next_stream['stream_id']
                profile_id = next_stream['profile_id']  # This is the M3U profile ID we need

                # Add to tried streams
                self.tried_stream_ids.add(stream_id)

                # Resolve stream info and the profile actually selected for this switch
                logger.info(f"Trying next stream ID {stream_id} with profile ID {profile_id} for channel {self.channel_id}")
                stream_info = get_stream_info_for_switch(self.channel_id, stream_id)
                status = self._selection_status(generation)
                if status is not True:
                    return status

                if 'error' in stream_info or not stream_info.get('url'):
                    logger.error(f"Error getting info for stream {stream_id} for channel {self.channel_id}: {stream_info.get('error', 'No URL')}")
                    continue  # Try next stream instead of giving up

                profile_id = stream_info['m3u_profile_id']

                # Update URL and user agent
                new_url = stream_info['url']
                new_user_agent = stream_info['user_agent']
                new_transcode = stream_info['transcode']

                logger.info(f"Switching from URL {self.url} to {new_url} for channel {self.channel_id}")

                # Just update the URL, don't stop the channel or release resources
                switch_result = self.update_url(
                    new_url, stream_id, profile_id, force=True,
                    expected_generation=generation, user_agent=new_user_agent,
                    stream_name=stream_info.get('stream_name'),
                    stream_profile=stream_info['stream_profile'], transcode=new_transcode,
                    fail_fast=not new_rotation,
                )
                if switch_result is None:
                    return None
                if not switch_result:
                    status = self._selection_status(generation)
                    if status is not True:
                        return status
                    logger.error(f"Failed to update URL for stream ID {stream_id} for channel {self.channel_id}")
                    continue  # Try next stream

                logger.info(f"Successfully switched to stream ID {stream_id} with URL {new_url} for channel {self.channel_id}")
                return self._selection_status(generation + 1)

            # If we get here, we tried all streams but none worked
            logger.error(f"Tried {len(untried_streams)} alternate streams but none were suitable for channel {self.channel_id}")
            return False

        except Exception as e:
            logger.error(f"Error trying next stream for channel {self.channel_id}: {e}", exc_info=True)
            status = self._selection_status(generation)
            return False if status is True else status

    # Add a new helper method to safely reset the URL switching state
    def _reset_url_switching_state(self):
        """Safely reset the URL switching state if it gets stuck"""
        self.url_switching = False
        self.url_switch_start_time = 0
        logger.info(f"Reset URL switching state for channel {self.channel_id}")
