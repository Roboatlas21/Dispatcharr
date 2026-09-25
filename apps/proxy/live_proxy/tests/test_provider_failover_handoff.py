"""Tests for provider/profile accounting during live failover."""

from unittest.mock import MagicMock, patch

from django.test import TransactionTestCase

from apps.channels.models import Channel, Stream
from apps.m3u.connection_pool import profile_connections_key, reserve_profile_slot
from apps.m3u.models import M3UAccount, M3UAccountProfile
from apps.proxy.live_proxy.input.manager import StreamManager
from apps.m3u.tests.test_connection_pool import FakeRedis


class ProviderFailoverHandoffTests(TransactionTestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.account_a, self.profile_a, self.stream_a = self._make_provider("A")
        self.account_b, self.profile_b, self.stream_b = self._make_provider("B")

        self.channel = Channel.objects.create(
            channel_number=9001,
            name="Failover Test",
        )
        self.channel.streams.add(self.stream_a, self.stream_b)

        self.assertTrue(reserve_profile_slot(self.profile_a, self.redis)[0])
        self.redis.set(f"channel_stream:{self.channel.id}", self.stream_a.id)
        self.redis.set(f"stream_profile:{self.stream_a.id}", self.profile_a.id)

    def _make_provider(self, suffix):
        account = M3UAccount.objects.create(name=f"Provider {suffix}", max_streams=1)
        profile = M3UAccountProfile.objects.get(m3u_account=account, is_default=True)
        profile.max_streams = 1
        profile.save(update_fields=["max_streams"])
        stream = Stream.objects.create(
            name=f"Provider {suffix} stream", m3u_account=account,
            url=f"http://provider-{suffix.lower()}.example/live/test.ts",
        )
        return account, profile, stream

    def _redis_int(self, key):
        value = self.redis.get(key)
        return None if value is None else int(value)

    def _profile_count(self, profile):
        return self._redis_int(profile_connections_key(profile.id)) or 0

    def _assert_assignment(self, stream, profile, counts):
        other = self.stream_b if stream == self.stream_a else self.stream_a
        self.assertEqual(self._redis_int(f"channel_stream:{self.channel.id}"), stream.id)
        self.assertEqual(self._redis_int(f"stream_profile:{stream.id}"), profile.id)
        self.assertIsNone(self.redis.get(f"stream_profile:{other.id}"))
        self.assertEqual((self._profile_count(self.profile_a), self._profile_count(self.profile_b)), counts)

    def _manager(self):
        buffer = MagicMock(index=0, published_index=0, redis_client=None)
        with patch("apps.proxy.config.TSConfig.get_proxy_settings", return_value={}):
            return StreamManager(
                str(self.channel.uuid), self.stream_a.url, buffer,
                stream_id=self.stream_a.id, channel_name=self.channel.name,
            )

    def _switch_manager_to_b(self, manager):
        return manager.update_url(self.stream_b.url, self.stream_b.id, self.profile_b.id, expected_generation=0)

    def test_owner_loss_rolls_back_provider_handoff(self):
        manager = self._manager()
        manager._ensure_owner_or_stop = MagicMock(side_effect=[True, False])

        with patch("core.utils.RedisClient.get_client", return_value=self.redis):
            self.assertFalse(self._switch_manager_to_b(manager))

        self.assertEqual(manager.url, self.stream_a.url)
        self._assert_assignment(self.stream_a, self.profile_a, (1, 0))

    def test_same_stream_profile_handoff_respects_capacity(self):
        for full in (False, True):
            with self.subTest(full=full):
                alternate = M3UAccountProfile.objects.create(
                    m3u_account=self.account_a, name=f"Alternate {full}", max_streams=1,
                )
                manager = self._manager()
                if full:
                    self.assertTrue(reserve_profile_slot(alternate, self.redis)[0])
                url = "http://provider-a.example/alternate-login.ts"
                with patch("core.utils.RedisClient.get_client", return_value=self.redis):
                    self.assertEqual(manager.update_url(url, self.stream_a.id, alternate.id), not full)
                    self.assertEqual(manager.url, self.stream_a.url if full else url)
                    self.assertEqual(self._profile_count(self.profile_a), int(full))
                    self.assertEqual(self._profile_count(alternate), 1)
                    self.assertEqual(self._redis_int(f"stream_profile:{self.stream_a.id}"),
                                     self.profile_a.id if full else alternate.id)
                    if not full:
                        self.assertTrue(self.channel.update_stream_profile(
                            self.profile_a.id, new_stream_id=self.stream_a.id,
                        ))

    def test_same_profile_restart_does_not_reserve_twice(self):
        manager = self._manager()
        with patch("core.utils.RedisClient.get_client", return_value=self.redis):
            self.assertTrue(manager.update_url(manager.url, self.stream_a.id, self.profile_a.id, force=True))
        self._assert_assignment(self.stream_a, self.profile_a, (1, 0))

    def test_superseded_handoff_rolls_back_or_stops_if_old_capacity_is_gone(self):
        for old_slot_taken in (False, True):
            with self.subTest(old_slot_taken=old_slot_taken):
                manager = self._manager()
                original_update = self.channel.update_stream_profile

                def update_and_supersede(new_profile_id, new_stream_id=None):
                    result = original_update(new_profile_id, new_stream_id=new_stream_id)
                    if result and new_profile_id == self.profile_b.id:
                        if old_slot_taken:
                            self.assertTrue(reserve_profile_slot(self.profile_a, self.redis)[0])
                        manager._rotation_generation += 1
                    return result

                with patch("core.utils.RedisClient.get_client", return_value=self.redis), \
                     patch.object(Channel.objects, "get", return_value=self.channel), \
                     patch.object(self.channel, "update_stream_profile", side_effect=update_and_supersede):
                    self.assertIsNone(self._switch_manager_to_b(manager))
                    self.assertEqual(manager.url, self.stream_a.url)
                    if old_slot_taken:
                        self.assertFalse(manager.running)
                        self.assertTrue(manager.stop_requested)
                        self._assert_assignment(self.stream_b, self.profile_b, (1, 1))
                        self.assertTrue(self.channel.release_stream())
                        self.assertEqual(self._profile_count(self.profile_a), 1)
                        self.assertEqual(self._profile_count(self.profile_b), 0)
                    else:
                        self._assert_assignment(self.stream_a, self.profile_a, (1, 0))
