"""The UI settings endpoint and the dedicated endpoint enforce the same limits."""
from unittest.mock import patch

from django.test import SimpleTestCase

from apps.proxy.config import TSConfig
from apps.proxy.live_proxy.config_helper import ConfigHelper
from core.models import CoreSettings
from core.serializers import CoreSettingsSerializer, ProxySettingsSerializer


class FailoverSettingsValidationTests(SimpleTestCase):
    controls = {
        "failover_init_grace_period": (1, 300, 30, ConfigHelper.failover_init_grace_period),
        "upstream_read_timeout": (0, 300, 10, ConfigHelper.upstream_read_timeout),
        "stream_connection_attempts": (1, 5, 3, ConfigHelper.max_retries),
        "min_failover_rotation_interval": (0, 300, 10, ConfigHelper.min_failover_rotation_interval),
    }

    def _serializers(self, key, value):
        instance = CoreSettings(key="proxy_settings", name="Proxy Settings", value={})
        return (
            CoreSettingsSerializer(instance, data={"value": {key: value}}, partial=True),
            ProxySettingsSerializer(data={key: value}, partial=True),
        )

    def test_both_endpoints_reject_invalid_controls(self):
        for key, (minimum, maximum, default, getter) in self.controls.items():
            for value in ("", None, True, False, "10", 1.5, minimum - 1, maximum + 1):
                for serializer in self._serializers(key, value):
                    with self.subTest(key=key, value=value, serializer=type(serializer).__name__):
                        self.assertFalse(serializer.is_valid())
                with self.subTest(key=key, value=value, runtime=True), \
                     patch.object(TSConfig, "get_proxy_settings", return_value={key: value}):
                    self.assertEqual(getter(), default)
            with patch.object(TSConfig, "get_proxy_settings", return_value=None):
                self.assertEqual(getter(), default)

    def test_both_endpoints_accept_boundaries(self):
        for key, (minimum, maximum, _default, getter) in self.controls.items():
            for value in (minimum, maximum):
                for serializer in self._serializers(key, value):
                    with self.subTest(key=key, value=value, serializer=type(serializer).__name__):
                        self.assertTrue(serializer.is_valid(), serializer.errors)
                with self.subTest(key=key, value=value, runtime=True), \
                     patch.object(TSConfig, "get_proxy_settings", return_value={key: value}):
                    self.assertEqual(getter(), value)
