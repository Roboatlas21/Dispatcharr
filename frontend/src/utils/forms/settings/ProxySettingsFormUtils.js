import { PROXY_SETTINGS_OPTIONS } from '../../../constants.js';

export const FAILOVER_SETTING_LIMITS = {
  failover_init_grace_period: [1, 300],
  upstream_read_timeout: [0, 300],
  stream_connection_attempts: [1, 5],
  min_failover_rotation_interval: [0, 300],
};

export const getFailoverSettingsValidation = () =>
  Object.fromEntries(
    Object.entries(FAILOVER_SETTING_LIMITS).map(([key, [minimum, maximum]]) => [
      key,
      (value) =>
        Number.isInteger(value) && value >= minimum && value <= maximum
          ? null
          : `Use a whole number between ${minimum} and ${maximum}.`,
    ])
  );

export const getProxySettingsFormInitialValues = () => {
  return Object.keys(PROXY_SETTINGS_OPTIONS).reduce((acc, key) => {
    acc[key] = '';
    return acc;
  }, {});
};

export const getProxySettingDefaults = () => {
  return {
    buffering_timeout: 15,
    buffering_speed: 1.0,
    redis_chunk_ttl: 60,
    channel_shutdown_delay: 0,
    channel_init_grace_period: 60,
    failover_init_grace_period: 30,
    upstream_read_timeout: 10,
    stream_connection_attempts: 3,
    min_failover_rotation_interval: 10,
    channel_client_wait_period: 5,
    new_client_behind_seconds: 5,
    validate_redirect_urls: true,
  };
};
