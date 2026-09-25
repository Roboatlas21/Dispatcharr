"""Atomic source publication; authority shares the metadata hash's lifetime."""
import json
import uuid

from .constants import REDIS_TTL_DEFAULT
from .redis_keys import RedisKeys


SOURCE_CAS = """
local meta, owner, stopping = KEYS[1], KEYS[2], KEYS[3]
local operation, worker, epoch, expected = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local state = redis.call('HGET', meta, 'state')
if redis.call('EXISTS', stopping) == 1 or state == 'stopping' or state == 'stopped' then return 0 end
local held = redis.call('GET', owner)
if operation == 'init' then
    if held and held ~= worker then return 0 end
    if redis.call('EXISTS', meta) == 1 then
        if held == worker and redis.call('HGET', meta, 'channel_epoch') == epoch then
            return tonumber(redis.call('HGET', meta, 'source_generation')) or 0
        end
        return 0
    end
    redis.call('SET', owner, worker, 'EX', 30)
else
    if held ~= worker or redis.call('HGET', meta, 'channel_epoch') ~= epoch
        or redis.call('HGET', meta, 'source_generation') ~= expected then return 0 end
    if operation == 'claim' then
        return redis.call('HINCRBY', meta, 'source_generation', 1)
    end
    if operation == 'check' then return tonumber(expected) end
    if operation ~= 'commit' then return 0 end
    redis.call('HDEL', meta, 'url', 'user_agent', 'stream_id', 'm3u_profile',
        'stream_profile', 'stream_name', 'stream_switch_time', 'stream_switch_reason')
end
for field, value in pairs(cjson.decode(ARGV[5])) do
    redis.call('HSET', meta, field, value)
end
if operation == 'init' then
    redis.call('HSET', meta, 'channel_epoch', epoch, 'source_generation', 1)
    redis.call('EXPIRE', meta, ARGV[6])
    return 1
end
return tonumber(expected)
"""


class SourceAuthority:
    def __init__(self, redis_client, channel_id, worker_id):
        self.redis = redis_client
        self.keys = (RedisKeys.channel_metadata(channel_id),
                     RedisKeys.channel_owner(channel_id), RedisKeys.channel_stopping(channel_id))
        self.worker = worker_id
        self.epoch = uuid.uuid4().hex
        self.generation = 0

    def _run(self, operation, metadata=None):
        return int(self.redis.eval(
            SOURCE_CAS, 3, *self.keys, operation, self.worker, self.epoch,
            self.generation, json.dumps(metadata or {}), REDIS_TTL_DEFAULT,
        ))

    def initialize(self, metadata):
        self.generation = self._run('init', metadata)
        return bool(self.generation)

    def claim(self):
        generation = self._run('claim')
        if generation:
            self.generation = generation
        return bool(generation)

    def current(self):
        return bool(self._run('check'))

    def commit(self, metadata):
        # A delayed COMMIT response must never replace a newer cached token.
        return bool(self._run('commit', metadata))
