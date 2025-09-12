import os
import redis
import time
from param_default import redis_databases


def _get_redis_connection(db_name: str):
    if "REDIS_SERVER" not in os.environ:
        return None

    redis_host = os.environ["REDIS_SERVER"]
    db_number = redis_databases[db_name]
    return redis.Redis(host=redis_host, db=db_number, decode_responses=True)


class AdaptiveRateLimiter:
    def __init__(
        self, db_name, base_backoff_sec=60, max_backoff_sec=3600, backoff_factor=1.5
    ):
        self.redis = _get_redis_connection(db_name)
        if not self.redis:
            return
        self.base_backoff_sec = base_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self.backoff_factor = backoff_factor
        self.lua_script = self._load_lua_script()

    def _load_lua_script(self):
        script = """
        local key_prefix = KEYS[1]
        local now = tonumber(ARGV[1])
        local base_backoff_sec = tonumber(ARGV[2])
        local max_backoff_sec = tonumber(ARGV[3])
        local backoff_factor = tonumber(ARGV[4])

        local total_calls_key = key_prefix .. ":total_calls"
        local backoff_sec_key = key_prefix .. ":backoff_sec"
        local last_call_timestamp_key = key_prefix .. ":last_call_timestamp"
        local rejections_key = key_prefix .. ":rejections_since_last_call"

        local backoff_sec = tonumber(redis.call('GET', backoff_sec_key) or 0)
        local last_call_timestamp = tonumber(redis.call('GET', last_call_timestamp_key) or 0)

        if now - last_call_timestamp < backoff_sec then
            local rejections = redis.call('INCR', rejections_key)
            return {0, rejections}
        else
            local total_calls = redis.call('INCR', total_calls_key)
            local rejections = tonumber(redis.call('GET', rejections_key) or 0)

            local new_backoff = base_backoff_sec * (backoff_factor ^ (total_calls - 1))
            if new_backoff > max_backoff_sec then
                new_backoff = max_backoff_sec
            end

            if rejections <= 1 then
                new_backoff = new_backoff / 2
            end

            redis.call('SET', backoff_sec_key, new_backoff)
            redis.call('SET', last_call_timestamp_key, now)
            redis.call('SET', rejections_key, 0)

            return {1, rejections}
        end
        """
        return self.redis.register_script(script)

    def is_allowed(self, key_identifier: str) -> (bool, int):
        if not self.redis:
            return True, 0

        redis_key = f"adaptive_ratelimit:{key_identifier}"
        now = time.time()

        result = self.lua_script(
            keys=[redis_key],
            args=[
                now,
                self.base_backoff_sec,
                self.max_backoff_sec,
                self.backoff_factor,
            ],
        )

        return bool(result[0]), int(result[1])


def record_hostname_failure(queue: str, hostname: str):
    """Tracks failure counts for hostnames in Redis."""
    if not (hostname and queue):
        print(f"Could not log failure: missing hostname or queue")
        return

    redis_key = f"{queue}_hostname_failures"
    host = hostname.split('.')[0]

    try:
        r = _get_redis_connection('SEURON')
        if r:
            r.hincrby(redis_key, host, 1)
        else:
            print("Could not get Redis connection.")
    except Exception as e:
        print(f"An unexpected error occurred in record_hostname_failure: {e}")


def get_hostname_failures(queue: str) -> dict[str, int]:
    """Retrieves failure counts for a queue from Redis."""
    redis_key = f"{queue}_hostname_failures"
    failures = {}
    try:
        r = _get_redis_connection('SEURON')
        if r:
            failures = r.hgetall(redis_key)
        else:
            print("Could not get Redis connection.")
    except Exception as e:
        print(f"An unexpected error occurred in get_hostname_failures: {e}")

    return {host: int(count) for host, count in failures.items()}
