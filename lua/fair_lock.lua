-- Fair lock using sorted set for FIFO queue
-- KEYS[1] = lock key
-- KEYS[2] = queue key (sorted set)
-- ARGV[1] = lock value (owner identifier)
-- ARGV[2] = current timestamp (score)
-- ARGV[3] = TTL in milliseconds
-- ARGV[4] = request ID (unique per attempt)
-- Returns: 1 = acquired, 0 = not acquired

local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
    return 1
end

redis.call('ZADD', KEYS[2], ARGV[2], ARGV[4])

local front = redis.call('ZRANGE', KEYS[2], 0, 0)
if front[1] == ARGV[4] then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
    redis.call('ZREM', KEYS[2], ARGV[4])
    return 1
else
    redis.call('ZREM', KEYS[2], ARGV[4])
    return 0
end