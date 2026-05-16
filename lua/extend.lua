-- KEYS[1] = lock key
-- ARGV[1] = lock value (owner identifier)
-- ARGV[2] = new TTL in milliseconds
-- Returns: 1 = extended, 0 = not owner
local lockVal = redis.call('GET', KEYS[1])
if lockVal == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
else
    return 0
end