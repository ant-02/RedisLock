-- KEYS[1] = lock key
-- ARGV[1] = lock value (owner identifier)
-- ARGV[2] = TTL in milliseconds
-- Returns: 1 = acquired, 0 = not acquired
local lockVal = redis.call('GET', KEYS[1])
if lockVal == false then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1
elseif lockVal == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
else
    return 0
end