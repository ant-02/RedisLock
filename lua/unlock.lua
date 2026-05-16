-- KEYS[1] = lock key
-- ARGV[1] = lock value (owner identifier)
-- Returns: 1 = released, 0 = not owner
local lockVal = redis.call('GET', KEYS[1])
if lockVal == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
else
    return 0
end