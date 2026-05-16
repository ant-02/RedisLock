-- Read lock using counter
-- KEYS[1] = read counter key
-- KEYS[2] = write lock key
-- ARGV[1] = lock value (owner identifier)
-- ARGV[2] = TTL in milliseconds
-- ARGV[3] = timeout for acquiring write lock
-- Returns: 1 = acquired, 0 = not acquired

local writeLocked = redis.call('GET', KEYS[2])
if writeLocked ~= false then
    return 0
end

local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
end

return 1