-- Write lock acquisition
-- KEYS[1] = write lock key
-- KEYS[2] = read counter key
-- ARGV[1] = lock value (owner identifier)
-- ARGV[2] = TTL in milliseconds
-- Returns: 1 = acquired, 0 = not acquired

local readCount = redis.call('GET', KEYS[2])
if readCount ~= false and tonumber(readCount) > 0 then
    return 0
end

local result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if result then
    return 1
end
return 0