-- Atomic leadership release
-- Deletes the leader key only if it is still held by the given scheduler
--
-- KEYS[1]: leader key (e.g., "chronos:scheduler:leader")
-- ARGV[1]: scheduler ID
--
-- Returns:
-- 1 if the key was deleted
-- 0 if leadership is held by someone else or has expired

local current_leader = redis.call('GET', KEYS[1])

if not current_leader then
    return 0
end

-- Value format: schedulerId:timestamp (scheduler IDs may themselves contain ':')
local prefix = ARGV[1] .. ':'
if current_leader ~= ARGV[1] and string.sub(current_leader, 1, #prefix) ~= prefix then
    return 0
end

redis.call('DEL', KEYS[1])
return 1
