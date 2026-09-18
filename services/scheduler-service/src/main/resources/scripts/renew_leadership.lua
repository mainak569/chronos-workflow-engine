-- Atomic leader election renewal script
-- Ensures that only the current leader can renew leadership
-- 
-- KEYS[1]: leader key (e.g., "chronos:scheduler:leader")
-- ARGV[1]: scheduler ID (to verify current leader)
-- ARGV[2]: new leadership value (schedulerId:timestamp)
-- ARGV[3]: TTL in milliseconds
--
-- Returns:
-- 1 if renewal successful (was leader and renewed)
-- 0 if not the leader (someone else holds leadership)
-- -1 if no leader exists (expired)

local current_leader = redis.call('GET', KEYS[1])

-- No current leader (expired)
if not current_leader then
    return -1
end

-- Value format: schedulerId:timestamp (scheduler IDs may themselves contain ':')
local prefix = ARGV[1] .. ':'
if current_leader ~= ARGV[1] and string.sub(current_leader, 1, #prefix) ~= prefix then
    return 0
end

-- We are the leader, renew atomically
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
