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

-- Extract scheduler ID from value (format: schedulerId:timestamp)
local current_scheduler_id = string.match(current_leader, "^([^:]+)")

-- Not the current leader
if current_scheduler_id ~= ARGV[1] then
    return 0
end

-- We are the leader, renew atomically
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
