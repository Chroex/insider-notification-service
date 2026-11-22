-- Mark a send finished: from now on its entry counts for one more second.
-- LT only ever lowers the score (from "now + max send time" to the finish
-- time); XX does nothing if the entry already left the window.
--
-- KEYS[1] window key, ARGV[1] the member acquire.lua returned

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
return redis.call('ZADD', KEYS[1], 'XX', 'LT', now, ARGV[1])
