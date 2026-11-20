-- Return unused tokens (e.g. we took 5 but only 2 rows were due), capped at
-- capacity. The refill timestamp is left alone: refill since then is applied
-- by the next acquire.
--
-- KEYS[1] bucket key, ARGV[1] tokens to return, ARGV[2] capacity (burst)

local key   = KEYS[1]
local n     = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])

local tokens = tonumber(redis.call('HGET', key, 'tokens'))
if tokens == nil then
  return 0 -- bucket expired: it will be recreated full anyway
end
redis.call('HSET', key, 'tokens', tostring(math.min(burst, tokens + n)))
return 1
