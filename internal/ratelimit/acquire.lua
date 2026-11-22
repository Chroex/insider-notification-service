-- Sliding window of sends: try to take one send slot from KEYS[1].
--
-- KEYS[1] is a sorted set with one entry per send that still counts against
-- the limit. An entry counts while its send is in flight and for one second
-- after the send finished, so whenever the provider sees a request (at some
-- moment between our start and its answer) no 1-second window it measures
-- can hold more than ARGV[1] requests, however late the network delivers
-- them. Scores encode that:
--   * on grant, score = now + max send time (ARGV[2]): the entry counts
--     while the send is in flight, and still expires if the worker dies;
--   * done.lua lowers it to the finish time once the send returns.
-- An entry leaves the window when its score is one second old.
--
-- Runs atomically inside Redis, so every worker replica sees one consistent
-- window with no read-modify-write races. Time comes from Redis (TIME), not
-- from the caller, so clock skew between worker hosts cannot inflate the rate.
--
-- KEYS[1]  window key, e.g. "ratelimit:{sms}"
-- KEYS[2]  sequence key that makes every entry unique
-- ARGV[1]  limit: sends allowed per 1-second window
-- ARGV[2]  max send time in microseconds (provider timeout plus a margin)
-- Returns  {member, wait_ms}: member > 0 when granted (pass it to done.lua);
--          otherwise wait_ms says when to try again, so callers sleep
--          instead of spinning.

local key      = KEYS[1]
local seqKey   = KEYS[2]
local limit    = tonumber(ARGV[1])
local maxSend  = tonumber(ARGV[2])
local windowUs = 1000000
local maxWaitMs = 100 -- a send may finish early; re-check at least this often

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2]) -- microseconds

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - windowUs)

-- Let the keys expire once every entry could have left the window, but
-- never shorten a TTL: another replica (e.g. during a rolling change of
-- PROVIDER_TIMEOUT) may hold a send with a longer max send time.
local function extend(k, ttl)
  local current = redis.call('PTTL', k) -- -1: no TTL yet, -2: no key
  if current < ttl then
    redis.call('PEXPIRE', k, ttl)
  end
end

local ttl = math.ceil((maxSend + windowUs) / 1000) + 1000
if redis.call('ZCARD', key) < limit then
  local member = redis.call('INCR', seqKey)
  redis.call('ZADD', key, now + maxSend, member)
  extend(key, ttl)
  extend(seqKey, ttl)
  return {member, 0}
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local wait = math.ceil((tonumber(oldest[2]) + windowUs - now) / 1000)
return {0, math.max(1, math.min(wait, maxWaitMs))}
