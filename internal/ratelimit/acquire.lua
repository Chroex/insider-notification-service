-- Sliding window log: take up to ARGV[3] send slots from KEYS[1].
--
-- KEYS[1] is a sorted set holding one entry per grant in the last second,
-- scored by grant time. A grant is allowed only while the set holds fewer
-- than ARGV[1] entries, so no 1-second window ever sees more than the
-- limit (the brief's "maximum 100 messages per second per channel"). A slot
-- frees exactly when its grant turns one second old, so unlike a token
-- bucket no capacity is lost when a caller comes back late.
--
-- Runs atomically inside Redis, so every worker replica sees one consistent
-- window with no read-modify-write races. Time comes from Redis (TIME), not
-- from the caller, so clock skew between worker hosts cannot inflate the rate.
--
-- KEYS[1]  window key, e.g. "ratelimit:sms"
-- KEYS[2]  sequence key that makes every entry unique
-- ARGV[1]  limit: grants allowed per 1-second window
-- ARGV[2]  tokens requested (>= 1)
-- Returns  {granted, wait_ms}: granted may be less than requested (partial
--          grant); wait_ms > 0 only when nothing was granted and says when
--          the oldest grant leaves the window, so callers sleep, not spin.

local key       = KEYS[1]
local seqKey    = KEYS[2]
local limit     = tonumber(ARGV[1])
local requested = tonumber(ARGV[2])
local windowUs  = 1000000

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2]) -- microseconds

-- Grants exactly one second old or older are outside the window.
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - windowUs)

local used    = redis.call('ZCARD', key)
local granted = math.max(0, math.min(requested, limit - used))

if granted > 0 then
  local last = redis.call('INCRBY', seqKey, granted)
  for i = last - granted + 1, last do
    redis.call('ZADD', key, now, i)
  end
end

local wait_ms = 0
if granted == 0 then
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  wait_ms = math.max(1, math.ceil((tonumber(oldest[2]) + windowUs - now) / 1000))
end

-- After a quiet second the window is empty and the keys are irrelevant, so
-- let them expire instead of keeping keys forever.
redis.call('PEXPIRE', key, 2000)
redis.call('PEXPIRE', seqKey, 2000)

return {granted, wait_ms}
