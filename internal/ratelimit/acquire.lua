-- Token bucket: take up to ARGV[3] tokens from KEYS[1].
--
-- Runs atomically inside Redis, so every worker replica sees one consistent
-- bucket with no read-modify-write races. Time comes from Redis (TIME), not
-- from the caller, so clock skew between worker hosts cannot inflate the rate.
--
-- KEYS[1]  bucket key, e.g. "ratelimit:sms"
-- ARGV[1]  refill rate in tokens per second
-- ARGV[2]  capacity (burst)
-- ARGV[3]  tokens requested (>= 1)
-- Returns  {granted, wait_ms}: granted may be less than requested (partial
--          grant); wait_ms > 0 only when nothing was granted and says when
--          the next whole token will exist, so callers sleep instead of spin.

local key       = KEYS[1]
local rate      = tonumber(ARGV[1])
local burst     = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2]) -- microseconds

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = burst -- a new (or expired) bucket starts full
  ts = now
end

-- Refill for the time elapsed since the last call, capped at capacity.
local elapsed = math.max(0, now - ts)
tokens = math.min(burst, tokens + elapsed * rate / 1000000)

local granted = math.min(requested, math.floor(tokens))
tokens = tokens - granted

local wait_ms = 0
if granted == 0 then
  wait_ms = math.ceil((1 - tokens) * 1000 / rate)
end

-- tostring keeps the fractional part of the token count.
redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
-- An idle bucket refills completely after burst/rate seconds; after that its
-- state is irrelevant, so let it expire instead of keeping keys forever.
redis.call('PEXPIRE', key, math.ceil(burst * 1000 / rate) + 1000)

return {granted, wait_ms}
