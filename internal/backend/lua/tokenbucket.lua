-- tokenbucket.lua — atomic token bucket Allow operation.
--
-- KEYS[1] = bucket key (a Redis hash storing tokens + last_refill_ms)
-- ARGV[1] = capacity (int)
-- ARGV[2] = refill_rate (float, tokens per second)
-- ARGV[3] = tokens requested (int)
--
-- Returns: {allowed (0|1), remaining (int), retry_after_ms (int)}
--
-- Atomicity: this script runs to completion in a single Redis cycle, so
-- the read-compute-write of tokens + last_refill_ms is race-free across
-- concurrent clients.

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

-- Server-side clock. Redis 7+ scripts are auto-replicated by effect, so
-- non-deterministic functions like TIME are fine.
local time = redis.call('TIME')
local now_ms = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)

-- Load state. Both fields missing = new bucket; start full.
local state = redis.call('HMGET', key, 'tokens', 'last_refill_ms')
local tokens = tonumber(state[1])
local last_refill_ms = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  last_refill_ms = now_ms
end

-- Lazy refill.
local elapsed_ms = now_ms - last_refill_ms
if elapsed_ms > 0 then
  local accrued = (elapsed_ms / 1000.0) * refill_rate
  tokens = math.min(capacity, tokens + accrued)
  last_refill_ms = now_ms
end

local allowed = 0
local retry_after_ms = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  local deficit = requested - tokens
  retry_after_ms = math.floor((deficit / refill_rate) * 1000)
end

-- Persist updated state.
redis.call('HSET', key, 'tokens', tokens, 'last_refill_ms', last_refill_ms)

-- Auto-expire abandoned buckets. TTL = max(60s, 2x time to refill from empty
-- to capacity) so an idle bucket eventually GCs but a quiet-then-active
-- bucket retains its accumulated state.
local ttl_seconds = math.max(60, math.ceil((capacity / refill_rate) * 2))
redis.call('EXPIRE', key, ttl_seconds)

return {allowed, math.floor(tokens), retry_after_ms}
