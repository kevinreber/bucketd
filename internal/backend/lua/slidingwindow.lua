-- slidingwindow.lua — atomic sliding-window Allow operation.
--
-- KEYS[1] = window key (a Redis sorted set, score = event timestamp ms)
-- KEYS[2] = sequence key (a Redis counter, used to mint unique sorted-set members)
-- ARGV[1] = limit (int) — max events permitted in any rolling window
-- ARGV[2] = window_ms (int) — window duration in milliseconds
-- ARGV[3] = events requested (int)
--
-- Returns: {allowed (0|1), remaining (int), retry_after_ms (int)}
--
-- Members in the sorted set are scored by the event timestamp; the member
-- value itself is a globally-unique integer (INCR'd from KEYS[2]). This
-- guarantees we can record multiple events at the same millisecond without
-- collisions.

local zset_key = KEYS[1]
local seq_key = KEYS[2]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local time = redis.call('TIME')
local now_ms = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)
local cutoff_ms = now_ms - window_ms

-- Drop expired members.
redis.call('ZREMRANGEBYSCORE', zset_key, '-inf', '(' .. cutoff_ms)

local count = tonumber(redis.call('ZCARD', zset_key))
local remaining = math.max(0, limit - count)

if count + requested > limit then
  -- Denied. Retry-after = when oldest in-window event ages out.
  local oldest = redis.call('ZRANGE', zset_key, 0, 0, 'WITHSCORES')
  local retry_after_ms = 0
  if oldest[2] then
    retry_after_ms = math.max(0, math.floor(tonumber(oldest[2])) + window_ms - now_ms)
  end
  return {0, remaining, retry_after_ms}
end

-- Allowed. Record the events with unique member IDs.
-- Batch the counter bump (one INCRBY) and the sorted-set inserts (one ZADD
-- with N members), so the script blocks Redis for O(1) round-trip work
-- instead of O(requested) serial commands.
local end_seq = tonumber(redis.call('INCRBY', seq_key, requested))
local start_seq = end_seq - requested + 1
local zadd_args = {}
for seq = start_seq, end_seq do
  table.insert(zadd_args, now_ms)
  table.insert(zadd_args, tostring(seq))
end
redis.call('ZADD', zset_key, unpack(zadd_args))

-- Auto-expire the window and sequence keys when no events have arrived
-- for 2x the window duration.
local ttl_ms = window_ms * 2
redis.call('PEXPIRE', zset_key, ttl_ms)
redis.call('PEXPIRE', seq_key, ttl_ms)

return {1, math.max(0, limit - count - requested), 0}
