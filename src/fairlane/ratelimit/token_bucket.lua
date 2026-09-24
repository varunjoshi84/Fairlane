-- src/fairlane/ratelimit/token_bucket.lua
-- KEYS[1] : ratelimit:{tenant_id}
-- ARGV[1] : rate_per_second
-- ARGV[2] : burst_capacity
-- ARGV[3] : tokens_requested (default 1)
-- ARGV[4] : key expiry in seconds (default 60)

local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local requested = tonumber(ARGV[3]) or 1
local expiry = tonumber(ARGV[4]) or 60

-- get current time from Redis clock (array: [unix_ts, microseconds])
local now_arr = redis.call('TIME')
local now_ms = tonumber(now_arr[1]) * 1000 + math.floor(tonumber(now_arr[2]) / 1000)

local hash = redis.call('HMGET', key, 'tokens', 'updated_ms')
local tokens = tonumber(hash[1])
local updated_ms = tonumber(hash[2])

if not tokens then
    tokens = burst
    updated_ms = now_ms
end

-- refill calculation
local elapsed_ms = math.max(0, now_ms - updated_ms)
local refill = (elapsed_ms / 1000) * rate
tokens = math.min(burst, tokens + refill)

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'updated_ms', now_ms)
    redis.call('EXPIRE', key, expiry)
    return {1, tostring(tokens), 0}
else
    -- Calculate retry after ms
    local deficit = requested - tokens
    local retry_after_ms = math.ceil((deficit / rate) * 1000)
    -- Do not consume tokens, but update time to save future refill calculation
    redis.call('HMSET', key, 'tokens', tokens, 'updated_ms', now_ms)
    redis.call('EXPIRE', key, expiry)
    return {0, tostring(tokens), retry_after_ms}
end
