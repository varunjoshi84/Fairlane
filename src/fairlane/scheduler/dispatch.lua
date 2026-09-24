-- src/fairlane/scheduler/dispatch.lua
-- Input:
--   KEYS[1] = tasks:stream
--   KEYS[2] = tenants:vtime (sorted set of tenant virtual times)
--   ARGV[1] = batch_size
--   ARGV[2] = max_in_stream_per_tenant
--   ARGV[3] = current_time_ms (from redis.call('TIME'))

local stream_name = KEYS[1]
local tenants_vtime_key = KEYS[2]
local batch_size = tonumber(ARGV[1])
local max_in_stream = tonumber(ARGV[2])
local current_time_ms = tonumber(ARGV[3])

local dispatched_tasks = {}
local dispatched_count = 0

-- Loop up to batch_size times
for i = 1, batch_size do
    -- Pick the best tenant by virtual time
    -- ZRANGE tenants:vtime 0 -1 WITHSCORES returns all tenants, but we need to find one that has capacity
    local tenants = redis.call('ZRANGE', tenants_vtime_key, 0, -1, 'WITHSCORES')
    
    local best_tenant = nil
    local best_vtime = nil
    
    if #tenants == 0 then
        break -- No tenants waiting
    end
    
    for j = 1, #tenants, 2 do
        local tenant_id = tenants[j]
        local vtime = tonumber(tenants[j+1])
        
        -- Check if tenant reached its in-stream cap
        local instream_key = 'instream:' .. tenant_id
        local instream_count = tonumber(redis.call('GET', instream_key) or '0')
        
        if instream_count < max_in_stream then
            best_tenant = tenant_id
            best_vtime = vtime
            break
        end
    end
    
    if not best_tenant then
        break -- All waiting tenants are at max capacity
    end
    
    -- Pop its best task
    local ready_key = 'ready:' .. best_tenant
    local best_tasks = redis.call('ZRANGE', ready_key, 0, 0, 'WITHSCORES')
    
    if #best_tasks == 0 then
        -- This tenant's waiting room is empty, remove it from tenants:vtime
        redis.call('ZREM', tenants_vtime_key, best_tenant)
    else
        local task_id = best_tasks[1]
        
        -- Remove the task from the ready queue
        redis.call('ZREM', ready_key, task_id)
        
        -- Get tenant weight (could be fetched from a Redis hash if we cache limits, or default to 1)
        -- Since Postgres is the source of truth for limits, let's assume we maintain a cached hash or just default 1
        -- Wait, the prompt says "weight comes from tenant_limits.weight". 
        -- If we don't pass the weight to Lua, we can read it from a key "tenant_weight:{tenant_id}".
        local weight_key = 'tenant_weight:' .. best_tenant
        local weight = tonumber(redis.call('GET', weight_key) or '1')
        
        -- Advance virtual time
        local new_vtime = best_vtime + (1.0 / weight)
        
        -- Check if the waiting room is now empty
        local remaining = redis.call('ZCARD', ready_key)
        if remaining == 0 then
            redis.call('ZREM', tenants_vtime_key, best_tenant)
        else
            redis.call('ZADD', tenants_vtime_key, new_vtime, best_tenant)
        end
        
        -- Increment instream
        local instream_key = 'instream:' .. best_tenant
        redis.call('INCR', instream_key)
        
        -- XADD the task to "tasks:stream"
        redis.call('XADD', stream_name, '*', 'task_id', task_id)
        
        table.insert(dispatched_tasks, task_id)
        dispatched_count = dispatched_count + 1
    end
end

return dispatched_tasks
