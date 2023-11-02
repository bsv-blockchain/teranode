
local hash = KEYS[1]
local blockHash = ARGV[1]

-- Get value from Redis
local v = redis.call('GET', hash)
if not v then
  return 'NOT_FOUND'
end

-- Update in Redis
redis.call('SET', hash, v .. blockHash)

return 'OK'
