
local hash = KEYS[1]
local spendingTxID = ARGV[1]
local blockHeight = ARGV[2]

-- Get value from Redis
local v = redis.call('GET', hash)
if not v then
  return 'NOT_FOUND'
end

-- Split comma-separated value
local lockTime, existingSpendingTxID = string.match(v, '(%d+),?(.*)')

lockTime = tonumber(lockTime)

-- Check if spendable
if existingSpendingTxID ~= "" then
  if existingSpendingTxID == spendingTxID then
    return 'OK'
  else
    return 'SPENT,' .. existingSpendingTxID
  end
end


if lockTime > 500000000 then
  local currentUnixTime = tonumber(redis.call('TIME')[1])
  if lockTime > currentUnixTime then
    return 'LOCKED'
  end
end

if lockTime > 0 then
  blockHeight = tonumber(blockHeight)
  if lockTime > blockHeight then
    return 'LOCKED'
  end
end

-- Create new comma-separated value string
local updatedValue = lockTime .. ',' .. spendingTxID

-- Update in Redis
redis.call('SET', hash, updatedValue)

return 'OK'
