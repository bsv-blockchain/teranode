-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- spendingTxID []byte - 32 byte little-endian hash of the spending transaction
-- currentBlockHeight number - the current block height
-- currentUnixTime number - the current Unix time
-- ttl number - the time-to-live for the UTXO
function spend(rec, utxoHash, spendingTxID, currentBlockHeight, currentUnixTime, ttl)
  if not aerospike:exists(rec) then
    return "TX not found"
  end

  -- Get the utxo value from the utxos map
  local m = rec['utxos']
  if m == nil then
    return "UTXOs map not found"
  end

  for key, value in map.pairs(m) do
    warn("%s = %s", key, value)
  end

  utxo = m[utxoHash]
  if utxo == nil then
    return "UTXO not found"
  end
  
  -- Split comma-separated value
  local lockTime, existingSpendingTxID = string.match(utxo, '(%d+),?(.*)')

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
    if lockTime > currentUnixTime then
      return 'LOCKED' .. lockTime
    end
  end

  if lockTime > 0 then
    if lockTime > currentBlockHeight then
      return 'LOCKED' .. lockTime .. ',' .. blockHeight
    end
  end

  -- Create new comma-separated value string
  local updatedValue = lockTime .. ',' .. spendingTxID

  -- Update the utxos map
  m[utxoHash] = updatedValue

  -- Update the record
  rec['utxos'] = m

  aerospike:update(rec)

  return 'OK'

end