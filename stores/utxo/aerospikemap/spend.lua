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
    local utxos = rec['utxos']
    if utxos == nil then
        return "UTXOs map not found"
    end

    existingSpendingTxID = utxos[utxoHash]
    if existingSpendingTxID == nil then
        return "UTXO not found"
    end

    lockTime = rec['locktime']

    -- Check if spendable
    if existingSpendingTxID ~= "" then
        if existingSpendingTxID == spendingTxID then
            return 'OK'
        else
            return 'SPENT:' .. existingSpendingTxID
        end
    end

    -- Check if locked
    if lockTime > 500000000 then
        if lockTime > currentUnixTime then
            return 'LOCKED:' .. lockTime
        end
    elseif lockTime > 0 then
        if lockTime > currentBlockHeight then
            return 'LOCKED:' .. lockTime
        end
    end

    -- Update the utxos map
    utxos[utxoHash] = spendingTxID

    -- Update the record
    rec['utxos'] = utxos
    rec['spentUtxos'] = rec['spentUtxos'] + 1

    -- check whether all utxos gave been spent
    if rec['spentUtxos'] == rec['nrUtxos'] then
        rec.set_ttl(rec, ttl)
    end

    aerospike:update(rec)

    return 'OK'
end
