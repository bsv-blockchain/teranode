-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- vout number - the output index of the UTXO
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- spendingTxID []byte - 32 byte little-endian hash of the spending transaction
-- ttl number - the time-to-live for the UTXO record
function spend(rec, vout, utxoHash, spendingTxID, ttl)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    if rec['big'] then
        return "ERROR:Big TX"
    end

    -- Get the correct output record
    local outputs = rec['outputs'] 
    if outputs == nil then
        return "ERROR:Outputs not found"
    end

    local output = outputs[vout]
    if output == nil then
        return "ERROR:Output not found"
    end

    if output.utxoHash ~= utxoHash then
        return "ERROR:Output utxohash mismatch"
    end

    existingSpendingTxID = output.spendingTxID
    if existingSpendingTxID == nil then
        return "ERROR:UTXO not found"
    end

    -- Check if spendable
    if existingSpendingTxID ~= "" then
        if existingSpendingTxID == spendingTxID then
            return 'OK'
        else
            return 'SPENT:' .. existingSpendingTxID
        end
    end

    -- Update the output to spend it
    output.spendingTxID = spendingTxID

    -- Update the record
    outputs[vout] = output
    rec['outputs'] = outputs
    rec['spentUtxos'] = rec['spentUtxos'] + 1

    -- check whether all utxos have been spent
    if rec['spentUtxos'] == rec['nrUtxos'] then
        rec['lastSpend'] = currentUnixTime
        record.set_ttl(rec, ttl)
    else
        -- why is this needed? the record should already have a non expiring ttl
        -- tests showed the ttl being set to some default value
        record.set_ttl(rec, -1)
    end

    aerospike:update(rec)

    return 'OK'
end
