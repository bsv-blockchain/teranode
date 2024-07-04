-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- vout number - the output index of the UTXO
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- spendingTxID []byte - 32 byte little-endian hash of the spending transaction
-- ttl number - the time-to-live for the UTXO record
function unSpend(rec, vout, utxoHash)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    if rec['big'] then
        return "ERROR:Big TX"
    end

    -- Get the correct output record
    local utxos = rec['utxos'] 
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    local utxo = utxos[vout+1] -- NB - lua arrays are 1-based!!!!
    if utxo == nil then
        return "ERROR:UTXO not found"
    end

    -- The first 32 bytes are the utxoHash
    local existingUTXOHash = bytes.get_bytes(utxo, 1, 32) -- NB - lua arrays are 1-based!!!!
    if not bytes_equal(existingUTXOHash, utxoHash) then
        return "ERROR:Output utxohash mismatch"
    end

    -- If the utxo has been spent, remove the spendingTxID
    if bytes.size(utxo) == 64 then
        local newUtxo = bytes(32)

        for i = 1, 32 do
            newUtxo[i] = utxo[i]
        end

        -- Update the record
        utxos[vout+1] = newUtxo -- NB - lua arrays are 1-based!!!!
        rec['utxos'] = utxos
        rec['spentUtxos'] = rec['spentUtxos'] - 1
        rec['lastSpend'] = nil
    end

    record.set_ttl(rec, -1)

    aerospike:update(rec)

    return 'OK'
end

function bytes_equal(a, b)
    if #a ~= #b then -- This syntax #a is the length of the array a and #b is the length of the array b.  They should be equal.
        return false
    end

    for i = 1, #a do
        if a:sub(i, i) ~= b:sub(i, i) then
            return false
        end
    end
    return true
end