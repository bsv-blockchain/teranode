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
    local utxos = rec['utxos'] 
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    local utxo = utxos[vout]
    if utxo == nil then
        return "ERROR:UTXO not found"
    end

    -- The first 32 bytes are the utxoHash
    local existingUTXOHash = bytes.get_bytes(utxo, 0, 32)
    if existingUTXOHash ~= utxoHash then
        return "ERROR:Output utxohash mismatch"
    end

    if bytes.size(utxo) == 64 then
        local existingSpendingTxID = bytes.get_bytes(utxo, 32, 32)
        if existingSpendingTxID == spendingTxID then
            return 'OK'
        else
            return 'SPENT:' .. existingSpendingTxID
        end
    end

    -- Update the output to spend it by appending the spendingTxID
    bytes.append_bytes(utxo, spendingTxID)

    -- Update the record
    utxos[vout] = utxo
    rec['utxos'] = utxos
    rec['spentUtxos'] = rec['spentUtxos'] + 1

    -- check whether all utxos have been spent
    if rec['spentUtxos'] == rec['nrUtxos'] then
        rec['lastSpend'] = os.time()
        record.set_ttl(rec, ttl)
    else
        -- why is this needed? the record should already have a non expiring ttl
        -- tests showed the ttl being set to some default value
        record.set_ttl(rec, -1)
    end

    aerospike:update(rec)

    return 'OK'
end
