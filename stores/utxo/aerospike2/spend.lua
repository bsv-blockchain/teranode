-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- vout number - the output index of the UTXO
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- spendingTxID []byte - 32 byte little-endian hash of the spending transaction
-- ttl number - the time-to-live for the UTXO record
function spend(rec, vout, utxoHash, spendingTxID, ttl)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

	if rec['frozen'] then
		return "FROZEN:TX is frozen"
	end

    if rec['big'] then
        return "ERROR:Big TX"
    end

    -- Get the utxos list from the record
    local utxos = rec['utxos'] 
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    -- Get the utxo that we want from the utxos list
    local utxo = utxos[vout+1] -- NB - lua arrays are 1-based!!!!
    if utxo == nil then
        return "ERROR:UTXO not found for vout " .. vout
    end

    -- The first 32 bytes are the utxoHash
    local existingUTXOHash = bytes.get_bytes(utxo, 1, 32) -- NB - lua arrays are 1-based!!!!

    if not bytes_equal(existingUTXOHash, utxoHash) then
        return "ERROR:Output utxohash mismatch"
    end

    if bytes.size(utxo) == 64 then
        local existingSpendingTxID = bytes.get_bytes(utxo, 33, 32) -- NB - lua arrays are 1-based!!!!
        if frozen(existingSpendingTxID) then
			return "FROZEN:UTXO is frozen"
		elseif bytes_equal(existingSpendingTxID, spendingTxID) then
            return 'OK'
        else
            return 'SPENT:' .. existingSpendingTxID
        end
    end

    -- Update the output to spend it by appending the spendingTxID
    -- Resize the utxo to 64 bytes
    local newUtxo = bytes(64)
    
    for i = 1, 32 do -- NB - lua arrays are 1-based!!!!
        newUtxo[i] = utxo[i]
    end
    
    for i = 1, 32 do -- NB - lua arrays are 1-based!!!!
        newUtxo[32 + i] = spendingTxID[i]
    end

    -- Update the record
    utxos[vout+1] = newUtxo -- NB - lua arrays are 1-based!!!!
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

function bytes_equal(a, b)
    if #a ~= #b then -- This syntax #a is the length of the array a and #b is the length of the array b.  They should be equal.
        return false
    end

    for i = 1, #a do 
        if a[i] ~= b[i] then
            return false
        end
    end
    return true
end

function frozen(a)
	if #a ~= 32 then -- Frozen utxos have 32 'FF' bytes.
        return false
    end

    for i = 1, #a do 
        if a[i] ~= 255 then
            return false
        end
    end
    return true
end