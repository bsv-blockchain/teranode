-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- offset number - the offset in the utxos list (vout % utxoBatchSize)
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- spendingTxID []byte - 32 byte little-endian hash of the spending transaction
-- currentBlockHeight number - the current block height
-- ttl number - the time-to-live for the UTXO record
function spend(rec, offset, utxoHash, spendingTxID, currentBlockHeight, ttl)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    -- TODO - when we implement the frozen logic in the RPC call, if the number of outputs are more than 20,000, we need to update
    -- each of the extra records.
	if rec['frozen'] then
		return "FROZEN:TX is frozen"
	end

    local coinbaseSpendingHeight = rec['spendingHeight']
    if coinbaseSpendingHeight and coinbaseSpendingHeight > 0 and coinbaseSpendingHeight > currentBlockHeight then
        return "ERROR:Coinbase UTXO can only be spent after 100 blocks"
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
    local utxo = utxos[offset+1] -- NB - lua arrays are 1-based!!!!
    if utxo == nil then
        return "ERROR:UTXO not found for offset " .. offset
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
            return 'SPENT:' .. bytes_to_hex(existingSpendingTxID)
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
    utxos[offset+1] = newUtxo -- NB - lua arrays are 1-based!!!!
    rec['utxos'] = utxos
    rec['spentUtxos'] = rec['spentUtxos'] + 1

    local signal = ""

    local nrRecords = rec['nrRecords']

    if nrRecords == nil then
        -- This is a pagination record: check if all the utxos are spent
        if rec['spentUtxos'] == rec['nrUtxos'] then
            signal = ":ALLSPENT"
            record.set_ttl(rec, ttl)
        else 
            record.set_ttl(rec, -1)
        end
    else
        -- The is the master record: only set_ttl if all the utxos are spent and the nrRecords is 1  
        if rec['spentUtxos'] == rec['nrUtxos'] and nrRecords == 1 then
            record.set_ttl(rec, ttl)
        else
            record.set_ttl(rec, -1)
        end
    end

    aerospike:update(rec)

    return 'OK' .. signal
end

function bytes_equal(a, b)
    if bytes.size(a) ~= bytes.size(b) then
        return false
    end

    for i = 1, bytes.size(a) do 
        if a[i] ~= b[i] then
            return false
        end
    end
    return true
end

function frozen(a)
    if bytes.size(a) ~= 32 then -- Frozen utxos have 32 'FF' bytes.
        return false
    end

    for i = 1, bytes.size(a) do
        if a[i] ~= 255 then
            return false
        end
    end

    return true
end

function bytes_to_hex(b)
    local hex = ""
    for i = bytes.size(b), 1, -1 do
        hex = hex .. string.format("%02x", b[i])
    end
    return hex
end

function incrementNrRecords(rec, inc, ttl)
    local nrRecords = rec['nrRecords']
    if nrRecords == nil then
       return 'ERROR: nrRecords not found in record. Possible non-master record?'
    end

    nrRecords = nrRecords + inc

    if nrRecords == 0 and rec['spentUtxos'] == rec['nrUtxos'] then
        record.set_ttl(rec, ttl)
    else
        record.set_ttl(rec, -1)
    end

    rec['nrRecords'] = nrRecords

    aerospike:update(rec)

    return 'OK'
end
