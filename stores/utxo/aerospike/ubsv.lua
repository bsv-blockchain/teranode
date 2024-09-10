-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- offset number - the offset in the utxos list (vout % utxoBatchSize)
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- spendingTxID []byte - 32 byte little-endian hash of the spending transaction
-- currentBlockHeight number - the current block height
-- ttl number - the time-to-live for the UTXO record
--                           _
--  ___ _ __   ___ _ __   __| |
-- / __| '_ \ / _ \ '_ \ / _` |
-- \__ \ |_) |  __/ | | | (_| |
-- |___/ .__/ \___|_| |_|\__,_|
--     |_|
--
function spend(rec, offset, utxoHash, spendingTxID, currentBlockHeight, ttl)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    if rec['frozen'] then
		return "FROZEN:TX is frozen"
	end

    local coinbaseSpendingHeight = rec['spendingHeight']
    if coinbaseSpendingHeight and coinbaseSpendingHeight > 0 and coinbaseSpendingHeight > currentBlockHeight then
        return "ERROR:Coinbase UTXO can only be spent after 100 blocks, in block " .. coinbaseSpendingHeight .. " or greater. The current block height is " .. currentBlockHeight 
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
        
        if bytes_equal(existingSpendingTxID, spendingTxID) then
            return 'OK'
        elseif frozen(existingSpendingTxID) then
			return "FROZEN:UTXO is frozen"
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

    local signal = setTTL(rec, ttl)

    aerospike:update(rec)

    return 'OK' .. signal
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- offset number - the offset in the utxos list (vout % utxoBatchSize)-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- spendingTxID []byte - 32 byte little-endian hash of the spending transaction
-- ttl number - the time-to-live for the UTXO record
--              ____                       _
--  _   _ _ __ / ___| _ __   ___ _ __   __| |
-- | | | | '_ \\___ \| '_ \ / _ \ '_ \ / _` |
-- | |_| | | | |___) | |_) |  __/ | | | (_| |
--  \__,_|_| |_|____/| .__/ \___|_| |_|\__,_|
--                   |_|
--
function unSpend(rec, offset, utxoHash)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    -- Get the correct output record
    local utxos = rec['utxos'] 
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    local utxo = utxos[offset+1] -- NB - lua arrays are 1-based!!!!
    if utxo == nil then
        return "ERROR:UTXO not found"
    end

    -- The first 32 bytes are the utxoHash
    local existingUTXOHash = bytes.get_bytes(utxo, 1, 32) -- NB - lua arrays are 1-based!!!!
    if not bytes_equal(existingUTXOHash, utxoHash) then
        return "ERROR:Output utxohash mismatch"
    end

    local signal = ""

    -- If the utxo has been spent, remove the spendingTxID
    if bytes.size(utxo) == 64 then
        local newUtxo = bytes(32)

        for i = 1, 32 do
            newUtxo[i] = utxo[i]
        end

        local nrUtxos = rec['nrUtxos']
        local spentUtxos = rec['spentUtxos']

        if nrUtxos == spentUtxos then
            signal = ":NOTALLSPENT"
        end

        -- Update the record
        utxos[offset+1] = newUtxo -- NB - lua arrays are 1-based!!!!
        rec['utxos'] = utxos
        rec['spentUtxos'] = spentUtxos - 1
    end

    record.set_ttl(rec, -1)

    aerospike:update(rec)

    return 'OK' .. signal
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- blockID number - the block ID
-- ttl number - the time-to-live for the UTXO record
--           _   __  __ _                _
--  ___  ___| |_|  \/  (_)_ __   ___  __| |
-- / __|/ _ \ __| |\/| | | '_ \ / _ \/ _` |
-- \__ \  __/ |_| |  | | | | | |  __/ (_| |
-- |___/\___|\__|_|  |_|_|_| |_|\___|\__,_|
--
function setMined(rec, blockID, ttl)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    -- Check if the bin exists; if not, initialize it as an empty list
    if rec['blockIDs'] == nil then
        rec['blockIDs'] = list()
    end

    -- Append the value to the list in the specified bin
    local blocks = rec['blockIDs']
    blocks[#blocks + 1] = blockID
    rec['blockIDs'] = blocks

    local signal = setTTL(rec, ttl)
    
    -- Update the record to save changes
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
   
    rec['nrRecords'] = nrRecords

    local signal = setTTL(rec, ttl)

    aerospike:update(rec)

    return 'OK' .. signal
end

function setTTL(rec, ttl)
    -- Check if all the utxos are spent and set the TTL, but only for transactions that have been in at least one block
    local blockIDs = rec['blockIDs']
    local nrRecords = rec['nrRecords']
    local signal = ""

    if nrRecords == nil then
        -- This is a pagination record: check if all the utxos are spent
        if rec['spentUtxos'] == rec['nrUtxos'] then
            signal = ":ALLSPENT"
        end
        -- TTL is determined externally for pagination records; do not set TTL here
        -- as we only want to set TTL for this record and all the other pagination records
        -- when the TX is fully spent and all the UTXOs are spent
    else
        -- This is a master record: only set TTL if nrRecords is 1 and blockIDs has at least one item
        if nrRecords == 1 and blockIDs and #blockIDs > 0 then
            record.set_ttl(rec, ttl)
            signal = ":TTLSET"
        else
            record.set_ttl(rec, -1)
        end
    end

    return signal
end
