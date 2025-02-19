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
function spend(rec, offset, utxoHash, spendingTxID, ignoreUnspendable, currentBlockHeight, ttl)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    if not ignoreUnspendable then
        if rec['conflicting'] then
            return "CONFLICTING:TX is conflicting"
        end

        if rec['unspendable'] then
            return "UNSPENDABLE:TX is unspendable"
        end
    end

    if rec['frozen'] then
        return "FROZEN:TX is frozen"
    end

    local coinbaseSpendingHeight = rec['spendingHeight']
    if coinbaseSpendingHeight and coinbaseSpendingHeight > 0 and coinbaseSpendingHeight > currentBlockHeight then
        return "COINBASE_IMMATURE:Coinbase UTXO can only be spent after 100 blocks, in block " .. coinbaseSpendingHeight .. " or greater. The current block height is " .. currentBlockHeight
    end

    -- Get the utxos list from the record
    local utxos = rec['utxos']
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    -- Get the utxo that we want from the utxos list
    local utxo = utxos[offset + 1] -- NB - lua arrays are 1-based!!!!
    if utxo == nil then
        return "ERROR:UTXO not found for offset " .. offset
    end

    -- Check if the utxo is spendable, this is used by the alert system
    local utxoSpendableIn = rec['utxoSpendableIn']
    if utxoSpendableIn and utxoSpendableIn[offset] and utxoSpendableIn[offset] < currentBlockHeight then
        return "FROZEN:UTXO is not spendable until block " .. utxoSpendableIn[offset]
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

    for i = 1, 32 do
        -- NB - lua arrays are 1-based!!!!
        newUtxo[i] = utxo[i]
    end

    for i = 1, 32 do
        -- NB - lua arrays are 1-based!!!!
        newUtxo[32 + i] = spendingTxID[i]
    end

    -- Update the record
    utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!
    rec['utxos'] = utxos
    rec['spentUtxos'] = rec['spentUtxos'] + 1

    local signal = setTTL(rec, ttl)

    aerospike:update(rec)

    return 'OK' .. signal
end

function spendMulti(rec, spends, ignoreUnspendable, currentBlockHeight, ttl)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    if not ignoreUnspendable then
        if rec['conflicting'] then
            return "CONFLICTING:TX is conflicting"
        end

        if rec['unspendable'] then
            return "UNSPENDABLE:TX is unspendable"
        end
    end

    if rec['frozen'] then
        return "FROZEN:TX is frozen"
    end

    local coinbaseSpendingHeight = rec['spendingHeight']
    if coinbaseSpendingHeight and coinbaseSpendingHeight > 0 and coinbaseSpendingHeight > currentBlockHeight then
        return "COINBASE_IMMATURE:Coinbase UTXO can only be spent after 100 blocks, in block " .. coinbaseSpendingHeight .. " or greater. The current block height is " .. currentBlockHeight
    end

    -- Get the utxos list from the record
    local utxos = rec['utxos']
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    -- loop through the spends
    for spend in list.iterator(spends) do
        local offset = spend['offset']
        local utxoHash = spend['utxoHash']
        local spendingTxID = spend['spendingTxID']

        -- Get the utxo that we want from the utxos list
        local utxo = utxos[offset + 1] -- NB - lua arrays are 1-based!!!!
        if utxo == nil then
            return "ERROR:UTXO not found for offset " .. offset
        end

        if rec['utxoSpendableIn'] then
            if rec['utxoSpendableIn'][offset] and rec['utxoSpendableIn'][offset] >= currentBlockHeight then
                return "FROZEN:UTXO is not spendable until block " .. rec['utxoSpendableIn'][offset]
            end
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

        for i = 1, 32 do
            -- NB - lua arrays are 1-based!!!!
            newUtxo[i] = utxo[i]
        end

        for i = 1, 32 do
            -- NB - lua arrays are 1-based!!!!
            newUtxo[32 + i] = spendingTxID[i]
        end

        -- Update the record
        utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!
        rec['spentUtxos'] = rec['spentUtxos'] + 1
    end

    -- Update the record with the new utxos
    rec['utxos'] = utxos

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
function unspend(rec, offset, utxoHash)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    -- Get the correct output record
    local utxos = rec['utxos']
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    local utxo = utxos[offset + 1] -- NB - lua arrays are 1-based!!!!
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

        -- Check if all the UTXOs were spent, which means the record had been marked as all spent
        -- the NOTALLSPENT signal sends back to the caller the information that not all UTXOs are spent (anymore)
        if nrUtxos == spentUtxos then
            if rec['external'] then
                signal = ":NOTALLSPENT:EXTERNAL"
            else
                signal = ":NOTALLSPENT"
            end
        end

        -- Update the record
        utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!
        rec['utxos'] = utxos
        rec['spentUtxos'] = spentUtxos - 1
    end

    -- unset the ttl directly, since we are already returning a possible signal
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
function setMined(rec, blockID, blockHeight, subtreeIdx, ttl)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    -- Check if the bin exists; if not, initialize it as an empty list
    if rec['blockIDs'] == nil then
        rec['blockIDs'] = list()
    end
    if rec['blockHeights'] == nil then
        rec['blockHeights'] = list()
    end
    if rec['subtreeIdxs'] == nil then
        rec['subtreeIdxs'] = list()
    end

    -- Append the value to the list in the specified bin
    local blocks = rec['blockIDs']
    blocks[#blocks + 1] = blockID
    rec['blockIDs'] = blocks

    local heights = rec['blockHeights']
    heights[#heights + 1] = blockHeight
    rec['blockHeights'] = heights

    local subtreeIdxs = rec['subtreeIdxs']
    subtreeIdxs[#subtreeIdxs + 1] = subtreeIdx
    rec['subtreeIdxs'] = subtreeIdxs

    local signal = setTTL(rec, ttl)

    -- Update the record to save changes
    aerospike:update(rec)

    return 'OK' .. signal
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- offset number - the offset in the utxos list (vout % utxoBatchSize)
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
--   __
--  / _|_ __ ___  ___ _______
-- | |_| '__/ _ \/ _ \_  / _ \
-- |  _| | |  __/  __// /  __/
-- |_| |_|  \___|\___/___\___|
function freeze(rec, offset, utxoHash)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    -- Get the correct output record
    local utxos = rec['utxos']
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    local utxo = utxos[offset + 1] -- NB - lua arrays are 1-based!!!!
    if utxo == nil then
        return "ERROR:UTXO not found"
    end

    -- The first 32 bytes are the utxoHash
    local existingUTXOHash = bytes.get_bytes(utxo, 1, 32) -- NB - lua arrays are 1-based!!!!
    if not bytes_equal(existingUTXOHash, utxoHash) then
        return "ERROR:Output utxohash mismatch"
    end

    local signal = ""

    -- If the utxo has been spent, trigger alert
    if bytes.size(utxo) == 64 then
        local existingSpendingTxID = bytes.get_bytes(utxo, 33, 32) -- NB - lua arrays are 1-based!!!!

        if frozen(existingSpendingTxID) then
            return "FROZEN:UTXO is already frozen"
        else
            return 'SPENT:' .. bytes_to_hex(existingSpendingTxID)
        end
    end

    if not bytes.size(utxo) == 32 then
        return "ERROR:UTXO has an invalid size"
    end

    -- Update the output to freeze it by setting the spendingTxID to 32 'FF' bytes
    -- Resize the utxo to 64 bytes
    local newUtxo = bytes(64)

    for i = 1, 32 do
        -- NB - lua arrays are 1-based!!!!
        newUtxo[i] = utxo[i]
    end

    for i = 1, 32 do
        -- NB - lua arrays are 1-based!!!!
        newUtxo[32 + i] = 255
    end

    -- Update the record
    utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!

    rec['utxos'] = utxos

    aerospike:update(rec)

    return 'OK' .. signal
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- offset number - the offset in the utxos list (vout % utxoBatchSize)
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
--               __
--  _   _ _ __  / _|_ __ ___  ___ _______
-- | | | | '_ \| |_| '__/ _ \/ _ \_  / _ \
-- | |_| | | | |  _| | |  __/  __// /  __/
--  \__,_|_| |_|_| |_|  \___|\___/___\___|
--
function unfreeze(rec, offset, utxoHash)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    -- Get the correct output record
    local utxos = rec['utxos']
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    local utxo = utxos[offset + 1] -- NB - lua arrays are 1-based!!!!
    if utxo == nil then
        return "ERROR:UTXO not found"
    end

    -- The first 32 bytes are the utxoHash
    local existingUTXOHash = bytes.get_bytes(utxo, 1, 32) -- NB - lua arrays are 1-based!!!!
    if not bytes_equal(existingUTXOHash, utxoHash) then
        return "ERROR:Output utxohash mismatch"
    end

    local signal = ""

    if not bytes.size(utxo) == 64 then
        return "ERROR:UTXO has an invalid size"
    end

    -- If the utxo has been spent, trigger alert
    local existingSpendingTxID = bytes.get_bytes(utxo, 33, 32) -- NB - lua arrays are 1-based!!!!

    if not frozen(existingSpendingTxID) then
        return "ERROR:UTXO is not frozen"
    end

    -- Update the output utxo to the new utxo
    local newUtxo = bytes(32)

    for i = 1, 32 do
        -- NB - lua arrays are 1-based!!!!
        newUtxo[i] = utxo[i]
    end

    -- Update the record
    utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!

    rec['utxos'] = utxos

    aerospike:update(rec)

    return 'OK' .. signal
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- offset number - the offset in the utxos list (vout % utxoBatchSize)
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- newUtxoHash []byte - 32 byte little-endian hash of the new UTXO
--                         _
--  _ __ ___  __ _ ___ ___(_) __ _ _ __
-- | '__/ _ \/ _` / __/ __| |/ _` | '_ \
-- | | |  __/ (_| \__ \__ \ | (_| | | | |
-- |_|  \___|\__,_|___/___/_|\__, |_| |_|
--                           |___/
function reassign(rec, offset, utxoHash, newUtxoHash, blockHeight, spendableAfter)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    -- Get the correct output record
    local utxos = rec['utxos']
    if utxos == nil then
        return "ERROR:UTXOs list not found"
    end

    local utxo = utxos[offset + 1] -- NB - lua arrays are 1-based!!!!
    if utxo == nil then
        return "ERROR:UTXO not found"
    end

    -- The first 32 bytes are the utxoHash
    local existingUTXOHash = bytes.get_bytes(utxo, 1, 32) -- NB - lua arrays are 1-based!!!!
    if not bytes_equal(existingUTXOHash, utxoHash) then
        return "ERROR:Output utxohash mismatch"
    end

    local signal = ""

    if not bytes.size(utxo) == 64 then
        return "ERROR:UTXO has an invalid size"
    end

    -- check whether the utxo is frozen, this is a requirement for reassignment
    local existingSpendingTxID = bytes.get_bytes(utxo, 33, 32) -- NB - lua arrays are 1-based!!!!
    if not frozen(existingSpendingTxID) then
        return "ERROR:UTXO is not frozen"
    end

    -- Update the output to the new utxoHash
    -- Resize the utxo to 64 bytes
    local newUtxo = bytes(32)

    for i = 1, 32 do
        -- NB - lua arrays are 1-based!!!!
        newUtxo[i] = newUtxoHash[i]
    end

    -- Update the record
    utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!

    rec['utxos'] = utxos

    -- check whether we have a reassignment record
    if rec['reassignments'] == nil then
        rec['reassignments'] = list()
    end

    if rec['utxoSpendableIn'] == nil then
        rec['utxoSpendableIn'] = map()
    end

    -- append the new utxoHash to the reassignments list
    rec['reassignments'][#rec['reassignments'] + 1] = map {
        offset = offset,
        utxoHash = utxoHash,
        newUtxoHash = newUtxoHash,
        blockHeight = blockHeight
    }

    rec['utxoSpendableIn'][offset] = blockHeight + spendableAfter

    -- increase the nr of utxos in this record, to make sure it is never ttl'ed, even if all utxos are spent
    rec['nrUtxos'] = rec['nrUtxos'] + 1

    aerospike:update(rec)

    return 'OK' .. signal
end

-- Function to compare two byte arrays for equality
-- Parameters:
--   a: byte array - The first byte array to compare
--   b: byte array - The second byte array to compare
-- Returns:
--   boolean - true if the byte arrays are equal, false otherwise
function bytes_equal(a, b)
    -- Check if the sizes of the byte arrays are different
    if bytes.size(a) ~= bytes.size(b) then
        return false
    end

    -- Compare each byte in the arrays
    for i = 1, bytes.size(a) do
        if a[i] ~= b[i] then
            return false
        end
    end

    -- If all bytes are equal, return true
    return true
end

-- Function to check if a UTXO is frozen
-- A UTXO is considered frozen if its 'frozen' field is a byte array of 32 'FF' bytes
-- Parameters:
--   a: byte array - The 'frozen' field of the UTXO
-- Returns:
--   boolean - true if the UTXO is frozen, false otherwise
--  __                        
-- / _|_ __ ___ _______ _ __  
-- | |_| '__/ _ \_  / _ \ '_ \ 
-- |  _| | | (_) / /  __/ | | |
-- |_| |_|  \___/___\___|_| |_|
--                           
function frozen(a)
    if bytes.size(a) ~= 32 then
        -- Frozen utxos have 32 'FF' bytes.
        return false
    end

    for i = 1, bytes.size(a) do
        if a[i] ~= 255 then
            return false
        end
    end

    return true
end

-- Function to convert a byte array to a hexadecimal string
-- Parameters:
--   b: byte array - The byte array to convert
-- Returns:
--   string - The hexadecimal representation of the byte array
function bytes_to_hex(b)
    local hex = ""
    for i = bytes.size(b), 1, -1 do
        hex = hex .. string.format("%02x", b[i])
    end
    return hex
end

-- Increment the number of records and set TTL if necessary
--  _                                          _   
-- (_)_ __   ___ _ __ ___ _ __ ___   ___ _ __ | |_ 
-- | | '_ \ / __| '__/ _ \ '_ ` _ \ / _ \ '_ \| __|
-- | | | | | (__| | |  __/ | | | | |  __/ | | | |_ 
-- |_|_| |_|\___|_|  \___|_| |_| |_|\___|_| |_|\__|
--                                                 
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

-- Function to set the Time-To-Live (TTL) for a record
-- Parameters:
--   rec: table - The record to update
--   ttl: number - The TTL value to set (in seconds)
-- Returns:
--   string - A signal indicating the action taken
--           _  _____ _____ _     
--  ___  ___| ||_   _|_   _| |    
-- / __|/ _ \ __|| |   | | | |    
-- \__ \  __/ |_ | |   | | | |___ 
-- |___/\___|\__||_|   |_| |_____|
--                               
function setTTL(rec, ttl)
    -- Check if all the UTXOs are spent and set the TTL, but only for transactions that have been in at least one block
    local blockIDs = rec['blockIDs']
    local nrRecords = rec['nrRecords']
    local signal = ""

    if rec["conflicting"] then
        record.set_ttl(rec, ttl)
        signal = ":TTLSET"
        return signal
    end

    if nrRecords == nil then
        -- This is a pagination record: check if all the UTXOs are spent
        if rec['spentUtxos'] == rec['nrUtxos'] then
            signal = ":ALLSPENT"
        end
        -- TTL is determined externally for pagination records; do not set TTL here
        -- as we only want to set TTL for this record and all the other pagination records
        -- when the TX is fully spent and all the UTXOs are spent
    else
        -- This is a master record: only set TTL if nrRecords is 1 and blockIDs has at least one item and all UTXOs are spent
        if nrRecords == 1 and blockIDs and list.size(blockIDs) > 0 and rec['spentUtxos'] == rec['nrUtxos'] then
            -- Set the TTL for the record
            record.set_ttl(rec, ttl)
            if rec['external'] then
                signal = ":TTLSET:EXTERNAL"
            else
                signal = ":TTLSET"
            end
        else
            -- Remove any existing TTL by setting it to -1 (never expire)
            record.set_ttl(rec, -1)
        end
    end

    -- Return the signal indicating the action taken (if any)
    return signal
end

-- Function to set the 'conflicting' field of a record
-- Parameters:
--   rec: table - The record to update
--   setValue: boolean - The value to set for the 'conflicting' field
--   ttl: number - The TTL value to set (in seconds)
-- Returns:
--   string - A signal indicating the action taken
--          _    ____             __ _ _      _   _
-- ___  ___| |_ / ___|___  _ __  / _| (_) ___| |_(_)_ __   __ _
--/ __|/ _ \ __| |   / _ \| '_ \| |_| | |/ __| __| | '_ \ / _` |
--\__ \  __/ |_| |__| (_) | | | |  _| | | (__| |_| | | | | (_| |
--|___/\___|\__|\____\___/|_| |_|_| |_|_|\___|\__|_|_| |_|\__, |
--                                                        |___/
--
function setConflicting(rec, setValue, ttl)
    if not aerospike:exists(rec) then
        return "ERROR:TX not found"
    end

    rec['conflicting'] = setValue

    local signal = setTTL(rec, ttl)

    aerospike:update(rec)

    return 'OK' .. signal
end
