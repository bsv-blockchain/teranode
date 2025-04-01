-- Constants for UTXO handling
local UTXO_HASH_SIZE = 32
local SPENDING_TX_SIZE = 32
local FULL_UTXO_SIZE = UTXO_HASH_SIZE + SPENDING_TX_SIZE
local FROZEN_BYTE = 255

-- Message constants
local MSG_OK = "OK"
local MSG_CONFLICTING = "CONFLICTING:TX is conflicting"
local MSG_UNSPENDABLE = "UNSPENDABLE:TX is unspendable"
local MSG_FROZEN = "FROZEN:UTXO is frozen"
local MSG_ALREADY_FROZEN = "FROZEN:UTXO is already frozen"
local MSG_FROZEN_UNTIL = "FROZEN:UTXO is not spendable until block "
local MSG_COINBASE_IMMATURE1 = "COINBASE_IMMATURE:Coinbase UTXO can only be spent after 100 blocks, in block "
local MSG_COINBASE_IMMATURE2 = " or greater. The current block height is "
local MSG_SPENT = "SPENT:"

local SIGNAL_ALL_SPENT = ":ALLSPENT"
local SIGNAL_NOT_ALL_SPENT = ":NOTALLSPENT"
local SIGNAL_TTL_SET = ":TTLSET:"
local SIGNAL_TTL_UNSET = ":TTLUNSET:"

-- Error message constants
local ERR_TX_NOT_FOUND = "ERROR:TX not found"
local ERR_UTXOS_NOT_FOUND = "ERROR:UTXOs list not found"
local ERR_UTXO_NOT_FOUND = "ERROR:UTXO not found for offset "
local ERR_UTXO_INVALID_SIZE = "ERROR:UTXO has an invalid size"
local ERR_UTXO_HASH_MISMATCH = "ERROR:Output utxohash mismatch"
local ERR_UTXO_NOT_FROZEN = "ERROR:UTXO is not frozen"
local ERR_SPENT_EXTRA_RECS_NEGATIVE = "ERROR: spentExtraRecs cannot be negative"
local ERR_SPENT_EXTRA_RECS_EXCEED = "ERROR: spentExtraRecs cannot be greater than totalExtraRecs"
local ERR_TOTAL_EXTRA_RECS = "ERROR: totalExtraRecs not found in record. Possible non-master record?"

-- Helper functions

-- Function to get error with stack trace
local function errorWithTrace(msg)
    return msg .. "\n" .. debug.traceback()
end

-- Function to compare two byte arrays for equality
local function bytes_equal(a, b)
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

-- Function to convert a byte array to a hexadecimal string
local function bytes_to_hex(b)
    local hex = ""
    for i = bytes.size(b), 1, -1 do
        hex = hex .. string.format("%02x", b[i])
    end
    return hex
end

-- Creates a new UTXO with spending transaction ID
local function createUTXOWithSpendingTxID(utxoHash, spendingTxID)
    local newUtxo
    
    if spendingTxID == nil then
        newUtxo = bytes(UTXO_HASH_SIZE)
    else
        newUtxo = bytes(FULL_UTXO_SIZE)
    end
    
    -- Copy utxoHash
    for i = 1, UTXO_HASH_SIZE do
        newUtxo[i] = utxoHash[i]
    end
    
    if spendingTxID == nil then
        return newUtxo
    end
    
    -- Copy spendingTxID
    for i = 1, SPENDING_TX_SIZE do
        newUtxo[UTXO_HASH_SIZE + i] = spendingTxID[i]
    end
    
    return newUtxo
end

--- Retrieves and validates a UTXO and its spending transaction ID
-- @param rec table The record containing UTXOs
-- @param offset number The offset into the UTXO array (0-based, will be adjusted for Lua)
-- @param expectedHash string The expected hash to validate against
-- @return table|nil utxos The full UTXOs array if found
-- @return string|nil utxo The specific UTXO if found
-- @return string|nil spendingTxID The spending transaction ID if present
-- @return string|nil err if an error occurs
local function getUTXOAndSpendingTxID(utxos, offset, expectedHash)
    assert(utxos ~= nil, "utxos must be non-nil")
    assert(type(offset) == "number" and offset >= 0, "offset must be a non-negative number")
    assert(expectedHash, "expectedHash is required")
    assert(bytes.size(expectedHash) == UTXO_HASH_SIZE, "expectedHash must be " .. UTXO_HASH_SIZE .. " bytes long")

    local utxo = utxos[offset + 1] -- Lua arrays are 1-based
    if utxo == nil then
        return nil, nil, ERR_UTXO_NOT_FOUND .. offset
    end
   
    local existingHash = bytes.get_bytes(utxo, 1, UTXO_HASH_SIZE)
    
    if not bytes_equal(existingHash, expectedHash) then
        return nil, nil, ERR_UTXO_HASH_MISMATCH
    end

    local spendingTxID = nil
    if bytes.size(utxo) == FULL_UTXO_SIZE then
        spendingTxID = bytes.get_bytes(utxo, UTXO_HASH_SIZE + 1, SPENDING_TX_SIZE)
    end

    return utxo, spendingTxID, nil
end

-- Function to check if a spending transaction ID indicates a frozen UTXO
local function isFrozen(spendingTxID)
    for i = 1, SPENDING_TX_SIZE do
        if spendingTxID[i] ~= FROZEN_BYTE then
            return false
        end
    end

    return true
end

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
function spend(rec, offset, utxoHash, spendingTxID, ignoreConflicting, ignoreUnspendable, currentBlockHeight, ttl)
    -- Create a single spend item for spendMulti
    local spend = map()
    spend['offset'] = offset
    spend['utxoHash'] = utxoHash
    spend['spendingTxID'] = spendingTxID
    
    local spends = list()
    list.append(spends, spend)

    return spendMulti(rec, spends, ignoreConflicting, ignoreUnspendable, currentBlockHeight, ttl)
end

--                           _ __  __       _ _   _ 
--  ___ _ __   ___ _ __   __| |  \/  |_   _| | |_(_)
-- / __| '_ \ / _ \ '_ \ / _` | |\/| | | | | __| |
-- \__ \ |_) |  __/ | | | (_| | |  | | |_| | | |_| |
-- |___/ .__/ \___|_| |_|\__,_|_|  |_|\__,_|_|\__|_|
--     |_|                                          
--
function spendMulti(rec, spends, ignoreConflicting, ignoreUnspendable, currentBlockHeight, ttl)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end
    
    if not ignoreConflicting then
        if rec['conflicting'] then
            return MSG_CONFLICTING
        end
    end
    
    if not ignoreUnspendable then
        if rec['unspendable'] then
            return MSG_UNSPENDABLE
        end
    end

    local coinbaseSpendingHeight = rec['spendingHeight']
    if coinbaseSpendingHeight and coinbaseSpendingHeight > 0 and coinbaseSpendingHeight > currentBlockHeight then
        return MSG_COINBASE_IMMATURE1 .. coinbaseSpendingHeight .. MSG_COINBASE_IMMATURE2 .. currentBlockHeight
    end

    local utxos = rec['utxos']
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    local blockIDString = ""
    if rec['blockIDs'] then
        blockIDString = table.concat(rec['blockIDs'], ",")
    end

    -- loop through the spends
    for spend in list.iterator(spends) do
        local offset = spend['offset']
        local utxoHash = spend['utxoHash']
        local spendingTxID = spend['spendingTxID']
        
        -- Get and validate specific UTXO
        local utxo, existingSpendingTxID, err = getUTXOAndSpendingTxID(utxos, offset, utxoHash)
        if err then return err end

        if rec['utxoSpendableIn'] then
            if rec['utxoSpendableIn'][offset] and rec['utxoSpendableIn'][offset] >= currentBlockHeight then
                return MSG_FROZEN_UNTIL .. rec['utxoSpendableIn'][offset]
            end
        end

        -- Get and validate specific UTXO
        local utxo, existingSpendingTxID, err = getUTXOAndSpendingTxID(utxos, offset, utxoHash)
        if err then return err end

        if rec['utxoSpendableIn'] then
            if rec['utxoSpendableIn'][offset] and rec['utxoSpendableIn'][offset] >= currentBlockHeight then
                return MSG_FROZEN_UNTIL .. rec['utxoSpendableIn'][offset]
            end
        end

        -- Handle already spent UTXO
        if existingSpendingTxID then            
            if bytes_equal(existingSpendingTxID, spendingTxID) then
                return MSG_OK
            elseif isFrozen(existingSpendingTxID) then
                return MSG_FROZEN
            else
                return MSG_SPENT .. bytes_to_hex(existingSpendingTxID)
            end
        end

        -- Create new UTXO with spending transaction
        local newUtxo = createUTXOWithSpendingTxID(utxoHash, spendingTxID)
        
        -- Update the record
        utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!
        rec['spentUtxos'] = rec['spentUtxos'] + 1
    end

    -- Update the record with the new utxos
    rec['utxos'] = utxos

    local signal = setTTL(rec, ttl)

    aerospike:update(rec)

    return MSG_OK .. ':[' .. blockIDString .. ']' .. signal
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- blockID number - the block ID
-- ttl number - the time-to-live for the UTXO record
--           ____                       _
--  _   _ _ __ / ___| _ __   ___ _ __   __| |
-- | | | | '_ \\___ \| '_ \ / _ \ '_ \ / _` |
-- | |_| | | | |___) | |_) |  __/ | | | (_| |
--  \__,_|_| |_|____/| .__/ \___|_| |_|\__,_|
--                   |_|
--
function unspend(rec, offset, utxoHash)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    local utxos = rec['utxos']
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    local utxo, existingSpendingTxID, err = getUTXOAndSpendingTxID(utxos, offset, utxoHash)
        if err then return err end

    local oldTtl = record.ttl(rec)

    local signal = ""

    -- If the utxo has been spent, remove the spendingTxID
    if bytes.size(utxo) == FULL_UTXO_SIZE then
        local newUtxo = createUTXOWithSpendingTxID(utxoHash, nil)
        
        -- Update the record
        utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!
        rec['utxos'] = utxos
        
        local spentUtxos = rec['spentUtxos']
        rec['spentUtxos'] = spentUtxos - 1
    end

    local signal = setTTL(rec, ttl)

    aerospike:update(rec)

    return 'OK' .. signal
end

--
function setMined(rec, blockID, blockHeight, subtreeIdx, ttl)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

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

    -- set the record to be spendable again, if it was unspendable, since if was just mined into a block
    if rec['unspendable'] then
        rec['unspendable'] = false
    end

    local signal = setTTL(rec, ttl)

    -- Update the record to save changes
    aerospike:update(rec)

    return MSG_OK .. signal
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
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    local utxos = rec['utxos']
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    -- Get and validate specific UTXO
    local utxo, existingSpendingTxID, err = getUTXOAndSpendingTxID(utxos, offset, utxoHash)
    if err then return err end

    -- If the utxo has been spent, check if it's already frozen
    if existingSpendingTxID then
        if isFrozen(existingSpendingTxID) then
            return MSG_ALREADY_FROZEN
        else
            return MSG_SPENT .. bytes_to_hex(existingSpendingTxID)
        end
    end

    if bytes.size(utxo) ~= UTXO_HASH_SIZE then
        return ERR_UTXO_INVALID_SIZE
    end

    -- Create frozen UTXO
    local frozenTxID = bytes(SPENDING_TX_SIZE)
    for i = 1, SPENDING_TX_SIZE do
        frozenTxID[i] = FROZEN_BYTE
    end

    local newUtxo = createUTXOWithSpendingTxID(utxoHash, frozenTxID)
    
    -- Update record
    utxos[offset + 1] = newUtxo
    rec['utxos'] = utxos

    aerospike:update(rec)

    return MSG_OK
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- offset number - the offset in the utxos list (vout % utxoBatchSize)
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
--               __
--  _   _ _ __  / _|_ __ ___  ___ _______
-- | | | | '_ \| |_| '__/ _ \/ _ \_  / _ \
-- | |_| | | | |  _| | |  __/  __// /  __/
--  \__,_|_| |_|_| |_|  \___|\___/___\___|
function unfreeze(rec, offset, utxoHash)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    local utxos = rec['utxos']
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    -- Get and validate specific UTXO
    local utxo, existingSpendingTxID, err = getUTXOAndSpendingTxID(utxos, offset, utxoHash)
    if err then return err end

    local signal = ""

    if not bytes.size(utxo) == 64 then
        return ERR_UTXO_INVALID_SIZE
    end

    -- If the utxo has been spent, trigger alert
    if not existingSpendingTxID then
        return ERR_UTXO_NOT_FROZEN
    end

    -- Update the output utxo to the new utxo
    local newUtxo = createUTXOWithSpendingTxID(utxoHash, nil)

    -- Update the record
    utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!

    rec['utxos'] = utxos

    aerospike:update(rec)

    return MSG_OK
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
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    local utxos = rec['utxos']
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    -- Get and validate specific UTXO
    local utxo, existingSpendingTxID, err = getUTXOAndSpendingTxID(utxos, offset, utxoHash)
    if err then return err end

    local signal = ""

    if not bytes.size(utxo) == 64 then
        return ERR_UTXO_INVALID_SIZE
    end

    -- Check if UTXO is frozen (required for reassignment)
    if not existingSpendingTxID then
        return ERR_UTXO_NOT_FROZEN
    end

    -- Create new UTXO with new hash
    local newUtxo = createUTXOWithSpendingTxID(newUtxoHash, nil)

    -- Update record
    utxos[offset + 1] = newUtxo
    rec['utxos'] = utxos

    -- Initialize reassignment tracking if needed
    if rec['reassignments'] == nil then
        rec['reassignments'] = list()
    end

    if rec['utxoSpendableIn'] == nil then
        rec['utxoSpendableIn'] = map()
    end

    -- Record reassignment details
    rec['reassignments'][#rec['reassignments'] + 1] = map {
        offset = offset,
        utxoHash = utxoHash,
        newUtxoHash = newUtxoHash,
        blockHeight = blockHeight
    }

    rec['utxoSpendableIn'][offset] = blockHeight + spendableAfter

    -- Ensure record is not TTL'd when all UTXOs are spent
    rec['recordUtxos'] = rec['recordUtxos'] + 1

    aerospike:update(rec)

    return MSG_OK .. signal
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
    local totalExtraRecs = rec['totalExtraRecs']
    local spentExtraRecs = rec['spentExtraRecs']
    local oldTtl = record.ttl(rec)
            
    if rec["conflicting"] then
        if oldTtl <= 0 then
            -- Set the TTL for the record
            record.set_ttl(rec, ttl)
            if rec['external'] then
                return SIGNAL_TTL_SET .. totalExtraRecs
            end
        end
        
        return ""
    end

    if totalExtraRecs == nil then
        -- This is a pagination record: check if all the UTXOs are spent
        if rec['spentUtxos'] == rec['recordUtxos'] then
            return SIGNAL_ALL_SPENT
        else
            return SIGNAL_NOT_ALL_SPENT
        end
    end
    
    if spentExtraRecs == nil then
        spentExtraRecs = 0
    end

    -- This is a master record: only set TTL if totalExtraRecs equals spentExtraRecs and blockIDs has at least one item and all UTXOs are spent
    if totalExtraRecs == spentExtraRecs and blockIDs and list.size(blockIDs) > 0 and rec['spentUtxos'] == rec['recordUtxos'] then
        if oldTtl <= 0 then
            -- Set the TTL for the record
            record.set_ttl(rec, ttl)
            if rec['external'] then
                return SIGNAL_TTL_SET .. totalExtraRecs
            end
        end
    else
        -- Remove any existing TTL by setting it to -1 (never expire)
        if oldTtl > 0 then
            record.set_ttl(rec, -1)
           
            if rec['external'] then
                return SIGNAL_TTL_UNSET .. totalExtraRecs
            end
        end
    end

    return ""
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
--\__ \  __/ |_ |__| (_) | | | |  _| | | (__| |_| | | | | (_| |
--|___/\___|\__|\____\___/|_| |_|_| |_|_|\___|\__|_|_| |_|\__, |
--                                                        |___/
--
function setConflicting(rec, setValue, ttl)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    rec['conflicting'] = setValue

    local signal = setTTL(rec, ttl)

    aerospike:update(rec)

    return MSG_OK .. signal
end

-- Increment the number of records and set TTL if necessary
--  _                                          _   
-- (_)_ __   ___ _ __ ___ _ __ ___   ___ _ __ | |_ 
-- | | '_ \ / __| '__/ _ \ '_ ` _ \ / _ \ '_ \| __|
-- | | | | | (__| | |  __/ | | | | |  __/ | | | |_ 
-- |_|_| |_|\___|_|  \___|_| |_| |_|\___|_| |_|\__|
--                                                 
function incrementSpentExtraRecs(rec, inc, ttl)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    local totalExtraRecs = rec['totalExtraRecs']
    if totalExtraRecs == nil then
        return ERR_TOTAL_EXTRA_RECS
    end

    local spentExtraRecs = rec['spentExtraRecs']
    if spentExtraRecs == nil then
        spentExtraRecs = 0
    end

    spentExtraRecs = spentExtraRecs + inc

    if spentExtraRecs < 0 then
        return ERR_SPENT_EXTRA_RECS_NEGATIVE
    end

    if spentExtraRecs > totalExtraRecs then
        return ERR_SPENT_EXTRA_RECS_EXCEED
    end

    rec['spentExtraRecs'] = spentExtraRecs

    local signal = setTTL(rec, ttl)

    aerospike:update(rec)

    return MSG_OK .. signal
end
