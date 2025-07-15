-- Constants for UTXO handling
local UTXO_HASH_SIZE = 32
local SPENDING_DATA_SIZE = 36
local FULL_UTXO_SIZE = UTXO_HASH_SIZE + SPENDING_DATA_SIZE
local FROZEN_BYTE = 255

-- Bin name constants
local BIN_BLOCK_HEIGHTS = "blockHeights"
local BIN_BLOCK_IDS = "blockIDs"
local BIN_CONFLICTING = "conflicting"
local BIN_DELETE_AT_HEIGHT = "deleteAtHeight"
local BIN_EXTERNAL = "external"
local BIN_UNMINED_SINCE = "unminedSince"
local BIN_PRESERVE_UNTIL = "preserveUntil"
local BIN_REASSIGNMENTS = "reassignments"
local BIN_RECORD_UTXOS = "recordUtxos"
local BIN_SPENDING_HEIGHT = "spendingHeight"
local BIN_SPENT_EXTRA_RECS = "spentExtraRecs"
local BIN_SPENT_UTXOS = "spentUtxos"
local BIN_SUBTREE_IDXS = "subtreeIdxs"
local BIN_TOTAL_EXTRA_RECS = "totalExtraRecs"
local BIN_UNSPENDABLE = "unspendable"
local BIN_UTXOS = "utxos"
local BIN_UTXO_SPENDABLE_IN = "utxoSpendableIn"

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
local SIGNAL_DELETE_AT_HEIGHT_SET = ":DAHSET:"
local SIGNAL_DELETE_AT_HEIGHT_UNSET = ":DAHUNSET:"
local SIGNAL_PRESERVE = ":PRESERVE"

-- Error message constants
local ERR_TX_NOT_FOUND = "ERROR:TX not found"
local ERR_UTXOS_NOT_FOUND = "ERROR:UTXOs list not found"
local ERR_UTXO_NOT_FOUND = "ERROR:UTXO not found for offset "
local ERR_UTXO_INVALID_SIZE = "ERROR:UTXO has an invalid size"
local ERR_UTXO_HASH_MISMATCH = "ERROR:Output utxohash mismatch"
local ERR_UTXO_NOT_FROZEN = "ERROR:UTXO is not frozen"
local ERR_UTXO_IS_FROZEN = "ERROR:UTXO is frozen"
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
local function spendingDataBytesToHex(b)
    local hex = ""

    -- The first 32 bytes are the txID
    -- And we want to reverse it
    for i = 32, 1, -1 do
        hex = hex .. string.format("%02x", b[i])
    end

    -- The next 4 bytes are the vin in little-endian
    for i = 33, 36, 1 do
        hex = hex .. string.format("%02x", b[i])
    end
    return hex
end

-- Creates a new UTXO with spending data
local function createUTXOWithSpendingData(utxoHash, spendingData)
    local newUtxo
    
    if spendingData == nil then
        newUtxo = bytes(UTXO_HASH_SIZE)
    else
        newUtxo = bytes(FULL_UTXO_SIZE)
    end
    
    -- Copy utxoHash
    for i = 1, UTXO_HASH_SIZE do
        newUtxo[i] = utxoHash[i]
    end
    
    if spendingData == nil then
        return newUtxo
    end
    
    -- Copy spendingTxID
    for i = 1, SPENDING_DATA_SIZE do
        newUtxo[UTXO_HASH_SIZE + i] = spendingData[i]
    end
    
    return newUtxo
end

--- Retrieves and validates a UTXO and its spending data
-- @param rec table The record containing UTXOs
-- @param offset number The offset into the UTXO array (0-based, will be adjusted for Lua)
-- @param expectedHash string The expected hash to validate against
-- @return table|nil utxos The full UTXOs array if found
-- @return string|nil utxo The specific UTXO if found
-- @return string|nil spendingData The spending data if present
-- @return string|nil err if an error occurs
local function getUTXOAndSpendingData(utxos, offset, expectedHash)
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

    local spendingData = nil
    if bytes.size(utxo) == FULL_UTXO_SIZE then
        spendingData = bytes.get_bytes(utxo, UTXO_HASH_SIZE + 1, SPENDING_DATA_SIZE)
    end

    return utxo, spendingData, nil
end

-- Function to check if a spending data indicates a frozen UTXO
local function isFrozen(spendingData)
    if spendingData == nil then
        return false
    end

    for i = 1, SPENDING_DATA_SIZE do
        if spendingData[i] ~= FROZEN_BYTE then
            return false
        end
    end
    
    return true
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- offset number - the offset in the utxos list (vout % utxoBatchSize)
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- spendingData []byte - 36 byte little-endian hash of the spending data
-- currentBlockHeight number - the current block height
-- blockHeightRetention number - the retention period for the UTXO record
--                           _
--  ___ _ __   ___ _ __   __| |
-- / __| '_ \ / _ \ '_ \ / _` |
-- \__ \ |_) |  __/ | | | (_| |
-- |___/ .__/ \___|_| |_|\__,_|
--     |_|
--
function spend(rec, offset, utxoHash, spendingData, ignoreConflicting, ignoreUnspendable, currentBlockHeight, blockHeightRetention)
    -- Create a single spend item for spendMulti
    local spend = map()
    spend['offset'] = offset
    spend['utxoHash'] = utxoHash
    spend['spendingData'] = spendingData
    
    local spends = list()
    list.append(spends, spend)

    return spendMulti(rec, spends, ignoreConflicting, ignoreUnspendable, currentBlockHeight, blockHeightRetention)
end

--                           _ __  __       _ _   _ 
--  ___ _ __   ___ _ __   __| |  \/  |_   _| | |_(_)
-- / __| '_ \ / _ \ '_ \ / _` | |\/| | | | | __| |
-- \__ \ |_) |  __/ | | | (_| | |  | | |_| | | |_| |
-- |___/ .__/ \___|_| |_|\__,_|_|  |_|\__,_|_|\__|_|
--     |_|                                          
--
function spendMulti(rec, spends, ignoreConflicting, ignoreUnspendable, currentBlockHeight, blockHeightRetention)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end
    
    if not ignoreConflicting then
        if rec[BIN_CONFLICTING] then
            return MSG_CONFLICTING
        end
    end
    
    if not ignoreUnspendable then
        if rec[BIN_UNSPENDABLE] then
            return MSG_UNSPENDABLE
        end
    end

    local coinbaseSpendingHeight = rec[BIN_SPENDING_HEIGHT]
    if coinbaseSpendingHeight and coinbaseSpendingHeight > 0 and coinbaseSpendingHeight > currentBlockHeight then
        return MSG_COINBASE_IMMATURE1 .. coinbaseSpendingHeight .. MSG_COINBASE_IMMATURE2 .. currentBlockHeight
    end

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    local blockIDString = ""
    if rec[BIN_BLOCK_IDS] then
        blockIDString = table.concat(rec[BIN_BLOCK_IDS], ",")
    end

    -- loop through the spends
    for spend in list.iterator(spends) do
        local offset = spend['offset']
        local utxoHash = spend['utxoHash']
        local spendingData = spend['spendingData']
        
        -- Get and validate specific UTXO
        local utxo, existingSpendingData, err = getUTXOAndSpendingData(utxos, offset, utxoHash)
        if err then return err end

        if rec[BIN_UTXO_SPENDABLE_IN] then
            if rec[BIN_UTXO_SPENDABLE_IN][offset] and rec[BIN_UTXO_SPENDABLE_IN][offset] >= currentBlockHeight then
                return MSG_FROZEN_UNTIL .. rec[BIN_UTXO_SPENDABLE_IN][offset]
            end
        end

        -- Handle already spent UTXO
        if existingSpendingData then            
            if bytes_equal(existingSpendingData, spendingData) then
                -- Already spent with same data, skip this one
                goto continue
            elseif isFrozen(existingSpendingData) then
                return MSG_FROZEN
            else
                return MSG_SPENT .. spendingDataBytesToHex(existingSpendingData)
            end
        end
            
        -- Create new UTXO with spending data
        local newUtxo = createUTXOWithSpendingData(utxoHash, spendingData)
        
        -- Update the record
        utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!
        rec[BIN_SPENT_UTXOS] = rec[BIN_SPENT_UTXOS] + 1

        ::continue::
    end

    -- Update the record with the new utxos
    rec[BIN_UTXOS] = utxos

    local signal = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

    aerospike:update(rec)

    return MSG_OK .. ':[' .. blockIDString .. ']' .. signal
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- blockID number - the block ID
-- currentBlockHeight number - the current block height
--           ____                       _
--  _   _ _ __ / ___| _ __   ___ _ __   __| |
-- | | | | '_ \\___ \| '_ \ / _ \ '_ \ / _` |
-- | |_| | | | |___) | |_) |  __/ | | | (_| |
--  \__,_|_| |_|____/| .__/ \___|_| |_|\__,_|
--                   |_|
--
function unspend(rec, offset, utxoHash, currentBlockHeight, blockHeightRetention)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    local utxo, existingSpendingData, err = getUTXOAndSpendingData(utxos, offset, utxoHash)
        if err then return err end


    local signal = ""

    -- Only unspend if the UTXO is spent and not frozen
    if bytes.size(utxo) == FULL_UTXO_SIZE then
        if isFrozen(existingSpendingData) then
            return ERR_UTXO_IS_FROZEN
        end
        
        local newUtxo = createUTXOWithSpendingData(utxoHash, nil)
        
        -- Update the record
        utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!
        rec[BIN_UTXOS] = utxos
        
        local spentUtxos = rec[BIN_SPENT_UTXOS]
        rec[BIN_SPENT_UTXOS] = spentUtxos - 1
    end

    local signal = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

    aerospike:update(rec)

    return 'OK' .. signal
end

--
function setMined(rec, blockID, blockHeight, subtreeIdx, currentBlockHeight, blockHeightRetention)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    -- Check if the bin exists; if not, initialize it as an empty list
    if rec[BIN_BLOCK_IDS] == nil then
        rec[BIN_BLOCK_IDS] = list()
    end
    if rec[BIN_BLOCK_HEIGHTS] == nil then
        rec[BIN_BLOCK_HEIGHTS] = list()
    end
    if rec[BIN_SUBTREE_IDXS] == nil then
        rec[BIN_SUBTREE_IDXS] = list()
    end

    -- Append the value to the list in the specified bin
    local blocks = rec[BIN_BLOCK_IDS]
    blocks[#blocks + 1] = blockID
    rec[BIN_BLOCK_IDS] = blocks

    local heights = rec[BIN_BLOCK_HEIGHTS]
    heights[#heights + 1] = blockHeight
    rec[BIN_BLOCK_HEIGHTS] = heights

    local subtreeIdxs = rec[BIN_SUBTREE_IDXS]
    subtreeIdxs[#subtreeIdxs + 1] = subtreeIdx
    rec[BIN_SUBTREE_IDXS] = subtreeIdxs

    rec[BIN_UNMINED_SINCE] = nil
    
    -- set the record to be spendable again, if it was unspendable, since if was just mined into a block
    if rec[BIN_UNSPENDABLE] then
        rec[BIN_UNSPENDABLE] = false
    end

    local signal = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

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

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    -- Get and validate specific UTXO
    local utxo, existingSpendingData, err = getUTXOAndSpendingData(utxos, offset, utxoHash)
    if err then return err end

    -- If the utxo has been spent, check if it's already frozen
    if existingSpendingData then
        if isFrozen(existingSpendingData) then
            return MSG_ALREADY_FROZEN
        else
            return MSG_SPENT .. spendingDataBytesToHex(existingSpendingData)
        end
    end

    if bytes.size(utxo) ~= UTXO_HASH_SIZE then
        return ERR_UTXO_INVALID_SIZE
    end

    -- Create frozen UTXO
    local frozenData = bytes(SPENDING_DATA_SIZE)
    for i = 1, SPENDING_DATA_SIZE do
        frozenData[i] = FROZEN_BYTE
    end

    local newUtxo = createUTXOWithSpendingData(utxoHash, frozenData)
    
    -- Update record
    utxos[offset + 1] = newUtxo
    rec[BIN_UTXOS] = utxos

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

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    -- Get and validate specific UTXO
    local utxo, existingSpendingData, err = getUTXOAndSpendingData(utxos, offset, utxoHash)
    if err then return err end

    local signal = ""

    if bytes.size(utxo) ~= FULL_UTXO_SIZE then
        return ERR_UTXO_INVALID_SIZE
    end

    -- Proper validation - check if the UTXO exists and is actually frozen
    if not existingSpendingData or not isFrozen(existingSpendingData) then
        return ERR_UTXO_NOT_FROZEN
    end

    -- Update the output utxo to the new utxo
    local newUtxo = createUTXOWithSpendingData(utxoHash, nil)

    -- Update the record
    utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!

    rec[BIN_UTXOS] = utxos

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

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        return ERR_UTXOS_NOT_FOUND
    end

    -- Get and validate specific UTXO
    local utxo, existingSpendingData, err = getUTXOAndSpendingData(utxos, offset, utxoHash)
    if err then return err end

    local signal = ""

    if bytes.size(utxo) ~= FULL_UTXO_SIZE then
        return ERR_UTXO_INVALID_SIZE
    end

    -- Check if UTXO is frozen (required for reassignment)
    if not existingSpendingData or not isFrozen(existingSpendingData) then
        return ERR_UTXO_NOT_FROZEN
    end

    -- Create new UTXO with new hash
    local newUtxo = createUTXOWithSpendingData(newUtxoHash, nil)

    -- Update record
    utxos[offset + 1] = newUtxo
    rec[BIN_UTXOS] = utxos

    -- Initialize reassignment tracking if needed
    if rec[BIN_REASSIGNMENTS] == nil then
        rec[BIN_REASSIGNMENTS] = list()
    end

    if rec[BIN_UTXO_SPENDABLE_IN] == nil then
        rec[BIN_UTXO_SPENDABLE_IN] = map()
    end

    -- Record reassignment details
    rec[BIN_REASSIGNMENTS][#rec[BIN_REASSIGNMENTS] + 1] = map {
        offset = offset,
        utxoHash = utxoHash,
        newUtxoHash = newUtxoHash,
        blockHeight = blockHeight
    }

    rec[BIN_UTXO_SPENDABLE_IN][offset] = blockHeight + spendableAfter

    -- Ensure record is not DAH'd when all UTXOs are spent
    rec[BIN_RECORD_UTXOS] = rec[BIN_RECORD_UTXOS] + 1

    aerospike:update(rec)

    return MSG_OK .. signal
end

-- Function to set the deleteAtHeight for a record
-- Parameters:
--   rec: table - The record to update
--   currentBlockHeight: number - The current block height
--   blockHeightRetention: number - The number of blocks to retain the record for
-- Returns:
--   string - A signal indicating the action taken

--           _   ____       _      _          _   _   _   _      _       _     _   
--  ___  ___| |_|  _ \  ___| | ___| |_ ___   / \ | |_| | | | ___(_) __ _| |__ | |_ 
-- / __|/ _ \ __| | | |/ _ \ |/ _ \ __/ _ \ / _ \| __| |_| |/ _ \ |/ _` | '_ \| __|
-- \__ \  __/ |_| |_| |  __/ |  __/ ||  __// ___ \ |_|  _  |  __/ | (_| | | | | |_ 
-- |___/\___|\__|____/ \___|_|\___|\__\___/_/   \_\__|_| |_|\___|_|\__, |_| |_|\__|
--                                                                 |___/           
function setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)
    if blockHeightRetention == 0 then
        return ""
    end

    if rec[BIN_PRESERVE_UNTIL] then
       return ""
    end
    
    -- Check if all the UTXOs are spent and set the deleteAtHeight, but only for transactions that have been in at least one block
    local blockIDs = rec[BIN_BLOCK_IDS]
    local totalExtraRecs = rec[BIN_TOTAL_EXTRA_RECS]
    local spentExtraRecs = rec[BIN_SPENT_EXTRA_RECS] or 0  -- Default to 0 if nil
    local existingDeleteAtHeight = rec[BIN_DELETE_AT_HEIGHT]
    local newDeleteHeight = currentBlockHeight + blockHeightRetention
            
    -- Handle conflicting transactions first
    if rec[BIN_CONFLICTING] then
        if not existingDeleteAtHeight then
            -- Set the deleteAtHeight for the record
            rec[BIN_DELETE_AT_HEIGHT] = newDeleteHeight
            if rec[BIN_EXTERNAL] then
                return SIGNAL_DELETE_AT_HEIGHT_SET .. totalExtraRecs
            end
        end

        return ""
    end

    -- Handle pagination records
    if totalExtraRecs == nil then
        -- This is a pagination record: check if all the UTXOs are spent
        if rec[BIN_SPENT_UTXOS] == rec[BIN_RECORD_UTXOS] then
            return SIGNAL_ALL_SPENT
        else
            return SIGNAL_NOT_ALL_SPENT
        end
    end
    
    if spentExtraRecs == nil then
        spentExtraRecs = 0
    end
    
    -- This is a master record: only set deleteAtHeight if all UTXOs are spent and transaction is in at least one block
    local allSpent = (totalExtraRecs == spentExtraRecs) and (rec[BIN_SPENT_UTXOS] == rec[BIN_RECORD_UTXOS])
    local hasBlockIDs = blockIDs and list.size(blockIDs) > 0

    -- Set or update deleteAtHeight if all UTXOs are spent and transaction is in at least one block
    if allSpent and hasBlockIDs then
        if not existingDeleteAtHeight or existingDeleteAtHeight < newDeleteHeight then
            rec[BIN_DELETE_AT_HEIGHT] = newDeleteHeight
            if rec[BIN_EXTERNAL] then
                return SIGNAL_DELETE_AT_HEIGHT_SET .. totalExtraRecs
            end
        end
    -- Clear deleteAtHeight if conditions are no longer met
    elseif existingDeleteAtHeight then
        rec[BIN_DELETE_AT_HEIGHT] = nil
        if rec[BIN_EXTERNAL] then
            return SIGNAL_DELETE_AT_HEIGHT_UNSET .. totalExtraRecs
        end
    end

    return ""
end

-- Function to set the 'conflicting' field of a record
-- Parameters:
--   rec: table - The record to update
--   setValue: boolean - The value to set for the 'conflicting' field
--   currentBlockHeight: number - The current block height
--   blockHeightRetention: number - The retention period for the UTXO record
-- Returns:
--   string - A signal indicating the action taken
--          _    ____             __ _ _      _   _
-- ___  ___| |_ / ___|___  _ __  / _| (_) ___| |_(_)_ __   __ _
--/ __|/ _ \ __| |   / _ \| '_ \| |_| | |/ __| __| | '_ \ / _` |
--\__ \  __/  |_ |__| (_) | | | |  _| | | (__| |_| | | | | (_| |
--|___/\___|\__|\____\___/|_| |_|_| |_|_|\___|\__|_|_| |_|\__, |
--                                                        |___/
--
function setConflicting(rec, setValue, currentBlockHeight, blockHeightRetention)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    rec[BIN_CONFLICTING] = setValue

    local signal = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

    aerospike:update(rec)

    return MSG_OK .. signal
end

-- Function to preserve a transaction until a specific block height
-- This removes any existing deleteAtHeight and sets preserveUntil
-- Parameters:
--   rec: table - The record to update
--   blockHeight: number - The block height to preserve until
-- Returns:
--   string - A signal indicating the action taken
--                                         _   _ _   _ 
--  _ __  _ __ ___  ___  ___ _ __ __   _____| | | | |_(_)| 
-- | '_ \| '__/ _ \/ __|/ _ \ '__\ \ / / _ \ | | | | __| |
-- | |_) | | |  __/\__ \  __/ |   \ V /  __/ |_| | |_| |
-- | .__/|_|  \___||___/\___|_|    \_/ \___|\___/ \__|_|
-- |_|                                                  
--
function preserveUntil(rec, blockHeight)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    -- Remove deleteAtHeight if it exists
    rec[BIN_DELETE_AT_HEIGHT] = nil
    
    -- Set preserveUntil
    rec[BIN_PRESERVE_UNTIL] = blockHeight
    
    -- Update the record
    aerospike:update(rec)
    
    -- Check if we need to signal external file handling
    if rec[BIN_EXTERNAL] then
        return MSG_OK .. SIGNAL_PRESERVE
    end
    
    return MSG_OK
end

-- Function to set the 'conflicting' field of a record
-- Parameters:
--   rec: table - The record to update
--   setValue: boolean - The value to set for the 'conflicting' field
-- Returns:
--   string - A signal indicating the action taken
--           _   _   _                                _       _     _
--  ___  ___| |_| | | |_ __  ___ _ __   ___ _ __   __| | __ _| |__ | | ___ 
-- / __|/ _ \ __| | | | '_ \/ __| '_ \ / _ \ '_ \ / _` |/ _` | '_ \| |/ _ \
-- \__ \  __/ |_| |_| | | | \__ \ |_) |  __/ | | | (_| | (_| | |_) | |  __/
-- |___/\___|\__|\___/|_| |_|___/ .__/ \___|_| |_|\__,_|\__,_|_.__/|_|\___|
--                              |_|                                        
--
function setUnspendable(rec, setValue)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    local oldUnspendable = rec[BIN_UNSPENDABLE]
    local existingDeleteAtHeight = rec[BIN_DELETE_AT_HEIGHT]
    local totalExtraRecs = rec[BIN_TOTAL_EXTRA_RECS]

    if totalExtraRecs == nil then
        totalExtraRecs = 0
    end

    if oldUnspendable == setValue then
        return MSG_OK .. ":" .. totalExtraRecs
    end

    rec[BIN_UNSPENDABLE] = setValue

    if rec[BIN_UNSPENDABLE] then
        -- Remove any existing deleteAtHeight
        if existingDeleteAtHeight then
            rec[BIN_DELETE_AT_HEIGHT] = nil
        end
    end

    aerospike:update(rec)

    return MSG_OK .. ":" .. totalExtraRecs
end


-- Increment the number of records and set deleteAtHeight if necessary
--  _                                          _   
-- (_)_ __   ___ _ __ ___ _ __ ___   ___ _ __ | |_ 
-- | | '_ \ / __| '__/ _ \ '_ ` _ \ / _ \ '_ \| __|
-- | | | | | (__| | |  __/ | | | | |  __/ | | | |_ 
-- |_|_| |_|\___|_|  \___|_| |_| |_|\___|_| |_|\__|
--                                                 
function incrementSpentExtraRecs(rec, inc, currentBlockHeight, blockHeightRetention)
    if not aerospike:exists(rec) then return ERR_TX_NOT_FOUND end

    local totalExtraRecs = rec[BIN_TOTAL_EXTRA_RECS]
    if totalExtraRecs == nil then
        return ERR_TOTAL_EXTRA_RECS
    end

    local spentExtraRecs = rec[BIN_SPENT_EXTRA_RECS]
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

    rec[BIN_SPENT_EXTRA_RECS] = spentExtraRecs

    local signal = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

    aerospike:update(rec)

    return MSG_OK .. signal
end


-- deleteExpired is a UDF that deletes a record
--
-- Parameters:
--   rec: table - The record to delete
-- Returns:
--   boolean - true if record was deleted, false otherwise
--
-- Usage:
--   deleteExpired(rec)

--      _      _      _       _____            _              _ 
--   __| | ___| | ___| |_ ___| ____|_  ___ __ (_)_ __ ___  __| |
--  / _` |/ _ \ |/ _ \ __/ _ \  _| \ \/ / '_ \| | '__/ _ \/ _` |
-- | (_| |  __/ |  __/ ||  __/ |___ >  <| |_) | | | |  __/ (_| |
--  \__,_|\___|_|\___|\__\___|_____/_/\_\ .__/|_|_|  \___|\__,_|
--                              |_|                                

local function deleteExpired(rec)
    -- Check if record exists
    if not aerospike:exists(rec) then
        return false  -- Skip non-existent records
    end
    
    -- Delete the record
    aerospike:remove(rec)
    
    -- Return true to indicate record was deleted
    return true
end

-- deleteScan is a background scan UDF that accepts
-- a stream of records and deletes them using the map function
--
-- Parameters:
--   stream: table - A stream of records to process
--   currentBlockHeight: number - The current block height
-- Returns:
--   table - A stream of records that were deleted
-- Usage:
--   deleteScan(stream, currentBlockHeight)

--      _      _      _       ____                  
--   __| | ___| | ___| |_ ___/ ___|  ___ __ _ _ __  
--  / _` |/ _ \ |/ _ \ __/ _ \___ \ / __/ _` | '_ \ 
-- | (_| |  __/ |  __/ ||  __/___) | (_| (_| | | | |
--  \__,_|\___|_|\___|\__\___|____/ \___\__,_|_| |_|
--                                               
function deleteScan(stream, currentBlockHeight)
    return stream : filter(function(rec)
        local deleteAtHeight = rec[BIN_DELETE_AT_HEIGHT]
        return deleteAtHeight and deleteAtHeight <= currentBlockHeight
    end) : map(deleteExpired)
end
