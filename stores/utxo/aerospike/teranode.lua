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
local BIN_LOCKED = "locked"
local BIN_CREATING = "creating"
local BIN_UTXOS = "utxos"
local BIN_UTXO_SPENDABLE_IN = "utxoSpendableIn"
local BIN_LAST_SPENT_STATE = "lastSpentState"  -- Tracks last signaled state: "ALLSPENT" or "NOTALLSPENT"
local BIN_DELETED_CHILDREN = "deletedChildren"  -- Tracks which child transactions have already been deleted

-- Status constants
local STATUS_OK = "OK"
local STATUS_ERROR = "ERROR"

-- Error code constants
local ERROR_CODE_TX_NOT_FOUND = "TX_NOT_FOUND"
local ERROR_CODE_CONFLICTING = "CONFLICTING"
local ERROR_CODE_LOCKED = "LOCKED"
local ERROR_CODE_CREATING = "CREATING"
local ERROR_CODE_FROZEN = "FROZEN"
local ERROR_CODE_ALREADY_FROZEN = "ALREADY_FROZEN"
local ERROR_CODE_FROZEN_UNTIL = "FROZEN_UNTIL"
local ERROR_CODE_COINBASE_IMMATURE = "COINBASE_IMMATURE"
local ERROR_CODE_SPENT = "SPENT"
local ERROR_CODE_INVALID_SPEND = "INVALID_SPEND"
local ERROR_CODE_UTXOS_NOT_FOUND = "UTXOS_NOT_FOUND"
local ERROR_CODE_UTXO_NOT_FOUND = "UTXO_NOT_FOUND"
local ERROR_CODE_UTXO_INVALID_SIZE = "UTXO_INVALID_SIZE"
local ERROR_CODE_UTXO_HASH_MISMATCH = "UTXO_HASH_MISMATCH"
local ERROR_CODE_UTXO_NOT_FROZEN = "UTXO_NOT_FROZEN"
local ERROR_CODE_INVALID_PARAMETER = "INVALID_PARAMETER"

-- Message constants
local MSG_CONFLICTING = "TX is conflicting"
local MSG_LOCKED = "TX is locked and cannot be spent"
local MSG_CREATING = "TX is being created and cannot be spent yet"
local MSG_FROZEN = "UTXO is frozen"
local MSG_ALREADY_FROZEN = "UTXO is already frozen"
local MSG_FROZEN_UNTIL = "UTXO is not spendable until block "
local MSG_COINBASE_IMMATURE = "Coinbase UTXO can only be spent when it matures"
local MSG_SPENT = "Already spent by "
local MSG_INVALID_SPEND = "Invalid spend"

local SIGNAL_ALL_SPENT = "ALLSPENT"
local SIGNAL_NOT_ALL_SPENT = "NOTALLSPENT"
local SIGNAL_DELETE_AT_HEIGHT_SET = "DAHSET"
local SIGNAL_DELETE_AT_HEIGHT_UNSET = "DAHUNSET"
local SIGNAL_PRESERVE = "PRESERVE"

-- Error message constants
local ERR_TX_NOT_FOUND = "TX not found"
local ERR_UTXOS_NOT_FOUND = "UTXOs list not found"
local ERR_UTXO_NOT_FOUND = "UTXO not found for offset "
local ERR_UTXO_INVALID_SIZE = "UTXO has an invalid size"
local ERR_UTXO_HASH_MISMATCH = "Output utxohash mismatch"
local ERR_UTXO_NOT_FROZEN = "UTXO is not frozen"
local ERR_UTXO_IS_FROZEN = "UTXO is frozen"
local ERR_SPENT_EXTRA_RECS_NEGATIVE = "spentExtraRecs cannot be negative"
local ERR_SPENT_EXTRA_RECS_EXCEED = "spentExtraRecs cannot be greater than totalExtraRecs"
local ERR_TOTAL_EXTRA_RECS = "totalExtraRecs not found in record. Possible non-master record?"

-- Response field name constants
local FIELD_STATUS = "status"
local FIELD_ERROR_CODE = "errorCode"
local FIELD_MESSAGE = "message"
local FIELD_SIGNAL = "signal"
local FIELD_BLOCK_IDS = "blockIDs"
local FIELD_ERRORS = "errors"
local FIELD_CHILD_COUNT = "childCount"
local FIELD_SPENDING_DATA = "spendingData"
-- local FIELD_DEBUG = "debug"

-- Helper functions
local bytes_size = bytes.size
local bytes_get_bytes = bytes.get_bytes
local string_format = string.format
local table_concat = table.concat
local list_append = list.append
local list_iterator = list.iterator

-- Pre-computed hex lookup table for fast byte-to-hex conversion
-- Eliminates repeated string_format() calls in hot paths
local HEX_LOOKUP = {}
for i = 0, 255 do
    HEX_LOOKUP[i] = string_format("%02x", i)
end

-- Function to get error with stack trace
local function errorWithTrace(msg)
    return msg .. "\n" .. debug.traceback()
end

-- Function to compare two byte arrays for equality
local function bytes_equal(a, b)
    local size_a = bytes_size(a)
    if size_a ~= bytes_size(b) then
        return false
    end

    for i = 1, size_a do
        if a[i] ~= b[i] then
            return false
        end
    end

    return true
end

-- Function to convert a byte array to a hexadecimal string
local function spendingDataBytesToHex(b)
    -- Build hex string using table for efficient concatenation (36 bytes: 32 txid + 4 vin)
    local t = {}

    -- The first 32 bytes are the txID (reversed)
    for i = 32, 1, -1 do
        t[#t+1] = HEX_LOOKUP[b[i]]
    end

    -- The next 4 bytes are the vin in little-endian
    for i = 33, 36 do
        t[#t+1] = HEX_LOOKUP[b[i]]
    end

    -- Single concatenation at the end (O(n) instead of O(n^2))
    return table_concat(t)
end

-- Function to convert a spending byte array to a reverse tx hexadecimal string
local function spendingDataBytesToTxHex(b)
    -- Build hex string using table for efficient concatenation (32 bytes)
    local t = {}

    -- The first 32 bytes are the txID (reversed)
    for i = 32, 1, -1 do
        t[#t+1] = HEX_LOOKUP[b[i]]
    end

    -- Single concatenation at the end (O(n) instead of O(n^2))
    return table_concat(t)
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
-- @return string|nil utxo The specific UTXO if found
-- @return string|nil spendingData The spending data if present
-- @return table|nil errorInfo A map containing errorCode and message if an error occurs
local function getUTXOAndSpendingData(utxos, offset, expectedHash)
    -- assert(utxos ~= nil, "utxos must be non-nil") -- Removed for performance, caller ensures
    -- assert(type(offset) == "number" and offset >= 0, "offset must be a non-negative number")
    -- assert(expectedHash, "expectedHash is required")
    -- assert(bytes_size(expectedHash) == UTXO_HASH_SIZE, "expectedHash must be " .. UTXO_HASH_SIZE .. " bytes long")

    local utxo = utxos[offset + 1] -- Lua arrays are 1-based
    if utxo == nil then
        local response = map()

        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_UTXO_NOT_FOUND

        return nil, nil, response
    end

    -- Inline hash comparison to avoid allocation of existingHash
    for i = 1, UTXO_HASH_SIZE do
        if utxo[i] ~= expectedHash[i] then
            local response = map()

            response[FIELD_STATUS] = STATUS_ERROR
            response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_HASH_MISMATCH
            response[FIELD_MESSAGE] = ERR_UTXO_HASH_MISMATCH

            return nil, nil, response
        end
    end

    local spendingData = nil

    if bytes_size(utxo) == FULL_UTXO_SIZE then
        spendingData = bytes_get_bytes(utxo, UTXO_HASH_SIZE + 1, SPENDING_DATA_SIZE)
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
function spend(rec, offset, utxoHash, spendingData, ignoreConflicting, ignoreLocked, currentBlockHeight, blockHeightRetention)
    -- Create a single spend item for spendMulti
    local spend = map()

    spend['offset'] = offset
    spend['utxoHash'] = utxoHash
    spend['spendingData'] = spendingData

    local spends = list()

    list.append(spends, spend)

    -- Just return the result from spendMulti - it already has the correct structure
    return spendMulti(rec, spends, ignoreConflicting, ignoreLocked, currentBlockHeight, blockHeightRetention)
end

--                           _ __  __       _ _   _
--  ___ _ __   ___ _ __   __| |  \/  |_   _| | |_(_)
-- / __| '_ \ / _ \ '_ \ / _` | |\/| | | | | | __| |
-- \__ \ |_) |  __/ | | | (_| | |  | | |_| | | |_| |
-- |___/ .__/ \___|_| |_|\__,_|_|  |_|\__,_|_|\__|_|
--     |_|
--
function spendMulti(rec, spends, ignoreConflicting, ignoreLocked, currentBlockHeight, blockHeightRetention)
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    -- Check creating flag - blocks spending during multi-record transaction creation
    -- Explicitly check for true since nil/absent means not creating
    if rec[BIN_CREATING] == true then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_CREATING
        response[FIELD_MESSAGE] = MSG_CREATING

        return response
    end

    if not ignoreConflicting then
        if rec[BIN_CONFLICTING] then
            response[FIELD_STATUS] = STATUS_ERROR
            response[FIELD_ERROR_CODE] = ERROR_CODE_CONFLICTING
            response[FIELD_MESSAGE] = MSG_CONFLICTING

            return response
        end
    end

    if not ignoreLocked then
        if rec[BIN_LOCKED] then
            response[FIELD_STATUS] = STATUS_ERROR
            response[FIELD_ERROR_CODE] = ERROR_CODE_LOCKED
            response[FIELD_MESSAGE] = MSG_LOCKED

            return response
        end
    end

    local coinbaseSpendingHeight = rec[BIN_SPENDING_HEIGHT]
    if coinbaseSpendingHeight and coinbaseSpendingHeight > 0 and coinbaseSpendingHeight > currentBlockHeight then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_COINBASE_IMMATURE
        response[FIELD_MESSAGE] = MSG_COINBASE_IMMATURE .. ", spendable in block " .. coinbaseSpendingHeight .. " or greater. Current block height is " .. currentBlockHeight

        return response
    end

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXOS_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_UTXOS_NOT_FOUND

        return response
    end

    local blockIDs = rec[BIN_BLOCK_IDS]
    local errors = map()
    local deletedChildren = rec[BIN_DELETED_CHILDREN]
    local spendableIn = rec[BIN_UTXO_SPENDABLE_IN]
    local spendCount = #spends
    local spentUtxos = rec[BIN_SPENT_UTXOS] or 0

    -- Use direct array indexing instead of iterator for better performance
    for i = 1, spendCount do
        local spend = spends[i]
        local offset = spend['offset']
        local utxoHash = spend['utxoHash']
        local spendingData = spend['spendingData']
        local idx = spend['idx']

        -- Get and validate specific UTXO
        local utxo, existingSpendingData, errorInfo = getUTXOAndSpendingData(utxos, offset, utxoHash)
        if errorInfo then
            local error = map()

            error[FIELD_ERROR_CODE] = errorInfo.errorCode
            error[FIELD_MESSAGE] = errorInfo.message

            errors[idx] = error

            goto continue
        end

        if spendableIn then
            local spendableHeight = spendableIn[offset]
            if spendableHeight and spendableHeight >= currentBlockHeight then
                local error = map()

                error[FIELD_ERROR_CODE] = ERROR_CODE_FROZEN_UNTIL
                error[FIELD_MESSAGE] = MSG_FROZEN_UNTIL .. spendableHeight

                errors[idx] = error

                goto continue
            end
        end

        -- Handle already spent UTXO
        if existingSpendingData then

            if bytes_equal(existingSpendingData, spendingData) then
                -- Already spent with same data

                if deletedChildren ~= nil then
                    -- Check whether this child tx (by txid) exists in the deletedChildren map, if yes, error out
                    local childTxID = spendingDataBytesToTxHex(existingSpendingData)
                    if deletedChildren[childTxID] then
                        local error = map()

                        error[FIELD_ERROR_CODE] = ERROR_CODE_INVALID_SPEND
                        error[FIELD_MESSAGE] = MSG_INVALID_SPEND
                        error[FIELD_SPENDING_DATA] = spendingDataBytesToHex(existingSpendingData)

                        errors[idx] = error
                    end
                end

                goto continue
            elseif isFrozen(existingSpendingData) then
                local error = map()

                error[FIELD_ERROR_CODE] = ERROR_CODE_FROZEN
                error[FIELD_MESSAGE] = MSG_FROZEN

                errors[idx] = error

                goto continue
            else
                local error = map()

                error[FIELD_ERROR_CODE] = ERROR_CODE_SPENT
                error[FIELD_MESSAGE] = MSG_SPENT
                error[FIELD_SPENDING_DATA] = spendingDataBytesToHex(existingSpendingData)

                errors[idx] = error

                goto continue
            end
        end

        -- Create new UTXO with spending data
        local newUtxo = createUTXOWithSpendingData(utxoHash, spendingData)

        -- Update the record
        utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!
        spentUtxos = spentUtxos + 1

        ::continue::
    end

    -- Update the record with the new values
    rec[BIN_UTXOS] = utxos
    rec[BIN_SPENT_UTXOS] = spentUtxos

    local signal, childCount = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

    aerospike:update(rec)

    -- Build response
    if map.size(errors) > 0 then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERRORS] = errors
    else
        response[FIELD_STATUS] = STATUS_OK
    end

    if blockIDs then
        response[FIELD_BLOCK_IDS] = blockIDs
    end

    if signal and signal ~= "" then
        response[FIELD_SIGNAL] = signal
        if childCount then
            response[FIELD_CHILD_COUNT] = childCount
        end
    end

    return response
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
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXOS_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_UTXOS_NOT_FOUND

        return response
    end

    local utxo, existingSpendingData, errorInfo = getUTXOAndSpendingData(utxos, offset, utxoHash)
    if errorInfo then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = errorInfo.errorCode
        response[FIELD_MESSAGE] = errorInfo.message

        return response
    end

    -- Only unspend if the UTXO is spent and not frozen
    if bytes.size(utxo) == FULL_UTXO_SIZE then
        if isFrozen(existingSpendingData) then
            response[FIELD_STATUS] = STATUS_ERROR
            response[FIELD_ERROR_CODE] = ERROR_CODE_FROZEN
            response[FIELD_MESSAGE] = ERR_UTXO_IS_FROZEN

            return response
        end

        local newUtxo = createUTXOWithSpendingData(utxoHash, nil)

        -- Update the record
        utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!
        rec[BIN_UTXOS] = utxos

        local spentUtxos = rec[BIN_SPENT_UTXOS]
        rec[BIN_SPENT_UTXOS] = spentUtxos - 1
    end

    local signal, childCount = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK
    if signal and signal ~= "" then
        response[FIELD_SIGNAL] = signal
        if childCount then
            response[FIELD_CHILD_COUNT] = childCount
        end
    end

    return response
end

--
function setMined(rec, blockID, blockHeight, subtreeIdx, currentBlockHeight, blockHeightRetention, onLongestChain, unsetMined)
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

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

    local blocks = rec[BIN_BLOCK_IDS]
    local heights = rec[BIN_BLOCK_HEIGHTS]
    local subtreeIdxs = rec[BIN_SUBTREE_IDXS]

    if unsetMined then
        -- Remove the block id and height/subtreeIdx at the same index from the bin if it exists, the block was invalidated
        -- Cache block count to avoid recalculation
        local blockCount = #blocks
        local foundIdx = nil

        for i = 1, blockCount do
            if blocks[i] == blockID then
                foundIdx = i
                break
            end
        end

        -- Use list.remove() instead of rebuilding arrays for better performance
        if foundIdx then
            list.remove(blocks, foundIdx)
            list.remove(heights, foundIdx)
            list.remove(subtreeIdxs, foundIdx)

            rec[BIN_BLOCK_IDS] = blocks
            rec[BIN_BLOCK_HEIGHTS] = heights
            rec[BIN_SUBTREE_IDXS] = subtreeIdxs
        end
    else
        -- Append the value to the list in the specified bin if it doesn't already exist
        -- Cache block count to avoid recalculation
        local blockCount = #blocks
        local blockExists = false

        for i = 1, blockCount do
            if blocks[i] == blockID then
                blockExists = true
                break
            end
        end

        if not blockExists then
            blocks[blockCount + 1] = blockID
            rec[BIN_BLOCK_IDS] = blocks

            heights[#heights + 1] = blockHeight
            rec[BIN_BLOCK_HEIGHTS] = heights

            subtreeIdxs[#subtreeIdxs + 1] = subtreeIdx
            rec[BIN_SUBTREE_IDXS] = subtreeIdxs
        end
    end

    -- Also add the block ids to the response
    response[FIELD_BLOCK_IDS] = blocks

    -- if we have a block in the record on the longest chain, then it is no longer unmined
    -- Cache block count to avoid recalculation
    local hasBlocks = #blocks > 0
    if hasBlocks then
        if onLongestChain then
            rec[BIN_UNMINED_SINCE] = nil
        end
    else
        rec[BIN_UNMINED_SINCE] = currentBlockHeight
    end

    -- set the record to not be locked again, if it was locked, since if was just mined into a block
    if rec[BIN_LOCKED] then
        rec[BIN_LOCKED] = false
    end

    -- Delete the creating bin entirely (nil removes it from record)
    -- This saves storage space - absence means not creating
    if rec[BIN_CREATING] then
        rec[BIN_CREATING] = nil
    end

    local signal, childCount = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

    -- Update the record to save changes
    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK
    if signal and signal ~= "" then
        response[FIELD_SIGNAL] = signal
        if childCount then
            response[FIELD_CHILD_COUNT] = childCount
        end
    end

    return response
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
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXOS_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_UTXOS_NOT_FOUND

        return response
    end

    -- Get and validate specific UTXO
    local utxo, existingSpendingData, errorInfo = getUTXOAndSpendingData(utxos, offset, utxoHash)
    if errorInfo then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = errorInfo.errorCode
        response[FIELD_MESSAGE] = errorInfo.message

        return response
    end

    -- If the utxo has been spent, check if it's already frozen
    if existingSpendingData then
        if isFrozen(existingSpendingData) then
            response[FIELD_STATUS] = STATUS_ERROR
            response[FIELD_ERROR_CODE] = ERROR_CODE_ALREADY_FROZEN
            response[FIELD_MESSAGE] = MSG_ALREADY_FROZEN

            return response
        else
            response[FIELD_STATUS] = STATUS_ERROR
            response[FIELD_ERROR_CODE] = ERROR_CODE_SPENT
            response[FIELD_MESSAGE] = MSG_SPENT
            response[FIELD_SPENDING_DATA] = spendingDataBytesToHex(existingSpendingData)
            return response
        end
    end

    if bytes.size(utxo) ~= UTXO_HASH_SIZE then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_INVALID_SIZE
        response[FIELD_MESSAGE] = ERR_UTXO_INVALID_SIZE

        return response
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

    response[FIELD_STATUS] = STATUS_OK

    return response
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
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXOS_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_UTXOS_NOT_FOUND

        return response
    end

    -- Get and validate specific UTXO
    local utxo, existingSpendingData, err = getUTXOAndSpendingData(utxos, offset, utxoHash)
    if err then
        response[FIELD_STATUS] = STATUS_ERROR
        local errorCode = getErrorCodeFromMessage(err)
        if errorCode then
            response[FIELD_ERROR_CODE] = errorCode
        end
        response[FIELD_MESSAGE] = err

        return response
    end

    if bytes.size(utxo) ~= FULL_UTXO_SIZE then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_INVALID_SIZE
        response[FIELD_MESSAGE] = ERR_UTXO_INVALID_SIZE

        return response
    end

    -- Proper validation - check if the UTXO exists and is actually frozen
    if not existingSpendingData or not isFrozen(existingSpendingData) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_NOT_FROZEN
        response[FIELD_MESSAGE] = ERR_UTXO_NOT_FROZEN

        return response
    end

    -- Update the output utxo to the new utxo
    local newUtxo = createUTXOWithSpendingData(utxoHash, nil)

    -- Update the record
    utxos[offset + 1] = newUtxo -- NB - lua arrays are 1-based!!!!

    rec[BIN_UTXOS] = utxos

    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK

    return response
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
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    local utxos = rec[BIN_UTXOS]
    if utxos == nil then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXOS_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_UTXOS_NOT_FOUND

        return response
    end

    -- Get and validate specific UTXO
    local utxo, existingSpendingData, err = getUTXOAndSpendingData(utxos, offset, utxoHash)
    if err then
        response[FIELD_STATUS] = STATUS_ERROR
        local errorCode = getErrorCodeFromMessage(err)
        if errorCode then
            response[FIELD_ERROR_CODE] = errorCode
        end
        response[FIELD_MESSAGE] = err

        return response
    end

    if bytes.size(utxo) ~= FULL_UTXO_SIZE then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_INVALID_SIZE
        response[FIELD_MESSAGE] = ERR_UTXO_INVALID_SIZE

        return response
    end

    -- Check if UTXO is frozen (required for reassignment)
    if not existingSpendingData or not isFrozen(existingSpendingData) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_NOT_FROZEN
        response[FIELD_MESSAGE] = ERR_UTXO_NOT_FROZEN

        return response
    end

    -- Create new UTXO with new hash
    local newUtxo = createUTXOWithSpendingData(newUtxoHash, nil)

    -- Update record
    utxos[offset + 1] = newUtxo
    rec[BIN_UTXOS] = utxos

    -- Initialize reassignment tracking if needed
    local reassignments = rec[BIN_REASSIGNMENTS]
    if reassignments == nil then
        reassignments = list()
        rec[BIN_REASSIGNMENTS] = reassignments
    end

    local spendableInMap = rec[BIN_UTXO_SPENDABLE_IN]
    if spendableInMap == nil then
        spendableInMap = map()
        rec[BIN_UTXO_SPENDABLE_IN] = spendableInMap
    end

    -- Record reassignment details
    reassignments[#reassignments + 1] = map {
        offset = offset,
        utxoHash = utxoHash,
        newUtxoHash = newUtxoHash,
        blockHeight = blockHeight
    }

    spendableInMap[offset] = blockHeight + spendableAfter

    -- Ensure record is not DAH'd when all UTXOs are spent
    rec[BIN_RECORD_UTXOS] = rec[BIN_RECORD_UTXOS] + 1

    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK

    return response
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
        return "", nil
    end

    if rec[BIN_PRESERVE_UNTIL] then
       return "", nil
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
                return SIGNAL_DELETE_AT_HEIGHT_SET, totalExtraRecs
            end
        end

        return "", nil
    end

    -- Handle pagination records
    if totalExtraRecs == nil then
        -- Default nil to NOTALLSPENT (initial state when record is created with unspent UTXOs)
        local lastState = rec[BIN_LAST_SPENT_STATE] or SIGNAL_NOT_ALL_SPENT

        local currentState
        -- Determine current state
        if rec[BIN_SPENT_UTXOS] == rec[BIN_RECORD_UTXOS] then
            currentState = SIGNAL_ALL_SPENT
        else
            currentState = SIGNAL_NOT_ALL_SPENT
        end

        -- Only signal if state has changed
        if lastState ~= currentState then
            -- State transition detected, update and signal
            rec[BIN_LAST_SPENT_STATE] = currentState
            return currentState, nil
        else
            -- No state change, don't signal
            return "", nil
        end
    end

    if spentExtraRecs == nil then
        spentExtraRecs = 0
    end

    -- Cache blockIDs size check to avoid recalculation
    local hasBlockIDs = blockIDs and #blockIDs > 0
    local isOnLongestChain = (rec[BIN_UNMINED_SINCE] == nil)

    -- This is a master record: only set deleteAtHeight if all UTXOs are spent and transaction is in at least one block
    local allSpent = (totalExtraRecs == spentExtraRecs) and (rec[BIN_SPENT_UTXOS] == rec[BIN_RECORD_UTXOS])

    -- Set or update deleteAtHeight if all UTXOs are spent, transaction is in at least one block, AND on longest chain
    if allSpent and hasBlockIDs and isOnLongestChain then
        if not existingDeleteAtHeight or existingDeleteAtHeight < newDeleteHeight then
            rec[BIN_DELETE_AT_HEIGHT] = newDeleteHeight
            if rec[BIN_EXTERNAL] then
                return SIGNAL_DELETE_AT_HEIGHT_SET, totalExtraRecs
            end
        end
    -- Clear deleteAtHeight if conditions are no longer met
    elseif existingDeleteAtHeight then
        rec[BIN_DELETE_AT_HEIGHT] = nil
        if rec[BIN_EXTERNAL] then
            return SIGNAL_DELETE_AT_HEIGHT_UNSET, totalExtraRecs
        end
    end

    return "", nil
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
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    rec[BIN_CONFLICTING] = setValue

    local signal, childCount = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK
    if signal and signal ~= "" then
        response[FIELD_SIGNAL] = signal
        if childCount then
            response[FIELD_CHILD_COUNT] = childCount
        end
    end

    return response
end

-- Function to preserve a transaction until a specific block height
-- This removes any existing deleteAtHeight and sets preserveUntil
-- Parameters:
--   rec: table - The record to update
--   blockHeight: number - The block height to preserve until
-- Returns:
--   string - A signal indicating the action taken
--                                          _   _       _   _ _
--  _ __  _ __ ___  ___  ___ _ ____   _____| | | |_ __ | |_(_) |
-- | '_ \| '__/ _ \/ __|/ _ \ '__\ \ / / _ \ | | | '_ \| __| | |
-- | |_) | | |  __/\__ \  __/ |   \ V /  __/ |_| | | | | |_| | |
-- | .__/|_|  \___||___/\___|_|    \_/ \___|\___/|_| |_|\__|_|_|
-- |_|

function preserveUntil(rec, blockHeight)
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    -- Remove deleteAtHeight if it exists
    rec[BIN_DELETE_AT_HEIGHT] = nil

    -- Set preserveUntil
    rec[BIN_PRESERVE_UNTIL] = blockHeight

    -- Update the record
    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK

    -- Check if we need to signal external file handling
    if rec[BIN_EXTERNAL] then
        response[FIELD_SIGNAL] = SIGNAL_PRESERVE
    end

    return response
end

-- Function to set the 'conflicting' field of a record
-- Parameters:
--   rec: table - The record to update
--   setValue: boolean - The value to set for the 'conflicting' field
-- Returns:
--   string - A signal indicating the action taken
--           _   _               _            _
--  ___  ___| |_| |    ___   ___| | _____  __| |
-- / __|/ _ \ __| |   / _ \ / __| |/ / _ \/ _` |
-- \__ \  __/ |_| |__| (_) | (__|   <  __/ (_| |
-- |___/\___|\__|_____\___/ \___|_|\_\___|\__,_|
--
function setLocked(rec, setValue)
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    local totalExtraRecs = rec[BIN_TOTAL_EXTRA_RECS] or 0

    rec[BIN_LOCKED] = setValue

    -- Remove any existing deleteAtHeight when locking
    if setValue and rec[BIN_DELETE_AT_HEIGHT] then
        rec[BIN_DELETE_AT_HEIGHT] = nil
    end

    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK
    response[FIELD_CHILD_COUNT] = totalExtraRecs

    return response
end


-- Increment the number of records and set deleteAtHeight if necessary
--  _                                          _   ____                   _   _____      _             ____
-- (_)_ __   ___ _ __ ___ _ __ ___   ___ _ __ | |_/ ___| _ __   ___ _ __ | |_| ____|_  _| |_ _ __ __ _|  _ \ ___  ___ ___
-- | | '_ \ / __| '__/ _ \ '_ ` _ \ / _ \ '_ \| __\___ \| '_ \ / _ \ '_ \| __|  _| \ \/ / __| '__/ _` | |_) / _ \/ __/ __|
-- | | | | | (__| | |  __/ | | | | |  __/ | | | |_ ___) | |_) |  __/ | | | |_| |___ >  <| |_| | | (_| |  _ <  __/ (__\__ \
-- |_|_| |_|\___|_|  \___|_| |_| |_|\___|_| |_|\__|____/| .__/ \___|_| |_|\__|_____/_/\_\\__|_|  \__,_|_| \_\___|\___|___/
--                                                     |_|
function incrementSpentExtraRecs(rec, inc, currentBlockHeight, blockHeightRetention)
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    local totalExtraRecs = rec[BIN_TOTAL_EXTRA_RECS]
    if totalExtraRecs == nil then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_INVALID_PARAMETER
        response[FIELD_MESSAGE] = ERR_TOTAL_EXTRA_RECS

        return response
    end

    local spentExtraRecs = rec[BIN_SPENT_EXTRA_RECS]
    if spentExtraRecs == nil then
        spentExtraRecs = 0
    end

    spentExtraRecs = spentExtraRecs + inc

    if spentExtraRecs < 0 then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_INVALID_PARAMETER
        response[FIELD_MESSAGE] = ERR_SPENT_EXTRA_RECS_NEGATIVE

        return response
    end

    if spentExtraRecs > totalExtraRecs then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_INVALID_PARAMETER
        response[FIELD_MESSAGE] = ERR_SPENT_EXTRA_RECS_EXCEED

        return response
    end

    rec[BIN_SPENT_EXTRA_RECS] = spentExtraRecs

    local signal, childCount = setDeleteAtHeight(rec, currentBlockHeight, blockHeightRetention)

    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK
    if signal and signal ~= "" then
        response[FIELD_SIGNAL] = signal
        if childCount then
            response[FIELD_CHILD_COUNT] = childCount
        end
    end

    return response
end
