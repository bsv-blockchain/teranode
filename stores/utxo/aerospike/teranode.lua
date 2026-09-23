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
-- Alert-system freeze record, stored per output offset (issue #1422). The consensus
-- window is half-open [from, until): from == 0 means "from genesis", until == nil/0 means
-- "no end". The from entry is ALWAYS written when a freeze is recorded, so its presence
-- is what says "this outpoint carries a freeze record" — independently of the 0xFF
-- sentinel in the spending-data slot, which a block-validation spend may legitimately
-- overwrite. utxoFreezeExp marks outputs whose policy freeze expires with the window.
local BIN_UTXO_FREEZE_FROM = "utxoFreezeFrom"
local BIN_UTXO_FREEZE_UNTIL = "utxoFreezeUntil"
local BIN_UTXO_FREEZE_EXP = "utxoFreezeExp"
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
local ERROR_CODE_CONSENSUS_FROZEN = "CONSENSUS_FROZEN"
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
local ERROR_CODE_FREEZE_RECORD_DAMAGED = "FREEZE_RECORD_DAMAGED"

-- Message constants
local MSG_CONFLICTING = "TX is conflicting"
local MSG_LOCKED = "TX is locked and cannot be spent"
local MSG_CREATING = "TX is being created and cannot be spent yet"
local MSG_FROZEN = "UTXO is frozen"
local MSG_CONSENSUS_FROZEN = "UTXO is frozen at block height "
local MSG_FREEZE_RECORD_DAMAGED = "freeze record on this output is malformed"
local MSG_FREEZE_RECORDED_ON_SPENT = "freeze recorded on spent output"
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

-- Function to check whether a freeze window [from, until) is being enforced at a block
-- height. from == 0 means "from genesis", until == 0 means "no end", so a freeze written
-- before the window bins existed (sentinel only) reads back as (0, 0) and is enforced at
-- every height. Mirrors utxo.FreezeWindowActiveAt line for line; a one-block disagreement
-- between the two would be a one-block fleet split.
local function freezeWindowActiveAt(freezeFrom, freezeUntil, currentBlockHeight)
    if currentBlockHeight < freezeFrom then
        return false
    end

    return freezeUntil == 0 or currentBlockHeight < freezeUntil
end

-- Function to check whether the policy tier of a freeze record still applies at a block
-- height: always, unless the record says the policy expires with consensus and the
-- consensus window has ended — either because the height has reached its end, or because
-- the window is empty (until <= from, the shape an unfreeze alert takes) and so ended
-- before it began. Mirrors utxo.FreezePolicyActiveAt.
local function freezePolicyActiveAt(freezeFrom, freezeUntil, policyExpires, currentBlockHeight)
    if not policyExpires or freezeUntil == 0 then
        return true
    end

    local windowEnded = currentBlockHeight >= freezeUntil or freezeUntil <= freezeFrom

    return not windowEnded
end

-- The largest height a freeze record can hold: heights are uint32 on the Go side.
local MAX_FREEZE_HEIGHT = 4294967295

-- Function to report whether a stored freeze height has the only shape FreezeUTXOs
-- writes: a number in the uint32 range. Anything else is storage damage. The Go reader
-- (mapBinEntryInt in freeze_record.go) applies the same bound, so the spend path and
-- block validation cannot diverge on a damaged record.
local function freezeHeightValid(h)
    return type(h) == "number" and h >= 0 and h <= MAX_FREEZE_HEIGHT
end

-- Function to read one output's freeze record out of maps the caller has already read
-- from the record, so a batch of spends does not re-read the bins per spend. Returns
-- present, from, until, policyExpires, damaged. Presence is the from entry: it is always
-- written when a freeze is recorded, whatever its value. policyExpires is the value of the
-- exp entry, not its presence: setFreezeRecord writes true or removes the entry, so
-- anything else is damage and reads as "does not expire" — the reading that keeps the
-- policy tier in force. damaged is set when a present height is not a number in the
-- uint32 range; the heights then read as 0 and the caller must not judge a window by
-- them — the spend path reports FREEZE_RECORD_DAMAGED, as the Go reader reports storage
-- damage, and a freeze overwrites the record rather than reporting it already stored.
local function freezeRecordFromMaps(freezeFromMap, freezeUntilMap, freezeExpMap, offset)
    if freezeFromMap == nil or freezeFromMap[offset] == nil then
        return false, 0, 0, false, false
    end

    local fromHeight = freezeFromMap[offset]
    local damaged = not freezeHeightValid(fromHeight)

    local untilHeight = 0
    if freezeUntilMap ~= nil and freezeUntilMap[offset] ~= nil then
        untilHeight = freezeUntilMap[offset]
        if not freezeHeightValid(untilHeight) then
            damaged = true
        end
    end

    if damaged then
        return true, 0, 0, false, true
    end

    local policyExpires = freezeExpMap ~= nil and freezeExpMap[offset] == true

    return true, fromHeight, untilHeight, policyExpires, false
end

-- Function to read one output's freeze record straight from the record.
local function getFreezeRecord(rec, offset)
    return freezeRecordFromMaps(rec[BIN_UTXO_FREEZE_FROM], rec[BIN_UTXO_FREEZE_UNTIL], rec[BIN_UTXO_FREEZE_EXP], offset)
end

-- Function to set or remove one entry of a per-offset map bin. A nil value removes the
-- entry and drops the bin once it is empty, so an unfrozen output leaves nothing behind
-- and the expression-based spend path's bin-existence guard stays as narrow as possible.
-- The caller is responsible for aerospike:update(rec).
local function setMapEntry(rec, binName, offset, value)
    local m = rec[binName]

    if value ~= nil then
        if m == nil then
            m = map()
        end

        m[offset] = value
        rec[binName] = m
    elseif m ~= nil and m[offset] ~= nil then
        map.remove(m, offset)

        if map.size(m) == 0 then
            rec[binName] = nil
        else
            rec[binName] = m
        end
    end
end

-- Function to write one output's freeze record. The from entry is always written, even
-- as 0, because its presence is the record; until and policyExpires are stored only when
-- set. The caller is responsible for aerospike:update(rec).
local function setFreezeRecord(rec, offset, freezeFrom, freezeUntil, policyExpires)
    setMapEntry(rec, BIN_UTXO_FREEZE_FROM, offset, freezeFrom or 0)

    if freezeUntil ~= nil and freezeUntil > 0 then
        setMapEntry(rec, BIN_UTXO_FREEZE_UNTIL, offset, freezeUntil)
    else
        setMapEntry(rec, BIN_UTXO_FREEZE_UNTIL, offset, nil)
    end

    if policyExpires == true then
        setMapEntry(rec, BIN_UTXO_FREEZE_EXP, offset, true)
    else
        setMapEntry(rec, BIN_UTXO_FREEZE_EXP, offset, nil)
    end
end

-- Function to drop one output's freeze record entirely. The caller is responsible for
-- aerospike:update(rec).
local function clearFreezeRecord(rec, offset)
    setMapEntry(rec, BIN_UTXO_FREEZE_FROM, offset, nil)
    setMapEntry(rec, BIN_UTXO_FREEZE_UNTIL, offset, nil)
    setMapEntry(rec, BIN_UTXO_FREEZE_EXP, offset, nil)
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
    -- The single-spend entry point has no block-validation caller, so both tiers of an
    -- alert-system freeze apply. Set explicitly so nobody re-enabling this path inherits
    -- the wrong default by accident.
    spend['ignorePolicyFreeze'] = false
    spend['ignoreConsensusFreeze'] = false

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
    local freezeFromMap = rec[BIN_UTXO_FREEZE_FROM]
    local freezeUntilMap = rec[BIN_UTXO_FREEZE_UNTIL]
    local freezeExpMap = rec[BIN_UTXO_FREEZE_EXP]
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

        -- The alert system's freeze carries two controls at once, mirroring SV Node
        -- (issue #1422), and they are checked HERE, before any spent-state branch:
        --
        --   consensus: the outpoint's enforceAtHeight window, judged against the height of
        --   the block being validated. Every node derives it identically from the chain,
        --   and it is a property of the OUTPOINT, not of what this node currently records
        --   about the coin — a spend already recorded as "spent by this same transaction"
        --   is still rejected at an in-window height (a fork block or a re-mine carrying a
        --   below-window spend), otherwise nodes that saw that earlier block would accept
        --   what nodes that did not reject.
        --
        --   policy: keeps the coin out of THIS node's mempool and templates from the moment
        --   the alert arrived. It lands at a different moment on every node, so a caller
        --   validating a block passes ignorePolicyFreeze and sees only the consensus tier.
        --
        -- The record is read from the freeze bins OR the 0xFF sentinel: a freeze written
        -- before the bins existed has only the sentinel and means "at every height", and a
        -- sentinel a block-validation spend overwrote has only the bins.
        local sentinelPresent = existingSpendingData ~= nil and isFrozen(existingSpendingData)
        local recordPresent, freezeFrom, freezeUntil, policyExpires, freezeDamaged = freezeRecordFromMaps(freezeFromMap, freezeUntilMap, freezeExpMap, offset)

        -- A damaged record cannot be judged, and must never read as "not frozen": the
        -- spend is refused with a code the Go side reports as storage damage, exactly as
        -- block validation's reader does for the same bins.
        if freezeDamaged then
            local error = map()

            error[FIELD_ERROR_CODE] = ERROR_CODE_FREEZE_RECORD_DAMAGED
            error[FIELD_MESSAGE] = MSG_FREEZE_RECORD_DAMAGED

            errors[idx] = error

            goto continue
        end

        if recordPresent or sentinelPresent then
            if not spend['ignoreConsensusFreeze'] and freezeWindowActiveAt(freezeFrom, freezeUntil, currentBlockHeight) then
                local error = map()

                error[FIELD_ERROR_CODE] = ERROR_CODE_CONSENSUS_FROZEN
                error[FIELD_MESSAGE] = MSG_CONSENSUS_FROZEN .. currentBlockHeight

                errors[idx] = error

                goto continue
            end

            -- The policy tier only has something to hold while the output is unspent (a
            -- bare hash, or the sentinel standing in for one).
            if not spend['ignorePolicyFreeze']
                and (existingSpendingData == nil or sentinelPresent)
                and freezePolicyActiveAt(freezeFrom, freezeUntil, policyExpires, currentBlockHeight) then
                local error = map()

                error[FIELD_ERROR_CODE] = ERROR_CODE_FROZEN
                error[FIELD_MESSAGE] = MSG_FROZEN

                errors[idx] = error

                goto continue
            end
        end

        -- The reassignment-maturity hold is judged AFTER the freeze, so that a later alert
        -- on a reassigned-but-immature output yields the consensus verdict at an in-window
        -- height rather than the retryable FROZEN_UNTIL — the SQL store's ordering.
        if spendableIn then
            local spendableHeight = spendableIn[offset]
            if spendableHeight and spendableHeight > currentBlockHeight then
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
            elseif sentinelPresent then
                -- A policy freeze bypassed by a block-validation spend: fall through and
                -- record the spend, overwriting the sentinel exactly as an unfrozen output
                -- would. The freeze bins stay, so the coin is still frozen if this spend is
                -- rolled back, and still consensus-frozen inside the window.
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
-- offset number - the offset in the utxos list (vout % utxoBatchSize)
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- expectedSpendingData []byte - 36 byte spending data the caller expects to be currently stored.
--                               This is mandatory — the Go caller guards nil before invoking
--                               the UDF — and is checked against the stored value to prove the
--                               caller owns the spend it's clearing.
-- currentBlockHeight number - the current block height
-- blockHeightRetention number - the retention period for the UTXO record
--           ____                       _
--  _   _ _ __ / ___| _ __   ___ _ __   __| |
-- | | | | '_ \\___ \| '_ \ / _ \ '_ \ / _` |
-- | |_| | | | |___) | |_) |  __/ | | | (_| |
--  \__,_|_| |_|____/| .__/ \___|_| |_|\__,_|
--                   |_|
--
function unspend(rec, offset, utxoHash, expectedSpendingData, currentBlockHeight, blockHeightRetention)
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

    -- Ownership check: only clear spending_data when the caller owns the current spend.
    -- Idempotent semantics: the safety guarantee is "never wipe a spend we don't own",
    -- not "error on every no-op". Callers like ProcessConflicting build affectedParentSpends
    -- from the loser's inputs, but the parent's stored spending_data may be nil (loser
    -- never actually spent) or belong to the winner (in which case we MUST NOT clear).
    -- In either no-op case we still fall through to setDeleteAtHeight + update so any
    -- DAH housekeeping stays consistent with the actual record state.
    local callerOwnsSpend = existingSpendingData ~= nil
        and bytes_equal(existingSpendingData, expectedSpendingData)

    if callerOwnsSpend then
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

    -- Stamp the DAH relative to the height of the block this tx is mined into
    -- (blockHeight), NOT the caller's cached chain tip (currentBlockHeight). The
    -- cached tip lags behind the block being validated during catchup/sync; using
    -- it stamped the DAH too low and let the pruner delete the record before the
    -- retention window elapsed. currentBlockHeight is still used above for the
    -- unmined_since marker, which legitimately wants the current height.
    local signal, childCount = setDeleteAtHeight(rec, blockHeight, blockHeightRetention)

    -- Update the record to save changes
    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK
    if signal and signal ~= "" then
        response[FIELD_SIGNAL] = signal
        if childCount then
            response[FIELD_CHILD_COUNT] = childCount
        end
    end

    -- #1037: On a mine, always surface the pagination/extra-record count so the
    -- client can clear the `locked` flag on the pagination records too. This UDF
    -- runs on (and can only mutate) the master record, so the lock-clear above
    -- only affects the master. For an external/paginated tx, any pagination
    -- record left locked (e.g. created WithLocked(true) by quick-validate or the
    -- validator 2PC, whose unlock then never ran) would stay locked forever, and
    -- a child spending an output that lives on a pagination record (vout >=
    -- utxoBatchSize) would fail permanently with TX_LOCKED. The client clears the
    -- pagination records using this count.
    --
    -- INVARIANT: this is the SAME value the DAH-signal branch above may already
    -- have written to FIELD_CHILD_COUNT. setDeleteAtHeight returns its childCount
    -- as exactly rec[BIN_TOTAL_EXTRA_RECS] (and only for external records, which
    -- are the ones that have pagination records), so the two are always equal and
    -- this unconditional overwrite is consistent. We overwrite unconditionally
    -- because the DAH signal only fires on a DAH transition, whereas the lock-clear
    -- must happen on every mine. If setDeleteAtHeight's childCount ever stops
    -- meaning totalExtraRecs, THIS overwrite is the source of truth for the
    -- lock-clear and must keep using totalExtraRecs.
    if not unsetMined then
        local totalExtraRecs = rec[BIN_TOTAL_EXTRA_RECS]
        if totalExtraRecs and totalExtraRecs > 0 then
            response[FIELD_CHILD_COUNT] = totalExtraRecs
        end
    end

    return response
end

-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- offset number - the offset in the utxos list (vout % utxoBatchSize)
-- utxoHash []byte - 32 byte little-endian hash of the UTXO
-- freezeFrom number - first block height the freeze is enforced at (0 = from genesis)
-- freezeUntil number - first block height the freeze is no longer enforced at (0 = no end)
-- policyExpires boolean - whether the policy tier lifts once the window ends
--   __
--  / _|_ __ ___  ___ _______
-- | |_| '__/ _ \/ _ \_  / _ \
-- |  _| | |  __/  __// /  __/
-- |_| |_|  \___|\___/___\___|
function freeze(rec, offset, utxoHash, freezeFrom, freezeUntil, policyExpires)
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

    local wantFrom = freezeFrom or 0
    local wantUntil = freezeUntil or 0
    local wantExp = policyExpires == true

    local sentinelPresent = existingSpendingData ~= nil and isFrozen(existingSpendingData)
    local recordPresent, storedFrom, storedUntil, storedExp, storedDamaged = getFreezeRecord(rec, offset)

    -- An authority can re-issue a freeze for an already-frozen output to extend, shorten
    -- or shift its enforceAtHeight window, so a repeat freeze that changes the record is
    -- an update rather than a no-op. Only a repeat that asks for exactly the record
    -- already stored is reported as ALREADY_FROZEN, which is what the alert system and
    -- the admin RPC have always seen for a re-freeze. A damaged record matches nothing:
    -- the freeze overwrites it, which is how such a record is repaired.
    if (recordPresent or sentinelPresent) and not storedDamaged
        and storedFrom == wantFrom and storedUntil == wantUntil and storedExp == wantExp then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_ALREADY_FROZEN
        response[FIELD_MESSAGE] = MSG_ALREADY_FROZEN

        return response
    end

    -- Spent by a real transaction: the consensus record is a property of the outpoint,
    -- not of this node's spent-state for it, so it is recorded regardless (issue #1422).
    -- A late alert must still govern a fork block or a re-mine of that spend inside the
    -- window, and must leave the coin frozen if the spend is ever rolled back. Only the
    -- policy sentinel is skipped: there is nothing unspent for it to hold.
    if existingSpendingData ~= nil and not sentinelPresent then
        setFreezeRecord(rec, offset, wantFrom, wantUntil, wantExp)

        aerospike:update(rec)

        response[FIELD_STATUS] = STATUS_OK
        response[FIELD_MESSAGE] = MSG_FREEZE_RECORDED_ON_SPENT

        return response
    end

    if not sentinelPresent then
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
    end

    setFreezeRecord(rec, offset, wantFrom, wantUntil, wantExp)

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

    local utxoSize = bytes.size(utxo)
    if utxoSize ~= FULL_UTXO_SIZE and utxoSize ~= UTXO_HASH_SIZE then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_INVALID_SIZE
        response[FIELD_MESSAGE] = ERR_UTXO_INVALID_SIZE

        return response
    end

    -- A freeze may be held by the sentinel, by the freeze bins, or both: an output whose
    -- sentinel a block-validation spend overwrote, or one that was already spent when the
    -- alert arrived, carries only the bins. Unfreeze must clear whichever is present,
    -- spent or not — it is the only way a consensus record is ever deleted.
    local sentinelPresent = existingSpendingData ~= nil and isFrozen(existingSpendingData)
    local recordPresent = getFreezeRecord(rec, offset)

    if not sentinelPresent and not recordPresent then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_NOT_FROZEN
        response[FIELD_MESSAGE] = ERR_UTXO_NOT_FROZEN

        return response
    end

    if sentinelPresent then
        -- Restore the plain unspent output
        utxos[offset + 1] = createUTXOWithSpendingData(utxoHash, nil) -- NB - lua arrays are 1-based!!!!
        rec[BIN_UTXOS] = utxos
    end

    clearFreezeRecord(rec, offset)

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

    local utxoSize = bytes.size(utxo)
    if utxoSize ~= FULL_UTXO_SIZE and utxoSize ~= UTXO_HASH_SIZE then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_UTXO_INVALID_SIZE
        response[FIELD_MESSAGE] = ERR_UTXO_INVALID_SIZE

        return response
    end

    -- Reassignment needs an UNSPENT output that carries a freeze — held by the sentinel,
    -- by the freeze bins (a rolled-back below-window spend leaves only the bins), or both.
    -- An output spent by a real transaction cannot be reassigned.
    local sentinelPresent = existingSpendingData ~= nil and isFrozen(existingSpendingData)
    local recordPresent = getFreezeRecord(rec, offset)
    local spentByRealTx = existingSpendingData ~= nil and not sentinelPresent

    if spentByRealTx or (not sentinelPresent and not recordPresent) then
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

    -- Reassignment clears the frozen sentinel, so the freeze record that went with it has
    -- to go too — otherwise a later re-freeze of this output would silently inherit the
    -- old authority's heights. Mirrors UnFreezeUTXOs and the SQL store.
    clearFreezeRecord(rec, offset)

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

--            _     _ ____       _      _           _  ____ _     _ _     _
--   __ _  __| | __| |  _ \  ___| | ___| |_ ___  __| |/ ___| |__ (_) | __| |_ __ ___ _ __
--  / _` |/ _` |/ _` | | | |/ _ \ |/ _ \ __/ _ \/ _` | |   | '_ \| | |/ _` | '__/ _ \ '_ \
-- | (_| | (_| | (_| | |_| |  __/ |  __/ ||  __/ (_| | |___| | | | | | (_| | | |  __/ | | |
--  \__,_|\__,_|\__,_|____/ \___|_|\___|\__\___|\__,_|\____|_| |_|_|_|\__,_|_|  \___|_| |_|
--

-- Adds child transaction hashes to the deletedChildren map on a parent record.
-- If the record does not exist, returns TX_NOT_FOUND (no error raised).
-- Parameters:
--   rec: record - The parent transaction record
--   childHashes: list - List of child transaction hash strings to mark as deleted
function addDeletedChildren(rec, childHashes)
    local response = map()

    if not aerospike:exists(rec) then
        response[FIELD_STATUS] = STATUS_ERROR
        response[FIELD_ERROR_CODE] = ERROR_CODE_TX_NOT_FOUND
        response[FIELD_MESSAGE] = ERR_TX_NOT_FOUND

        return response
    end

    local deletedChildren = rec[BIN_DELETED_CHILDREN]
    if deletedChildren == nil then
        deletedChildren = map()
    end

    for childHash in list.iterator(childHashes) do
        deletedChildren[childHash] = true
    end

    rec[BIN_DELETED_CHILDREN] = deletedChildren
    aerospike:update(rec)

    response[FIELD_STATUS] = STATUS_OK

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

    -- Clamp to valid range instead of erroring. The counter can drift out of
    -- sync when spend/unspend rollbacks are interrupted (e.g. context cancellation
    -- during DEVICE_OVERLOAD). Clamping keeps the node alive — Go verifies
    -- children before acting on DAH signals, so a drifted counter is harmless.
    if spentExtraRecs < 0 then
        spentExtraRecs = 0
    end

    if spentExtraRecs > totalExtraRecs then
        spentExtraRecs = totalExtraRecs
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
