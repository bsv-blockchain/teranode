#!lua name=teranode____VERSION___

--                           _
--  ___ _ __   ___ _ __   __| |
-- / __| '_ \ / _ \ '_ \ / _` |
-- \__ \ |_) |  __/ | | | (_| |
-- |___/ .__/ \___|_| |_|\__,_|
--     |_|
--
local function spend____VERSION___(keys, args)
    local tx_key = keys[1] -- LUA is 1-based
    local offset = tonumber(args[1])
    local utxoHash = args[2]
    local spendingTxID = args[3]
    local currentBlockHeight = tonumber(args[4])
    local ttl = tonumber(args[5])

    -- Get all transaction data in one call
    local tx = redis.call('HGETALL', tx_key)
    if #tx == 0 then
        return "ERROR:TX not found"
    end

    -- Convert array of key-value pairs to a table for easier access
    local tx_data = {}
    for i = 1, #tx, 2 do
        tx_data[tx[i]] = tx[i + 1]
    end

    -- Check if frozen
    if tx_data['frozen'] == "1" then
        return "FROZEN:TX is frozen"
    end

    -- Check if conflicting
    if tx_data['conflicting'] == "1" then
        return "CONFLICTING:TX is conflicting"
    end

    -- Check coinbase spending rules
    local spendingHeight = tonumber(tx_data['spendingHeight'])

    if spendingHeight and spendingHeight > 0 and spendingHeight > currentBlockHeight then
        return "COINBASE_IMMATURE:Coinbase UTXO can only be spent after 100 blocks, in block " .. spendingHeight
    end

    -- Get UTXO data
    local utxo = tx_data['utxo:' .. offset]

    if not utxo then
        return "ERROR:UTXO not found"
    end

    -- Check spendable height
    local spendableIn = tonumber(tx_data['spendableIn:' .. offset])

    if spendableIn and spendableIn > currentBlockHeight then
        return "FROZEN:UTXO is not spendable until block " .. spendableIn
    end

    -- Verify UTXO hash
    local existingUTXOHash = string.sub(utxo, 1, 64)

    if existingUTXOHash ~= utxoHash then
        return "ERROR:Output utxohash mismatch"
    end

    -- Check if already spent
    if #utxo == 128 then
        local existingSpendingTxID = string.sub(utxo, 65, 128)

        if existingSpendingTxID == spendingTxID then
            return 'OK'
        elseif existingSpendingTxID == "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" then
            return "FROZEN:UTXO is frozen"
        else
            return 'SPENT:' .. existingSpendingTxID
        end
    end

    -- Spend the UTXO
    local newUtxo = utxoHash .. spendingTxID
    redis.call('HSET', tx_key, 'utxo:' .. offset, newUtxo)
    
    -- Update spent count
    local spentUtxos = tonumber(tx_data['spentUtxos'] or 0) + 1
    redis.call('HSET', tx_key, 'spentUtxos', spentUtxos)

    -- Handle TTL
    local nrUtxos = tonumber(tx_data['nrUtxos'])
    
    -- redis.log(redis.LOG_WARNING, "ttl: " .. ttl)

    
    local blockIDs = tx_data['blockIDs'] or ""

    if nrUtxos == spentUtxos and blockIDs ~= "" then
        -- ttl is in seconds
        redis.call('EXPIRE', tx_key, ttl)
        return 'OK:TTLSET'
    end

    return 'OK'
end


--              ____                       _
--  _   _ _ __ / ___| _ __   ___ _ __   __| |
-- | | | | '_ \\___ \| '_ \ / _ \ '_ \ / _` |
-- | |_| | | | |___) | |_) |  __/ | | | (_| |
--  \__,_|_| |_|____/| .__/ \___|_| |_|\__,_|
--                   |_|
--
local function unspend____VERSION___(keys, args)
    local tx_key = keys[1]
    local offset = tonumber(args[1])
    local utxoHash = args[2]

    -- Get all transaction data in one call
    local tx = redis.call('HGETALL', tx_key)
    if #tx == 0 then
        return "ERROR:TX not found"
    end

    -- Convert array of key-value pairs to a table
    local tx_data = {}
    for i = 1, #tx, 2 do
        tx_data[tx[i]] = tx[i + 1]
    end

    -- Get UTXO data
    local utxo = tx_data['utxo:' .. offset]
    if not utxo then
        return "ERROR:UTXO not found"
    end

    -- Verify UTXO hash
    local existingUTXOHash = string.sub(utxo, 1, 64)

    if existingUTXOHash ~= utxoHash then
        return "ERROR:Output utxohash mismatch"
    end

    local signal = ""

    -- If the utxo has been spent, remove the spendingTxID
    if #utxo == 128 then
        local newUtxo = string.sub(utxo, 1, 64)
        local nrUtxos = tonumber(tx_data['nrUtxos'])
        local spentUtxos = tonumber(tx_data['spentUtxos'])

        -- Check if all UTXOs were spent
        if nrUtxos == spentUtxos then
            signal = ":NOTALLSPENT"
        end

        -- Update the record
        redis.call('HSET', tx_key, 
            'utxo:' .. offset, newUtxo,
            'spentUtxos', spentUtxos - 1
        )
    end

    -- Remove TTL
    redis.call('PERSIST', tx_key) -- This removes any existing TTL

    return 'OK' .. signal
end

--           _   __  __ _                _
--  ___  ___| |_|  \/  (_)_ __   ___  __| |
-- / __|/ _ \ __| |\/| | | '_ \ / _ \/ _` |
-- \__ \  __/ |_| |  | | | | | |  __/ (_| |
-- |___/\___|\__|_|  |_|_|_| |_|\___|\__,_|
--
local function setMined____VERSION___(keys, args)
    local tx_key = keys[1]
    local blockID = tonumber(args[1])
    local ttl = tonumber(args[2])

    -- Get all transaction data in one call
    local tx = redis.call('HGETALL', tx_key)
    if #tx == 0 then
        return "ERROR:TX not found"
    end

    -- Convert array of key-value pairs to a table
    local tx_data = {}
    for i = 1, #tx, 2 do
        tx_data[tx[i]] = tx[i + 1]
    end

    -- Get or initialize blockIDs string
    local blockIDs = tx_data['blockIDs'] or ""
    if blockIDs ~= "" then
        blockIDs = blockIDs .. ","
    end
    blockIDs = blockIDs .. blockID

    -- Update the record
    redis.call('HSET', tx_key, 'blockIDs', blockIDs)

    -- Handle TTL
    local signal = ""
    local nrUtxos = tonumber(tx_data['nrUtxos'])
    local spentUtxos = tonumber(tx_data['spentUtxos'] or 0)
    
    if nrUtxos == spentUtxos then
       -- ttl is in seconds
       redis.call('EXPIRE', tx_key, ttl)
       signal = ":TTLSET"
    end

    return 'OK' .. signal
end


-- KEYS[1]: transaction key
-- ARGV[1]: spends (JSON array of {offset, utxoHash, spendingTxID})
-- ARGV[2]: currentBlockHeight
-- ARGV[3]: ttl
local function spendMulti____VERSION___(keys, args)
    local tx_key = keys[1]
    local spends = cjson.decode(args[1])
    local currentBlockHeight = tonumber(args[2])
    local ttl = tonumber(args[3])

    -- Get all transaction data in one call
    local tx = redis.call('HGETALL', tx_key)
    if #tx == 0 then
        return "ERROR:TX not found"
    end

    -- Convert array of key-value pairs to a table
    local tx_data = {}
    for i = 1, #tx, 2 do
        tx_data[tx[i]] = tx[i + 1]
    end

    -- Check if frozen
    if tx_data['frozen'] == "1" then
        return "FROZEN:TX is frozen"
    end

    -- Check coinbase spending rules
    local spendingHeight = tonumber(tx_data['spendingHeight'])
    if spendingHeight and spendingHeight > 0 and spendingHeight > currentBlockHeight then
        return "ERROR:Coinbase UTXO can only be spent after 100 blocks, in block " .. spendingHeight
    end

    local updates = {}
    local spentUtxos = tonumber(tx_data['spentUtxos'] or 0)

    -- Process each spend
    for _, spend in ipairs(spends) do
        local offset = tonumber(spend.offset)
        local utxoHash = spend.utxoHash
        local spendingTxID = spend.spendingTxID

        -- Get UTXO data
        local utxo = tx_data['utxo:' .. offset]
        if not utxo then
            return "ERROR:UTXO not found for offset " .. offset
        end

        -- Check spendable height
        if tx_data['spendableIn:' .. offset] then
            local spendableIn = tonumber(tx_data['spendableIn:' .. offset])
            if spendableIn and spendableIn >= currentBlockHeight then
                return "ERROR:UTXO is not spendable until block " .. spendableIn
            end
        end

        -- Verify UTXO hash
        local existingUTXOHash = string.sub(utxo, 1, 64)

        if existingUTXOHash ~= utxoHash then
            return "ERROR:Output utxohash mismatch"
        end

        if #utxo == 128 then
            local existingSpendingTxID = string.sub(utxo, 65, 128)

            if existingSpendingTxID == spendingTxID then
                return 'OK'
            elseif existingSpendingTxID == "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" then
                return "FROZEN:UTXO is frozen"
            else
                return 'SPENT:' .. existingSpendingTxID
            end
        end

        -- Prepare the update
        local newUtxo = utxoHash .. spendingTxID
        updates['utxo:' .. offset] = newUtxo
        spentUtxos = spentUtxos + 1
    end

    -- Apply all updates atomically
    updates['spentUtxos'] = spentUtxos
    redis.call('HSET', tx_key, unpack(updates))

    -- Handle TTL
    local nrUtxos = tonumber(tx_data['nrUtxos'])
    if nrUtxos == spentUtxos then
        redis.call('EXPIRE', tx_key, ttl)
        return 'OK:TTLSET'
    end

    return 'OK'
end


-- KEYS[1]: transaction key
-- ARGV[1]: offset
-- ARGV[2]: utxoHash
local function freeze____VERSION___(keys, args)
    local tx_key = keys[1]
    local offset = tonumber(args[1])
    local utxoHash = args[2]

    -- Get all transaction data in one call
    local tx = redis.call('HGETALL', tx_key)
    if #tx == 0 then
        return "ERROR:TX not found"
    end

    -- Convert array of key-value pairs to a table
    local tx_data = {}
    for i = 1, #tx, 2 do
        tx_data[tx[i]] = tx[i + 1]
    end

    -- Get UTXO data
    local utxo = tx_data['utxo:' .. offset]
    if not utxo then
        return "ERROR:UTXO not found"
    end

    -- Verify UTXO hash
    local existingUTXOHash = string.sub(utxo, 1, 64)

    if existingUTXOHash ~= utxoHash then
        return "ERROR:Output utxohash mismatch"
    end

    if #utxo == 128 then
        local existingSpendingTxID = string.sub(utxo, 65, 128)

        if existingSpendingTxID == "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" then
            return "FROZEN:UTXO is already frozen"
        else
            return 'SPENT:' .. existingSpendingTxID
        end
    end

    if #utxo ~= 64 then
        return "ERROR:UTXO has an invalid size"
    end

    -- Create frozen UTXO (64 bytes utxoHash + 64 0xFF bytes)
    local frozenUtxo = utxo .. "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    redis.call('HSET', tx_key, 'utxo:' .. offset, frozenUtxo)

    return 'OK'
end

-- KEYS[1]: transaction key
-- ARGV[1]: offset
-- ARGV[2]: utxoHash
local function unfreeze____VERSION___(keys, args)
    local tx_key = keys[1]
    local offset = tonumber(args[1])
    local utxoHash = args[2]

    -- Get all transaction data in one call
    local tx = redis.call('HGETALL', tx_key)
    if #tx == 0 then
        return "ERROR:TX not found"
    end

    -- Convert array of key-value pairs to a table
    local tx_data = {}
    for i = 1, #tx, 2 do
        tx_data[tx[i]] = tx[i + 1]
    end

    -- Get UTXO data
    local utxo = tx_data['utxo:' .. offset]
    if not utxo then
        return "ERROR:UTXO not found"
    end

    -- Verify UTXO hash
    local existingUTXOHash = string.sub(utxo, 1, 64)
    if existingUTXOHash ~= utxoHash then
        return "ERROR:Output utxohash mismatch"
    end

    if #utxo ~= 128 then
        return "ERROR:UTXO has an invalid size"
    end

    local existingSpendingTxID = string.sub(utxo, 65, 128)

    if existingSpendingTxID ~= "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" then
        return "ERROR:UTXO is not frozen"
    end

    -- Unfreeze by removing the spending txid
    local unfrozenUtxo = string.sub(utxo, 1, 64)
    redis.call('HSET', tx_key, 'utxo:' .. offset, unfrozenUtxo)

    return 'OK'
end

-- KEYS[1]: transaction key
-- ARGV[1]: offset
-- ARGV[2]: utxoHash
-- ARGV[3]: newUtxoHash
-- ARGV[4]: blockHeight
-- ARGV[5]: spendableAfter
local function reassign____VERSION___(keys, args)
    local tx_key = keys[1]
    local offset = tonumber(args[1])
    local utxoHash = args[2]
    local newUtxoHash = args[3]
    local blockHeight = tonumber(args[4])
    local spendableAfter = tonumber(args[5])

    -- Get all transaction data in one call
    local tx = redis.call('HGETALL', tx_key)
    if #tx == 0 then
        return "ERROR:TX not found"
    end

    -- Convert array of key-value pairs to a table
    local tx_data = {}
    for i = 1, #tx, 2 do
        tx_data[tx[i]] = tx[i + 1]
    end

    -- Get UTXO data
    local utxo = tx_data['utxo:' .. offset]
    if not utxo then
        return "ERROR:UTXO not found"
    end

    -- Verify UTXO hash
    local existingUTXOHash = string.sub(utxo, 1, 64)
    if existingUTXOHash ~= utxoHash then
        return "ERROR:Output utxohash mismatch"
    end

    if #utxo ~= 128 then
        return "ERROR:UTXO has an invalid size"
    end

    -- Check if frozen
    local existingSpendingTxID = string.sub(utxo, 65, 128)

    if existingSpendingTxID ~= "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" then
        return "ERROR:UTXO is not frozen"
    end

    -- Get or initialize reassignments list
    local reassignments = tx_data['reassignments'] and cjson.decode(tx_data['reassignments']) or {}
    table.insert(reassignments, {
        offset = offset,
        utxoHash = utxoHash,
        newUtxoHash = newUtxoHash,
        blockHeight = blockHeight
    })

    -- Update the record
    redis.call('HSET', tx_key,
        'utxo:' .. offset, newUtxoHash,
        'reassignments', cjson.encode(reassignments),
        'spendableIn:' .. offset, blockHeight + spendableAfter,
        'nrUtxos', tonumber(tx_data['nrUtxos'] or 0) + 1
    )

    return 'OK'
end

redis.register_function('reassign____VERSION___', reassign____VERSION___)
redis.register_function('unspend____VERSION___', unspend____VERSION___)
redis.register_function('unfreeze____VERSION___', unfreeze____VERSION___)
redis.register_function('freeze____VERSION___', freeze____VERSION___)
redis.register_function('spendMulti____VERSION___', spendMulti____VERSION___)
redis.register_function('setMined____VERSION___', setMined____VERSION___)
redis.register_function('spend____VERSION___', spend____VERSION___)
