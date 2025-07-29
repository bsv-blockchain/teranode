
# Aerospike UTXO Store

This document describes the Aerospike UTXO store implementation, including the data structures and Lua User-Defined Functions (UDFs) that provide the bridge between the Go application and Aerospike.

## Data Structure

### Normal Transaction
```
inputs
outputs  
version
locktime
fee
sizeInBytes
utxos - list of UTXOs in the transaction
totalUtxos - total number of UTXOs in the transaction
recordUtxos - number of UTXOs in this record
spentUtxos - number of spent UTXOs in this record
blockIDs - array of block identifiers
blockHeights - array of block heights
subtreeIdxs - array of subtree indices
isCoinbase
spendingHeight - coinbase maturity height
frozen
conflicting
locked
preserveUntil
deleteAtHeight
external - true if transaction data is in external store
totalExtraRecs - total number of extra records for pagination
spentExtraRecs - number of spent extra records
utxoSpendableIn - map of UTXO-specific freeze heights
```

### Large Transaction with External Storage (< 20,000 outputs)
```
external = true
fee
sizeInBytes
utxos - list of UTXOs (up to batch size)
totalUtxos - total number in transaction
recordUtxos - number in this record
spentUtxos - spent count in this record
blockIDs
isCoinbase
spendingHeight
frozen
[other fields as above]
```

### Paginated Transaction (> 20,000 outputs)

**Record 1 - Key: TXID (the _0 is implied)**
```
external = true
fee
sizeInBytes
utxos - utxo[0..19999]
totalUtxos - total in entire transaction
recordUtxos - 20000 (in this record)
spentUtxos - spent count in this record
totalExtraRecs - number of additional records
spentExtraRecs - number of fully spent extra records
[other fields as above]
```

**Record 2 - Key: TXID_1**
```
utxos - utxo[20000..39999]
recordUtxos - count in this record
spentUtxos - spent count in this record
[subset of fields]
```

### Key Calculation

For a UTXO with vout 25000:
```
key = txid
recordNumber = floor(vout/utxoBatchSize)
if recordNumber > 0 then
    key = key + "_" + recordNumber
end
```

## Lua Bridge Interface

### Recent Changes (Object Response Format)

**All Lua functions now return structured map objects instead of string responses.** This change was made to:
- Provide individual error responses for batch operations (especially `spendMulti`)
- Maintain consistent response format across all functions
- Simplify error handling in Go code
- Support richer response data structures
- Remove "ERROR:" prefix from error messages (status field indicates error state)
- Use consistent status constants (STATUS_OK/STATUS_ERROR) matching Go's LuaStatus type

### Response Format

All functions return a map with at least a `status` field:

```lua
{
    status = STATUS_OK | STATUS_ERROR,  -- Using Lua constants
    message = "error description",      -- Only present when status="ERROR"
    signal = "ALLSPENT",               -- Optional signal
    blockIDs = [1, 2, 3],              -- Optional array of block IDs
    errors = { offset = "error msg" }, -- For spendMulti batch errors
    childCount = 2                     -- Optional, number of child records
}
```

## Function Reference

### spend
Spends a single UTXO by delegating to `spendMulti`.

**Parameters:**
- `rec`: The transaction record
- `offset`: The UTXO offset within the transaction
- `utxoHash`: The UTXO hash for verification
- `spendingData`: 36-byte spending transaction data
- `ignoreConflicting`: Boolean to ignore conflicting status
- `ignoreLocked`: Boolean to ignore locked status
- `currentBlockHeight`: Current blockchain height
- `blockHeightRetention`: Number of blocks to retain

**Returns:** Map response from `spendMulti`

### spendMulti
Spends multiple UTXOs in a single atomic operation.

**Parameters:**
- `rec`: The transaction record
- `spends`: Array of spend objects, each containing:
  - `offset`: UTXO offset
  - `utxoHash`: UTXO hash
  - `spendingData`: Spending data
- `ignoreConflicting`: Boolean to ignore conflicting status
- `ignoreLocked`: Boolean to ignore locked status
- `currentBlockHeight`: Current blockchain height
- `blockHeightRetention`: Number of blocks to retain

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    errors = {                    -- Only present when status="ERROR"
        [offset1] = "SPENT:abc...",
        [offset2] = "FROZEN"
    },
    blockIDs = [101, 102],       -- Optional
    signal = "ALLSPENT"          -- Optional signal
}
```

**Error Types:**
- `TX not found`: Transaction doesn't exist
- `CONFLICTING`: Transaction is marked as conflicting
- `LOCKED`: Transaction is locked
- `COINBASE_IMMATURE`: Coinbase not yet spendable
- `SPENT:xxx`: Already spent by transaction xxx
- `FROZEN`: UTXO is frozen
- `FROZEN until X`: UTXO frozen until block height X

### unspend
Reverses a spend operation on a UTXO.

**Parameters:**
- `rec`: The transaction record
- `offset`: The UTXO offset
- `utxoHash`: The UTXO hash for verification
- `currentBlockHeight`: Current blockchain height
- `blockHeightRetention`: Number of blocks to retain

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    message = "error description",  -- Only when status="ERROR"
    signal = "NOTALLSPENT"         -- Optional signal
}
```

### setMined
Marks a transaction as mined in a specific block.

**Parameters:**
- `rec`: The transaction record
- `blockID`: The block identifier
- `blockHeight`: The block height
- `subtreeIdx`: The subtree index
- `currentBlockHeight`: Current blockchain height
- `blockHeightRetention`: Number of blocks to retain

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    message = "error description",  -- Only when status="ERROR"
    signal = "DAHSET",             -- Optional signal
    childCount = 2                 -- Optional, number of child records
}
```

### freeze
Freezes a UTXO to prevent spending.

**Parameters:**
- `rec`: The transaction record
- `offset`: The UTXO offset
- `utxoHash`: The UTXO hash for verification

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    message = "error description"   -- Only when status="ERROR"
}
```

### unfreeze
Removes the freeze status from a UTXO.

**Parameters:**
- `rec`: The transaction record
- `offset`: The UTXO offset
- `utxoHash`: The UTXO hash for verification

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    message = "error description"   -- Only when status="ERROR"
}
```

### reassign
Changes the UTXO hash and optionally sets spending restrictions.

**Parameters:**
- `rec`: The transaction record
- `offset`: The UTXO offset
- `utxoHash`: Current UTXO hash
- `newUtxoHash`: New UTXO hash
- `blockHeight`: Optional block height for spending restriction
- `spendableAfter`: Block height after which UTXO is spendable

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    message = "error description"   -- Only when status="ERROR"
}
```

### setConflicting
Marks a transaction as conflicting.

**Parameters:**
- `rec`: The transaction record
- `setValue`: Boolean to set/unset conflicting status
- `currentBlockHeight`: Current blockchain height
- `blockHeightRetention`: Number of blocks to retain

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    message = "error description",  -- Only when status="ERROR"
    signal = "DAHSET"              -- Optional signal
}
```

### preserveUntil
Prevents deletion of a transaction until a specific block height.

**Parameters:**
- `rec`: The transaction record
- `blockHeight`: Block height to preserve until

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    message = "error description",  -- Only when status="ERROR"
    signal = "PRESERVE"            -- Optional, for external files
}
```

### setLocked
Sets or unsets the locked status of a transaction.

**Parameters:**
- `rec`: The transaction record
- `setValue`: Boolean to set/unset locked status

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    message = "error description"   -- Only when status="ERROR"
}
```

### incrementSpentExtraRecs
Updates the count of spent extra records for paginated transactions.

**Parameters:**
- `rec`: The transaction record
- `inc`: Increment value (can be negative)
- `currentBlockHeight`: Current blockchain height
- `blockHeightRetention`: Number of blocks to retain

**Returns:**
```lua
{
    status = "OK" | "ERROR",
    message = "error description",  -- Only when status="ERROR"
    signal = "DAHSET",             -- Optional signal
    childCount = 2                 -- Optional, number of child records
}
```

## Signals

Signals provide additional information about operations that may require follow-up actions:

- `ALLSPENT` - All UTXOs in the transaction are spent
- `NOTALLSPENT` - Some UTXOs remain unspent
- `DAHSET` - Delete-At-Height was set (check childCount field for number of child records)
- `DAHUNSET` - Delete-At-Height was removed (check childCount field for number of child records)
- `PRESERVE` - External files need preservation

**Note:** Child count information is now returned as a separate `childCount` field in the response, not embedded in the signal string.

## Error Handling

When `status="ERROR"`:
- For single operations: Check the `message` field
- For batch operations: Check the `errors` map with offset keys
- Common errors include transaction not found, UTXO already spent, frozen status, etc.

## Go Integration

The Go code uses `ParseLuaMapResponse()` to handle these map responses:

```go
res, err := store.ParseLuaMapResponse(response.Bins["SUCCESS"])
if err != nil {
    // Handle parsing error
}

if res.Status == LuaStatusOK {
    // Success - check for signals
    if res.Signal != "" {
        switch res.Signal {
        case LuaSignalDAHSet:
            // Handle DAH set with res.ChildCount
        case LuaSignalAllSpent:
            // Handle all spent
        }
    }
} else if res.Status == LuaStatusError {
    // Error - check res.Message or res.Errors
}
```

## Migration Notes

This is a breaking change from the previous string-based response format. However, since Lua and Go changes are released together, no backward compatibility is maintained. All functions now consistently return map objects, making the interface more robust and easier to extend.
