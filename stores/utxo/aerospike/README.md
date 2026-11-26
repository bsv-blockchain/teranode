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

### Large Transaction with External Storage (outputs <= `utxostore_utxoBatchSize`)
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

### Paginated Transaction (outputs > `utxostore_utxoBatchSize`)

The default `utxostore_utxoBatchSize` is 128, so a transaction with 129 or more outputs is paginated.

**Record 1 - Key: TXID (the _0 is implied)**
```
external = true
fee
sizeInBytes
utxos - utxo[0..utxoBatchSize-1]
totalUtxos - total in entire transaction
recordUtxos - utxoBatchSize (in this record)
spentUtxos - spent count in this record
totalExtraRecs - number of additional records
spentExtraRecs - number of fully spent extra records
creating - true while storeExternallyWithLock writes the transaction's records (absent when not creating)
[other fields as above]
```

**Record 2 - Key: TXID_1**
```
utxos - utxo[utxoBatchSize..2*utxoBatchSize-1]
recordUtxos - count in this record
spentUtxos - spent count in this record
creating - as on the master record
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

## Multi-Record Creation (the `creating` bin)

A transaction with many outputs is stored as a master record plus extra records. The store writes them in two phases, so a reader never sees a half-written transaction as spendable. The code is `storeExternallyWithLock` and `clearCreatingFlag` in `create.go`.

`storeExternallyWithLock` sets the `creating` bin on every record that it writes. It handles two cases:

- A transaction split across several records.
- A single-record transaction that `sendStoreBatch` retries after `RECORD_TOO_BIG`. The bin is then set on its only record.

The plain single-record path, including a single-record `external` transaction, does not set the bin.

1. **Write phase**: the store creates every record with `creating` set to `true`.
2. **Clear phase**: `clearCreatingFlag` deletes the bin from the extra records first. It deletes the bin from the master record only if every extra record cleared.

A failure in the clear phase leaves one of these states:

- The batch write to the extra records fails as a whole: every extra record and the master can stay marked.
- Some extra records fail: those extra records and the master stay marked.
- The master write fails: every extra record is clear, and only the master stays marked.

While `creating` is `true`, the Lua `spend` function refuses to spend the record and returns the error code `CREATING`.

### Recovery

If the clear phase fails or the process stops, the records stay in the `creating` state. These paths act on them:

1. **Subtree validation**: subtree validation reads only the master record. If the master has `Creating == true`, the transaction counts as missing, and subtree validation processes it again. See `processTxMetaUsingStore.go`.
2. **Re-create**: a second store of a multi-record transaction goes through `storeExternallyWithLock` again. It runs `clearCreatingFlag` only when all of these are true:
   - It gets the creation lock. A lock left by a writer that crashed blocks it until the lock TTL expires.
   - The blob write succeeds.
   - The record batch has no top-level error, and every record returns OK or `KEY_EXISTS_ERROR`.

   A second store of a single-record transaction that went through the `RECORD_TOO_BIG` retry does not do this. It takes the plain `sendStoreBatch` put, gets `KEY_EXISTS_ERROR`, and returns `ErrTxExists` without a call to `clearCreatingFlag`. Only `setMined` recovers that case.
3. **setMined**: when the transaction is mined, `setMined` deletes the `creating` bin from the master record and from every extra record that exists. The Lua `setMined` function or `set_mined_expressions.go` clears the master. `clearLockedOnRecordsMulti` in `set_mined.go` clears the extra records in the same write that clears `locked`. On the expressions path, a re-mine of a block the master already lists comes back `FILTERED_OUT` and skips the master write, so `applyLockClearWork` in `set_mined.go` deletes `creating` on the master and on every extra record in its follow-up write.

One gap remains after `setMined`: an extra record that the write phase never created. `setMined` clears the master, and the clear of the missing record gets `KEY_NOT_FOUND`, which `setMined` treats as benign. The record stays missing. Subtree validation then sees a complete master and does not process the transaction again. A master record without the bin therefore does not prove that every extra record exists.

The validator retry does not act on the records. A child transaction that hits `ErrTxCreating` on a parent retries validation with exponential backoff (see `ValidateWithOptions` in `Validator.go`). This covers only the window while the clear phase is still in flight. The setting `validator_txlocked_maxRetries` controls the retry count. The default is 3 (10/20/40ms, about 70ms in total), and values above 10 are clamped. The `TX_LOCKED` retry uses the same budget. If the clear phase failed for good, the child uses all of its retries and the validator rejects it. The metric `teranode_validator_parent_commit_exhausted` counts these rejections, with the label `condition="TX_CREATING"`.

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

**Delete-at-height floor:** The Lua `setDeleteAtHeight` raises every stamp to the record's highest mined block height plus retention, so a lagging cached tip cannot stamp a mined record for deletion inside its retention window. With a `DAHSET` signal the response also carries a `deleteAtHeight` field holding the master's stamp, and the Go side stamps pagination (child) records with that value so they are never pruned before the master. The spend, setMined, SetConflicting, unspend and increment-spent-extra-records paths all propagate it, and a `DAHUNSET` clears the pages the same way. The `setConflicting` and `unspend` responses for a paginated master always carry `childCount`, the current `deleteAtHeight` (absent when unset) and `dahVer`. The Go side reconciles the pages on every `SetConflicting` and `Unspend` evaluation of the master: it writes them on every such response for a master with children, whatever the signal, and a response with no signal and no stamp clears them. A lost response or a crash between the master write and the page write on those two paths is therefore repaired by the next `SetConflicting` or `Unspend` of that master, for example the caller's retry. The spend and setMined paths write pages only on a `DAHSET` or `DAHUNSET` signal, so a lost response there is not repaired by a signal-less retry; that is the known limit of those two paths. The write is versioned, so a page that already holds the response's version or a newer one refuses it as `FILTERED_OUT`, which counts as success. The cost is one filtered round trip per page per evaluation. The `dahVer` master bin is bumped on every evaluation of the master's `deleteAtHeight`, not only when a stamp is written: every Lua path that sets, raises or clears the stamp bumps it, and every expression path bumps it whether or not the stamp changes. The writers are: the Lua `setDeleteAtHeight`, preserve and lock paths, the setMined, spend, longest-chain, preserve and expired-preservation expressions, their restores, the master write of the setDAH batcher and the pruner's clear of a protected unmined master all increment it. A lock also clears the pages of the locked master with the master's version, from the `setLocked` response. The initial create is the one exception: it leaves the bin absent, which counts as 0. A pagination record never stamps or versions itself, whatever conflicting flag it was created with. The version counts master writes and expression evaluations, so an expression setMined or longest-chain call that changes no stamp still bumps it, while a Lua call that leaves the stamp alone does not. With `aerospike_use_native_teranode_ops` the server dispatcher neither bumps nor reports `dahVer`, so page writes after a native-op call are not ordered and the page-first rule is not applied natively. A page write carries the version its response reported. It is applied only when the page has no version or an older one, and it stamps the version on the page, so a response from before a later transition cannot overwrite the pages that transition wrote. Every follow-up that writes pages (spend, setMined, SetConflicting, unspend, longest chain, preserve, expired preservation and the pruner's unmined clear) carries the version of the master write it follows. Only a write from a Lua package or server that reports no version stays unconditional. A `DAHSET` that raises a conflicting master, which is prunable whatever its spent state, also carries `conflicting=true`, so the increment handler stamps the pages without the all-spent verification and does not clear the master as counter drift. A package that omits the field makes the Go side fall back to the caller's height plus retention. With `aerospike_use_native_teranode_ops` enabled the native dispatcher in the aerospike-server fork does not apply the floor yet, so keep native ops off where a lagging tip matters.

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
