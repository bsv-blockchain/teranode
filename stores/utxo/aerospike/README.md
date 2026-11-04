
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

## Create-First Two-Phase Commit (the `creating` flag)

`SpendAndCreate`'s default (combined) path normally spends the inputs then creates the
record — so a crash in that window leaves parent outputs marked spent by a record that
never landed (a dangling spender reference). When `utxostore_useCreateFirstOrder` is
enabled and the store reports `SupportsCreateFirst() == true` (Aerospike only), the
shared `SequentialSpendAndCreate` instead runs **create-first**:

1. **Create tentative** — the record is written with the `creating` bin set (via
   `WithCreating(true)`); every spend of its outputs is rejected with `ErrTxCreating`
   (Lua spend gate + expression filter) until the flag is cleared. Written only when
   true; absence == not creating.
2. **Spend inputs** — the transaction's parent outputs are spent.
3. **Finalize** — `FinalizeTransaction(tx)` clears the `creating` bin (children first,
   master last), making the outputs spendable.

Because the record exists before its spends are written, a crash between steps 2 and 3
leaves an inspectable, recoverable record instead of a dangling reference. There is **no
native Aerospike TTL** on creating records — the record is the recovery WAL and must
survive until finalize. `WithCreateOnly` / `WithSpendOnly` callers (coinbase, seeding,
reorg/conflict helpers) are single-phase and bypass the create-first ordering.

### Recovery paths (roll-forward everywhere)

- **Retry** — a re-encounter hits `ErrTxExists` on a still-`creating` record; the
  create-first `SpendAndCreate` rolls it forward in the same call (re-spend under the
  caller's ignore flags, then finalize) and returns an `ErrTxExists` for which
  `utxo.IsRolledForward` is true, so the validator hands the transaction to block assembly.
  A record that is rolling back is refused with a transient error until its owner, or the
  sweep, has deleted it.
- **Pruner sweeper** — `QueryStaleCreatingTxs` (secondary index on `unminedSince` +
  `creating` filter) finds abandoned creating records older than
  `pruner_creatingTxSweepMinAgeBlocks`; the sweeper re-spends (`SpendAndCreate(WithSpendOnly)`,
  without ignoring locks) and finalizes them, rolls back and deletes a loser whose input
  belongs to another transaction, and finishes a rollback a crashed owner left marked.
- **setMined** — a pre-flight in `SetMinedMulti` rolls a still-creating record forward
  (re-spend, then finalize) before the mined write. The write paths no longer clear the
  `creating` bin. The pre-flight runs on every call, whatever `utxostore_useCreateFirstOrder`
  says, so turning the flag off cannot strand creating records. Correctness rests on the
  pre-flight, which fails closed, because the native sub-op write path's handling of the bin
  is not verifiable from this repository.

The healers (setMined pre-flight and pruner sweep) are not gated on the flag.

### Rollback of a failed create-first spend

A spend that fails after the tentative create is rolled back by `utxo.RollBackCreating`:
`BeginCreateFirstRollback` sets the `rollingBack` bin on the master with a filter on the
`creating` bin, the spends that succeeded are unspent, then `DeleteComplete` removes the
record. While the marker is set, `FinalizeTransaction` (single and multi-record) refuses
the record with `ErrTxNotFound`, so a concurrent roll-forward cannot complete it; the
roll-forward then unspends the spends it made. The Lua spend gate still reads `creating`,
so a rolling-back record's outputs stay unspendable. A spend whose batch may still be in
flight (a context error, or the `utxostore_spendWaitTimeout` ServiceUnavailable) is not
rolled back: the record stays creating for the sweep. The fresh create-first spend sets
`RequireParentOutputs`, because the record the blessed-probe would find is its own.

### Sweep visibility of mined creating records

The sweep selects by the `unminedSince` index. A creating record created with mined block
info (the block-validation path) also gets `unminedSince = blockHeight`, so the sweep can
find it. `FinalizeTransaction` removes `unminedSince` from the master record when the record
has block IDs, on both the single-record and multi-record paths; a record without block IDs
keeps it. While such a record is creating, the unmined iterator skips it, and
`setDeleteAtHeight` sets no `deleteAtHeight` because the record does not count as on the
longest chain; both are correct for a record that must not be pruned or re-added yet.

### Duplicate create of a multi-record transaction

When every record of a multi-record transaction already exists, the duplicate `Create`
returns `ErrTxExists`. With `utxostore_useCreateFirstOrder` on, it leaves the `creating` flag
untouched, because the duplicate's own options say nothing about another attempt's tentative
record: only `FinalizeTransaction` and the roll-forward healers clear it. With the flag off,
the legacy cleanup still clears it.

### Lua package version and re-registration

The Lua package name carries a version (`teranode_v63`). A UDF registered under the same
filename is no longer trusted: at startup the registered body is read back (`udf-get`) and
compared byte for byte with the embedded script. On a mismatch the store logs a warning with
both SHA-1 digests and the listed hash, and registers the embedded script again
(`RegisterUDF` overwrites). The `udf-list` `hash` field is a 40-character lowercase hex
string, but it is not a plain SHA-1 of the file bytes, so it is only logged, never compared.

### Spend of a creating record whose parent is missing

If a parent is missing (`ErrTxNotFound`) and the spender record exists, creating or not, the
input counts as already blessed. A parent is pruned only once every output is spent, so the
input has been taken, by this tx or by a rival. A rival's take is the same exposure
spend-first has, and the conflict path bounds it. This lets a roll-forward of a creating
record whose parent was pruned converge.

Creating records are excluded from the block-assembly unmined-restore iterator (default
mode) and rejected as parents during parent-chain validation; the PRUNER-mode iterator
still sees them so permanently-failed ones are eventually evicted. On a finalize failure
after successful spends the seam logs-and-continues (`teranode_utxo_create_first_finalize_failed_total`).
