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
3. **setMined**: when the transaction is mined, `setMined` deletes the `creating` bin from the master record and from every extra record that exists. A pre-flight in `SetMinedMulti` clears the master before the mined write (`clearCreatingWithoutAttempt` in `set_mined.go`). It clears only a master that carries no `attempt` token and is not rolling back, in one write that the server filters on those bins. A master with a token belongs to a create-first attempt, and the pre-flight rolls it forward instead (see "Create-First Two-Phase Commit"). The Lua `setMined` function and `set_mined_expressions.go` do not touch the bin. `clearLockedOnRecordsMulti` in `set_mined.go` clears the extra records in the same write that clears `locked`.

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

## Create-First Two-Phase Commit (the `creating` flag)

`SpendAndCreate`'s default (combined) path normally spends the inputs then creates the
record — so a crash in that window leaves parent outputs marked spent by a record that
never landed (a dangling spender reference). When `utxostore_useCreateFirstOrder` is
enabled and the store reports `SupportsCreateFirst() == true` (Aerospike only), the
shared `SequentialSpendAndCreate` instead runs **create-first**:

1. **Create tentative** — the record is written with the `creating` bin set (via
   `WithCreating(true)`); every spend of its outputs is rejected with `ErrTxCreating`
   (Lua spend gate + expression filter) until the flag is cleared. Written only when
   true; absence == not creating. The same write puts a random 16-byte `attempt` token on the
   master and on every pagination child; see "Attempt token and hold" below.
2. **Spend inputs** — the transaction's parent outputs are spent.
3. **Finalize** — `FinalizeTransaction(tx, attempt)` clears the `creating` bin (children first,
   master last), making the outputs spendable. The clear is filtered on the token.

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
  A record that is rolling back, or a hold a finished rollback left, is refused with a
  transient error until the hold expires. A create-only call gets the same transient error, never
  `ErrTxExists`: a placeholder is not a stored transaction, and a caller that reads `ErrTxExists`
  as done would spend the parent outputs under it. A create in block context clears a hold at once.
  A rollback that its owner left open is first finished by the same call
  (`utxo.FinishRollBackCreating`), when no holder is busy and no spend lease is live, so a block
  that contains the transaction does not depend on the sweep. With the flag off, a spend-first
  `SpendAndCreate` that finds a hold or an open rollback reverses its own spends and returns a
  transient error, and it creates over a hold it may clear.
- **Duplicate create of a record with no token** — a create that finds the master of a creating
  record with no `attempt` token clears the record (`clearCreatingFlag`), whatever
  `utxostore_useCreateFirstOrder` says. No create-first attempt wrote such a record: it is a
  coinbase, a seeded transaction or a spend-first create whose clear failed, and no healer of
  create-first reaches it once it is mined. A mined create whose clear fails again returns the
  retryable `creatingNotClearedError`, not `ErrTxExists`.
- **Pruner sweeper** — `QueryStaleCreatingTxs` (secondary index on `unminedSince` +
  `creating` filter) finds abandoned creating records older than
  `pruner_creatingTxSweepMinAgeBlocks`; the sweeper re-spends (`SpendAndCreate(WithSpendOnly)`,
  without ignoring locks) and finalizes them, rolls back a loser whose input belongs to
  another transaction, finishes a rollback a crashed owner left marked, and deletes a hold that
  a namespace without record expiry kept past its time. Every step names the token it read from
  the record. The sweep reverses nothing while a spend lease of the attempt is live (see "Spend
  lease" below): it leaves the record for a later pass. A creating record with no `attempt` token
  is only cleared (`FinalizeTransaction` with no token): the sweep spends nothing for it, never
  rolls it back and does not announce it.
- **setMined** — a pre-flight in `SetMinedMulti` rolls a still-creating record forward
  (re-spend, then finalize) before the mined write. It does this only for a record that carries
  an `attempt` token. A creating record with no token was not written by a create-first attempt:
  its `clearCreatingFlag` failed, and the pre-flight deletes its `creating` bin and spends
  nothing. The write paths no longer clear the `creating` bin. The pre-flight runs on every call, whatever `utxostore_useCreateFirstOrder`
  says, so turning the flag off cannot strand creating records. Correctness rests on the
  pre-flight, which fails closed, because the native sub-op write path's handling of the bin
  is not verifiable from this repository.

The healers (setMined pre-flight and pruner sweep) are not gated on the flag.

### Spend lease

A hold carries no transaction, so nothing can reverse a spend that lands after the rollback has
reversed the inputs. The store therefore makes a spend and a rollback of one attempt exclude each
other, with a lease on the master record.

- **Who holds one.** Every actor that spends the inputs of a creating record. The create writes
  the creator's lease in the `spenders` bin, a map from a lease id to the unix time by which the
  spend must end, now plus `utxostore_spendRollbackTimeout`. The creator's id is its attempt token.
  A roll-forward (the `ErrTxExists` retry, the sweep, the setMined pre-flight) calls
  `TakeSpendLease(hash, attempt)` before it spends: one write, filtered on `creating`, the token
  and no `rollingBack` bin, that adds a random id. The spend runs under a context that ends at the
  time the lease names.
- **The fence.** `BeginCreateFirstRollback` sets `rollingBack` in one write that is filtered on
  the record carrying no live lease, and `TakeSpendLease` is refused once `rollingBack` is set.
  Both are single-record writes with the same base filter, so the server orders them. While a
  spender holds a lease no rollback begins, and once a rollback has begun no spender starts. Every
  spend of the attempt therefore lands before the reversal, and the reversal, which covers every
  input of the transaction, undoes it. A lease holder that is clearing the `creating` bins of the
  pagination children is also never overtaken by a rollback that deletes them, so a descendant
  cannot spend an output of a page that a rollback then removes.
- **Release.** A spender calls `ReleaseSpendLease` when its spend has returned with a known
  outcome and it will not finalize the record: after a finalize that failed, or one that found
  the record final under another actor's write. A roll-forward never reverses a spend itself. A
  finalize removes the lease of its caller only, and the bin goes with the last lease. A lease
  of another spender therefore stays live on a record that is final, and `DeleteComplete`
  refuses to delete the master while it is: the shed unwind of the validator deletes a record and
  then unspends its inputs, and a roll-forward that still held a lease could spend them after
  that. No lease is granted on a record that is final, so no spender starts after the delete.
- **A queued spend ends with its lease.** The context of a spend under a lease ends at the time
  the lease names. The caller stops waiting then, but its batch is queued on the context of the
  store. The store keeps the deadline for it (`splitSpendsByDeadline`), but only for a context
  that a lease bounds (`utxo.WithLeaseBound`, set by the create-first spend and the
  roll-forward): the deadline of any other caller says only when it stops waiting. An item whose
  deadline has passed is completed with an error and is not sent. The items with a deadline are
  sent apart from the others, in groups of one second, each under the earliest deadline of its
  group, so no item is limited by a deadline more than a second shorter than its own and one
  caller that is nearly out of time cannot fail the spends of unrelated ones. Each send, a repeat after an
  overload included, carries only the time that is left as its timeout
  (`uaerospike.Client.BatchOperateBefore`). The client stops at that timeout, and the server
  does not start a command whose timeout has passed. No write of a spend therefore starts after
  the time its lease names, and the lease is live for a further
  `utxostore_spendRollbackTimeout` after it. `aerospike_batchPolicy` can no longer make a spend
  under a lease outlast it. The queued finalize follows the same rules (`sendFinalizeBatch`): a
  finalize whose caller has given up is completed with that error and not sent, and the writes
  are grouped by deadline. Its write would otherwise land late, clear the creating bin and stamp
  the record with the lease of a caller that already returned an error, and then no caller owns
  the hand-off. The roll-forward finalizes under a context of its own, bounded by the rollback
  timeout, as the creator does, so a caller that gives up after the spends does not leave a
  finalize with nobody to read its answer.
- **Unknown outcome and crash.** A spend that gave no per-input result, whose context ended, that
  hit `utxostore_spendWaitTimeout`, or that carries a per-input error which is not a verdict of the
  store on that input (a storage fault, a timeout) may still commit. Its lease is not released. A
  lease is live until the time it names has passed by a further `utxostore_spendRollbackTimeout`.
  `RollBackCreating` tries its claim for `utxostore_spendWaitTimeout`, the longest one healthy
  spend may run. If a lease is still live then, the claim stays refused, nothing is written or
  reversed and the record stays creating. A roll-forward completes it, or the sweep rolls it back
  on a pass after the lease is stale. `TakeSpendLease` drops stale leases from the bin.
- **Clocks.** A lease time, like `holdUntil`, is the unix clock of the host that writes it, and
  the host that reads it compares it with its own clock. The margin is one
  `utxostore_spendRollbackTimeout`: the hosts that share a store must agree on the time to within
  that setting (120 s by default). The order of a lease and a rollback claim does not depend on
  clocks.

### One finalize owns the hand-off

`FinalizeTransaction` returns nil only to the call whose write cleared the `creating` bin. A call
that finds the record already final returns an `ErrTxExists` for which `utxo.IsAlreadyFinal` is
true. The creator and a roll-forward of the same attempt can both reach the finalize. The one
that cleared the state hands the transaction to block assembly, and only it may unwind a shed
hand-off. The other reports an ordinary duplicate and takes neither step, so a creator cannot
delete a record that another instance has already delivered.

The store does not infer which call that was. The finalize write records its caller: every
caller passes the spend lease it holds on the attempt, the attempt token for the call that
created the record and the lease of `TakeSpendLease` for a roll-forward, and the write puts it in
the `attempt` bin of the master, which no write filters on once the record is final. A call that
finds the record final reads that bin. Its own lease there means that an earlier send of this
same finalize cleared the state and its answer was lost, and the call returns nil. Any other
value means that another actor did. A finalize can therefore be repeated after any error: a
client-internal resend, a timeout and a try that failed all end in the same exact answer, and
`RollForwardCreating` repeats its finalize as the creator does. Two callers never both own the
hand-off, and a hand-off is not lost with an answer. What stays open: three tries that all fail
with the store unreachable leave the caller with an error and the record possibly final, and
the transaction then waits for the unmined reload.

### Rollback of a failed create-first spend

A spend that fails after the tentative create is rolled back by `utxo.RollBackCreating`. The
creator first releases its own spend lease.
`BeginCreateFirstRollback(hash, attempt)` sets the `rollingBack` bin on the master with a filter
on the `creating` bin and the token. The same write sets `busy` and `holdUntil`, so the owner
holds the hold for its whole unspend and releases it with `ReleaseRollbackHold` before it ends
the rollback. The claim is refused while a spend lease of the attempt is live, and the owner
tries again for `utxostore_spendWaitTimeout`. Then every input of the tx is
unspent (not only the spends that
succeeded in the failing pass, because an earlier attempt may have spent an input that now fails
for another reason; the Lua ownership check makes an input the tx does not own a no-op, and an
input whose UTXO hash does not match or whose vout does not exist is skipped, because the tx can
never have spent it), then `EndCreateFirstRollback(hash, attempt)` removes the pagination
children (filtered on the token) and the external blobs and turns the master into a hold. Its
first write sets the `reversed` bin, which says that every spend is undone, so that a recovery
that finds the record after the blob is gone ends the rollback without reading the transaction.
That write is filtered on no live `busy` bin and sets `busy` itself, so one caller owns the
cleanup from the `reversed` write to the hold: a second caller, and an actor that still reverses
spends under the hold, make it fail, and the reversal is not recorded while anyone is still
reversing. A cleanup that fails releases `busy`.
While
the marker is set, `FinalizeTransaction` (single and multi-record) refuses the record with
`ErrTxNotFound`, so a concurrent roll-forward cannot complete it. A roll-forward holds its lease
until its finalize returns, and the claim of a rollback is refused until that lease is released,
so no finalize is in flight when the marker is set. The Lua spend
gate still reads `creating`, so a rolling-back record's outputs stay unspendable. A spend whose
batch may still be in flight (a context error, or the `utxostore_spendWaitTimeout`
ServiceUnavailable) is not rolled back: the record stays creating for the sweep. The fresh
create-first spend sets `RequireParentOutputs`, because the record the blessed-probe would find
is its own.

### Attempt token and hold

A parent's spend record is the spending txid and the input index, so a reversal of spends cannot
tell two attempts at the same txid apart. Two mechanisms keep an actor left over from one attempt
from touching the next.

- **`attempt` token.** `Create(..., WithCreating(true))` draws 16 random bytes and writes them
  as the `attempt` bin next to `creating`, on the master and on every pagination child. It
  returns them in `meta.Data.Attempt`. `FinalizeTransaction`, `BeginCreateFirstRollback`, the
  rollback end and the child delete are all filtered on the token, and a mismatch is a
  `FILTERED_OUT` that reads back as "not mine" (`ErrTxNotFound` for a finalize). Every actor takes
  its token from the record it read: the fresh create from its own create, the `ErrTxExists`
  roll-forward, the sweep and the setMined pre-flight from their `Get`. A multi-record create
  that finds children left by an earlier attempt gives them its own token. A create that finds
  the master of an earlier attempt still creating does the reverse: it gives every child,
  including one it has just written, the token of that master, because only the master's attempt
  can be finalized. If a child cannot be
  given the token, the create leaves the master it wrote creating and returns a transient error
  that carries the token (`utxo.MarkRollBackAttempt`). Another actor may already have read that
  master and begun a roll-forward, so it is not deleted: the create-first caller rolls the
  attempt back with `RollBackCreating`, which reverses every input of the transaction and leaves a
  hold. A
  finalize that finds a child filtered out reads it back: only a child that is no longer
  creating counts as cleared, and the master stays creating otherwise.
- **Hold.** The rollback end does not delete the master. It deletes the children and the
  external blobs while the master is still there, so a same-txid create is refused and writes
  nothing, and then rewrites the master in one `REPLACE_ONLY` write, filtered on the token, the
  `rollingBack` and `reversed` bins and no `hold` bin. The hold keeps `txID`, `creating`, `rollingBack`,
  the token and `unminedSince` (so the sweep query finds it), and adds `hold` and `holdUntil`,
  now plus `utxostore_spendRollbackTimeout`. The write carries that time as the record's time to
  live, so the server removes the hold on its own and a rejected double-spend leaves no durable
  record. A namespace that forbids a time to live gets the hold without one, and the sweep
  deletes it. It has no outputs, so every gate that keys on
  `creating` refuses it, `GetMeta` reports `Creating`, `RollingBack` and `Hold`, and a create of
  the txid gets `ErrTxExists` and then a transient error.
- **Take the hold before unspending.** An actor that unspends the inputs of a transaction after
  the mark calls `TakeRollbackHold(hash, attempt)`: one write filtered on `rollingBack`, the
  token and no live `busy` bin, which sets `busy` and renews `holdUntil`. It unspends under a
  context bounded by `utxostore_spendRollbackTimeout`, then calls `ReleaseRollbackHold`. A failed
  take means a replacement owns the inputs, another actor holds the hold, or the cleanup is
  running, and nothing is reversed. Two states can be taken. An open rollback that does not yet
  carry `reversed` is taken by `utxo.FinishRollBackCreating` (used by the sweep and by a
  create-first `SpendAndCreate`) and by the direct owner, which takes it when it marks the record.
  A hold is taken by a spend-first caller that must reverse spends it made itself beside the
  hold; the take renews the hold's time to live, and a create in block context does not adopt a
  busy hold. A record that carries `reversed` and is not yet a hold cannot be taken: its cleanup
  is running and its transaction data may be gone. The sweep calls the rollback end for it
  directly.
- **Spend-first beside a rollback (flag off).** A spend-first `SpendAndCreate` takes no lease, so
  it treats the record of a rollback at its txid, a hold included, as follows. It never creates
  over spends it made before a hold was gone: after it has cleared a hold it spends again and
  then creates. It reverses its own spends only under the hold of the attempt, and then returns a
  transient error. While the hold is taken, by the owner of the rollback, its cleanup or another
  holder, the call waits at the record and spends nothing more, for up to twice
  `utxostore_spendRollbackTimeout`: each of those ends within one timeout, and a holder that
  crashed counts as gone after two. A spend that fails after the call went around once reverses
  the spends of the earlier round as well, under the hold when a rollback record is back at the
  txid, and when no record is at the txid the call claims it with the tentative create of the
  transaction, which the store writes with CREATE_ONLY and so refuses when another attempt wrote
  the record first, and rolls that attempt back. This is the one fence the store can order for a
  txid with no record: a read of the record followed by an unspend would clear the spends of a
  create that landed between the two. The cleanup runs on a context of its own, because the spend
  that failed often failed with the context of the caller. The store's own partial-spend rollback (`partialSpendRollbackDecision`) follows the same
  rule and waits in the same way, so a spend that takes one input and fails on another beside a
  busy rollback is reverted and not left beside a hold that expires.
- **What a reader sees.** A hold has no `outputs` bin. `ParentOutputsForValidation` reads the
  `hold` bin and answers a missing parent for it, so a child of a rolled-back txid is deferred or
  parked as for any missing parent, and is not classified as a fault of this node's storage.
- **A parent that is still committing.** When every input that failed is behind a parent that is
  locked or still creating, the creator releases its lease and leaves the record creating, with
  the spends that landed. It does not roll back: a rollback would leave a hold for
  `utxostore_spendRollbackTimeout`, which a retry of the same transaction meets before the parent
  has committed. A repeat finds the record, spends again and finalizes it, and the sweep does the
  same or rolls it back once an input can no longer be spent. One input with any other error
  decides for the rollback. A parent that stays locked or creating is not committing: the sweep
  rolls such a record back once it is older than ten times `pruner_creatingTxSweepMinAgeBlocks`
  (`lockedParentRollbackAgeMultiple`), because until then it keeps the inputs that did spend,
  and its own outputs, unspendable for as long as the lock lasts.
- **Expiry.** A hold expires with its time to live. `ClearCreateFirstHold(hash, adopt)` deletes
  one earlier, or one that has no time to live, filtered on `hold` and on the `holdUntil` that
  was read. Without `adopt` it goes
  once `holdUntil` has passed and no holder is busy, or has passed by a further timeout (a
  holder that crashed). The next create that finds the hold and the sweep both call it.
- **Block context.** A create that ignores locks, or carries mined block info, passes `adopt`:
  the hold goes at once unless a holder is busy, so a block that contains a transaction that lost
  a double-spend locally does not wait out the hold. The create then runs again and writes a
  tentative record under a fresh token, and every actor left over from the earlier attempt fails
  its take.

`RollForwardCreating` and the fresh create-first spend run under a context bounded by
`utxostore_spendRollbackTimeout`, the time their lease names, and the store sends no spend of
theirs after it (see "A queued spend ends with its lease"). What this does not close: an owner
of a rollback that stalls past its own bound and then unspends after a new attempt of the same
txid has spent. Closing that needs the attempt in the spend record (issue 1291).

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

The Lua package name carries a version (`teranode_v72`). A UDF registered under the same
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
