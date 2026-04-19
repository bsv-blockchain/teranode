# Recovering from FSM IDLE (Block Assembly frozen)

When the Block Assembler detects inconsistent UTXO-store state on startup —
for example an unmined transaction whose parent is flagged `Conflicting=true`
with `UnminedSince>0`, or an unmined transaction whose parent is mined on a
block that is not on the best chain (orphan fork) — it parks the blockchain
FSM in `IDLE` and prints:

```text
block assembly paused in IDLE. Run 'teranode-cli cleanup-unmined' to fix;
block assembly will resume automatically once the FSM leaves IDLE.
```

Services that reach IDLE (Block Assembly, Subtree Validation, Block Validation,
Propagation, Legacy netsync) stop processing until the FSM leaves IDLE.

## Fix

```bash
teranode-cli cleanup-unmined [--skip-unmined-since-scan] [--dry-run]
```

The command connects to the live node's blockchain gRPC, so leave the node
running. It performs the following passes:

1. (optional, `--skip-unmined-since-scan` to skip) Clears stray `UnminedSince`
   markers on transactions already mined on the best chain.
2. Deletes every record with `Conflicting=true` and `UnminedSince>0`.
3. Iterates non-conflicting unmined transactions and deletes any whose parent
   is mined on a block that is not on the best chain (orphan fork) or whose
   parent is a mined record with no block_ids at all.

`--dry-run` reports the records that would be deleted without writing anything.

## Resume

```bash
teranode-cli setfsmstate --fsmstate RUNNING
```

Block Assembly's FSM watcher detects the transition out of IDLE, retries
`loadUnminedTransactions`, and unfreezes. A node restart is not required.

## Notes

- Missing parents (e.g. parents deleted by step 2) are tolerated by Block
  Assembly's `validateParentChain`; cleanup-unmined does not cascade deletes
  to their non-conflicting children.
- Running the command a second time is a no-op; it only finds records that
  still match the deletion filters.
- Unmined subtree blobs are left to the pruner / TTL — they are content-
  addressed, unique by hash, and a stale blob costs only disk.
- **Deleted tx in a peer's blessed subtree is safe.** If a later block
  arrives referencing a subtree that contains a tx we deleted, block
  validation does not hard-fail. `SubtreeValidation.processMissingTransactions`
  (services/subtreevalidation/SubtreeValidation.go:1001) refetches the tx
  bytes from the peer via `getSubtreeMissingTxs` and reconstructs the UTXO
  metadata as part of normal validation. `BatchDecorate` TX_NOT_FOUND is
  treated as a miss counter, not a fatal error
  (services/subtreevalidation/processTxMetaUsingStore.go:140-148).
- `errors.ErrRepairNeeded` / `NewRepairNeededError` keep their names — the
  operator-intervention semantic is unchanged, only the fix command name
  changed.
