# Recovering from FSM IDLE (Block Assembly frozen)

When the Block Assembler detects inconsistent UTXO-store state on startup — for
example an unmined transaction whose parent is flagged `Conflicting=true` with
`UnminedSince>0` — it parks the blockchain FSM in `IDLE` and prints:

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
running. It performs a single pass that:

1. (optional, `--skip-unmined-since-scan` to skip) Clears stray `UnminedSince`
   markers on transactions already mined on the best chain.
2. Collects every record with `Conflicting=true` and `UnminedSince>0`.
3. Deletes the collected records.

`--dry-run` reports the set that would be deleted without writing anything.

## Resume

```bash
teranode-cli setfsmstate --fsmstate RUNNING
```

Block Assembly's FSM watcher detects the transition out of IDLE, retries
`loadUnminedTransactions`, and unfreezes. A node restart is not required.

## Notes

- Non-conflicting children of a purged parent are left in place. Block Assembly's
  `validateParentChain` tolerates the dangling reference — those children will
  be mined in the next block or swept by the pruner.
- Running the command a second time is a no-op; it only finds records that
  still match the purge filter.
- `errors.ErrRepairNeeded` / `NewRepairNeededError` keep their names — the
  operator intervention semantic is unchanged, only the fix command is renamed.
