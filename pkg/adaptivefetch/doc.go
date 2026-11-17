// Package adaptivefetch provides a small in-process state machine that
// decides whether block validation or subtree validation should fetch the
// subtreeData file from peers or skip it and recover any missing
// transactions individually.
//
// The state machine operates in two modes: pessimistic (always fetch
// subtreeData) and optimistic (skip subtreeData; recover missing txs
// individually). Mode transitions are driven entirely by counts of
// transactions hit in the local UTXO store vs transactions that had to
// be recovered from peers. No FSM state and no wall-clock time is
// consulted by this package.
//
// Cold-start safety via Arm: a freshly constructed State is always pinned
// pessimistic and "unarmed" regardless of the configured BootstrapMode, so it
// can never skip subtreeData during initial sync. The integrating service is
// responsible for calling Arm exactly once, the first time the node is proven
// fully synced. In Teranode that is the first time the blockchain FSM reaches
// RUNNING — but this package deliberately knows nothing about the FSM: Arm is a
// generic "you may now apply the configured bootstrap behaviour" trigger, which
// keeps the no-FSM / no-wall-clock invariant (see TestNoWallClockOrFSMDependency)
// confined to the algorithm while the sync-state knowledge lives in the service
// layer that already owns the blockchain client. Once armed the latch never
// re-locks, so a catch-up burst after the node has been RUNNING may still use
// optimistic mode.
package adaptivefetch
