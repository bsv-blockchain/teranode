// Package rewindblockchain implements the blockchain/UTXO rewind operation
// invoked by cmd/rewindblockchain/main.go. It walks the blockchain DB down
// to a target height, unspending and deleting transactions from the UTXO
// store, removing orphaned subtree blobs, and resetting persisted state
// keys so the node can restart cleanly.
package rewindblockchain

import "io"

// Options configures a Rewind run.
type Options struct {
	// TargetHeight, when >= 0, forces the target block height. When < 0,
	// the tool resolves the target from state["BlockAssembler"] in the
	// blockchain DB.
	TargetHeight int64

	// DryRun logs planned actions without mutating any store.
	DryRun bool

	// AssumeYes skips the interactive confirmation prompt.
	AssumeYes bool

	// ForceNotIdle allows the tool to proceed even when the FSM state is
	// not IDLE. Dangerous; exposed only for recovery from partial-run
	// crashes where the FSM state is already wrong.
	ForceNotIdle bool

	// ForceDeep allows rewinds deeper than the coinbase maturity window
	// (100 blocks). Dangerous because removeCoinbaseUtxos may find children
	// mined on surviving blocks.
	ForceDeep bool

	// Verify enables Phase 4 post-rewind consistency checks.
	Verify bool

	// Concurrency sets the subtree-load concurrency. 0 means "read from
	// settings.BlockAssembly.MoveBackBlockConcurrency, default to 4".
	Concurrency int

	// Stdin and Stdout are used for the confirmation prompt. In tests
	// callers can supply buffers; defaults come from main.go.
	Stdin  io.Reader
	Stdout io.Writer

	// Stores, when non-nil, bypasses Rewind's own store construction and
	// uses the supplied stores directly. Used by integration tests so the
	// test fixture and the rewind pass share the same in-memory backends.
	Stores *Stores
}

// coinbaseMaturity is the number of blocks after which a coinbase output
// becomes spendable.
const coinbaseMaturity uint32 = 100
