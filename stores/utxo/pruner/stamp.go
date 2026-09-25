package pruner

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
)

// ErrStampDrainBusy is returned by OpenDrain while another drain holds the session lock. The
// pruner service tests for it with errors.Is, so it lives here, beside the interface, rather
// than in any one store.
var ErrStampDrainBusy = errors.NewProcessingError("[stamp] another drain holds the session lock")

// StampFloors are the three stored values of the single tx_mined_floor row, all as heights.
//
// DroppedFloor is the first height of the lowest containment window that may still exist:
// everything below it has been dropped. StampFence is the exclusive upper bound of the highest
// window whose stamp has started. StampCompleteFloor is the exclusive upper bound of the highest
// window whose stamp has completed, which is the first height of the next window to stamp. It is
// read from the column and never derived from the completion records.
type StampFloors struct {
	DroppedFloor, StampFence, StampCompleteFloor uint32
}

// StampWindowState is what BeginWindow found when it looked at a window.
type StampWindowState int

const (
	// StampWindowNotDeep: the window's last block is not yet the stamp depth below the anchor.
	// Nothing was written and the drain has nothing more to do.
	StampWindowNotDeep StampWindowState = iota
	// StampWindowReady: step 1, the fence raise and the loser delete, ran. Pages follow.
	StampWindowReady
	// StampWindowResumed: step 1 had already committed for this window in an earlier drain that
	// did not complete it, so it was skipped. Pages follow, and every row an earlier pass
	// handled is already gone.
	StampWindowResumed
	// StampWindowSkipped: the window has no table at all. It is counted as empty, both stamp
	// floors have been advanced past it, and it needs no pages and no completion record.
	StampWindowSkipped
)

// Stamper is implemented by a store whose UTXOs get their block from a deep pass. The pruner
// service finds it by type assertion, exactly as it finds the pruner provider. The SQL and
// aerospike stores do not implement it and get no stamp.
type Stamper interface {
	// StampDepth is the depth, in blocks, below the tip at which a window may be stamped. The
	// pruner service uses it to count the windows still stampable when a drain exits.
	StampDepth() uint32

	// WindowBlocks is the width of one containment window in blocks. Every floor is a multiple
	// of it, and the pruner service steps from one window to the next by it.
	WindowBlocks() uint32

	// Floors is one read of the tx_mined_floor row.
	Floors(ctx context.Context) (StampFloors, error)

	// OpenDrain pins one connection, takes the stamp worker's session lock on it, runs the
	// completion-record check, and returns the floors it found. A second drain while one is
	// open is refused with ErrStampDrainBusy and counted.
	OpenDrain(ctx context.Context) (StampDrain, StampFloors, error)
}

// StampDrain is one drain. Each call runs one step of the pass and returns, so the caller can
// re-read the chain between steps without the store ever making a chain call.
type StampDrain interface {
	// BeginWindow checks the preconditions for the window starting at wLo, then runs the fence
	// raise and the loser delete, or skips them on a resume, or advances the floors past a
	// window that has no table. anc must cover the whole window and be anchored on the tip.
	BeginWindow(ctx context.Context, wLo uint32, anc *chainancestry.Ancestry) (StampWindowState, error)

	// StampPage runs one page of the window as one database transaction: it writes the winning
	// block onto every live UTXO still at (0,0) of the transactions the page holds and deletes
	// their identity rows, then judges the page's suspects. page runs from 0 to PagesPerWindow-1.
	StampPage(ctx context.Context, wLo uint32, anc *chainancestry.Ancestry, page int) (StampPageResult, error)

	// CompleteWindow runs the sampled audit and then the completion transaction, which writes
	// the completion record and advances the completion floor together. liveTip is the best
	// height from an uncached read made just before this call; it is never a cached tip and
	// never the pruner's notification height.
	CompleteWindow(ctx context.Context, wLo uint32, anc *chainancestry.Ancestry, liveTip uint32) error

	// PagesPerWindow is how many StampPage calls one window takes.
	PagesPerWindow() int

	// Close releases the session lock and the connection.
	Close() error
}

// StampPageResult is what one page did.
type StampPageResult struct {
	// Rows is the number of winning containment rows the page scanned.
	Rows int64
	// Transactions is the number of identity rows the page deleted, one per transaction stamped.
	Transactions int64
	// UTXOs is the number of live UTXOs the page wrote a pair onto.
	UTXOs int64
	// MarkerSetWinners is how many of the stamped transactions still carried the unmined marker.
	MarkerSetWinners int64
	// SuspectsMarked is how many identity rows with no winner anywhere the page marked unmined.
	SuspectsMarked int64
}
