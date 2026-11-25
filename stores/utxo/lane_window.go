package utxo

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// LaneWindow is the stretch of records block assembly's restart dry run reads at
// utxostore_laneMigration shadow: every record written since the window's start (the lanes design,
// "Job two: block assembly's restart"). Each store reads it by what it can bound a read with.
type LaneWindow struct {
	// From is the window's start: the hand-out time of the first grant at or above the cap height.
	// Margin is how far before From the read begins, for the clocks of the store and the blockchain
	// database being out of step. Aerospike reads every record last updated at or after
	// From - Margin.
	From   time.Time
	Margin time.Duration

	// FromHeight is the window's start as a height: the height that first grant was handed out at.
	// MarginBlocks is how far below it the read begins. utxoset reads every tx_body row created at
	// or above FromHeight - MarginBlocks, and every row of tx_body_carry.
	FromHeight   uint32
	MarginBlocks uint32
}

// LaneWindowRecord is one record a window read returned, with what the dry run judges it by.
type LaneWindowRecord struct {
	Hash chainhash.Hash
	// LanePosition is the record's own lane position, 0 for none.
	LanePosition uint64
	// CreatedAt is the record's creation time in Unix milliseconds, 0 if the store has none.
	CreatedAt int64
	// Conflicting is a record marked as having lost a double-spend.
	Conflicting bool
	// Missing is a row of the window whose transaction has no record the reload would read:
	// utxoset's tx_body row with no tx_ident row, which the deep stamp deletes once a block is
	// settled, or whose transaction was first seen in a block.
	Missing bool
	// NotMaster is a record that is not a transaction's own: an Aerospike pagination record.
	NotMaster bool
	// InMargin is a record read only because of the margin: utxoset sets it, Aerospike cannot tell.
	InMargin bool
}

// LaneWindowStats is what a window read cost.
type LaneWindowStats struct {
	// Records is every record or row the read returned, Missing and NotMaster ones included.
	Records uint64
	// Bytes is the bytes of the values the read returned, as the store decoded them.
	Bytes uint64
	// MarginRecords is the records read only because of the margin, when the store can tell.
	MarginRecords uint64
	// MarginKnown reports whether the store can tell which records the margin added.
	MarginKnown bool
	// External is Aerospike records whose inputs are in the external store, which the read does
	// not fetch; a reload would.
	External uint64
	// Duration is how long the read took.
	Duration time.Duration
}

// LaneLastUpdateReader is a store that can say when a record was last written, for the dry run's
// log lines about the records it disagrees on.
type LaneLastUpdateReader interface {
	LaneLastUpdate(ctx context.Context, hash chainhash.Hash) (time.Time, error)
}

// LaneWindowReader is a store that can read block assembly's restart window without a whole-store
// scan or a new index. fn is called with each batch from one goroutine at a time; it must not keep
// the slice. An error from fn stops the read and is returned.
type LaneWindowReader interface {
	ReadLaneWindow(ctx context.Context, w LaneWindow, fn func([]LaneWindowRecord) error) (LaneWindowStats, error)
}
