// Package replayrecovery audits and repairs recreated confirmed transactions.
// Evidence, storage mutations, and maintenance checks have separate boundaries
// so neither missing metadata nor an unverified write can authorize deletion.
package replayrecovery

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
)

const (
	FullySpent  = "fully-spent"
	Live        = "live"
	Unconfirmed = "unconfirmed"
	Confirmed   = "confirmed"
	Unknown     = "unknown"
)

type Tip struct {
	Hash   string `json:"hash"`
	Height uint32 `json:"height"`
}

type Evidence struct {
	TxID           string `json:"txid"`
	RawTx          string `json:"raw_tx"`
	BlockHash      string `json:"block_hash"`
	BlockHeight    uint32 `json:"block_height"`
	Tip            Tip    `json:"tip"`
	Source         string `json:"source"`
	Classification string `json:"classification"`
	Reason         string `json:"reason"`
}

type Source interface {
	Tip(context.Context) (Tip, error)
	Check(context.Context, string, Tip) (Evidence, error)
}

type Record struct {
	Key        []byte `json:"key"`
	Data       []byte `json:"data"`
	Generation uint32 `json:"generation"`
	ExpiresAt  int64  `json:"expires_at"`
}

type Parent struct {
	Record Record `json:"record"`
	Child  string `json:"child"`
}

type Snapshot struct {
	TxID string `json:"txid"`
	// Records are ordered master first, then pages. Apply deletes in reverse.
	Records      []Record `json:"records"`
	Parents      []Parent `json:"parents"`
	Dependencies []string `json:"dependencies"`
}

// Backend must fail closed on schema errors, generation conflicts, and ambiguous
// writes. Read returns nil only for authoritative record absence. Mark verifies
// readback before returning and preserves every field other than the marker.
type Backend interface {
	Identity() string
	Inputs(context.Context, string) ([]string, error)
	MatchesMarked(Record, Record, string) bool
	Scan(context.Context, func(string) error) error
	Snapshot(context.Context, *bt.Tx) (Snapshot, error)
	Read(context.Context, []byte) (*Record, error)
	Equal(Record, Record) bool
	HasMarker(Record, string) bool
	Mark(context.Context, Record, string) (Record, error)
	Delete(context.Context, Record) error
}

// Guard checks persisted maintenance state and the pinned chain tip.
// A guard is required at every scan and mutation boundary.
type Guard func(context.Context) error

type InventoryRecord struct {
	Record        Record
	TxID          string
	Master        bool
	Page          uint32
	ExpectedPages uint32
	Candidate     bool
	Reason        string
}
type SpendReference struct {
	ParentKey  []byte
	ParentTxID string
	Vout       uint32
	ChildTxID  string
	Vin        uint32
	Marked     bool
}
type CensusBackend interface {
	Inventory(context.Context, func(InventoryRecord) error) error
	SpendReferences(context.Context, func(SpendReference) error) error
}
type AbsentBackend interface {
	SnapshotAbsent(context.Context, *bt.Tx) (Snapshot, error)
}
