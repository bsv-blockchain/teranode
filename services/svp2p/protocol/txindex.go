package protocol

import (
	"context"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// ErrTxUnknown is returned by TxIndex.Open when the requested transaction is
// not held.
var ErrTxUnknown = errors.New(errors.ERR_NOT_FOUND, "svp2p: transaction not held")

// TxIndex is the seam compact-block reconstruction (spec §6) reads through:
// protocol owns the state machine and the wire exchange, and TxIndex is what
// only Teranode knows — which transactions bridge holds, and their bytes.
type TxIndex interface {
	// Match returns, for each short ID, the transaction hash it identifies in
	// the index under (k0,k1), or nil; and reports a collision (two indexed
	// hashes map to one short ID).
	//
	// A collision does NOT abort reconstruction. The colliding slot is left
	// unfilled and joins the gaps the getblocktxn asks for, which is what
	// SVNode does: "If we find two mempool txn that match the short id, just
	// request it. This should be rare enough that the extra bandwidth doesn't
	// matter, but eating a round-trip due to FillBlock failure would be
	// annoying" (blockencodings.cpp:174-183). The flag is reported for
	// diagnostics; compactState.matchIndex acts on the nil hashes alone.
	Match(k0, k1 uint64, shortIDs []uint64) (hashes []*chainhash.Hash, collision bool)
	// Open returns the raw bytes of a transaction the node holds as a
	// reader, or ErrTxUnknown. The answer comes from the node's own
	// storage, not from whatever Match matched against: an implementation
	// may hold bytes for a transaction its index no longer names.
	Open(ctx context.Context, hash chainhash.Hash) (io.ReadCloser, uint64, error)
}
