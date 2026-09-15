package util

import (
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
)

// CountingReader counts the bytes a parser consumes, so a caller that cannot
// instrument the parse itself can still compare what came off the wire against
// what the parsed result serializes back to (issue 1421).
type CountingReader struct {
	r io.Reader
	n int64
}

// NewCountingReader wraps r. Wrap the reader the PARSER reads from (inside any
// buffering), so the count is bytes consumed by the parse rather than bytes
// pulled from the network including read-ahead.
//
// Not safe for concurrent use: the counter is a plain field, so read the reader
// and then BytesConsumed from the same goroutine (which is how a parse-then-
// compare caller naturally uses it).
func NewCountingReader(r io.Reader) *CountingReader {
	return &CountingReader{r: r}
}

func (c *CountingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}

// BytesConsumed returns the number of bytes read so far.
func (c *CountingReader) BytesConsumed() int64 {
	return c.n
}

// CanonicalTxSize returns the size of tx in the serialization it actually uses,
// without serializing it. tx must be non-nil.
//
// Note one go-bt quirk: bt.Tx.IsExtended reports false for an extended
// transaction with no inputs (it tests tx.Inputs == nil before the extended
// flag), so such a transaction is measured in its standard form and a caller
// comparing against consumed wire bytes sees a mismatch. Harmless in practice —
// a transaction with no inputs is consensus-invalid regardless — but it means
// the rejection reason can misattribute for that one shape.
func CanonicalTxSize(tx *bt.Tx) int {
	if tx.IsExtended() {
		return ExtendedTxSize(tx)
	}

	return tx.Size()
}

// CheckCanonicalSubtreeData rejects a subtree-data payload whose wire bytes are
// not the shortest encoding of themselves (issue 1421).
//
// Bitcoin and SV Node require every CompactSize length prefix to be minimal;
// go-bt does not check, and RE-SERIALIZES the over-long form canonically, so a
// non-minimal payload parses to transactions with canonical txids. Every
// hash-based check downstream therefore passes, and the caller stores the
// canonicalised bytes — silently normalising the payload and discarding the
// evidence, after which nothing can tell that this node accepted bytes SV Node
// would reject at parse. The parse is the only place that can still see it.
//
// This is a fail-closed guard on malformed peer input, not a fleet-split fix:
// subtree data is a Teranode-internal format, a receiving node relays the
// canonical form, so SV Node never sees these bytes by this route. The legacy
// p2p path it does share is already covered by go-wire's minimality check.
//
// The subtree-data parsers do not expose per-transaction byte counts, so the
// check is made over the whole payload: the bytes the parser consumed must
// equal the bytes the parsed data serializes back to. Any non-minimal prefix
// anywhere in the payload makes the wire form longer.
//
// omittedCoinbase must be supplied when the payload carried a coinbase that
// the serialization deliberately omits — the first subtree of a block, where
// go-subtree parses the coinbase into Txs[0] but writes the file from index 1.
// Without it a perfectly canonical first-subtree payload is rejected, because
// the consumed count legitimately exceeds the serialized length by exactly the
// coinbase. Its canonical size is added rather than the consumed bytes being
// ignored, so a non-minimal coinbase is still caught.
//
// The failure is a ProcessingError, deliberately NOT a TxInvalidError, and must
// stay that way. Non-minimality is a property of the DELIVERY, not of the block:
// because go-bt canonicalizes, the txids and therefore the merkle root are
// identical either way, so the same subtree fetched from an honest peer
// validates fine, and marking the block invalid would be simply wrong. The two
// callers reach two different verdict machines, and TxInvalidError is wrong at
// both:
//
//   - subtreevalidation's getSubtreeMissingTxs runs under
//     ValidateSubtreeInternal, whose error is WrapGRPC'd back to blockvalidation.
//     BlockValidation.ValidateBlock tests errors.Is(err, ErrTxInvalid) on the
//     validateBlockSubtrees result, so a TxInvalidError reaches
//     storeInvalidBlock and marks a perfectly valid block permanently invalid,
//     surviving restart; the resulting BlockInvalidError then makes
//     isUnvalidatablePeerError true and stops us trying another source.
//   - blockvalidation's catchup prewarm (fetchAndStoreSubtreeData) never reaches
//     ValidateBlock, so it cannot store the block invalid. Instead
//     fetchAndStoreSubtreeAndSubtreeData reclassifies an all-peers-failed fetch
//     as ErrExternal, and releaseCatchupLock's classification tests
//     ErrBlockInvalid/ErrTxInvalid FIRST — so a TxInvalidError there reports the
//     serving peer as malicious for delivering bytes that are not in fact
//     invalid.
//
// Wrapping does not contain the code either: errors.Is walks the chain by code
// (errors/errors.go (*Error).Is), so a TxInvalidError inside a ProcessingError
// still matches ErrTxInvalid.
//
// What ProcessingError buys instead is bounded retry rather than a false
// verdict. On the catchup route the error rotates through the alternative peers
// for the subtree, then counts against CatchupMaxAttemptsPerBlock (default 5)
// with a 10-minute cooldown, and ReportPeerFailure rotates the sync peer. If
// every peer serves the same non-minimal bytes the node stops advancing at that
// height instead of recording a verdict it cannot justify, and recovers by
// itself as soon as one peer serves canonical bytes. That matches the
// established sibling check — the "subtree data does not match subtree"
// ProcessingError in getSubtreeMissingTxs — and it is the right trade for a
// consensus-divergence hazard: stalling is recoverable, a wrong verdict is not.
func CheckCanonicalSubtreeData(consumed int64, serialized []byte, omittedCoinbase *bt.Tx) error {
	canonicalSize := int64(len(serialized))
	if omittedCoinbase != nil {
		canonicalSize += int64(CanonicalTxSize(omittedCoinbase))
	}

	if consumed != canonicalSize {
		return errors.NewProcessingError("subtree data is not canonically encoded: %d wire bytes for a %d-byte canonical serialization (non-minimal CompactSize prefix)", consumed, canonicalSize)
	}

	return nil
}
