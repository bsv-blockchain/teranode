package subtreevalidation

import (
	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
)

// checkCanonicalTxEncoding rejects a transaction whose wire bytes are not the
// shortest encoding of themselves (issue 1421).
//
// Bitcoin and SV Node require every CompactSize length prefix to be written in
// its minimal form — writing the value 1 as `fd 01 00` instead of `01` is a
// hard parse error there. go-bt's varint reader has no such check: it accepts
// the over-long form and then RE-SERIALIZES it canonically, so the txid
// Teranode computes is the canonical one.
//
// That re-serialization is the whole problem, and it cuts both ways. Because
// the txid is unchanged, so is the merkle root — every hash-based check
// downstream passes, precisely because canonicalization defeats them, so the
// parse is the only place that can still see the over-long encoding. But by
// the same token a non-minimal prefix says nothing about whether the block is
// valid: the same subtree from an honest peer parses to identical transactions.
// So the point of this check is NOT to stop a fleet split — subtree data is a
// Teranode-internal format, a receiving node relays the canonical form, and SV
// Node never sees these bytes by this route (the legacy p2p path it does share
// is already covered by go-wire's minimality check). The point is the plainer
// one: fail closed on malformed peer input instead of normalising it, storing
// the canonicalized bytes, and discarding the evidence that this node accepted
// something SV Node would reject at parse.
//
// The check is a length comparison, not a re-parse: any non-minimal prefix
// makes the consumed wire bytes longer than the canonical serialization, so
// comparing the bytes consumed against the canonical size detects every such
// encoding at negligible cost. Do not "fix up" the transaction by accepting
// the canonical form — that would let a malformed transaction change identity
// mid-flight; reject it.
//
// The failure is a ProcessingError, deliberately NOT a TxInvalidError, and
// must stay that way. Non-minimality is a property of the DELIVERY, not of the
// block: because go-bt canonicalizes, the txids and therefore the merkle root
// are identical either way, so the same subtree fetched from an honest peer
// validates fine, and marking the block invalid would be simply wrong.
//
// Concretely, from the readTxFromReader caller the error travels
// getMissingTransactionsBatch -> getSubtreeMissingTxs -> ValidateSubtreeInternal
// -> validateMissingSubtreesWithOrderedRetry, which wraps it with
// errors.WrapGRPC, so the code survives the gRPC hop to blockvalidation. There
// BlockValidation.ValidateBlock tests errors.Is(err, ErrTxInvalid) on the
// validateBlockSubtrees result: a TxInvalidError would reach storeInvalidBlock
// and mark a perfectly valid block permanently invalid, surviving restart,
// while the resulting BlockInvalidError makes isUnvalidatablePeerError true and
// stops us trying another source — handing any peer we fetch from a cheap way
// to poison a valid block. Wrapping does not contain the code either:
// errors.Is walks the chain by code (errors/errors.go (*Error).Is), so a
// TxInvalidError inside a ProcessingError still matches ErrTxInvalid.
//
// From the readTransactionsFromSubtreeDataStream caller that branch is not
// reachable TODAY only by accident: CheckBlockSubtrees returns the batch
// pipeline's error without errors.WrapGRPC, so grpc-go flattens it to
// codes.Unknown with no TError detail and UnwrapGRPC rebuilds a bare
// ERR_ERROR. Adding the missing WrapGRPC there is a plausible future fix, and
// would immediately restore the hazard — so do not rely on the code being
// dropped in transit.
//
// What ProcessingError buys instead is bounded retry rather than poisoning: on
// the catchup route the block is retried against alternative peers and then
// counted against CatchupMaxAttemptsPerBlock (default 5) with a 10-minute
// cooldown, and ReportPeerFailure rotates the sync peer. If every peer serves
// the same non-minimal bytes the node stops advancing at that height instead of
// recording a false verdict, and recovers by itself as soon as one peer serves
// canonical bytes. That is the same liveness profile as the established sibling
// check in this package — the "subtree data does not match subtree"
// ProcessingError in getSubtreeMissingTxs — and it is the right trade for a
// consensus-divergence hazard: stalling is recoverable, a wrong verdict is not.
//
// Subtree data may carry EXTENDED transactions (each input also carrying its
// parent's satoshis and locking script), which serialize longer than tx.Size()
// reports — a subtree-data file written from parsed transactions keeps whatever
// form they were in, while the asset service's on-demand generator writes the
// standard form. The comparison is therefore made against whichever
// serialization the transaction actually uses (util.CanonicalTxSize), or a
// legitimate extended transaction would be rejected as non-canonical. That size
// is computed arithmetically: calling ExtendedBytes here would allocate and copy
// every transaction twice, on the very loop that was built to read transactions
// without allocating.
func checkCanonicalTxEncoding(tx *bt.Tx, bytesRead int64) error {
	if tx == nil {
		return errors.NewProcessingError("nil transaction")
	}

	canonicalSize := int64(util.CanonicalTxSize(tx))

	if bytesRead != canonicalSize {
		return errors.NewProcessingError("transaction %s is not canonically encoded: %d wire bytes for a %d-byte canonical serialization (non-minimal CompactSize prefix)", tx.TxIDChainHash().String(), bytesRead, canonicalSize)
	}

	return nil
}

// omittedCoinbaseTx returns the coinbase the subtree-data serialization leaves
// out, or nil. go-subtree parses a leading coinbase into Txs[0] for a subtree
// whose first node is the coinbase placeholder, but writes the file from index
// 1 — so the parser consumes bytes the serialization never re-emits, and the
// canonical-size comparison has to account for them (issue 1421).
func omittedCoinbaseTx(subtree *subtreepkg.Subtree, data *subtreepkg.Data) *bt.Tx {
	if subtree == nil || data == nil || len(subtree.Nodes) == 0 || len(data.Txs) == 0 {
		return nil
	}

	if !subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		return nil
	}

	return data.Txs[0]
}
