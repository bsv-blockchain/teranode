package bridge

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// maxRejectedTxns bounds the rejected-tx set below, ported from legacy's own
// constant (services/legacy/netsync/manager.go:96, "maxRejectedTxns is the
// maximum number of rejected transactions ... to prevent memory exhaustion").
// Per-instance cost: at most 10,000 chainhash.Hash keys (32 bytes each) plus
// map overhead, well under 1 MiB. Aggregate cost across a running node is
// this single figure, not multiplied by peer count: rejectedTxns is owned
// once by the bridge (mirroring legacy's own single SyncManager-wide set),
// never per-peer — unlike the pending-inv deque (Task 10) and knownBlocks
// (Task 12), which are genuinely per-connection state.
const maxRejectedTxns = 10_000

// IngestTxResult classifies one inbound transaction. TxHash is always
// populated once txBytes decodes; every other field is meaningful only for
// the outcome it names. A result with Accepted, Orphan and Reject all at
// their zero value is the "already rejected, silently ignored" outcome
// (legacy's own unsolicited-and-previously-rejected short-circuit,
// netsync/manager.go:1218-1224): nothing further happens for it.
type IngestTxResult struct {
	// TxHash is the transaction's hash.
	TxHash chainhash.Hash

	// Accepted reports the transaction validated successfully.
	Accepted bool

	// Fee and Size are populated only when Accepted: exactly the shape
	// Task 13's tx announcement seam needs (txAnnouncer.put(hash, fee,
	// size)), taken from the validator's own meta.Data rather than
	// recomputed here, so the two paths agree on what "size" means.
	Fee  uint64
	Size uint64

	// Orphan reports a missing-parent/locked classification
	// (errors.ErrTxMissingParent or errors.ErrTxLocked). The transaction is
	// added to the orphan pool (orphans.go) before this returns; a reject
	// is "we refuse this", an orphan is "we cannot judge this yet" — the
	// two sets stay distinct (see svp2pBridge.orphanPool's own doc
	// comment).
	Orphan bool

	// ReleasedOrphans lists orphans the release walk promoted to accepted
	// as a side effect of THIS tx's acceptance (orphans.go's release,
	// legacy's own processOrphanTransactions, netsync/manager.go:1309).
	// Populated only when Accepted; nil on every other outcome. The
	// caller feeds each one to the same announce seam it feeds TxHash/Fee/
	// Size for the primary accepted tx (services/svp2p/ingest.go).
	ReleasedOrphans []ReleasedOrphan

	// Reject is the wire.MsgReject to send to the peer, when the
	// transaction failed validation for a reason other than orphan. nil on
	// every other outcome, including the "already rejected" short-circuit,
	// which legacy deliberately does NOT re-reject (see the type doc above).
	// Bridge stays I/O-free toward peers (spec §4.4): this is returned for
	// the caller to send, never written to a socket here.
	Reject *wire.MsgReject
}

// IngestTx runs one inbound transaction through the relocated netsync
// handleTxMsg core (services/legacy/netsync/manager.go:1194-1310, no SVNode
// counterpart — this task is a fidelity relocation, not a reimplementation).
// txBytes is the transaction's wire-serialized bytes; peerAddr identifies
// the sending peer for logging only.
//
// Classification, in order:
//  1. Already rejected (rejectedTxns.Get) — ignored silently, no validator
//     call, no reject message (manager.go:1218-1224: BitcoinJ interop means
//     legacy never disconnects or scores an unsolicited peer for this, and
//     it does not re-reject a tx it already told the network was invalid).
//  2. errors.ErrTxMissingParent / errors.ErrTxLocked — orphan. Task 15's
//     pool is the next step; this method classifies and stops
//     (manager.go:1256-1273).
//  3. Any other validation error — rejected: recorded in rejectedTxns (not
//     requested again "until a new block has been processed",
//     manager.go:1276-1282) and returned as a wire.MsgReject for the caller
//     to send (manager.go PushRejectMsg, relocated as data rather than a
//     socket write per the bridge I/O-free contract). One omission,
//     disclosed rather than silent (review round 1, Minor 2): legacy's
//     PushRejectMsg suppresses the reject entirely when the peer's
//     negotiated protocol version is below wire.RejectVersion
//     (services/legacy/peer/peer.go:1135-1137). This method has no peer
//     handle to check that against — the caller would have to withhold the
//     Reject it gets back instead — and is not carried here. Irrelevant in
//     practice against any modern peer, RejectVersion having shipped in
//     Bitcoin protocol version 70002.
//  4. Success — accepted; Fee/Size are populated for the tx announcement
//     relay (manager.go:1295-1305, acceptedTxs / AnnounceNewTransactions).
//
// block height 0 is passed to Validate, which defaults to the UTXO store's
// own block height (the same comment legacy carries verbatim at
// manager.go:1242).
func (sm *svp2pBridge) IngestTx(ctx context.Context, txBytes []byte, peerAddr string) (IngestTxResult, error) {
	start := time.Now()
	defer func() {
		prometheusSvp2pBridgeHandleTxMsg.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	btTx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return IngestTxResult{}, errors.NewProcessingError("[IngestTx] failed to create transaction from bytes", err)
	}

	txHash := *btTx.TxIDChainHash()

	if _, exists := sm.rejectedTxns.Get(txHash); exists {
		sm.logger.Debugf("[IngestTx][%s] ignoring unsolicited previously rejected transaction from %s", txHash, peerAddr)
		return IngestTxResult{TxHash: txHash}, nil
	}

	// passing in block height 0, which will default to utxo store block
	// height in validator (manager.go:1242, carried verbatim).
	validateStart := time.Now()
	txMeta, err := sm.validationClient.Validate(ctx, btTx, 0)
	prometheusSvp2pBridgeHandleTxMsgValidate.Observe(float64(time.Since(validateStart).Microseconds()) / 1_000_000)

	// Not carried here (review round 1, Minor 7): immediately after
	// Validate, and REGARDLESS of its outcome (err nil or not — the check
	// below runs after this, not before it), legacy deletes txHash from two
	// per-peer/per-node "we asked for this" maps
	// (manager.go:1247-1252, state.requestedTxns.Delete /
	// sm.requestedTxns.Delete): "Either the mempool/chain already knows
	// about it ... or we failed to insert and thus we'll retry next time we
	// get an inv." Neither map has a counterpart in this port yet — they
	// belong to the tx-inv round trip (Task 16, see TxRejected's own doc
	// comment for the same ownership boundary) — so this is named here
	// rather than silently dropped: whoever builds that round trip needs to
	// know the cleanup is unconditional on outcome and runs at exactly this
	// point in the relocated flow.
	if err != nil {
		if errors.Is(err, errors.ErrTxMissingParent) || errors.Is(err, errors.ErrTxLocked) {
			sm.logger.Debugf("[IngestTx][%s] orphan transaction from %s: %v", txHash, peerAddr, err)

			if sm.orphanPool != nil {
				sm.orphanPool.add(btTx)
			}

			return IngestTxResult{TxHash: txHash, Orphan: true}, nil
		}

		// Do not request this transaction again until a new block has been
		// processed (manager.go:1276-1278; the clearing site is
		// IngestBlock's own accepted-block path, see ingest.go).
		sm.rejectedTxns.Set(txHash, struct{}{})

		sm.logger.Errorf("[IngestTx][%s] failed to process transaction from %s: %v", txHash, peerAddr, err)

		reject := wire.NewMsgReject(wire.CmdTx, wire.RejectInvalid, "rejected")
		reject.Hash = txHash

		return IngestTxResult{TxHash: txHash, Reject: reject}, nil
	}

	var released []ReleasedOrphan
	if sm.orphanPool != nil {
		// process any orphan transactions that were waiting for this
		// transaction to be accepted (manager.go:1295-1305, the recursive
		// call this task ports as orphanPool.release's iterative
		// worklist — see that method's own doc comment for G3).
		released = sm.orphanPool.release(ctx, txHash)
	}

	return IngestTxResult{
		TxHash:          txHash,
		Accepted:        true,
		Fee:             txMeta.Fee,
		Size:            txMeta.SizeInBytes,
		ReleasedOrphans: released,
	}, nil
}

// TxRejected reports whether hash is in the rejected-transaction set.
//
// This is Task 16's seam (F1): legacy's inv handler reads the identical set
// before deciding to request a tx via getdata (netsync/manager.go:2400,
// "Skip the transaction if it has already been rejected"), so a peer that
// re-announces a tx we just refused is not asked for it again. That
// suppression is NOT wired here — Task 16 owns the tx-inv round trip this
// method feeds, and is expected to call it from within the protocol package
// (PeerManager already holds a reference to whatever exposes this, via the
// TxIngestor seam in protocol/peer.go). Deliberately not called from
// anywhere in this task.
func (sm *svp2pBridge) TxRejected(hash chainhash.Hash) bool {
	_, exists := sm.rejectedTxns.Get(hash)
	return exists
}
