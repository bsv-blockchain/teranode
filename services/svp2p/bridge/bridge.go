// Package bridge is the Teranode-side half of the svp2p rewrite (spec §7):
// it owns the relocated block-ingestion pipeline (handle_block.go,
// subtree_partition.go — moved here with logic intact from the legacy
// netsync package) and the eight Teranode service dependencies that
// pipeline needs. `protocol` (services/svp2p/protocol) never imports this
// package directly; per spec §4.4 it sees bridge through its own narrow,
// locally-declared interface, which the Bridge interface here is shaped to
// satisfy.
package bridge

import (
	"bytes"
	"context"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// Bridge grows one phase at a time (Decision 2, svp2p Phase 2 plan): a
// method is added only in the phase that implements it, never early as an
// unreachable no-op. Phase 2 shipped PreAdmit, IngestBlock and HeaderEvents;
// Phase 3 (this task) adds the two read-side methods a getdata answerer
// needs, FetchBlock and FetchTx. Task 14 adds IngestTx and TxRejected.
type Bridge interface {
	// PreAdmit runs the two blockchain lookups IngestBlock makes before it
	// touches the block payload — does this block already exist, and is its
	// parent in our chain — so the caller can bound them on its own context.
	// Legacy did exactly that (services/legacy/peer_server.go OnBlock, PR
	// 1281): the pre-admission phase is deadline-bounded because a wedged
	// blockchain client would otherwise park the transport read loop, while
	// the admission budget wait deliberately is NOT bounded, being our own
	// backpressure. IngestBlock still repeats both lookups, so it stays
	// correct when called on its own; they are sub-millisecond on a healthy
	// node.
	PreAdmit(ctx context.Context, header *wire.BlockHeader) (PreAdmitResult, error)

	// IngestBlock runs a received block through the relocated netsync
	// ingestion pipeline: existence check, parent-height validation,
	// block-assembly readiness wait, then createTxMap -> prepareSubtrees ->
	// extendTransactions -> createUtxos -> createSubtrees -> writeSubtree ->
	// ProcessBlock. header and txCount come from the wire message envelope;
	// txReader streams the block's transactions (transport hands bridge a
	// bounded reader instead of a materialized block — spec §5). peerAddr
	// identifies the sending peer for logging only; the pipeline has no
	// other use for it (no peer-liveness or per-peer state lives in bridge).
	IngestBlock(ctx context.Context, header *wire.BlockHeader, txCount uint64, txReader io.Reader, peerAddr string) error

	// IngestTx runs one inbound transaction through the relocated netsync
	// handleTxMsg core (services/legacy/netsync/manager.go:1194-1310, no
	// SVNode counterpart): rejected-txns short-circuit, validation, and
	// outcome classification (accepted / orphan / rejected). txBytes is the
	// transaction's wire-serialized bytes; peerAddr identifies the sending
	// peer for logging only. See ingest_tx.go for the full classification
	// and the reject message construction — bridge stays I/O-free toward
	// peers, so a rejected tx's wire.MsgReject is returned for the caller to
	// send, never written to a socket here.
	IngestTx(ctx context.Context, txBytes []byte, peerAddr string) (IngestTxResult, error)

	// TxRejected reports whether hash is in the rejected-transaction set
	// IngestTx maintains. This is Task 16's seam (see ingest_tx.go's own doc
	// comment): the inv handler's "already rejected, skip the getdata"
	// suppression (netsync/manager.go:2400) is made reachable here but
	// deliberately not wired to anything in this task.
	TxRejected(hash chainhash.Hash) bool

	// HeaderEvents delivers tip-change notifications sourced from the
	// blockchain service's subscription (spec §4.4). The channel is never
	// closed by Bridge.
	//
	// OPEN QUESTION (Task 12, Phase 3): this method has no producer and no
	// consumer anywhere in this codebase, and no task in the 27-task Phase 3
	// plan gives it one. The block announcement relay (protocol/relay.go)
	// takes its trigger from the blocks-final Kafka topic instead — the
	// point at which finality is actually decided, and what legacy netsync
	// does (kafkaBlocksFinalListener, services/legacy/netsync/manager.go:3443).
	// So either spec §4.4's sketch of this method is aspirational, or
	// something is meant to feed it that Phase 3 never identified. Kept per
	// spec §4.4, which lists it on the Bridge interface sketch and is the
	// binding authority over the plan; deleting it is not this task's call
	// to make. See the method's own doc comment below for what stays
	// unchanged: the channel is real, just never written to.
	HeaderEvents() <-chan HeaderEvent

	// FetchBlock streams a block's legacy-wire bytes (header + varint(txCount)
	// + transactions) from the asset service, and reports the declared length
	// the caller (Task 10) is to write into the wire message header before
	// streaming the body. See fetch.go for why the length comes from the
	// blockchain service rather than the HTTP response.
	FetchBlock(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, uint64, error)

	// FetchTx returns a transaction's serialized bytes from the UTXO store.
	// A missing or not-fully-retained transaction is reported as a typed
	// not-found error (errors.ErrTxNotFound) rather than legacy's silence —
	// see fetch.go.
	FetchTx(ctx context.Context, hash *chainhash.Hash) ([]byte, error)
}

// PreAdmitResult is what the bounded pre-admission lookups found. Both fields
// are answers, not failures: an error from PreAdmit means the lookup itself
// did not complete.
type PreAdmitResult struct {
	// Exists reports that we already hold this block, so there is nothing to
	// ingest and nothing to charge against the admission budget.
	Exists bool

	// ParentMissing reports that the block's parent is not in our chain yet.
	// Under svp2p the download scheduler only ever requests blocks in chain
	// order, so this means our own validation is behind the header index, not
	// that the peer sent something it should not have.
	ParentMissing bool
}

// HeaderEvent is a tip-change notification. It is kept deliberately minimal
// (Phase 2 width): enough for a consumer to know the chain advanced and to
// what, without carrying anything IngestBlock's caller already has cheaper
// access to (e.g. the full header or block).
type HeaderEvent struct {
	// Hash is the new tip's block hash.
	Hash chainhash.Hash
	// Height is the new tip's height.
	Height uint32
}

// svp2pBridge is the concrete Bridge. It stores the eight dependencies
// startSvp2pService injects (daemon/daemon_services.go:1103-1180, mirroring
// legacy.New's parameter list) plus logger/settings, and backs the relocated
// pipeline in handle_block.go / subtree_partition.go — those files reference
// these exact field names via the "sm" receiver, unchanged from netsync's
// SyncManager except for the receiver's type.
//
// It has two entry points into that pipeline. HandleBlockDirect takes a fully
// decoded *wire.MsgBlock, which is the shape the relocated netsync code
// arrived in. IngestBlock (ingest.go) is the streaming entry the transport
// uses: it feeds the same pipeline from a reader, decoding one transaction at
// a time, so a multi-GB block is never materialized in memory — the reason the
// streaming interface exists at all (see the decode-arena-release comments
// throughout handle_block.go). Both run the same checks and the same pipeline
// stages; only the transaction source differs.
type svp2pBridge struct {
	logger      ulogger.Logger
	settings    *settings.Settings
	chainParams *chaincfg.Params

	blockchainClient  blockchain.ClientI
	validationClient  validator.Interface
	subtreeStore      blob.Store
	tempStore         blob.Store
	utxoStore         utxo.Store
	subtreeValidation subtreevalidation.Interface
	blockValidation   blockvalidation.Interface
	blockAssembly     blockassembly.ClientI

	headerEvents chan HeaderEvent

	// rejectedTxns is IngestTx's short-circuit set (ingest_tx.go), ported
	// from legacy's own sm.rejectedTxns (netsync/manager.go:489, bounded at
	// construction :3167 with maxRejectedTxns). Owned once here, not
	// per-peer: see maxRejectedTxns's doc comment for the aggregate-cost
	// reasoning. Cleared on the accepted-block path (ingest.go IngestBlock,
	// mirroring manager.go:1855).
	rejectedTxns *txmap.SyncedMap[chainhash.Hash, struct{}]

	// orphanPool holds transactions IngestTx classified Orphan
	// (orphans.go), ported from legacy's own sm.orphanTxs
	// (netsync/manager.go:458, bounded and timed out at construction :3165
	// with legacy_maxOrphanTxs/legacy_orphanEvictionDuration). Owned once
	// here, not per-peer, for the same reason rejectedTxns is: see that
	// field's own doc comment. Kept distinct from rejectedTxns by meaning,
	// not just by type — a reject is "we refuse this", an orphan is "we
	// cannot judge this yet" (see IngestTxResult's own doc comment).
	orphanPool *orphanPool

	// recentTx is the bounded ring of recently seen transaction hashes
	// compact-block reconstruction matches short IDs against (recenttx.go),
	// this node's stand-in for the mempool SVNode walks. It is always
	// present; legacy_compactBlocks decides whether it has a capacity, and
	// a capacity of zero is the disabled state (nothing kept, nothing
	// matched), the same way an empty peers.json path is addrman's.
	recentTx *RecentTxIndex
}

// New constructs the bridge with its eight injected Teranode dependencies,
// in the same order startSvp2pService will fetch and pass them
// (daemon/daemon_services.go:1103-1180, matching legacy.New's signature).
// tempStore is stored but has no consumer yet: the relocated pipeline
// (handle_block.go, subtree_partition.go) never reads it — legacy's netsync
// package doesn't either, it is used elsewhere in the legacy service.
func New(
	logger ulogger.Logger,
	tSettings *settings.Settings,
	blockchainClient blockchain.ClientI,
	validationClient validator.Interface,
	subtreeStore blob.Store,
	tempStore blob.Store,
	utxoStore utxo.Store,
	subtreeValidation subtreevalidation.Interface,
	blockValidation blockvalidation.Interface,
	blockAssemblyClient *blockassembly.Client,
) *svp2pBridge {
	initPrometheusMetrics()

	sm := &svp2pBridge{
		logger:            logger,
		settings:          tSettings,
		chainParams:       tSettings.ChainCfgParams,
		blockchainClient:  blockchainClient,
		validationClient:  validationClient,
		subtreeStore:      subtreeStore,
		tempStore:         tempStore,
		utxoStore:         utxoStore,
		subtreeValidation: subtreeValidation,
		blockValidation:   blockValidation,
		blockAssembly:     blockAssemblyClient,
		headerEvents:      make(chan HeaderEvent, 16),
		rejectedTxns:      txmap.NewSyncedMap[chainhash.Hash, struct{}](maxRejectedTxns),
	}

	// Sized only when compact blocks are on: the ring is the feature's
	// whole memory cost (legacy_compactBlocksRecentTxs hashes plus the
	// dedup map), and a node with the flag off must not pay any of it.
	recentTxCapacity := 0
	if tSettings.Legacy.CompactBlocks {
		recentTxCapacity = tSettings.Legacy.CompactBlocksRecentTxs
	}

	sm.recentTx = NewRecentTxIndex(recentTxCapacity, sm.openTx)

	sm.orphanPool = newOrphanPool(tSettings, logger, func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
		return validationClient.Validate(ctx, tx, 0)
	})

	return sm
}

// TxIndex returns the recent-transaction index as the seam the peer manager
// takes (protocol/txindex.go). Server hands it to PeerManager.SetTxIndex
// when legacy_compactBlocks is on.
func (b *svp2pBridge) TxIndex() protocol.TxIndex {
	return b.recentTx
}

// RecentTxIndex returns the same index in its concrete type, which is the
// write side: the txmeta consumer and the orphan pool Add to it. Two
// accessors rather than one because the two sides are genuinely different —
// protocol only ever reads through TxIndex, and TxIndex has no Add.
func (b *svp2pBridge) RecentTxIndex() *RecentTxIndex {
	return b.recentTx
}

// openTx is the fetch seam RecentTxIndex.Open reads through: the bridge's
// own FetchTx (fetch.go), the same UTXO-store read that answers getdata tx.
// FetchTx hands back one transaction's bytes, which openTx presents as a
// reader with its length, because a reader is the shape block assembly
// consumes each transaction in.
func (b *svp2pBridge) openTx(ctx context.Context, hash chainhash.Hash) (io.ReadCloser, uint64, error) {
	raw, err := b.FetchTx(ctx, &hash)
	if err != nil {
		return nil, 0, err
	}

	return io.NopCloser(bytes.NewReader(raw)), uint64(len(raw)), nil
}

// HeaderEvents returns the bridge's tip-change notification channel. It is on
// the interface because Decision 2 fixed the Phase 2 width there, not because
// Phase 2 uses it: block sync pulls headers from peers, so nothing in this
// phase needs telling that our own tip moved, and nothing publishes into the
// channel. The channel is real rather than nil — a consumer can range over it
// without an adapter if one is ever wired up — but Phase 3 deliberately did
// NOT wire the blockchain-service subscription (spec §4.4) that would feed
// it: the block announcement relay (Task 12) takes its trigger from the
// blocks-final Kafka topic instead, because that is where finality is
// decided and it is what legacy does. So this channel stays silent, with no
// producer and no consumer anywhere in this codebase — see the interface
// doc comment's OPEN QUESTION above.
func (b *svp2pBridge) HeaderEvents() <-chan HeaderEvent {
	return b.headerEvents
}

// Stop releases the orphan pool's background goroutines: expiringmap's own
// TTL ticker and this task's eviction-validation worker (orphans.go, fix
// round 1 Issues I1/I4). Not part of the Bridge interface — spec §4.4 keeps
// that interface narrow to what protocol needs, and protocol never stops a
// bridge — so a caller reaches this through a concrete *svp2pBridge or a
// small local interface, the same way Server.go already handles
// bridge.Admission's own Stop. Safe to call at most once per bridge
// lifetime; a nil orphanPool (a depless test harness that built a
// svp2pBridge by literal rather than through New) is a no-op.
func (b *svp2pBridge) Stop() {
	if b.orphanPool != nil {
		b.orphanPool.stop()
	}
}
