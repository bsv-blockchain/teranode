package svp2p

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
)

// maxTxAnnounceBatch and txAnnounceBatchTimeout are legacy netsync's own
// tx-announce batcher constants, ported verbatim:
//
//   - maxTxAnnounceBatch mirrors maxRequestedTxns (netsync/manager.go:121,
//     "= wire.MaxInvPerMsg"), the batcher's size cap — a full batch flushes
//     immediately rather than waiting for the timeout.
//   - txAnnounceBatchTimeout mirrors the batcher's own construction
//     (netsync/manager.go:3212, "batcher.NewWithDeduplicationAndPool[...]
//     (maxRequestedTxns, 1*time.Second, ...)").
//
// The batcher's own deduplication window (go-batcher's
// NewWithDeduplicationAndPool: a fixed 1 minute, not configurable) is NOT a
// port of anything legacy-specific — it is go-batcher's own constant, the
// same library legacy already depends on for this exact batcher.
const (
	maxTxAnnounceBatch     = wire.MaxInvPerMsg
	txAnnounceBatchTimeout = 1 * time.Second
)

// txAnnouncer owns the tx-announce batcher and the Put-vs-Close race guard
// legacy carries alongside it (netsync/manager.go txAnnounceMu /
// txAnnounceClosed, "go-batcher v2.0.4 PANICS on Put-after-Close, and the
// txmeta Kafka listener ... is a fire-and-forget goroutine not joined by
// Stop()"). The same hazard applies here: bridge.StartTxMetaConsumer's
// Kafka callback runs on its own goroutine, tied to the Start ctx, and is
// not joined before Server.Stop calls close — the RLock/RWLock pairing
// below is what makes a Put racing a Close safe rather than a crash.
type txAnnouncer struct {
	mu      sync.RWMutex
	closed  bool
	batcher *batcher.BatcherWithDedup[protocol.TxHashAndFee]

	// canRelay is spec §7's FSM RUNNING gate (legacy's own canRelayTx,
	// services/legacy/peer_server.go:3105-3114), applied HERE — at put, the
	// choke point both of this port's producers share — rather than only at
	// Task 13's Kafka-listener control channel (bridge/kafka.go
	// pollTxRunningGate). Legacy itself gates in two places for the same
	// reason a single gate here would miss: the Kafka-sourced txmeta path
	// and the peer-sourced path (Task 14, IngestTx's accepted branch) both
	// eventually call legacy's own AnnounceNewTransactions
	// (netsync/manager.go:1305 and :3216), which is where canRelayTx
	// actually runs (peer_server.go:1957-1966). Task 13's listener gate
	// stops ITS producer from even reading off Kafka; it was never meant to
	// — and structurally cannot — cover a second producer that never goes
	// through Kafka at all. Both gates staying in place is the faithful
	// shape, not redundant caution.
	//
	// nil means "no gate" (every existing txrelay_test.go call site not
	// about this gate uses nil); Server.go always passes a real one in
	// production.
	canRelay func() bool
}

// newTxAnnouncer builds the tx-announce batcher, wiring its flush directly
// to relay (in practice, *protocol.PeerManager.RelayTxs) — the same
// composition shape Server.go already uses for the blocks-final path
// (bridge.StartBlocksFinalConsumer's onFinal is bound to
// s.manager.RelayBlock). Kept as its own function so txrelay_test.go can
// construct one against a recording relay func without a live Server.
//
// canRelay is checked in put, not here: see txAnnouncer.canRelay's own doc
// comment for why the gate lives at that choke point.
func newTxAnnouncer(logger ulogger.Logger, relay func([]protocol.TxHashAndFee), canRelay func() bool) *txAnnouncer {
	b := batcher.NewWithDeduplicationAndPool[protocol.TxHashAndFee](maxTxAnnounceBatch, txAnnounceBatchTimeout,
		func(batch []*protocol.TxHashAndFee) {
			items := make([]protocol.TxHashAndFee, len(batch))
			for i, item := range batch {
				items[i] = *item
			}

			relay(items)
		},
		true, // background: matches legacy's own construction (manager.go:3212)
		batcher.WithName("svp2p_tx_announce"),
		batcher.WithLogger(logger),
		batcher.WithMetrics(batchermetrics.Provider()),
	)

	return &txAnnouncer{batcher: b, canRelay: canRelay}
}

// put queues one tx for announcement, unless the announcer has already been
// closed — legacy's own announceTx (netsync/manager.go:3101-3107) — or the
// node is not currently allowed to relay tx inventory (canRelay's own doc
// comment). Both checks happen before the tx ever reaches the batcher, the
// same way legacy's canRelayTx short-circuits AnnounceNewTransactions before
// relayTransactions ever runs (peer_server.go:1957-1966): a suppressed tx is
// never merely delayed by the batcher's timeout, it never enters it.
//
// Matches bridge.TxMetaHandler's signature exactly, so it can be passed
// straight to bridge.StartTxMetaConsumer as the onTx callback, and equally
// to the peer-sourced accepted-tx path (services/svp2p/ingest.go
// txIngestor.Ingest).
func (a *txAnnouncer) put(hash chainhash.Hash, fee, size uint64) {
	// Checked BEFORE taking the lock (re-review round 1 follow-up):
	// canRelay reads no announcer state — not a.closed, not a.batcher — so
	// there is nothing here for the lock to protect it against, and
	// something real to lose by holding it anyway. canRelayTx can be a
	// deadline-free gRPC call while blockchain.Client's FSM-state cache is
	// cold (see this file's own canRelayTx doc comment and the task
	// report's Addendum 2 trace); close()'s write lock would then park
	// behind an in-flight put until the Start ctx is cancelled, which is
	// exactly the "no blocking call under a lock" contract this package
	// already states for PeerManager.syncMu (protocol/manager.go).
	if a.canRelay != nil && !a.canRelay() {
		return
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return
	}

	item := protocol.TxHashAndFee{TxHash: hash, Fee: fee, Size: size}
	a.batcher.Put(&item)
}

// close marks the announcer closed (so any Put racing this call, or arriving
// after it, becomes a no-op instead of a panic) and then drains the batcher
// under a bounded timeout — legacy's own closeTxAnnounceBatcher
// (netsync/manager.go:3110-3125). Taking the write lock first waits for any
// in-flight put holding the read lock, exactly as legacy's own comment
// describes for its RWMutex pair.
//
// Idempotent: a second call after closed is already true skips the drain,
// matching legacy's own alreadyClosed short-circuit.
func (a *txAnnouncer) close(logger ulogger.Logger) {
	a.mu.Lock()
	alreadyClosed := a.closed
	a.closed = true
	a.mu.Unlock()

	if alreadyClosed {
		return
	}

	util.DrainBatcher(logger, "svp2p_tx_announce", util.DefaultBatcherDrainTimeout, a.batcher.Close)
}

// canRelayTx reports whether the node may currently relay transaction
// inventory to peers — spec §7's FSM RUNNING gate, ported from legacy's own
// canRelayTx (services/legacy/peer_server.go:3105-3114): "Transactions must
// only be relayed once the node is fully synced (FSM RUNNING). While
// syncing (CATCHINGBLOCKS) the local chain tip may sit below the Genesis
// activation height, in which case the validator accepts pre-Genesis-only
// outputs such as P2SH. Re-broadcasting those to post-Genesis peers earns
// an instant ban for `bad-txns-vout-p2sh`."
//
// Fails closed, exactly as legacy does ("Fails closed: any error reading
// the state suppresses relay"): an error reading FSM state is treated as
// "not running", never as "keep the previous answer". A nil client mirrors
// legacy's own nil check there too — relay is never suppressed for a
// service that was never wired with one (a depless test server).
//
// This is called from txAnnouncer.put on every single Put — up to
// wire.MaxInvPerMsg (50,000) times per flush, from both producers — so,
// like legacy's own equivalent, it NEEDS to stay cheap. Legacy's own
// comment on this claims exactly that: IsFSMCurrentState "serves ... from a
// locally-cached atomic, so callers may invoke this per-inv without RPC
// cost." Read-only verification after this task's fix round (task-14
// report, Addendum 2) found that claim is not reliably true, in either
// codebase, and traced why:
//
//   - blockchain.Client's cache (fmsState, an atomic.Pointer) is populated
//     two ways: an FSM-transition notification, and a one-time
//     fetch-on-subscribe fallback (Client.go fetchAndRestoreFSMState) that
//     runs once, in the background, when the client's own subscription
//     stream first opens.
//   - GetFSMCurrentState's own fallback path — hit whenever the cache is
//     nil — does NOT store what it fetches (Client.go:1825-1837), so a
//     cold cache stays cold: every call becomes a deadline-free gRPC round
//     trip, indefinitely, not just once.
//   - The steady-state populator (the FSM-transition notification) fires
//     only from the FSM's own enter_state callback. A node that restarts
//     already RUNNING and stays RUNNING never transitions again, so a
//     client that (re)subscribes to it can stay nil for its entire process
//     lifetime if its own one-time fetch never lands.
//
// A cold cache here therefore means a full gRPC round trip on every Put,
// from both producers, at exactly the moment tx volume is highest. That is
// real, but it is booked as a residual against
// services/blockchain/Client.go, not this task: the one-line fix is to
// store the fetched value in GetFSMCurrentState's own fallback, or prime
// fmsState at construction. It is also not new — legacy's own canRelayTx
// runs at the identical frequency (relayTransactions loops per tx, and
// RelayInventory's own gate check, peer_server.go:3121-3126, runs once per
// tx too; the batch-level AnnounceNewTransactions check is additional, not
// instead) — so legacy's comment above is wrong about its own premise,
// not only about this port's.
//
// Why this port does not hit the failure mode during ordinary startup is
// an ordering coincidence in Server.Start, not a guarantee: no tx can
// reach put before PeerManager.Start opens a listener, which runs only
// after WaitUntilFSMTransitionFromIdleState returns — and that call
// neither reads nor populates fmsState itself; it only blocks long enough,
// in practice, for the client's own background subscription goroutine to
// have already populated it on an unrelated timeline. Nothing in the code
// ties the two together, so a future reordering of Server.Start, or any
// other caller building a txAnnouncer against a freshly constructed client
// outside that sequence, could reopen this window without warning.
func canRelayTx(ctx context.Context, blockchainClient blockchain.ClientI, logger ulogger.Logger) bool {
	if blockchainClient == nil {
		return true
	}

	running, err := blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
	if err != nil {
		logger.Debugf("[svp2p] tx relay gate: failed to read FSM state, suppressing relay: %v", err)
		return false
	}

	return running
}
