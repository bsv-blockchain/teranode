package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

// newTxIngressSyncManager builds the smallest SyncManager that can run handleTxMsg.
//
// The validator is primed to report a missing parent. That keeps handleTxMsg on the orphan path,
// which is self-contained, and it gives the test two independent ways to observe whether the
// transaction reached validation at all: the queued error is consumed, and the transaction lands in
// the orphan pool.
func newTxIngressSyncManager(t *testing.T, full bool) (*SyncManager, *peerpkg.Peer, *validator.MockValidator) {
	t.Helper()

	blockchainClient := &blockchain.Mock{}
	blockchainClient.BlockAssemblyFull.Store(full)

	validationClient := &validator.MockValidator{
		UtxoStore: &nullstore.NullStore{},
		Errors:    []error{errors.NewTxMissingParentError("parent not found")},
	}

	peer := &peerpkg.Peer{}

	sm := &SyncManager{
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		validationClient: validationClient,
		utxoStore:        &nullstore.NullStore{},
		orphanTxs:        expiringmap.New[chainhash.Hash, *orphanTxAndParents](10 * time.Minute),
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](),
		requestedTxns:    expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		ctx:              t.Context(),
	}

	sm.peerStates.Set(peer, &peerSyncState{
		syncCandidate:   true,
		requestQueue:    txmap.NewSyncedSlice[wire.InvVect](10),
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	})

	return sm, peer, validationClient
}

// TestHandleTxMsgDropsTransactionWhenBlockAssemblyFull checks the legacy ingress gate.
//
// Legacy peers reach the validator directly rather than through propagation, so this is a second
// ingress point that needs its own gate. While block assembly is full the transaction must not
// reach validation, must not be recorded as permanently rejected, and must be cleared from the
// requested-transaction bookkeeping so a later inv message can fetch it again.
func TestHandleTxMsgDropsTransactionWhenBlockAssemblyFull(t *testing.T) {
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	sm, peer, validationClient := newTxIngressSyncManager(t, true)

	tx := bsvutil.NewTx(wire.NewMsgTx(1))
	txHash := tx.Hash()

	state, exists := sm.peerStates.Get(peer)
	require.True(t, exists)

	// Pretend we asked this peer for the transaction, as the real flow would have.
	state.requestedTxns.Set(*txHash, struct{}{})
	sm.requestedTxns.Set(*txHash, struct{}{})

	sm.handleTxMsg(&txMsg{tx: tx, peer: peer})

	require.Len(t, validationClient.Errors, 1,
		"the transaction must never reach validation while block assembly is full")

	require.Empty(t, sm.orphanTxs.Items(),
		"a transaction dropped at the gate must not enter the orphan pool")

	_, rejected := sm.rejectedTxns.Get(*txHash)
	require.False(t, rejected,
		"a full block assembly is transient, so the transaction must not be marked permanently rejected")

	_, stillRequestedOnPeer := state.requestedTxns.Get(*txHash)
	require.False(t, stillRequestedOnPeer,
		"the peer request must be cleared so a later inv can fetch the transaction again")

	_, stillRequested := sm.requestedTxns.Get(*txHash)
	require.False(t, stillRequested,
		"the global request must be cleared so a later inv can fetch the transaction again")
}

// TestHandleTxMsgAcceptsTransactionWhenBlockAssemblyNotFull checks that the gate is inert when
// block assembly has room, so the default configuration keeps the previous behaviour.
func TestHandleTxMsgAcceptsTransactionWhenBlockAssemblyNotFull(t *testing.T) {
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	sm, peer, validationClient := newTxIngressSyncManager(t, false)

	require.False(t, sm.blockchainClient.IsBlockAssemblyFull(),
		"the default must be to accept transactions")

	tx := bsvutil.NewTx(wire.NewMsgTx(1))
	txHash := tx.Hash()

	sm.handleTxMsg(&txMsg{tx: tx, peer: peer})

	require.Empty(t, validationClient.Errors,
		"the transaction must reach validation when block assembly has room")

	_, isOrphan := sm.orphanTxs.Get(*txHash)
	require.True(t, isOrphan,
		"the transaction reached validation and was reported as an orphan, so it must be held for its parent")
}
