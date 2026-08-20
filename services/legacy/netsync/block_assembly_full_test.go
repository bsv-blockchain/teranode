package netsync

import (
	"net/url"
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
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
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

// newInvSyncManager builds the smallest SyncManager that can run processInvMsg for a transaction
// inventory item.
//
// The UTXO store is an empty sqlitememory store rather than a NullStore, because NullStore answers
// every Get with default data, which makes haveInventory report that we already hold the
// transaction. An empty real store reports not-found, which is the state an announcement of a
// transaction we have never seen actually produces.
func newInvSyncManager(t *testing.T, full bool) (*SyncManager, *peerpkg.Peer, *peerSyncState) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///tx_inv_gate")
	require.NoError(t, err)

	utxoStore, err := utxosql.New(t.Context(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	blockchainClient := &blockchain.Mock{}
	blockchainClient.BlockAssemblyFull.Store(full)

	// A real peer, because processInvMsg records the announcement in the peer's known-inventory
	// map before it decides anything, and a bare struct literal has no map to record it in.
	peer := peerpkg.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{})

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		utxoStore:        utxoStore,
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](),
		requestedTxns:    expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		ctx:              t.Context(),
	}

	state := &peerSyncState{
		syncCandidate:   true,
		requestQueue:    txmap.NewSyncedSlice[wire.InvVect](10),
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	sm.peerStates.Set(peer, state)

	return sm, peer, state
}

// TestProcessInvMsgSkipsTransactionsWhenBlockAssemblyFull checks that a full node stops asking for
// transactions instead of downloading them only to throw them away.
//
// handleTxMsg already drops the transaction on arrival, but by then the node has paid the inbound
// bandwidth and the wire deserialization. Worse, a dropped transaction never reaches the UTXO store
// and is deliberately not recorded in rejectedTxns, so haveInventory keeps reporting not-have: every
// later inv, from this peer and from every other peer announcing the same transaction, would request
// it again for as long as block assembly stays full.
func TestProcessInvMsgSkipsTransactionsWhenBlockAssemblyFull(t *testing.T) {
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	sm, peer, state := newInvSyncManager(t, true)

	iv := &wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{0x01}}

	sm.processInvMsg(0, iv, true, peer, false, state, 0)

	require.Zero(t, state.requestQueue.Length(),
		"a full node must not queue a getdata for a transaction it would discard on arrival")

	// The same announcement arriving again must stay suppressed, which is the loop this prevents.
	sm.processInvMsg(0, iv, true, peer, false, state, 0)
	require.Zero(t, state.requestQueue.Length())
}

// TestProcessInvMsgRequestsTransactionsWhenBlockAssemblyNotFull checks the gate is inert when block
// assembly has room, so normal relay is unaffected.
func TestProcessInvMsgRequestsTransactionsWhenBlockAssemblyNotFull(t *testing.T) {
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	sm, peer, state := newInvSyncManager(t, false)

	iv := &wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{0x02}}

	sm.processInvMsg(0, iv, true, peer, false, state, 0)

	require.Equal(t, 1, state.requestQueue.Length(),
		"a transaction we do not have must be queued for request when block assembly has room")
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
