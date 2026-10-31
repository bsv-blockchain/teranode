package netsync

import (
	"bytes"
	"context"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// lookupStore wraps a real store and counts the two ways inventory can be
// looked up: one Get per item, or one BatchDecorate per chunk. When gate is
// set, BatchDecorate waits for it to close.
type lookupStore struct {
	utxo.Store

	gets      atomic.Int64
	batches   atomic.Int64
	batchSize atomic.Int64
	gate      chan struct{}

	// fail, when set, is what BatchDecorate returns instead of reading.
	fail error
}

func (s *lookupStore) Get(ctx context.Context, hash *chainhash.Hash, fieldNames ...fields.FieldName) (*meta.Data, error) {
	s.gets.Add(1)

	return s.Store.Get(ctx, hash, fieldNames...)
}

func (s *lookupStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, fieldNames ...fields.FieldName) error {
	if s.gate != nil {
		<-s.gate
	}

	if s.fail != nil {
		return s.fail
	}

	s.batches.Add(1)

	for {
		p := s.batchSize.Load()
		if int64(len(items)) <= p || s.batchSize.CompareAndSwap(p, int64(len(items))) {
			break
		}
	}

	return s.Store.BatchDecorate(ctx, items, fieldNames...)
}

func newLookupStore(t *testing.T, tSettings *settings.Settings, name string) *lookupStore {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	realStore, err := sql.New(t.Context(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	return &lookupStore{Store: realStore}
}

// newInvHarness builds a SyncManager in the given FSM state with one sync peer
// whose request queue holds a whole inv message.
func newInvHarness(t *testing.T, tSettings *settings.Settings, store utxo.Store, fsm blockchain2.FSMStateType) (*SyncManager, *peerpkg.Peer, *peerSyncState, *blockchain2.Mock) {
	t.Helper()

	// A dropped announcement increments a counter that is nil until the
	// metrics are registered; New does this in production.
	initPrometheusMetrics()

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "localhost:8333")
	require.NoError(t, err)

	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&fsm, nil)

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		ctx:              context.Background(),
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		blockchainClient: client,
		utxoStore:        store,
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](10),
		requestedTxns:    expiringmap.New[chainhash.Hash, struct{}](time.Hour),
		blockDownloads:   newBlockDownloadTracker(blockRequestAssignmentTTL),
	}
	sm.headersFirstMode.Store(false)
	sm.initInvQueue(invQueueMaxHashes(ulogger.TestLogger{}, tSettings))

	state := &peerSyncState{
		requestQueue: txmap.NewSyncedSlice[wire.InvVect](wire.MaxInvPerMsg),
	}
	sm.peerStates.Set(p, state)
	sm.storeSyncPeer(p, &syncPeerState{})

	return sm, p, state, client
}

// newHarnessPeer adds a second peer, registered with its own state, to an inv
// harness, for the tests where two peers share one announcement queue.
func newHarnessPeer(t *testing.T, sm *SyncManager) (*peerpkg.Peer, *peerSyncState) {
	t.Helper()

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, sm.settings, &peerpkg.Config{}, "localhost:8334")
	require.NoError(t, err)

	state := &peerSyncState{
		requestQueue: txmap.NewSyncedSlice[wire.InvVect](wire.MaxInvPerMsg),
	}
	sm.peerStates.Set(p, state)

	return p, state
}

// TestHandleInvMsg_LooksUpTransactionsInBatches reproduces the inventory
// fan-out of 2026-10-19: a message full of transaction announcements, each of
// which used to get its own goroutine and its own store Get. The items must now
// be looked up in chunks of txInvLookupChunk with no Get at all, and the result
// must be what the per-item path gave: a transaction the store holds and one
// already rejected are not asked for, and every other one is.
func TestHandleInvMsg_LooksUpTransactionsInBatches(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	store := newLookupStore(t, tSettings, "relay_bound_batches")
	sm, p, state, _ := newInvHarness(t, tSettings, store, blockchain2.FSMStateRUNNING)

	held := transactions.CreateTestTransactionChainWithCount(t, 3)[1]
	_, err := store.Store.(*sql.Store).Create(t.Context(), held, 1)
	require.NoError(t, err)

	rejected := chainhash.Hash{0xde, 0xad}
	sm.rejectedTxns.Set(rejected, struct{}{})

	const unknown = 2*txInvLookupChunk + 1

	inv := wire.NewMsgInv()
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, held.TxIDChainHash())))
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &rejected)))

	for i := 0; i < unknown; i++ {
		h := chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), 0x7a}
		require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &h)))
	}

	// checkTxInvs rather than handleInvMsg, whose getdata drains the queue
	// this test reads.
	sm.checkTxInvs(p, state, inv.InvList)

	require.Zero(t, store.batches.Load(), "the handler goroutine does no store read of its own")
	require.Len(t, sm.invQueue, 3, "the items are queued in chunks of txInvLookupChunk")

	// What the lookup workers do, one batch at a time.
	for i := 0; i < 3; i++ {
		sm.lookupInvBatch(<-sm.invQueue)
	}

	require.Zero(t, store.gets.Load(), "transaction inventory must not be looked up one Get per item")
	require.Equal(t, int64(3), store.batches.Load(), "items must be looked up in chunks of txInvLookupChunk")
	require.Equal(t, int64(txInvLookupChunk), store.batchSize.Load())
	require.Equal(t, unknown, state.requestQueue.Length(),
		"every unknown transaction must be asked for, and the held and rejected ones must not")
}

// TestHandleInvMsg_BlockItemsDoNotWaitForTransactionLookups pins that a block
// announcement is acted on while the message's transaction lookups are still
// in the store, including when it comes after transaction items in the same
// message. Both outcomes of a block item are covered: a block the chain holds
// ends in a locator lookup for a getblocks, and a block the node lacks ends in
// a getdata.
func TestHandleInvMsg_BlockItemsDoNotWaitForTransactionLookups(t *testing.T) {
	blockHash := chainhash.Hash{0xb1}

	cases := []struct {
		name string
		// arrange sets up the chain's answer for blockHash and returns the
		// check that the block item has been acted on.
		arrange func(client *blockchain2.Mock, sm *SyncManager) func() bool
	}{
		{
			name: "a block the chain holds",
			arrange: func(client *blockchain2.Mock, _ *SyncManager) func() bool {
				// The lookup fails so that no getblocks is pushed.
				var asked atomic.Bool

				client.On("GetBlockHeader", mock.Anything, &blockHash).
					Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 1}, nil)
				client.On("GetBlockLocator", mock.Anything, &blockHash, uint32(0)).
					Run(func(mock.Arguments) { asked.Store(true) }).
					Return(nil, errors.NewProcessingError("no locator in this test"))

				return asked.Load
			},
		},
		{
			name: "a block the node lacks",
			arrange: func(client *blockchain2.Mock, sm *SyncManager) func() bool {
				// The ledger records the request as the getdata is built.
				client.On("GetBlockHeader", mock.Anything, &blockHash).
					Return(nil, nil, errors.NewNotFoundError("not in the chain"))

				return func() bool { return sm.blockDownloads.RequestedWithin(blockHash, time.Hour) }
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			store := newLookupStore(t, tSettings, "relay_bound_block_first")

			sm, p, _, client := newInvHarness(t, tSettings, store, blockchain2.FSMStateRUNNING)
			blockActedOn := tc.arrange(client, sm)

			// Transaction items first, the block last.
			inv := wire.NewMsgInv()
			require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{0x7a})))
			require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{0x7b})))
			require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &blockHash)))

			// The handler returns with the block acted on and the transaction
			// items queued, having read nothing from the store itself.
			sm.handleInvMsg(&invMsg{inv: inv, peer: p})

			require.True(t, blockActedOn(), "the block item waited behind the transaction lookups")
			require.Zero(t, store.batches.Load(), "the handler goroutine does no store read of its own")
			require.Equal(t, int64(2), sm.invQueued.Load())

			sm.lookupInvBatch(<-sm.invQueue)

			require.Equal(t, int64(1), store.batches.Load(), "both transactions must be looked up in one batch")
		})
	}
}

// TestHandleInvMsg_SkipsTransactionItemsOutsideRunning pins that a transaction
// inventory item outside RUNNING costs no store read: the item is dropped
// before the lookup, as processInvMsg already dropped it.
func TestHandleInvMsg_SkipsTransactionItemsOutsideRunning(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	store := newLookupStore(t, tSettings, "relay_bound_catching")
	sm, p, state, _ := newInvHarness(t, tSettings, store, blockchain2.FSMStateCATCHINGBLOCKS)

	inv := wire.NewMsgInv()
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{0x7a})))

	sm.handleInvMsg(&invMsg{inv: inv, peer: p})

	require.Zero(t, store.gets.Load()+store.batches.Load())
	require.Zero(t, state.requestQueue.Length())
	require.Zero(t, sm.invQueued.Load(), "nothing is queued for a lookup outside RUNNING")
	require.Empty(t, sm.invQueue)
}

func newRelayHarness(t *testing.T, queueLen int, budget int64) (*SyncManager, *peerpkg.Peer) {
	t.Helper()

	// A drop increments prometheusLegacyNetsyncRelayedTxDropped, which is nil
	// until the metrics are registered; New does this in production.
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "localhost:8333")
	require.NoError(t, err)

	sm := &SyncManager{
		logger:             ulogger.TestLogger{},
		settings:           tSettings,
		ctx:                context.Background(),
		relayTxQueue:       make(chan *txMsg, queueLen),
		relayTxBytesBudget: budget,
	}

	return sm, p
}

// testRelayBudget is the byte budget the relay tests run with unless they are
// about the budget itself: the production default, as the setting's loader
// gives it.
const testRelayBudget int64 = 256 << 20

// queueTxWithin runs QueueTx and fails the test if it does not return, since
// it runs on the peer's read loop and must never wait.
func queueTxWithin(t *testing.T, sm *SyncManager, tx *bsvutil.Tx, p *peerpkg.Peer) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		sm.QueueTx(tx, p, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "QueueTx blocked the peer's read loop")
	}
}

// TestQueueTx_DropsWhenTheRelayQueueIsFull pins that a relayed transaction
// arriving with the queue full is dropped at once, not waited for, and that the
// bytes it would have held are not left charged.
func TestQueueTx_DropsWhenTheRelayQueueIsFull(t *testing.T) {
	sm, p := newRelayHarness(t, 1, testRelayBudget)
	tx := bsvutil.NewTx(wire.NewMsgTx(1))
	size := int64(tx.MsgTx().SerializeSize())

	queueTxWithin(t, sm, tx, p)
	require.Len(t, sm.relayTxQueue, 1)

	dropped := testutil.ToFloat64(prometheusLegacyNetsyncRelayedTxDropped)

	queueTxWithin(t, sm, tx, p)
	require.Len(t, sm.relayTxQueue, 1, "a transaction over the queue's capacity must be dropped")
	require.Equal(t, size, sm.relayTxBytes.Load(), "a dropped transaction must not stay charged to the byte budget")
	require.InDelta(t, dropped+1, testutil.ToFloat64(prometheusLegacyNetsyncRelayedTxDropped), 0, "a drop must be counted")
}

// TestQueueTx_TakesOneTransactionLargerThanTheBudget pins the exception to the
// byte bound: a transaction larger than the byte budget is queued when
// nothing else is held, so it is not refused for ever, and a second one is
// dropped while the first is held.
func TestQueueTx_TakesOneTransactionLargerThanTheBudget(t *testing.T) {
	const budget int64 = 1 << 20

	sm, p := newRelayHarness(t, 4, budget)

	big := wire.NewMsgTx(1)
	big.AddTxOut(wire.NewTxOut(0, make([]byte, budget+1)))
	tx := bsvutil.NewTx(big)
	size := int64(big.SerializeSize())
	require.Greater(t, size, budget)

	queueTxWithin(t, sm, tx, p)
	require.Len(t, sm.relayTxQueue, 1, "a transaction larger than the budget must be taken when nothing else is held")
	require.Equal(t, size, sm.relayTxBytes.Load())

	dropped := testutil.ToFloat64(prometheusLegacyNetsyncRelayedTxDropped)

	queueTxWithin(t, sm, tx, p)
	require.Len(t, sm.relayTxQueue, 1, "a second transaction over the budget must be dropped while the first is held")
	require.Equal(t, size, sm.relayTxBytes.Load())
	require.InDelta(t, dropped+1, testutil.ToFloat64(prometheusLegacyNetsyncRelayedTxDropped), 0, "a drop must be counted")
}

// TestQueueTx_DropsOverTheByteBudget pins the byte bound: a transaction that
// would take the held bytes over the budget the setting gives is dropped, and
// one that fits it exactly is queued.
func TestQueueTx_DropsOverTheByteBudget(t *testing.T) {
	const budget int64 = 1 << 20

	sm, p := newRelayHarness(t, 4, budget)
	tx := bsvutil.NewTx(wire.NewMsgTx(1))
	size := int64(tx.MsgTx().SerializeSize())

	sm.relayTxBytes.Store(budget - size + 1)
	queueTxWithin(t, sm, tx, p)
	require.Empty(t, sm.relayTxQueue, "a transaction over the byte budget must be dropped")
	require.Equal(t, budget-size+1, sm.relayTxBytes.Load())

	sm.relayTxBytes.Store(budget - size)
	queueTxWithin(t, sm, tx, p)
	require.Len(t, sm.relayTxQueue, 1, "a transaction that fits the budget exactly must be queued")
}

// TestQueueTx_ZeroBudgetTakesOneTransactionAtATime pins what the setting's
// floor means: with legacy_relayTxBytesBudget at zero the first transaction is
// taken because nothing is held, and the next is dropped until it is handled.
func TestQueueTx_ZeroBudgetTakesOneTransactionAtATime(t *testing.T) {
	sm, p := newRelayHarness(t, 8, 0)
	tx := bsvutil.NewTx(wire.NewMsgTx(1))

	queueTxWithin(t, sm, tx, p)
	require.Len(t, sm.relayTxQueue, 1, "the first transaction is taken when nothing is held")

	queueTxWithin(t, sm, tx, p)
	require.Len(t, sm.relayTxQueue, 1, "the second is dropped while the first is held")
}

// TestRelayTxWorker_HandsBackTheBytesAfterHandlingATx runs the real block
// handler, which starts the workers, and pins that a worker answers the
// transaction and hands its bytes back. Without that the budget fills and
// every relayed transaction after it is dropped for the life of the node.
func TestRelayTxWorker_HandsBackTheBytesAfterHandlingATx(t *testing.T) {
	sm, p := newRelayHarness(t, 4, testRelayBudget)
	sm.relayTxWorkers = 2

	running := blockchain2.FSMStateRUNNING
	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm.blockchainClient = client
	sm.peerStates = txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState]()
	sm.msgChan = make(chan interface{})
	sm.quit = make(chan struct{})
	sm.handlerDone = make(chan struct{})

	go sm.blockHandler()

	t.Cleanup(func() {
		close(sm.quit)
		<-sm.handlerDone
	})

	done := make(chan struct{}, 1)
	sm.QueueTx(bsvutil.NewTx(wire.NewMsgTx(1)), p, done)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "no worker handled the relayed transaction")
	}

	require.Zero(t, sm.relayTxBytes.Load(), "the worker kept the transaction's bytes charged after handling it")
}

// TestRelayTxWorkerCount pins that the pool is at least twice the largest
// per-transaction batch on the validation path, so that pool never stops a
// batch filling, and never below relayTxMinWorkers.
func TestRelayTxWorkerCount(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.UtxoStore.StoreBatcherSize = 2048
	tSettings.UtxoStore.SpendBatcherSize = 1024
	tSettings.Validator.SendBatchSize = 1000
	require.Equal(t, 4096, relayTxWorkerCount(tSettings))

	tSettings.Validator.SendBatchSize = 3000
	require.Equal(t, 6000, relayTxWorkerCount(tSettings))

	tSettings.UtxoStore.StoreBatcherSize = 10
	tSettings.UtxoStore.SpendBatcherSize = 10
	tSettings.Validator.SendBatchSize = 0
	require.Equal(t, relayTxMinWorkers, relayTxWorkerCount(tSettings))
}

// TestDrainRequestQueue_SendsATransactionTheWorkerMarked pins that the drain
// no longer skips a transaction whose hash is in the request set: the lookup
// worker marks a hash before the store read and then appends it, so a check
// here would drop every transaction the worker asked for.
func TestDrainRequestQueue_SendsATransactionTheWorkerMarked(t *testing.T) {
	sm, p, state, _ := newInvHarness(t, test.CreateBaseTestSettings(t), nil, blockchain2.FSMStateRUNNING)

	h := chainhash.Hash{0x7a}
	sm.requestedTxns.Set(h, struct{}{})
	state.requestQueue.Append(wire.NewInvVect(wire.InvTypeTx, &h))

	gdmsg := sm.drainRequestQueue(p, state)
	require.Len(t, gdmsg.InvList, 1, "the request set is checked before the read now, not here")
}

// gatedFSMChain is the real LocalClient over a sqlitememory store whose FSM
// lookup waits for gate to close, the way a slow blockchain service holds up
// handleNewPeerMsg. entered is closed on the first lookup.
type gatedFSMChain struct {
	blockchain2.ClientI

	gate      chan struct{}
	entered   chan struct{}
	enterOnce sync.Once
	gateOnce  sync.Once
}

func (c *gatedFSMChain) GetFSMCurrentState(ctx context.Context) (*blockchain2.FSMStateType, error) {
	c.enterOnce.Do(func() { close(c.entered) })
	<-c.gate

	return c.ClientI.GetFSMCurrentState(ctx)
}

func (c *gatedFSMChain) open() {
	c.gateOnce.Do(func() { close(c.gate) })
}

// relayRegistrationHarness is a SyncManager running its real block handler and
// relay workers, with a blockchain client whose FSM lookup is gated, a
// validator that records what it is asked to validate, and two ends of one
// live connection as peers, since handleNewPeerMsg registers only a connected
// peer.
type relayRegistrationHarness struct {
	sm        *SyncManager
	peer      *peerpkg.Peer
	other     *peerpkg.Peer
	chain     *gatedFSMChain
	validated chan chainhash.Hash
	stopped   bool
}

var relayRegistrationStoreCounter atomic.Int64

func newRelayRegistrationHarness(t *testing.T, queueLen int) *relayRegistrationHarness {
	t.Helper()

	sm, _ := newRelayHarness(t, queueLen, testRelayBudget)
	sm.relayTxWorkers = 2

	chainURL, err := url.Parse("sqlitememory:///relay_registration_" + strconv.FormatInt(relayRegistrationStoreCounter.Add(1), 10))
	require.NoError(t, err)

	bcStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, chainURL, sm.settings)
	require.NoError(t, err)

	local, err := blockchain2.NewLocalClient(ulogger.TestLogger{}, sm.settings, bcStore, nil, nil)
	require.NoError(t, err)

	h := &relayRegistrationHarness{
		sm:        sm,
		chain:     &gatedFSMChain{ClientI: local, gate: make(chan struct{}), entered: make(chan struct{})},
		validated: make(chan chainhash.Hash, 16),
	}

	sm.blockchainClient = h.chain
	sm.chainParams = &chaincfg.MainNetParams
	sm.validationClient = &validator.MockValidator{ValidateFunc: func(_ context.Context, tx *bt.Tx) (*meta.Data, error) {
		h.validated <- *tx.TxIDChainHash()

		return &meta.Data{}, nil
	}}
	sm.peerNotifier = &recordingNotifier{}
	sm.peerStates = txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState]()
	sm.orphanTxs = expiringmap.New[chainhash.Hash, *orphanTxAndParents](time.Minute)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}]()
	sm.requestedTxns = expiringmap.New[chainhash.Hash, struct{}](time.Minute)
	sm.msgChan = make(chan interface{})
	sm.quit = make(chan struct{})
	sm.handlerDone = make(chan struct{})

	// Services are left at zero, so neither peer is a sync candidate and
	// registering one does not start a sync.
	peerCfg := peerpkg.Config{
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}

	h.other, h.peer, err = MakeConnectedPeers(t, peerCfg, peerCfg, 61)
	require.NoError(t, err)

	go sm.blockHandler()

	t.Cleanup(func() {
		h.chain.open()
		h.stop()
		h.peer.DisconnectWithInfo("test over")
		h.other.DisconnectWithInfo("test over")
		_ = bcStore.Close(context.Background())
	})

	return h
}

// stop stops the block handler and waits for it.
func (h *relayRegistrationHarness) stop() {
	if h.stopped {
		return
	}

	h.stopped = true

	close(h.sm.quit)
	<-h.sm.handlerDone
}

// relayedTx is a real transaction in the wire form a peer relays it in.
func relayedTx(t *testing.T) *bsvutil.Tx {
	t.Helper()

	msg := wire.NewMsgTx(1)
	require.NoError(t, msg.Deserialize(bytes.NewReader(transactions.CreateTestTransactionChainWithCount(t, 3)[1].Bytes())))

	return bsvutil.NewTx(msg)
}

// waitDone fails the test unless done is answered within the timeout.
func waitDone(t *testing.T, done chan struct{}, what string) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, what)
	}
}

// TestQueueTx_HandlesATransactionOnlyAfterItsPeerIsRegistered reproduces the
// reordering the relay queue introduced: both handshake handlers register a
// peer with NewPeer and do not wait, so the transactions the peer sends next
// reach QueueTx while the block handler is still registering it, here held in
// the FSM lookup. The shared message queue used to run the registration
// first. The transaction must not be discarded as coming from an unknown
// peer: it must be validated, and only once the peer is registered.
func TestQueueTx_HandlesATransactionOnlyAfterItsPeerIsRegistered(t *testing.T) {
	h := newRelayRegistrationHarness(t, 4)

	go h.sm.NewPeer(h.peer, nil)

	select {
	case <-h.chain.entered:
	case <-time.After(5 * time.Second):
		require.Fail(t, "the block handler never started registering the peer")
	}

	tx := relayedTx(t)
	done := make(chan struct{}, 1)
	queueTxWithinDone(t, h.sm, tx, h.peer, done)

	// Handled while the registration is held means handled against an
	// unknown peer, which discards it.
	handledEarly := false
	select {
	case <-done:
		handledEarly = true
	case <-time.After(300 * time.Millisecond):
	}

	h.chain.open()

	if !handledEarly {
		waitDone(t, done, "the transaction was never handled after its peer registered")
	}

	select {
	case got := <-h.validated:
		require.Equal(t, *tx.Hash(), got)
	default:
		require.Fail(t, "the transaction was discarded instead of validated")
	}

	require.False(t, handledEarly, "the transaction was handled before its peer was registered")
	require.Zero(t, h.sm.relayTxBytes.Load(), "the transaction's bytes were not handed back")
}

// TestQueueTx_DropsHeldTransactionsOfAPeerThatLeftBeforeRegistration pins the
// other ending: a peer that disconnects before its registration is handled is
// never registered, so what it held is dropped, as handleTxMsg dropped a
// transaction from an unknown peer. Its done channel is answered and its bytes
// given back, or the budget would stay charged for the life of the node.
func TestQueueTx_DropsHeldTransactionsOfAPeerThatLeftBeforeRegistration(t *testing.T) {
	h := newRelayRegistrationHarness(t, 4)

	// The other end's registration holds the block handler in the FSM lookup,
	// so the peer's registration waits in msgChan behind it.
	go h.sm.NewPeer(h.other, nil)

	select {
	case <-h.chain.entered:
	case <-time.After(5 * time.Second):
		require.Fail(t, "the block handler never started registering the first peer")
	}

	go h.sm.NewPeer(h.peer, nil)

	require.Eventually(t, func() bool {
		h.sm.awaitingRegistrationMu.Lock()
		defer h.sm.awaitingRegistrationMu.Unlock()

		_, awaiting := h.sm.awaitingRegistration[h.peer]

		return awaiting
	}, 5*time.Second, 5*time.Millisecond)

	tx := relayedTx(t)
	done := make(chan struct{}, 1)
	queueTxWithinDone(t, h.sm, tx, h.peer, done)

	h.peer.DisconnectWithInfo("left before registration")
	require.Eventually(t, func() bool { return !h.peer.Connected() }, 5*time.Second, 5*time.Millisecond)

	dropped := testutil.ToFloat64(prometheusLegacyNetsyncRelayedTxDropped)

	h.chain.open()
	waitDone(t, done, "the held transaction's done channel was never answered")

	require.False(t, h.sm.peerStates.Exists(h.peer), "a peer that left must not be registered")
	require.Empty(t, h.validated, "a transaction from a peer that left must not be validated")
	require.Equal(t, dropped+1, testutil.ToFloat64(prometheusLegacyNetsyncRelayedTxDropped))
	require.Zero(t, h.sm.relayTxBytes.Load(), "the dropped transaction's bytes were not handed back")
}

// TestQueueTx_DropsHeldTransactionsWhenTheHandlerStops pins that a
// transaction held for a registration the block handler will now never handle
// is answered and its bytes given back when the handler stops.
func TestQueueTx_DropsHeldTransactionsWhenTheHandlerStops(t *testing.T) {
	h := newRelayRegistrationHarness(t, 4)

	h.sm.markAwaitingRegistration(h.peer)

	done := make(chan struct{}, 1)
	queueTxWithinDone(t, h.sm, relayedTx(t), h.peer, done)
	require.NotZero(t, h.sm.relayTxBytes.Load(), "harness check: the transaction is held")

	h.stop()

	waitDone(t, done, "the held transaction's done channel was never answered")
	require.Empty(t, h.validated)
	require.Zero(t, h.sm.relayTxBytes.Load(), "the dropped transaction's bytes were not handed back")
}

// TestQueueTx_HoldsAtMostAQueuesWorthPerPeer pins the count bound on what one
// peer may hold before it is registered: as many transactions as the relay
// queue holds, since that is all the registration can hand on without
// dropping. The next one is dropped at once, without waiting.
func TestQueueTx_HoldsAtMostAQueuesWorthPerPeer(t *testing.T) {
	sm, p := newRelayHarness(t, 2, testRelayBudget)
	sm.markAwaitingRegistration(p)

	tx := bsvutil.NewTx(wire.NewMsgTx(1))
	size := int64(tx.MsgTx().SerializeSize())

	queueTxWithin(t, sm, tx, p)
	queueTxWithin(t, sm, tx, p)

	done := make(chan struct{}, 1)
	queueTxWithinDone(t, sm, tx, p, done)
	waitDone(t, done, "the transaction over the hold limit was not answered at once")

	require.Len(t, sm.awaitingRegistration[p], 2)
	require.Empty(t, sm.relayTxQueue, "nothing goes to the queue before the peer is registered")
	require.Equal(t, 2*size, sm.relayTxBytes.Load(), "held transactions stay charged, the dropped one does not")
}

// queueTxWithinDone is queueTxWithin with a done channel.
func queueTxWithinDone(t *testing.T, sm *SyncManager, tx *bsvutil.Tx, p *peerpkg.Peer, done chan struct{}) {
	t.Helper()

	returned := make(chan struct{})
	go func() {
		sm.QueueTx(tx, p, done)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		require.Fail(t, "QueueTx blocked the peer's read loop")
	}
}
