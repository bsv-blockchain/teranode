package netsync

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// concurrencyStore wraps a real store and records how many Gets run at once.
// Each Get holds for a moment so that an unbounded caller visibly piles up.
type concurrencyStore struct {
	utxo.Store

	inFlight atomic.Int64
	peak     atomic.Int64
	calls    atomic.Int64
}

func (s *concurrencyStore) Get(ctx context.Context, hash *chainhash.Hash, fieldNames ...fields.FieldName) (*meta.Data, error) {
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	s.calls.Add(1)

	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}

	time.Sleep(2 * time.Millisecond)

	return s.Store.Get(ctx, hash, fieldNames...)
}

// TestHandleInvMsg_BoundsTransactionChecks reproduces the inventory fan-out
// that took Hetzner mainnet to 223,118 goroutines on 2026-10-19: one inv
// message full of transaction announcements, each checked against the UTXO
// store. Every item used to get its own goroutine at once. The checks must now
// run no more than maxConcurrentTxInvChecks at a time, and every item must
// still be checked.
func TestHandleInvMsg_BoundsTransactionChecks(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///relay_bound_inv")
	require.NoError(t, err)

	realStore, err := sql.New(t.Context(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	store := &concurrencyStore{Store: realStore}

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "localhost:8333")
	require.NoError(t, err)

	running := blockchain2.FSMStateRUNNING
	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		ctx:              context.Background(),
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		blockchainClient: client,
		utxoStore:        store,
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](10),
		requestedTxns:    expiringmap.New[chainhash.Hash, struct{}](time.Hour),
		txInvChecks:      semaphore.NewWeighted(maxConcurrentTxInvChecks),
	}
	sm.headersFirstMode.Store(false)

	state := &peerSyncState{
		requestQueue:  txmap.NewSyncedSlice[wire.InvVect](wire.MaxInvPerMsg),
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](time.Hour),
	}
	sm.peerStates.Set(p, state)
	sm.storeSyncPeer(p, &syncPeerState{})

	const items = 4 * maxConcurrentTxInvChecks

	inv := wire.NewMsgInv()
	for i := 0; i < items; i++ {
		h := chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), 0x7a}
		require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &h)))
	}

	sm.handleInvMsg(&invMsg{inv: inv, peer: p})

	require.Equal(t, int64(items), store.calls.Load(), "every announced transaction must still be checked")
	require.LessOrEqual(t, store.peak.Load(), int64(maxConcurrentTxInvChecks),
		"transaction inventory checks ran unbounded")
}

// TestHandleInvMsg_BlockItemsDoNotWaitForTransactionSlots pins that a block
// announcement is acted on even while every transaction-check slot is taken,
// including when it comes after transaction items in the same message. The
// slots are shared by every peer, so a block that waited for one would wait on
// store latency across the whole node. Both outcomes of a block item are
// covered: a block the chain holds ends in a locator lookup for a getblocks,
// and a block the node lacks ends in a getdata.
func TestHandleInvMsg_BlockItemsDoNotWaitForTransactionSlots(t *testing.T) {
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

			storeURL, err := url.Parse("sqlitememory:///relay_bound_block_first")
			require.NoError(t, err)

			realStore, err := sql.New(t.Context(), ulogger.TestLogger{}, tSettings, storeURL)
			require.NoError(t, err)

			store := &concurrencyStore{Store: realStore}

			p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "localhost:8333")
			require.NoError(t, err)

			running := blockchain2.FSMStateRUNNING
			client := &blockchain2.Mock{}
			client.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

			slots := semaphore.NewWeighted(maxConcurrentTxInvChecks)

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
				txInvChecks:      slots,
			}
			sm.headersFirstMode.Store(false)

			blockActedOn := tc.arrange(client, sm)

			state := &peerSyncState{
				requestQueue:  txmap.NewSyncedSlice[wire.InvVect](wire.MaxInvPerMsg),
				requestedTxns: expiringmap.New[chainhash.Hash, struct{}](time.Hour),
			}
			sm.peerStates.Set(p, state)
			sm.storeSyncPeer(p, &syncPeerState{})

			// Transaction items first, the block last.
			inv := wire.NewMsgInv()
			require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{0x7a})))
			require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{0x7b})))
			require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &blockHash)))

			// Every slot is held, as it would be with other peers' checks in flight.
			require.True(t, slots.TryAcquire(maxConcurrentTxInvChecks))

			done := make(chan struct{})
			go func() {
				sm.handleInvMsg(&invMsg{inv: inv, peer: p})
				close(done)
			}()

			require.True(t, WaitUntil(blockActedOn, 2*time.Second),
				"the block item waited behind the transaction-check bound")

			slots.Release(maxConcurrentTxInvChecks)

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				require.Fail(t, "handleInvMsg did not finish once the slots were free")
			}

			require.Equal(t, int64(2), store.calls.Load(), "both transactions must still be checked once the slots are free")
		})
	}
}

// TestHandleInvMsg_SkipsTransactionItemsOutsideRunning pins that a transaction
// inventory item outside RUNNING costs no store read and no goroutine: the
// item is dropped before the check, as processInvMsg already dropped it after.
func TestHandleInvMsg_SkipsTransactionItemsOutsideRunning(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///relay_bound_catching")
	require.NoError(t, err)

	realStore, err := sql.New(t.Context(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	store := &concurrencyStore{Store: realStore}

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "localhost:8333")
	require.NoError(t, err)

	catching := blockchain2.FSMStateCATCHINGBLOCKS
	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&catching, nil)

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		ctx:              context.Background(),
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		blockchainClient: client,
		utxoStore:        store,
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](10),
		txInvChecks:      semaphore.NewWeighted(maxConcurrentTxInvChecks),
	}

	state := &peerSyncState{
		requestQueue: txmap.NewSyncedSlice[wire.InvVect](wire.MaxInvPerMsg),
	}
	sm.peerStates.Set(p, state)
	sm.storeSyncPeer(p, &syncPeerState{})

	inv := wire.NewMsgInv()
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{0x7a})))

	sm.handleInvMsg(&invMsg{inv: inv, peer: p})

	require.Zero(t, store.calls.Load())
	require.Zero(t, state.requestQueue.Length())
}

// TestQueueTx_DropsWhenRelayBoundIsFull pins the relayed-transaction bound. A
// transaction that arrives with every slot taken is dropped at once rather
// than queued, so the peer's read loop is never held; once a slot frees, the
// next transaction is queued again.
func TestQueueTx_DropsWhenRelayBoundIsFull(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "localhost:8333")
	require.NoError(t, err)

	sm := &SyncManager{
		logger:     ulogger.TestLogger{},
		settings:   tSettings,
		ctx:        context.Background(),
		msgChan:    make(chan interface{}, 4),
		relayedTxs: semaphore.NewWeighted(1),
	}

	tx := bsvutil.NewTx(wire.NewMsgTx(1))

	require.True(t, sm.relayedTxs.TryAcquire(1), "take the only slot")

	done := make(chan struct{})
	go func() {
		sm.QueueTx(tx, p, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "QueueTx blocked the peer's read loop with the relay bound full")
	}

	require.Zero(t, len(sm.msgChan), "a transaction over the bound must be dropped, not queued")

	sm.relayedTxs.Release(1)
	sm.QueueTx(tx, p, nil)
	require.Equal(t, 1, len(sm.msgChan), "with a slot free the transaction is queued")
}

// TestBlockHandler_ReleasesTheRelaySlotAfterHandlingATx pins the other half of
// the relayed-transaction bound: the slot QueueTx takes is handed back once the
// block handler has handled the transaction. Without that release the bound
// fills after maxConcurrentRelayedTxs transactions and every relayed
// transaction after it is dropped for the life of the node.
func TestBlockHandler_ReleasesTheRelaySlotAfterHandlingATx(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "localhost:8333")
	require.NoError(t, err)

	running := blockchain2.FSMStateRUNNING
	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		ctx:              context.Background(),
		blockchainClient: client,
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		msgChan:          make(chan interface{}),
		quit:             make(chan struct{}),
		handlerDone:      make(chan struct{}),
		relayedTxs:       semaphore.NewWeighted(1),
	}

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
		require.Fail(t, "the block handler never handled the relayed transaction")
	}

	// The release runs after the reply, so poll for the slot rather than read
	// it once.
	require.True(t, WaitUntil(func() bool { return sm.relayedTxs.TryAcquire(1) }, 2*time.Second),
		"the block handler kept the relay slot after handling the transaction")
}
