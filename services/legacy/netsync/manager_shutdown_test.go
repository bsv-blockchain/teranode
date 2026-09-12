package netsync

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// Hold an inventory lookup immediately before it enters the real SQL batcher.
type heldInventoryStore struct {
	utxo.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (s *heldInventoryStore) Get(ctx context.Context, hash *chainhash.Hash, selected ...fields.FieldName) (*meta.Data, error) {
	s.calls.Add(1)
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.Store.Get(ctx, hash, selected...)
}

func TestStop_WaitsForInventoryBeforeClosingStore(t *testing.T) {
	sm, p, state := newHeaderProvenanceManager(t)
	sm.quit = make(chan struct{})
	sm.handlerDone = make(chan struct{})
	sm.msgChan = make(chan interface{}, 1)
	sm.orphanTxs = expiringmap.New[chainhash.Hash, *orphanTxAndParents](time.Hour)
	sm.requestedTxns = expiringmap.New[chainhash.Hash, struct{}](time.Hour)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}]()
	sm.headersFirstMode.Store(false)
	state.requestQueue = txmap.NewSyncedSlice[wire.InvVect](1)
	state.requestedTxns = expiringmap.New[chainhash.Hash, struct{}](time.Hour)
	t.Cleanup(state.requestedTxns.Stop)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := sql.New(context.Background(), ulogger.TestLogger{}, sm.settings, storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	held := &heldInventoryStore{Store: store, entered: make(chan struct{}), release: make(chan struct{})}
	sm.utxoStore = held
	sm.Start()

	inv := wire.NewMsgInv()
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{1})))
	handled := make(chan struct{})
	go func() {
		sm.handleInvMsg(&invMsg{inv: inv, peer: p})
		close(handled)
	}()
	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("inventory lookup did not start")
	}

	remaining := 2
	stopped := make(chan error, 2)
	go func() { stopped <- sm.Stop() }()
	<-sm.quit
	// The service and its context watcher can call Stop concurrently. Both must join.
	go func() { stopped <- sm.Stop() }()
	select {
	case err := <-stopped:
		remaining--
		t.Errorf("Stop returned before the inventory lookup completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(held.release)
	<-handled
	for i := 0; i < remaining; i++ {
		select {
		case err := <-stopped:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not finish after inventory completed")
		}
	}
	// A delayed Kafka callback must not submit another lookup after shutdown.
	sm.handleInvMsg(&invMsg{inv: inv, peer: p})
	require.EqualValues(t, 1, held.calls.Load())
}

func TestStop_BeforeStart(t *testing.T) {
	sm, _, _ := newHeaderProvenanceManager(t)
	sm.quit = make(chan struct{})
	sm.handlerDone = make(chan struct{})
	sm.orphanTxs = expiringmap.New[chainhash.Hash, *orphanTxAndParents](time.Hour)
	sm.requestedTxns = expiringmap.New[chainhash.Hash, struct{}](time.Hour)
	stopped := make(chan error, 1)
	go func() { stopped <- sm.Stop() }()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop waited for a block handler that was never started")
	}
	sm.Start()
	require.Zero(t, atomic.LoadInt32(&sm.started), "Start must not revive a stopped manager")
	require.NoError(t, sm.Stop())
}

func TestStop_WhilePaused(t *testing.T) {
	sm, _, _ := newHeaderProvenanceManager(t)
	sm.quit = make(chan struct{})
	sm.handlerDone = make(chan struct{})
	sm.msgChan = make(chan interface{})
	sm.orphanTxs = expiringmap.New[chainhash.Hash, *orphanTxAndParents](time.Hour)
	sm.requestedTxns = expiringmap.New[chainhash.Hash, struct{}](time.Hour)
	sm.Start()
	// The unbuffered queue makes Pause wait until blockHandler receives it.
	unpause := sm.Pause()
	defer close(unpause)
	stopped := make(chan error, 1)
	go func() { stopped <- sm.Stop() }()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop waited for the paused caller to resume")
	}
}

func TestShutdown_ReleasesBlockedQueueCalls(t *testing.T) {
	for _, operation := range []string{"inventory", "headers", "block", "transaction", "new peer", "done peer", "sync peer", "pause"} {
		t.Run(operation, func(t *testing.T) {
			sm, p, _ := newHeaderProvenanceManager(t)
			sm.quit = make(chan struct{})
			sm.msgChan = make(chan interface{}) // no consumer can accept the send
			sm.orphanTxs = expiringmap.New[chainhash.Hash, *orphanTxAndParents](time.Hour)
			sm.requestedTxns = expiringmap.New[chainhash.Hash, struct{}](time.Hour)
			returned := make(chan struct{})
			go func() {
				switch operation {
				case "inventory":
					sm.QueueInv(wire.NewMsgInv(), p)
				case "headers":
					sm.QueueHeaders(wire.NewMsgHeaders(), p)
				case "block":
					sm.QueueBlock(nil, p, make(chan error, 1))
				case "transaction":
					sm.QueueTx(nil, p, nil)
				case "new peer":
					sm.NewPeer(p, nil)
				case "done peer":
					sm.DonePeer(p, nil)
				case "sync peer":
					sm.SyncPeerID()
				case "pause":
					close(sm.Pause())
				}
				close(returned)
			}()
			select {
			case <-returned:
				t.Fatal("queue call returned without a consumer or shutdown")
			case <-time.After(50 * time.Millisecond):
			}
			require.NoError(t, sm.Stop())
			select {
			case <-returned:
			case <-time.After(5 * time.Second):
				t.Fatal("queue call remained blocked after shutdown")
			}
		})
	}
}
