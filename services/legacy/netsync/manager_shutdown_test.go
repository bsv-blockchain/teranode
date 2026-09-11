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
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// Exhaust the real SQL connection pool so the inventory Get has entered the
// batcher and reached SQL, rather than pausing a wrapper before Store.Get.
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
	var closeStoreOnce sync.Once
	closeStore := func() { closeStoreOnce.Do(func() { require.NoError(t, store.Close(context.Background())) }) }
	t.Cleanup(closeStore)
	sm.utxoStore = store
	sm.Start()

	store.RawDB().SetMaxOpenConns(1)
	connection, err := store.RawDB().Conn(t.Context())
	require.NoError(t, err)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { require.NoError(t, connection.Close()) }) }
	t.Cleanup(release)
	waits := store.RawDB().Stats().WaitCount
	inv := wire.NewMsgInv()
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{1})))
	handled := make(chan struct{})
	go func() {
		defer close(handled)
		sm.handleInvMsg(&invMsg{inv: inv, peer: p})
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-handled:
		case <-time.After(5 * time.Second):
			t.Error("inventory handler did not finish")
		}
		require.NoError(t, sm.Stop())
	})
	require.Eventually(t, func() bool {
		return store.RawDB().Stats().WaitCount > waits
	}, 5*time.Second, time.Millisecond, "the batcher must reach the real SQL connection pool")

	stopped := make(chan error, 2)
	go func() { stopped <- sm.Stop() }()
	<-sm.quit
	go func() { stopped <- sm.Stop() }()
	require.Never(t, func() bool { return len(stopped) != 0 }, 100*time.Millisecond, time.Millisecond,
		"both Stop callers must wait for the SQL read")
	release()
	for range 2 {
		select {
		case err := <-stopped:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not finish after SQL became available")
		}
	}
	closeStore()
	// Delayed network/Kafka handlers must not submit work to the closed batcher.
	require.NotPanics(t, func() {
		sm.handleInvMsg(&invMsg{inv: inv, peer: p})
		sm.handleTxMsg(nil)
		sm.handleHeadersMsg(nil)
	})
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
