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
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Hold a real UTXO read open to reproduce an inventory handler that outlives
// the message loop. The store must remain usable until that read has finished.
type heldInventoryStore struct {
	utxo.Store
	entered chan struct{}
	release chan struct{}
	reads   atomic.Int32
}

func (s *heldInventoryStore) Get(ctx context.Context, hash *chainhash.Hash, fieldNames ...fields.FieldName) (*meta.Data, error) {
	if s.reads.Add(1) == 1 {
		close(s.entered)
		<-s.release
	}
	return s.Store.Get(ctx, hash, fieldNames...)
}

func TestStopWaitsForInventoryReads(t *testing.T) {
	ctx := t.Context()
	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)
	chainStore, err := blockchainstore.NewStore(logger, storeURL, settings)
	require.NoError(t, err)
	client, err := blockchain.NewLocalClient(logger, settings, chainStore, nil, nil)
	require.NoError(t, err)
	store, err := sql.New(ctx, logger, settings, storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	held := &heldInventoryStore{Store: store, entered: make(chan struct{}), release: make(chan struct{})}
	p := peer.NewInboundPeer(logger, settings, &peer.Config{})
	sm := &SyncManager{
		ctx: ctx, logger: logger, blockchainClient: client, utxoStore: held,
		peerStates:  txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		handlerDone: make(chan struct{}), quit: make(chan struct{}),
		orphanTxs:       expiringmap.New[chainhash.Hash, *orphanTxAndParents](time.Minute),
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		rejectedTxns:    txmap.NewSyncedMap[chainhash.Hash, struct{}](),
	}
	state := &peerSyncState{
		requestQueue:  txmap.NewSyncedSlice[wire.InvVect](1),
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(state.requestedTxns.Stop)
	sm.peerStates.Set(p, state)
	sm.storeSyncPeer(p, nil)
	msg := &invMsg{peer: p, inv: &wire.MsgInv{InvList: []*wire.InvVect{{Type: wire.InvTypeTx}}}}
	readDone := make(chan struct{})
	go func() { defer close(readDone); sm.handleInvMsg(msg) }()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(held.release) }) }
	t.Cleanup(func() { release(); <-readDone })
	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("inventory handler did not reach the UTXO read")
	}
	// Simulate the message loop having exited while its inventory worker is busy.
	close(sm.handlerDone)
	stopped := make(chan error, 2)
	go func() { stopped <- sm.Stop() }()
	<-sm.quit
	go func() { stopped <- sm.Stop() }()
	require.Never(t, func() bool { return len(stopped) != 0 }, 100*time.Millisecond, time.Millisecond,
		"every Stop caller must wait for the in-flight UTXO read")
	release()
	for range 2 {
		select {
		case err := <-stopped:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not finish after the inventory read")
		}
	}
	// A queued inventory goroutine may start late; shutdown must reject it
	// before touching the store, which the daemon can now safely close.
	sm.handleInvMsg(msg)
	require.EqualValues(t, 1, held.reads.Load())
}
