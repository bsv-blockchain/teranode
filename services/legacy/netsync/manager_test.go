// Copyright (c) 2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/txscript"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// nullTime is an empty time defined for convenience
var nullTime time.Time

type testConfig struct {
	dbName      string
	chainParams *chaincfg.Params
}

type testContext struct {
	cfg          testConfig
	peerNotifier *MockPeerNotifier
	syncManager  *SyncManager
}

func (tc *testContext) Setup(t *testing.T, config *testConfig) error {
	tc.cfg = *config

	tSettings := test.CreateBaseTestSettings(t)

	peerNotifier := NewMockPeerNotifier()

	storeURL, _ := url.Parse("sqlitememory://")

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	if err != nil {
		return errors.NewServiceError("failed to create blockchain store", err)
	}

	blockchainClient, err := blockchain2.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
	if err != nil {
		return errors.NewServiceError("failed to create blockchain client", err)
	}

	blockAssemblyClient, err := blockassembly.NewClient(context.Background(), ulogger.TestLogger{}, tSettings)
	if err != nil {
		return errors.NewServiceError("failed to create block assembly client", err)
	}

	ctx := context.Background()

	logger := ulogger.TestLogger{}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	if err != nil {
		return errors.NewServiceError("failed to create utxo store", err)
	}

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	if err != nil {
		return errors.NewServiceError("failed to create utxo store", err)
	}

	validatorClient, err := validator.New(context.Background(), ulogger.TestLogger{}, tSettings, utxoStore, nil, nil, nil, blockAssemblyClient, nil)
	if err != nil {
		return errors.NewServiceError("failed to create validator client", err)
	}

	subtreeStore := blob_memory.New()

	// A real file-backed temp store, because the out-of-order block park needs a
	// store whose contents a restart could enumerate — anything else and the park
	// switches itself off.
	tempStoreURL, err := url.Parse("file://" + t.TempDir())
	if err != nil {
		return errors.NewServiceError("failed to parse temp store url", err)
	}

	tSettings.Legacy.TempStore = tempStoreURL

	tempStore, err := blob.NewStore(ulogger.TestLogger{}, tempStoreURL)
	if err != nil {
		return errors.NewServiceError("failed to create temp store", err)
	}

	subtreeValidation := &subtreevalidation.MockSubtreeValidation{}

	blockvalidationClient, err := blockvalidation.NewClient(context.Background(), ulogger.TestLogger{}, tSettings, "manager_test")
	if err != nil {
		return errors.NewServiceError("failed to create block validation client", err)
	}

	syncMgr, err := New(context.Background(),
		ulogger.TestLogger{},
		tSettings,
		blockchainClient,
		validatorClient,
		utxoStore,
		subtreeStore,
		tempStore,
		subtreeValidation,
		blockvalidationClient,
		nil,
		&Config{
			PeerNotifier: peerNotifier,
			ChainParams:  tc.cfg.chainParams,
			MaxPeers:     8,
		})
	if err != nil {
		return errors.NewServiceError("failed to create SyncManager", err)
	}

	tc.syncManager = syncMgr
	tc.peerNotifier = peerNotifier

	return nil
}

func (tc *testContext) Teardown() {
}

// TestPeerConnections tests that the SyncManager tracks the set of connected
// peers.
func TestPeerConnections(t *testing.T) {
	chainParams := &chaincfg.MainNetParams

	var ctx testContext

	err := ctx.Setup(t, &testConfig{
		dbName:      "TestPeerConnections",
		chainParams: chainParams,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer ctx.Teardown()

	syncMgr := ctx.syncManager
	syncMgr.Start()

	peerCfg := peer.Config{
		Listeners:        peer.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
		Services:         0,
	}

	_, localNode1, err := MakeConnectedPeers(t, peerCfg, peerCfg, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Used to synchronize with calls to SyncManager
	syncChan := make(chan struct{})

	// Register the peer with the sync manager. SyncManager should not start
	// syncing from this peer because it is not a full node.
	syncMgr.NewPeer(localNode1, syncChan)
	select {
	case <-syncChan:
	case <-time.After(30 * time.Second):
		t.Fatalf("Timeout waiting for sync manager to register peer %d",
			localNode1.ID())
	}

	if syncMgr.SyncPeerID() != 0 {
		t.Fatalf("Sync manager is syncing from an unexpected peer %d",
			syncMgr.SyncPeerID())
	}

	// Now connect the SyncManager to a full node, which it should start syncing
	// from.
	peerCfg.Services = wire.SFNodeNetwork

	_, localNode2, err := MakeConnectedPeers(t, peerCfg, peerCfg, 1)
	if err != nil {
		t.Fatal(err)
	}

	localNode2.UpdateLastBlockHeight(100)

	syncMgr.NewPeer(localNode2, syncChan)
	select {
	case <-syncChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to register peer %d",
			localNode2.ID())
	}

	if syncMgr.SyncPeerID() != localNode2.ID() {
		t.Fatalf("Expected sync manager to be syncing from peer %d got %d",
			localNode2.ID(), syncMgr.SyncPeerID())
	}

	// Register another full node peer with the manager. Even though the new
	// peer is a valid sync peer, manager should not change from the first one.
	_, localNode3, err := MakeConnectedPeers(t, peerCfg, peerCfg, 2)
	if err != nil {
		t.Fatal(err)
	}

	localNode3.UpdateLastBlockHeight(100)

	syncMgr.NewPeer(localNode3, syncChan)
	select {
	case <-syncChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to register peer %d",
			localNode3.ID())
	}

	if syncMgr.SyncPeerID() != localNode2.ID() {
		t.Fatalf("Sync manager is syncing from an unexpected peer %d; "+
			"expected %d", syncMgr.SyncPeerID(), localNode2.ID())
	}

	// SyncManager should unregister peer when it is done. When sync peer drops,
	// manager should start syncing from another valid peer.
	syncMgr.DonePeer(localNode2, syncChan)
	select {
	case <-syncChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to unregister peer %d",
			localNode2.ID())
	}

	if syncMgr.SyncPeerID() != localNode3.ID() {
		t.Fatalf("Expected sync manager to be syncing from peer %d",
			localNode3.ID())
	}

	// Expect SyncManager to stop syncing when last valid peer is disconnected.
	syncMgr.DonePeer(localNode3, syncChan)
	select {
	case <-syncChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to unregister peer %d",
			localNode3.ID())
	}

	if syncMgr.SyncPeerID() != 0 {
		t.Fatalf("Expected sync manager to stop syncing after peer disconnect")
	}

	err = syncMgr.Stop()
	if err != nil {
		t.Fatalf("failed to stop SyncManager: %v", err)
	}
}

func TestSyncManager_QueueInv(t *testing.T) {
	t.Run("empty message - no kafka", func(t *testing.T) {
		msgChan := make(chan interface{})
		sm := SyncManager{
			msgChan: msgChan,
		}

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			msg := <-msgChan
			invMsg, ok := msg.(*invMsg)
			require.True(t, ok)
			assert.Len(t, invMsg.inv.InvList, 0)
			wg.Done()
		}()

		sm.QueueInv(&wire.MsgInv{}, nil)

		wg.Wait()
	})

	t.Run("tx message", func(t *testing.T) {
		msgChan, legacyKafkaInvCh, sm, smPeer := setupQueueInvTests()

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			// no message should be sent here
			msg := <-msgChan
			require.Nil(t, msg)
		}()

		go func() {
			msg := <-legacyKafkaInvCh

			var value kafkamessage.KafkaInvTopicMessage
			err := proto.Unmarshal(msg.Value, &value)
			require.NoError(t, err)

			wireInvMsg, err := sm.newInvFromKafkaMessage(&value)
			require.NoError(t, err)
			assert.Len(t, wireInvMsg.inv.InvList, 2)
			wg.Done()
		}()

		inv := &wire.MsgInv{}
		err := inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}})
		require.NoError(t, err)
		err = inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}})
		require.NoError(t, err)

		sm.QueueInv(inv, smPeer)

		wg.Wait()
	})

	t.Run("block message", func(t *testing.T) {
		msgChan, legacyKafkaInvCh, sm, smPeer := setupQueueInvTests()

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			msg := <-msgChan
			wireInvMsg, ok := msg.(*invMsg)
			require.True(t, ok)
			assert.Len(t, wireInvMsg.inv.InvList, 2)
			wg.Done()
		}()

		go func() {
			msg := <-legacyKafkaInvCh
			require.Nil(t, msg)
		}()

		inv := &wire.MsgInv{}
		err := inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeBlock, Hash: chainhash.Hash{}})
		require.NoError(t, err)
		err = inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeBlock, Hash: chainhash.Hash{}})
		require.NoError(t, err)

		sm.QueueInv(inv, smPeer)

		wg.Wait()
	})

	t.Run("mixed message", func(t *testing.T) {
		msgChan, legacyKafkaInvCh, sm, smPeer := setupQueueInvTests()

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			// no message should be sent here
			msg := <-msgChan
			wireInvMsg, ok := msg.(*invMsg)
			require.True(t, ok)
			assert.Len(t, wireInvMsg.inv.InvList, 1)
			wg.Done()
		}()

		go func() {
			msg := <-legacyKafkaInvCh

			var value kafkamessage.KafkaInvTopicMessage
			err := proto.Unmarshal(msg.Value, &value)
			require.NoError(t, err)

			wireInvMsg, err := sm.newInvFromKafkaMessage(&value)
			require.NoError(t, err)
			assert.Len(t, wireInvMsg.inv.InvList, 1)
			wg.Done()
		}()

		inv := &wire.MsgInv{}
		err := inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeBlock, Hash: chainhash.Hash{}})
		require.NoError(t, err)
		err = inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}})
		require.NoError(t, err)

		sm.QueueInv(inv, smPeer)

		wg.Wait()
	})
}

func setupQueueInvTests() (chan interface{}, chan *kafka.Message, *SyncManager, *peer.Peer) {
	msgChan := make(chan interface{})
	legacyKafkaInvCh := make(chan *kafka.Message)

	sm := SyncManager{
		msgChan:          msgChan,
		legacyKafkaInvCh: legacyKafkaInvCh,
		logger:           ulogger.TestLogger{},
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}

	smPeer := &peer.Peer{}
	sm.peerStates.Set(smPeer, &peerSyncState{})

	return msgChan, legacyKafkaInvCh, &sm, smPeer
}

func TestSendDuringShutdown(t *testing.T) {
	t.Run("open channel delivers", func(t *testing.T) {
		ch := make(chan int, 1)
		require.True(t, sendDuringShutdown(ch, 7))
		require.Equal(t, 7, <-ch)
	})

	t.Run("closed channel drops without panic", func(t *testing.T) {
		ch := make(chan int)
		close(ch)
		require.NotPanics(t, func() {
			require.False(t, sendDuringShutdown(ch, 1))
		})
	})
}

// TestQueueInv_NoPanicWhenChannelsClosedDuringShutdown reproduces the shutdown
// race that previously crashed the process: inv delivery runs on a peer
// goroutine (OnInv -> QueueInv) while teardown closes the target channels — the
// kafka async producer closes legacyKafkaInvCh in its Stop(), and the block
// handler stops draining msgChan. QueueInv's shutdown-flag check cannot make the
// subsequent send atomic against that close, so a late inv hit a closed channel
// and panicked. The send must now drop the inv instead.
func TestQueueInv_NoPanicWhenChannelsClosedDuringShutdown(t *testing.T) {
	t.Run("tx inv after legacyKafkaInvCh closed", func(t *testing.T) {
		_, legacyKafkaInvCh, sm, smPeer := setupQueueInvTests()
		close(legacyKafkaInvCh)

		inv := &wire.MsgInv{}
		require.NoError(t, inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}}))

		require.NotPanics(t, func() { sm.QueueInv(inv, smPeer) })
	})

	t.Run("block inv after msgChan closed", func(t *testing.T) {
		msgChan, _, sm, smPeer := setupQueueInvTests()
		close(msgChan)

		inv := &wire.MsgInv{}
		require.NoError(t, inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeBlock, Hash: chainhash.Hash{}}))

		require.NotPanics(t, func() { sm.QueueInv(inv, smPeer) })
	})

	t.Run("non-kafka path after msgChan closed", func(t *testing.T) {
		msgChan, _, sm, smPeer := setupQueueInvTests()
		sm.legacyKafkaInvCh = nil // exercise the else branch
		close(msgChan)

		inv := &wire.MsgInv{}
		require.NoError(t, inv.AddInvVect(&wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{}}))

		require.NotPanics(t, func() { sm.QueueInv(inv, smPeer) })
	})
}

type msgChans struct {
	memPoolChan    chan *wire.MsgMemPool
	txChan         chan *wire.MsgTx
	blockChan      chan *wire.MsgBlock
	invChan        chan *wire.MsgInv
	headersChan    chan *wire.MsgHeaders
	getDataChan    chan *wire.MsgGetData
	getBlocksChan  chan *wire.MsgGetBlocks
	getHeadersChan chan *wire.MsgGetHeaders
	rejectChan     chan *wire.MsgReject
}

func newMessageChans() *msgChans {
	var instance msgChans
	instance.memPoolChan = make(chan *wire.MsgMemPool)
	instance.txChan = make(chan *wire.MsgTx)
	instance.blockChan = make(chan *wire.MsgBlock)
	instance.invChan = make(chan *wire.MsgInv)
	instance.headersChan = make(chan *wire.MsgHeaders)
	instance.getDataChan = make(chan *wire.MsgGetData)
	instance.getBlocksChan = make(chan *wire.MsgGetBlocks)
	instance.getHeadersChan = make(chan *wire.MsgGetHeaders)
	instance.rejectChan = make(chan *wire.MsgReject)

	return &instance
}

func buildBlockInv(blocks ...*bsvutil.Block) *wire.MsgInv {
	msg := wire.NewMsgInv()

	for _, block := range blocks {
		invVect := wire.NewInvVect(wire.InvTypeBlock, block.Hash())
		_ = msg.AddInvVect(invVect)
	}

	return msg
}

// createSpendingTx constructs a transaction spending from the provided one
// which sends the entire value of one output to the given address.
func createSpendingTx(prevTx *bsvutil.Tx, index uint32, scriptSig []byte, address bsvutil.Address) (*bsvutil.Tx, error) {
	scriptPubKey, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}

	prevTxMsg := prevTx.MsgTx()
	prevOut := prevTxMsg.TxOut[index]
	prevOutPoint := &wire.OutPoint{Hash: prevTxMsg.TxHash(), Index: index}

	spendTx := wire.NewMsgTx(1)
	spendTx.AddTxIn(wire.NewTxIn(prevOutPoint, scriptSig))
	spendTx.AddTxOut(wire.NewTxOut(prevOut.Value, scriptPubKey))

	return bsvutil.NewTx(spendTx), nil
}

func TestHandleCheckSyncPeer_HeadersFirstMode(t *testing.T) {
	t.Run("headers-first mode detects last block time violation", func(t *testing.T) {
		sp := &peer.Peer{} // zero-value peer is sufficient for this test
		sps := &syncPeerState{
			lastBlockTime: time.Now().Add(-10 * time.Minute), // way past maxLastBlockTime (3 min)
			ticks:         1,                                 // non-zero so validNetworkSpeed runs
		}

		sm := &SyncManager{
			logger:     ulogger.TestLogger{},
			peerStates: txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(true)
		sm.peerStates.Set(sp, &peerSyncState{})

		// Last-block-time violations are no longer skipped during headers-first mode.
		// The violation is detected and the peer rotation path is entered, which panics
		// here because the test uses a minimal SyncManager without full peer setup.
		assert.Panics(t, func() {
			sm.handleCheckSyncPeer()
		})
	})

	t.Run("headers-first mode skips network speed violation", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := &syncPeerState{
			lastBlockTime: time.Now(), // recent — no time violation
			ticks:         1,
			violations:    maxNetworkViolations, // at violation threshold
		}

		sm := &SyncManager{
			logger:                  ulogger.TestLogger{},
			peerStates:              txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed: 1000, // high threshold to ensure violation
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(true)
		sm.peerStates.Set(sp, &peerSyncState{})

		sm.handleCheckSyncPeer()

		assert.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("normal mode retains peer when no violations", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := &syncPeerState{
			lastBlockTime: time.Now(), // recent — no violation
			ticks:         1,
		}

		sm := &SyncManager{
			logger:     ulogger.TestLogger{},
			peerStates: txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(false) // normal mode
		sm.peerStates.Set(sp, &peerSyncState{})

		sm.handleCheckSyncPeer()

		// No violations, sync peer should still be set
		assert.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("headers-first mode keeps actively-downloading peer despite last-block-time", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := &syncPeerState{
			lastBlockTime:          time.Now().Add(-10 * time.Minute), // past maxLastBlockTime
			ticks:                  1,
			assocReadBytes:         10 * 1024 * 1024, // 10 MB pulled in over the last tick
			assocReadBytesLastTick: 0,
		}

		sm := &SyncManager{
			logger:                  ulogger.TestLogger{},
			peerStates:              txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed: 51200,
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(true)
		sm.peerStates.Set(sp, &peerSyncState{})

		// A large block is still streaming in (healthy association throughput),
		// so the peer must NOT be rotated even though no block completed within
		// maxLastBlockTime. If it rotated, the minimal SyncManager would panic.
		require.NotPanics(t, func() { sm.handleCheckSyncPeer() })
		assert.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("rotates a slow-drip peer once past the wall-clock cap", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := &syncPeerState{
			// No completed block for longer than peer.MaxBlockDownloadTime.
			lastBlockTime:          time.Now().Add(-peer.MaxBlockDownloadTime - time.Minute),
			ticks:                  1,
			assocReadBytes:         10 * 1024 * 1024, // still "healthy" throughput
			assocReadBytesLastTick: 0,
		}

		sm := &SyncManager{
			logger:                  ulogger.TestLogger{},
			peerStates:              txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed: 51200,
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(true)
		sm.peerStates.Set(sp, &peerSyncState{})

		// Past the cap, throughput no longer protects the peer — it is rotated
		// (which panics in this minimal SyncManager).
		assert.Panics(t, func() { sm.handleCheckSyncPeer() })
	})
}

// TestNewBlockFailureBackoffMap verifies the per-block backoff map is built only
// when both knobs are positive, and is nil (disabled, no zero-TTL leak) otherwise.
func TestNewBlockFailureBackoffMap(t *testing.T) {
	require.Nil(t, newBlockFailureBackoffMap(0, time.Minute, time.Minute), "base 0 must disable")
	require.Nil(t, newBlockFailureBackoffMap(5*time.Second, 0, time.Minute), "window 0 must disable")
	require.Nil(t, newBlockFailureBackoffMap(-1, time.Minute, time.Minute), "negative base must disable")
	require.Nil(t, newBlockFailureBackoffMap(5*time.Second, -1, time.Minute), "negative window must disable")

	m := newBlockFailureBackoffMap(5*time.Second, 5*time.Minute, 3*time.Minute)
	require.NotNil(t, m, "both knobs positive must enable")
	m.Stop()

	// maxAttempt <= 0 still builds (TTL falls back to window alone).
	m2 := newBlockFailureBackoffMap(5*time.Second, 5*time.Minute, 0)
	require.NotNil(t, m2, "non-positive maxAttempt must still build")
	m2.Stop()
}

// TestProcessTXmetaBatchMessage_SkipsInBlockTx verifies the tx announce path
// drops txmeta entries flagged InBlock. The txmeta Kafka topic carries every
// validated transaction — including those that arrived as part of a block or
// announced subtree (block validation, subtree validation, legacy sync, which
// feed the subtree-validation cache) — and announcing those as fresh mempool
// txs floods peers with getdata for transactions that are long mined and
// often already pruned.
func TestProcessTXmetaBatchMessage_SkipsInBlockTx(t *testing.T) {
	inBlockHash := chainhash.Hash{0xAA}
	mempoolHash := chainhash.Hash{0xBB}

	inBlockBytes, err := (&meta.Data{Fee: 1, SizeInBytes: 100, InBlock: true}).MetaBytes()
	require.NoError(t, err)

	mempoolBytes, err := (&meta.Data{Fee: 2, SizeInBytes: 200}).MetaBytes()
	require.NoError(t, err)

	// Build a v1 wire message with both entries.
	buf := new(bytes.Buffer)
	require.NoError(t, binary.Write(buf, binary.LittleEndian, uint32(2)))

	for _, entry := range []struct {
		hash    chainhash.Hash
		content []byte
	}{
		{inBlockHash, inBlockBytes},
		{mempoolHash, mempoolBytes},
	} {
		buf.Write(entry.hash[:])
		buf.WriteByte(txmetacache.WireActionADD)
		require.NoError(t, binary.Write(buf, binary.LittleEndian, uint32(len(entry.content))))
		buf.Write(entry.content)
	}

	var (
		mu        sync.Mutex
		announced []chainhash.Hash
	)

	sm := &SyncManager{logger: ulogger.TestLogger{}}
	sm.txAnnounceBatcher = batcher.NewWithDeduplicationAndPool[TxHashAndFee](10, 10*time.Millisecond, func(batch []*TxHashAndFee) {
		mu.Lock()
		defer mu.Unlock()
		for _, item := range batch {
			announced = append(announced, item.TxHash)
		}
	}, true)

	require.NoError(t, sm.processTXmetaBatchMessage(buf.Bytes()))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(announced) > 0
	}, 2*time.Second, 10*time.Millisecond, "expected the mempool tx to be announced")

	// Give the batcher one more flush window so a wrongly-announced in-block
	// tx would have surfaced.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []chainhash.Hash{mempoolHash}, announced, "only the mempool tx must be announced")
}

func TestHasHealthyDownloadThroughput(t *testing.T) {
	const minSpeed = 51200 // 50 KiB/s, matches default minSyncPeerNetworkSpeed

	t.Run("no prior sample", func(t *testing.T) {
		sps := &syncPeerState{ticks: 0, assocReadBytes: 10 * 1024 * 1024}
		require.False(t, sps.hasHealthyDownloadThroughput(minSpeed))
	})

	t.Run("no bytes moved is never healthy, even with zero threshold", func(t *testing.T) {
		sps := &syncPeerState{ticks: 1, assocReadBytes: 100, assocReadBytesLastTick: 100}
		require.False(t, sps.hasHealthyDownloadThroughput(0))
	})

	t.Run("chatter below threshold", func(t *testing.T) {
		// ~33 B/s over a 30s tick — far below 50 KiB/s.
		sps := &syncPeerState{ticks: 1, assocReadBytes: 1000, assocReadBytesLastTick: 0}
		require.False(t, sps.hasHealthyDownloadThroughput(minSpeed))
	})

	t.Run("active download above threshold", func(t *testing.T) {
		// 10 MB over the tick — well above 50 KiB/s.
		sps := &syncPeerState{ticks: 1, assocReadBytes: 10 * 1024 * 1024, assocReadBytesLastTick: 0}
		require.True(t, sps.hasHealthyDownloadThroughput(minSpeed))
	})

	t.Run("counter decrease (stream removed) is not healthy", func(t *testing.T) {
		// A stream dropped between samples, so the association sum fell. The
		// unsigned subtraction must not wrap to a huge "healthy" value.
		sps := &syncPeerState{ticks: 2, assocReadBytes: 1000, assocReadBytesLastTick: 10 * 1024 * 1024}
		require.False(t, sps.hasHealthyDownloadThroughput(minSpeed))
	})
}

// TestSyncPeerStateFor verifies the last-block-time refresh matches not just the
// sync peer itself but any stream of its association — under BlockPriority the
// block is delivered on the DATA1 stream peer, not the GENERAL sync peer.
func TestSyncPeerStateFor(t *testing.T) {
	general := &peer.Peer{}
	sps := &syncPeerState{lastBlockTime: time.Now()}

	sm := &SyncManager{
		logger:     ulogger.TestLogger{},
		peerStates: txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}
	sm.storeSyncPeer(general, sps)

	assoc := peer.NewAssociation([]byte{0x01}, general)
	general.SetAssociation(assoc)

	// The DATA1 stream peer is a different Peer sharing the same association.
	data1 := &peer.Peer{}
	require.True(t, assoc.AddStream(wire.StreamTypeData1, data1))
	data1.SetAssociation(assoc)

	t.Run("sync peer itself matches", func(t *testing.T) {
		got, ok := sm.syncPeerStateFor(general)
		require.True(t, ok)
		require.Equal(t, sps, got)
	})

	t.Run("association sibling (DATA1) matches", func(t *testing.T) {
		got, ok := sm.syncPeerStateFor(data1)
		require.True(t, ok)
		require.Equal(t, sps, got)
	})

	t.Run("unrelated peer does not match", func(t *testing.T) {
		other := &peer.Peer{}
		_, ok := sm.syncPeerStateFor(other)
		require.False(t, ok)
	})
}

// TestHandleNewPeerMsg_NilFSMState exercises the path where the blockchain
// client returns (nil, err) from GetFSMCurrentState — common during transient
// gRPC failures or service restarts. The pre-fix code dereferenced the nil
// pointer and panicked. The fix must guard the dereference and still register
// the peer.
func TestHandleNewPeerMsg_NilFSMState(t *testing.T) {
	chainParams := &chaincfg.MainNetParams

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).
		Return((*blockchain2.FSMStateType)(nil), errors.NewServiceError("transient gRPC error"))

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		chainParams:      chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}

	// Use a real connected peer: handleNewPeerMsg now refuses to register peers
	// whose socket has been torn down by the time the newPeerMsg drains.
	peerCfg := peer.Config{
		Listeners:        peer.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
		Services:         0,
	}
	_, smPeer, err := MakeConnectedPeers(t, peerCfg, peerCfg, 99)
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			require.Failf(t, "handleNewPeerMsg panicked", "panic: %v", r)
		}
	}()

	sm.handleNewPeerMsg(smPeer)

	require.True(t, sm.peerStates.Exists(smPeer), "peer must be registered even when FSM state is unavailable")
	require.Equal(t, uint64(0), sm.currentFeeFilter.Load(), "fee filter must not be set when FSM state is unavailable")
}

// TestHandleNewPeerMsg_SetsFeeFilterWhenCatchingBlocks verifies that EVERY peer
// connecting while the node is catching up is asked (via a raised feefilter) to
// hold back transaction announcements, reducing load during sync. It asserts the
// observable behaviour — the feefilter message is actually delivered to each
// peer's remote end — not just the internal marker, and covers the second peer
// (regression guard: an earlier version only raised it for the first connector).
// The filter is restored to the policy default once the node reaches RUNNING
// (resetFeeFilterToDefault).
func TestHandleNewPeerMsg_SetsFeeFilterWhenCatchingBlocks(t *testing.T) {
	chainParams := &chaincfg.MainNetParams

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).
		Return(&catchingBlocks, nil)

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		chainParams:      chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}

	// connectPeer returns a peer for handleNewPeerMsg to operate on; gotFee
	// records the MinFee of any feefilter its remote end receives.
	connectPeer := func(idx uint8, gotFee *atomic.Int64) *peer.Peer {
		remoteCfg := peer.Config{
			Listeners: peer.MessageListeners{
				OnFeeFilter: func(_ *peer.Peer, msg *wire.MsgFeeFilter) {
					gotFee.Store(msg.MinFee)
				},
			},
			UserAgentName:    "btcdtest",
			UserAgentVersion: "1.0",
			ChainParams:      chainParams,
		}
		localCfg := peer.Config{
			Listeners:        peer.MessageListeners{},
			UserAgentName:    "btcdtest",
			UserAgentVersion: "1.0",
			ChainParams:      chainParams,
		}
		remote, smPeer, err := MakeConnectedPeers(t, remoteCfg, localCfg, idx)
		require.NoError(t, err)
		require.True(t, remote.Connected())
		return smPeer
	}

	var fee1, fee2 atomic.Int64
	p1 := connectPeer(101, &fee1)
	p2 := connectPeer(102, &fee2)

	sm.handleNewPeerMsg(p1)
	sm.handleNewPeerMsg(p2)

	want := int64(bsvutil.SatoshiPerBitcoin)
	require.True(t, WaitUntil(func() bool { return fee1.Load() == want }, 2*time.Second),
		"first peer must receive the raised feefilter")
	require.True(t, WaitUntil(func() bool { return fee2.Load() == want }, 2*time.Second),
		"second peer must also receive the raised feefilter, not just the first")

	require.Equal(t, uint64(bsvutil.SatoshiPerBitcoin), sm.currentFeeFilter.Load(),
		"fee filter marker must be set while catching up")
	require.True(t, sm.peerStates.Exists(p1), "first peer must be registered")
	require.True(t, sm.peerStates.Exists(p2), "second peer must be registered")
}

// TestHandleNewPeerMsg_SkipsDisconnectedPeer verifies that a peer whose socket
// was torn down before the queued newPeerMsg drained is not inserted into
// peerStates. Pairs with the Connected() filter in startSync to prevent a dead
// pointer from being elected as the sync peer.
func TestHandleNewPeerMsg_SkipsDisconnectedPeer(t *testing.T) {
	chainParams := &chaincfg.MainNetParams

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).
		Return((*blockchain2.FSMStateType)(nil), errors.NewServiceError("transient gRPC error")).Maybe()

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		chainParams:      chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
	}

	// A zero-value Peer has connected=0, so Connected() returns false. This
	// mirrors the state of a peer whose underlying socket has already closed
	// by the time handleNewPeerMsg pulls its newPeerMsg off msgChan.
	disconnectedPeer := &peer.Peer{}

	sm.handleNewPeerMsg(disconnectedPeer)

	require.False(t, sm.peerStates.Exists(disconnectedPeer), "disconnected peer must not be registered in peerStates")
}

// TestHandleCheckSyncPeer_SpeedArmRespectsAssociationThroughput covers the arm
// the healthy-throughput suppression used to miss.
//
// validNetworkSpeed reads BytesReceived, which is the sync peer object's own
// counter. Under the BlockPriority stream policy a large block arrives on DATA1,
// so that counter barely moves while the association is pulling at full rate,
// and the peer records a speed violation every tick. The suppression guarded
// only the last-block-time arm, so the speed arm rotated a peer that was
// downloading perfectly well. Observed on mainnet at 124 MB average blocks: the
// sync peer was demoted three times in seven minutes, mid-transfer, each
// demotion reopening its assignments and rewinding the cursor, so the work was
// done twice.
func TestHandleCheckSyncPeer_SpeedArmRespectsAssociationThroughput(t *testing.T) {
	// speedViolatingState has a clean last-block-time arm so only the speed arm
	// can fire, which is what isolates the arm under test. The general stream is
	// silent and the association is pulling hard: a large block on DATA1.
	speedViolatingState := func(assocBytes uint64) *syncPeerState {
		return &syncPeerState{
			lastBlockTime:          time.Now(),
			ticks:                  maxNetworkViolations + 1,
			violations:             maxNetworkViolations,
			recvBytes:              0,
			recvBytesLastTick:      0,
			assocReadBytes:         assocBytes,
			assocReadBytesLastTick: 0,
		}
	}

	t.Run("a peer downloading on DATA1 is not rotated for a quiet general stream", func(t *testing.T) {
		sm := newDemotionManager(t)
		sm.headersFirstMode.Store(false) // speed checks only run outside headers-first
		sm.minSyncPeerNetworkSpeed = 51200

		sp, _, _ := connectRacePeer(t, 90, 1000)
		state := registerRacePeer(sm, sp)
		sm.storeSyncPeer(sp, speedViolatingState(64<<20))

		sm.handleCheckSyncPeer()

		// The demotion cooldown is the observable, not the sync-peer pointer: a
		// demoted peer is re-elected immediately when it is the only candidate,
		// so the pointer looks unchanged either way.
		require.False(t, state.inDemotionCooldown(),
			"a peer whose association is downloading at a healthy rate must not be demoted for a quiet general stream")
	})

	t.Run("a genuinely silent peer is still rotated", func(t *testing.T) {
		sm := newDemotionManager(t)
		sm.headersFirstMode.Store(false)
		sm.minSyncPeerNetworkSpeed = 51200

		sp, _, _ := connectRacePeer(t, 91, 1000)
		state := registerRacePeer(sm, sp)
		sm.storeSyncPeer(sp, speedViolatingState(0)) // nothing anywhere on the association

		sm.handleCheckSyncPeer()

		require.True(t, state.inDemotionCooldown(),
			"a peer sending nothing on any stream is stalled and must still be demoted")
	})
}
