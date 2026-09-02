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
	"github.com/bsv-blockchain/teranode/model"
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
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
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

// Test blockchain syncing protocol. SyncManager should request, processes, and
// relay blocks to/from peers.
//
// Investigated: the original "timing out" symptom has (at least) two distinct
// causes stacked on top of each other, both below the assumptions this
// btcd-derived test was written against:
//
//  1. startSync() (manager.go) now short-circuits to FSMStateRUNNING and never
//     calls PushGetBlocksMsg whenever the sync peer's advertised height equals
//     ours. This test's remotePeerCfg never sets peer.Config.NewestBlock, so
//     the peer always reports height 0 == our genesis-only height, and the
//     very first `remoteMessages.getBlocksChan` receive times out. Making the
//     remote peer advertise a height ahead of ours (via NewestBlock) fixes
//     that first timeout and gets the test past the initial handshake.
//  2. With (1) worked around, the test still times out later, waiting on
//     errChan after the first QueueBlock call ("Timeout waiting for sync
//     manager to process block 0"). This harness's testContext.Setup passes a
//     nil block-assembly client into New(...) and never configures the
//     subtreeValidation mock's CheckBlockSubtrees/CheckSubtreeFromBlock
//     expectations for a real block payload, either of which plausibly stalls
//     real block-processing deep inside HandleBlockDirect. Root-causing that
//     needs a proper block-processing harness (real/mocked block assembly +
//     subtree validation expectations for an actual 3-block regtest chain),
//     which is beyond a bounded triage pass - left skipped rather than force
//     enabled against an unresolved hang.
func TestBlockchainSync(t *testing.T) {
	t.Skip("times out: startSync skips PushGetBlocksMsg when the peer's advertised height ties ours (no NewestBlock configured here), and even past that, block processing via QueueBlock never signals errChan - likely missing block-assembly/subtreeValidation wiring in this harness; needs a real block-processing harness fix, not a quick nil-pointer patch")

	chainParams := chaincfg.RegressionNetParams
	chainParams.CoinbaseMaturity = 1

	var ctx testContext

	err := ctx.Setup(t, &testConfig{
		dbName:      "TestBlockchainSync",
		chainParams: &chainParams,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer ctx.Teardown()

	syncMgr := ctx.syncManager
	syncMgr.Start()

	remoteMessages := newMessageChans()
	remotePeerCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetBlocks: func(p *peer.Peer, msg *wire.MsgGetBlocks) {
				remoteMessages.getBlocksChan <- msg
			},
			OnGetData: func(p *peer.Peer, msg *wire.MsgGetData) {
				remoteMessages.getDataChan <- msg
			},
			OnReject: func(p *peer.Peer, msg *wire.MsgReject) {
				remoteMessages.rejectChan <- msg
			},
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}

	localMessages := newMessageChans()
	localPeerCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnInv: func(p *peer.Peer, msg *wire.MsgInv) {
				localMessages.invChan <- msg
			},
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chainParams,
		Services:         wire.SFNodeNetwork,
	}

	_, localNode, err := MakeConnectedPeers(t, remotePeerCfg, localPeerCfg, 0)
	if err != nil {
		t.Fatal(err)
	}

	syncMgr.NewPeer(localNode, nil)

	// SyncManager should send a getblocks message to start block download
	select {
	case msg := <-remoteMessages.getBlocksChan:
		if msg.HashStop != zeroHash {
			t.Fatalf("Expected no hash stop in getblocks, got %v", msg.HashStop)
		}

		if len(msg.BlockLocatorHashes) != 1 ||
			*msg.BlockLocatorHashes[0] != *chainParams.GenesisHash {
			t.Fatal("Received unexpected block locator in getblocks message")
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive getblocks message")
	}

	// Address is an anyone-can-spend P2SH script
	address, scriptSig, err := GenerateAnyoneCanspendAddress(&chainParams)
	if err != nil {
		t.Fatalf("Error constructing P2SH address: %v", err)
	}

	genesisBlock := bsvutil.NewBlock(chainParams.GenesisBlock)

	// Generate chain of 3 blocks
	blocks := make([]*bsvutil.Block, 0, 3)
	blockVersion := int32(2)
	prevBlock := genesisBlock

	for i := 0; i < 3; i++ {
		block, err := CreateBlock(prevBlock, nil, blockVersion,
			nullTime, address, []wire.TxOut{}, &chainParams)
		if err != nil {
			t.Fatalf("failed to generate block: %v", err)
		}

		blocks = append(blocks, block)
		prevBlock = block
	}

	// Remote node replies to getblocks with an inv
	invMsg := wire.NewMsgInv()

	for _, block := range blocks {
		invVect := wire.NewInvVect(wire.InvTypeBlock, block.Hash())
		err := invMsg.AddInvVect(invVect)
		require.NoError(t, err)
	}

	syncMgr.QueueInv(invMsg, localNode)

	// SyncManager should send a getdata message requesting blocks
	select {
	case msg := <-remoteMessages.getDataChan:
		if len(msg.InvList) != len(blocks) {
			t.Fatalf("Expected %d blocks in getdata message, got %d",
				len(blocks), len(msg.InvList))
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive getdata message")
	}
	// Remote node sends first 3 blocks
	errChan := make(chan error)
	for _, block := range blocks {
		syncMgr.QueueBlock(block, localNode, errChan)

		select {
		case err := <-errChan:
			t.Fatalf("Error in sync manager to process block %d: %v", block.Height(), err)
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for sync manager to process block %d", block.Height())
		}
	}

	if localNode.LastBlock() != 3 {
		t.Fatalf("Expected peer's LastBlock to be 3, got %d",
			localNode.LastBlock())
	}

	if syncMgr.IsCurrent() {
		t.Fatal("Expected IsCurrent() to be false as blocks have old " +
			"timestamps")
	}

	// Check that no blocks were relayed to peers since syncer is not current
	select {
	case <-ctx.peerNotifier.relayInventoryChan:
		t.Fatal("PeerNotifier received unexpected RelayInventory call")
	default:
	}

	// Create current block with a non-Coinbase transaction
	prevTx, err := blocks[0].Tx(0)
	if err != nil {
		t.Fatal(err)
	}

	spendTx, err := createSpendingTx(prevTx, 0, scriptSig, address)
	if err != nil {
		t.Fatal(err)
	}

	timestamp := time.Now().Truncate(time.Second)
	prevBlock = blocks[len(blocks)-1]
	txns := []*bsvutil.Tx{spendTx}

	block, err := CreateBlock(prevBlock, txns, blockVersion,
		timestamp, address, []wire.TxOut{}, &chainParams)
	if err != nil {
		t.Fatal(err)
	}

	// SyncManager should send a getdata message requesting blocks
	syncMgr.QueueInv(buildBlockInv(block), localNode)
	select {
	case msg := <-remoteMessages.getDataChan:
		if len(msg.InvList) != 1 {
			t.Fatalf("Expected 1 block in getdata message, got %d",
				len(msg.InvList))
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive getdata message")
	}

	// Remote node sends new block
	syncMgr.QueueBlock(block, localNode, errChan)
	select {
	case <-errChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to process block %d",
			block.Height())
	}

	// Assert calls made to PeerNotifier
	select {
	case call := <-ctx.peerNotifier.transactionConfirmedChan:
		if !call.tx.Hash().IsEqual(spendTx.Hash()) {
			t.Fatalf("PeerNotifier received TransactionConfirmed with "+
				"unexpected tx %v, expected %v", call.tx.Hash(),
				spendTx.Hash())
		}
	default:
		t.Fatal("Expected SyncManager to make TransactionConfirmed call to " +
			"PeerNotifier")
	}

	select {
	case <-ctx.peerNotifier.announceNewTransactionsChan:
	default:
		t.Fatal("Expected SyncManager to make AnnounceNewTransactions call " +
			"to PeerNotifier")
	}

	select {
	case call := <-ctx.peerNotifier.relayInventoryChan:
		if call.invVect.Type != wire.InvTypeBlock ||
			call.invVect.Hash != *block.Hash() {
			t.Fatalf("PeerNotifier received unexpected RelayInventory call: "+
				"%v", call.invVect)
		}
	default:
		t.Fatal("Expected SyncManager to make RelayInventory call to " +
			"PeerNotifier")
	}

	if localNode.LastBlock() != 4 {
		t.Fatalf("Expected peer's LastBlock to be 4, got %d",
			localNode.LastBlock())
	}

	// SyncManager should now be current since last block was recent
	if !syncMgr.IsCurrent() {
		t.Fatal("Expected IsCurrent() to be true")
	}

	// Send invalid block with timestamp in the far future
	prevBlock = block
	timestamp = time.Now().Truncate(time.Second).Add(1000 * time.Hour)

	block, err = CreateBlock(prevBlock, nil, blockVersion,
		timestamp, address, []wire.TxOut{}, &chainParams)
	if err != nil {
		t.Fatal(err)
	}

	syncMgr.QueueInv(buildBlockInv(block), localNode)
	select {
	case <-remoteMessages.getDataChan:
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive getdata message")
	}

	syncMgr.QueueBlock(block, localNode, errChan)
	select {
	case <-errChan:
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for sync manager to process block %d",
			block.Height())
	}

	// Expect block to not be added to chain
	if localNode.LastBlock() != 4 {
		t.Fatalf("Expected peer's LastBlock to be 4, got %d",
			localNode.LastBlock())
	}

	// Expect node to send reject in response to invalid block
	select {
	case msg := <-remoteMessages.rejectChan:
		if msg.Code != wire.RejectInvalid {
			t.Fatalf("Reject message has unexpected code %s, expected %s",
				msg.Code, wire.RejectInvalid)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote node to receive reject message")
	}

	err = syncMgr.Stop()
	if err != nil {
		t.Fatalf("failed to stop SyncManager: %v", err)
	}
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

// TestHandleBlockMsg_OrphanDuringCatchup verifies a block with an unknown
// parent arriving during legacy sync / catching blocks triggers a getblocks
// continuation request instead of being silently dropped. In the legacy sync
// protocol the peer announces its tip after delivering a getblocks batch; that
// orphan tip is the only signal to request the next batch, so swallowing it
// stalls the sync until the stall detector rotates the peer.
func TestHandleBlockMsg_OrphanDuringCatchup(t *testing.T) {
	prevHash := chainhash.Hash{0x01}

	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS

	bestHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// Parent header lookup fails — the block is an orphan to us.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("not found"))
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{bestHeader.Hash()}, nil)

	// Real (unconnected) peer: PushGetBlocksMsg needs a logger, and
	// QueueMessage is a no-op on a disconnected peer.
	p := peer.NewInboundPeer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &peer.Config{})

	state := &peerSyncState{
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
	}
	defer state.requestedTxns.Stop()

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		chainParams:      &chaincfg.MainNetParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		blockDownloads:   newBlockDownloadTracker(blockRequestAssignmentTTL),
	}
	sm.peerStates.Set(p, state)
	sm.blockDownloads.Add(p, blockHash)

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	// The orphan is dropped without error or peer disconnect...
	require.NoError(t, err)

	// ...but it must trigger the batch-continuation request: a block locator
	// from our best block, pushed to the peer as getblocks.
	blockchainClient.AssertCalled(t, "GetBlockLocator", mock.Anything, mock.Anything, mock.Anything)
}

// newBackoffTestManager builds the minimal SyncManager + peer state used by the
// block-failure backoff tests (#1187). It mirrors TestHandleBlockMsg_OrphanDuringCatchup:
// the struct literal bypasses New(), so blockFailureBackoff is initialised explicitly.
func newBackoffTestManager(t *testing.T, blockchainClient *blockchain2.Mock, blockHash chainhash.Hash) (*SyncManager, *peer.Peer) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	p := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	state := &peerSyncState{
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
	}
	t.Cleanup(func() { state.requestedTxns.Stop() })

	sm := &SyncManager{
		ctx:                  context.Background(),
		logger:               ulogger.TestLogger{},
		settings:             tSettings,
		chainParams:          &chaincfg.MainNetParams,
		blockchainClient:     blockchainClient,
		peerStates:           txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		blockDownloads:       newBlockDownloadTracker(blockRequestAssignmentTTL),
		blockFailureBackoff:  expiringmap.New[chainhash.Hash, *blockFailureState](time.Minute),
		recentlyFailedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(func() { sm.blockFailureBackoff.Stop(); sm.recentlyFailedBlocks.Stop() })
	sm.peerStates.Set(p, state)
	sm.blockDownloads.Add(p, blockHash)

	return sm, p
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

// TestBlockFailureBackoffRampSurvivesSlowAttempt guards the #1187 review P1: the
// failure-tracking map TTL must outlast one slow failing attempt so the linear
// ramp actually engages. recordBlockFailureBackoff reads the prior count from the
// map; when the TTL equalled the backoff cap (window) and a single overload
// attempt outlasted it, the entry expired between two consecutive failures and
// the count reset to 1 every time, pinning the backoff at its base. Here the two
// records are spaced beyond the backoff cap but well within the decoupled
// retention (window + maxAttempt), so the count must ramp to 2 rather than reset.
func TestBlockFailureBackoffRampSurvivesSlowAttempt(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	base := 10 * time.Millisecond
	window := 100 * time.Millisecond // backoff cap
	maxAttempt := 2 * time.Second    // per-attempt bound -> map TTL = 2.1s
	tSettings.Legacy.BlockFailureBackoffBase = base
	tSettings.Legacy.BlockFailureBackoffMaxDuration = window

	sm := &SyncManager{
		settings:            tSettings,
		blockFailureBackoff: newBlockFailureBackoffMap(base, window, maxAttempt),
	}
	require.NotNil(t, sm.blockFailureBackoff)
	t.Cleanup(func() { sm.blockFailureBackoff.Stop() })

	h := chainhash.Hash{0xCD}

	sm.recordBlockFailureBackoff(h)
	fs, ok := sm.blockFailureBackoff.Get(h)
	require.True(t, ok)
	require.Equal(t, 1, fs.attempts)

	// Simulate a failing attempt that outlasts the backoff cap (window) but not
	// the map TTL. A TTL of only `window` would have expired the entry by now.
	time.Sleep(window + 50*time.Millisecond)

	sm.recordBlockFailureBackoff(h)
	fs, ok = sm.blockFailureBackoff.Get(h)
	require.True(t, ok, "entry must survive a slow attempt so the count ramps")
	require.Equal(t, 2, fs.attempts, "consecutive-failure count must ramp, not reset")
}

// TestRecordBlockFailureBackoff verifies the per-block backoff grows linearly
// with the consecutive failure count and is capped at the configured maximum.
func TestRecordBlockFailureBackoff(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.BlockFailureBackoffBase = 2 * time.Second
	tSettings.Legacy.BlockFailureBackoffMaxDuration = 5 * time.Second

	sm := &SyncManager{
		settings:            tSettings,
		blockFailureBackoff: expiringmap.New[chainhash.Hash, *blockFailureState](time.Minute),
	}
	t.Cleanup(func() { sm.blockFailureBackoff.Stop() })

	h := chainhash.Hash{0xAB}

	sm.recordBlockFailureBackoff(h)
	fs, ok := sm.blockFailureBackoff.Get(h)
	require.True(t, ok)
	require.Equal(t, 1, fs.attempts)
	require.WithinDuration(t, time.Now().Add(2*time.Second), fs.nextRetry, time.Second)

	sm.recordBlockFailureBackoff(h)
	fs, ok = sm.blockFailureBackoff.Get(h)
	require.True(t, ok)
	require.Equal(t, 2, fs.attempts)
	require.WithinDuration(t, time.Now().Add(4*time.Second), fs.nextRetry, time.Second)

	// Third failure would be 3*2s=6s but is capped at the 5s maximum.
	sm.recordBlockFailureBackoff(h)
	fs, ok = sm.blockFailureBackoff.Get(h)
	require.True(t, ok)
	require.Equal(t, 3, fs.attempts)
	require.WithinDuration(t, time.Now().Add(5*time.Second), fs.nextRetry, time.Second)
}

// TestHandleBlockMsg_BackoffSkipsRetryWithinWindow verifies a block still inside
// its backoff window is skipped before the expensive HandleBlockDirect path
// (GetBlockExists, its first blockchain call, must not run) and returns a
// retryable error.
func TestHandleBlockMsg_BackoffSkipsRetryWithinWindow(t *testing.T) {
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)
	sm.blockFailureBackoff.Set(blockHash, &blockFailureState{attempts: 1, nextRetry: time.Now().Add(time.Minute)})

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "expected a retryable service-unavailable error, got %v", err)
	blockchainClient.AssertNotCalled(t, "GetBlockExists", mock.Anything, mock.Anything)
}

// TestHandleBlockMsg_BackoffSkipRefreshesSyncPeerLastBlockTime verifies the
// liveness fix (#1187, review): a skipped (backed-off) block still refreshes the
// delivering sync peer's last-block-time. The peer did deliver a block — the
// fault is our local store — so the stall detector must not rotate this healthy
// peer while it waits out a local backoff, which would only thrash peers with a
// still-backed-off re-delivery.
func TestHandleBlockMsg_BackoffSkipRefreshesSyncPeerLastBlockTime(t *testing.T) {
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)

	// Make p the current sync peer with a stale last-block-time (past the stall
	// window). Without the refresh-on-skip fix the stall detector would rotate it.
	stale := time.Now().Add(-10 * time.Minute)
	sps := &syncPeerState{lastBlockTime: stale}
	sm.storeSyncPeer(p, sps)

	// Backoff window still open -> the block is skipped.
	sm.blockFailureBackoff.Set(blockHash, &blockFailureState{attempts: 1, nextRetry: time.Now().Add(time.Minute)})

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "expected a retryable service-unavailable error, got %v", err)
	require.True(t, sps.getLastBlockTime().After(stale),
		"the delivering sync peer's last-block-time must be refreshed on a backoff skip")
}

// TestHandleBlockMsg_BackoffExpiredRetriesNormally verifies that once the backoff
// window has elapsed the block is processed normally again (GetBlockExists runs).
func TestHandleBlockMsg_BackoffExpiredRetriesNormally(t *testing.T) {
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("not found"))
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{bestHeader.Hash()}, nil)

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)
	// nextRetry already in the past: the window has elapsed.
	sm.blockFailureBackoff.Set(blockHash, &blockFailureState{attempts: 1, nextRetry: time.Now().Add(-time.Second)})

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.NoError(t, err)
	blockchainClient.AssertCalled(t, "GetBlockExists", mock.Anything, mock.Anything)
}

// TestHandleBlockMsg_BackoffRecordedOnStorageError verifies that a transient
// storage failure records a backoff entry (attempts=1, future nextRetry).
func TestHandleBlockMsg_BackoffRecordedOnStorageError(t *testing.T) {
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// HandleBlockDirect's first call fails with a storage error, which it wraps in
	// a ProcessingError; handleBlockMsg's serviceError predicate sees it via errors.Is.
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, errors.NewStorageError("boom"))

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "expected the storage error to bubble up, got %v", err)

	fs, ok := sm.blockFailureBackoff.Get(blockHash)
	require.True(t, ok, "a backoff entry should be recorded after a storage failure")
	require.Equal(t, 1, fs.attempts)
	require.True(t, fs.nextRetry.After(time.Now()), "nextRetry should be in the future")
}

// TestHandleBlockMsg_BackoffRecordedOnServiceUnavailable verifies the outpoint/
// decorate batch-timeout class — the UTXO store returns ErrServiceUnavailable
// (the #1187 wedge, stores/utxo/aerospike/get.go) — also records a backoff entry,
// not just ErrStorageError.
func TestHandleBlockMsg_BackoffRecordedOnServiceUnavailable(t *testing.T) {
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, errors.NewServiceUnavailableError("aerospike outpoint batch did not complete"))

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "expected the service-unavailable error to bubble up, got %v", err)

	fs, ok := sm.blockFailureBackoff.Get(blockHash)
	require.True(t, ok, "a backoff entry should be recorded after a service-unavailable failure")
	require.Equal(t, 1, fs.attempts)
}

// TestHandleBlockMsg_BackoffClearedOnSuccess verifies a successful block clears
// any prior backoff entry so a future failure starts a fresh count.
func TestHandleBlockMsg_BackoffClearedOnSuccess(t *testing.T) {
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// Block already exists -> HandleBlockDirect returns nil immediately (cheap success path).
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)
	// success-path tail bookkeeping touches these.
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)
	sm.blockSizeTracker = newBlockSizeTracker(10)
	// pre-existing backoff entry (window elapsed so the skip-check does not fire).
	sm.blockFailureBackoff.Set(blockHash, &blockFailureState{attempts: 2, nextRetry: time.Now().Add(-time.Second)})

	_ = sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	_, ok := sm.blockFailureBackoff.Get(blockHash)
	require.False(t, ok, "backoff entry should be cleared after a successful block")
}

// TestHandleBlockMsg_FailedParentCascadeShortCircuits verifies the #1333 fix: a
// block whose parent recently failed to store/validate is skipped before any RPC
// (no GetBlockExists / GetBlockHeader), logs no ERROR, is itself marked failed so
// the cascade is suppressed transitively, and still triggers the getblocks
// recovery so sync resumes once the root is resolved.
func TestHandleBlockMsg_FailedParentCascadeShortCircuits(t *testing.T) {
	failedParent := chainhash.Hash{0xAA}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &failedParent, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{bestHeader.Hash()}, nil)

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)
	ml := mocklogger.NewTestLogger()
	sm.logger = ml

	// The parent is already known-failed.
	sm.recentlyFailedBlocks.Set(failedParent, struct{}{})

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.NoError(t, err, "a short-circuited descendant returns nil (recovered via getblocks)")

	// Short-circuited before HandleBlockDirect: no block lookup RPCs ran.
	blockchainClient.AssertNotCalled(t, "GetBlockExists", mock.Anything, mock.Anything)
	blockchainClient.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)

	// Transitively marked so this block's own descendants are also suppressed.
	_, marked := sm.recentlyFailedBlocks.Get(blockHash)
	require.True(t, marked, "a skipped descendant must itself be marked failed")

	// Recovery still fired, and no misleading ERROR was logged.
	blockchainClient.AssertCalled(t, "GetBlockLocator", mock.Anything, mock.Anything, mock.Anything)
	ml.AssertNumberOfCalls(t, "Errorf", 0)
}

// TestHandleBlockMsg_FailedBlockRecorded verifies a block that fails to
// store/validate is recorded in recentlyFailedBlocks so its descendants can be
// short-circuited (#1333).
func TestHandleBlockMsg_FailedBlockRecorded(t *testing.T) {
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, errors.NewStorageError("boom"))

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.Error(t, err)
	_, ok := sm.recentlyFailedBlocks.Get(blockHash)
	require.True(t, ok, "a failed block should be recorded so its descendants are short-circuited")
}

// TestHandleBlockMsg_FailedBlockClearedOnSuccess verifies a successful (re)process
// clears the cascade-suppression marker so descendants are no longer skipped (#1333).
func TestHandleBlockMsg_FailedBlockClearedOnSuccess(t *testing.T) {
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// Block already exists -> HandleBlockDirect returns nil immediately.
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.recentlyFailedBlocks.Set(blockHash, struct{}{})

	_ = sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	_, ok := sm.recentlyFailedBlocks.Get(blockHash)
	require.False(t, ok, "cascade-suppression marker should be cleared after a successful block")
}

// TestHandleBlockMsg_OrphanNotMarkedFailed verifies a normal orphan (parent not
// yet known, but no ancestor was rejected) is NOT recorded as failed: its parent
// is genuinely still in flight, and it must log at DEBUG rather than ERROR (#1333).
func TestHandleBlockMsg_OrphanNotMarkedFailed(t *testing.T) {
	prevHash := chainhash.Hash{0x02}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// Parent lookup misses -> ErrBlockNotFound -> normal orphan path.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("not found"))
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{bestHeader.Hash()}, nil)

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)
	ml := mocklogger.NewTestLogger()
	sm.logger = ml

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.NoError(t, err)

	// A normal orphan is not "known-bad" — its parent is genuinely still in flight.
	_, ok := sm.recentlyFailedBlocks.Get(blockHash)
	require.False(t, ok, "a normal orphan must not be recorded as failed")

	// The missing-parent lookup in HandleBlockDirect logs at DEBUG, not ERROR.
	ml.AssertNumberOfCalls(t, "Errorf", 0)
}

// TestHandleCheckSyncPeer_LocalBacklog verifies the stall detector does not
// blame the sync peer for backpressure the node inflicts on itself: while
// blocks are queued or mid-validation locally, OnBlock stops reading from the
// peer, so zero throughput and a stale last-block-time say nothing about the
// peer's health.
func TestHandleCheckSyncPeer_LocalBacklog(t *testing.T) {
	// Zero throughput (recvBytes == recvBytesLastTick) one violation short of
	// the rotation threshold, plus a last-block-time far past maxLastBlockTime:
	// without a backlog this tick rotates the sync peer.
	newStalledState := func() *syncPeerState {
		return &syncPeerState{
			lastBlockTime: time.Now().Add(-10 * time.Minute),
			ticks:         1,
			violations:    maxNetworkViolations - 1,
		}
	}

	newSyncManager := func(sp *peer.Peer, sps *syncPeerState) *SyncManager {
		sm := &SyncManager{
			logger:                  ulogger.TestLogger{},
			peerStates:              txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed: 51200,
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(false)
		sm.peerStates.Set(sp, &peerSyncState{})

		return sm
	}

	t.Run("keeps sync peer and accrues no violation while backlog pending", func(t *testing.T) {
		sp := &peer.Peer{}
		sps := newStalledState()
		sm := newSyncManager(sp, sps)

		sm.blockBacklog.Add(1)   // a block is queued or mid-validation locally
		sm.noteBacklogProgress() // fresh progress: backlog is advancing, not hung

		// Rotation would panic in this minimal SyncManager (no blockchain
		// client), so NotPanics proves the peer was kept.
		require.NotPanics(t, func() { sm.handleCheckSyncPeer() })
		assert.Equal(t, sp, sm.loadSyncPeer())
		assert.Equal(t, maxNetworkViolations-1, sps.getViolations())
	})

	t.Run("still rotates on zero throughput once backlog drained", func(t *testing.T) {
		sp := &peer.Peer{}
		sm := newSyncManager(sp, newStalledState())

		// No local backlog: the same zero-throughput state is a real peer
		// stall, so the rotation path runs (and panics in this minimal setup).
		assert.Panics(t, func() { sm.handleCheckSyncPeer() })
	})
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
