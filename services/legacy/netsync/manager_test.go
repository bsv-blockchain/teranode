// Copyright (c) 2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	blob_memory "github.com/bitcoin-sv/ubsv/stores/blob/memory"

	blockchain2 "github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/netsync"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/txscript"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	blockchainstore "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
)

// zeroHash is the zero value hash (all zeros).
var zeroHash chainhash.Hash

// nullTime is an empty time defined for convenience
var nullTime time.Time

type testConfig struct {
	dbName      string
	chainParams *chaincfg.Params
}

type testContext struct {
	cfg          testConfig
	peerNotifier *MockPeerNotifier
	syncManager  *netsync.SyncManager
}

func (ctx *testContext) Setup(config *testConfig) error {
	ctx.cfg = *config

	peerNotifier := NewMockPeerNotifier()

	storeURL, _ := url.Parse("sqlitememory://")
	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL)
	if err != nil {
		return fmt.Errorf("failed to create blockchain store: %v", err)
	}
	blockchainClient, err := blockchain2.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create blockchain client: %v", err)
	}

	utxoStore := utxostore.New(ulogger.TestLogger{})
	validatorClient, err := validator.New(context.Background(), ulogger.TestLogger{}, utxoStore)
	if err != nil {
		return fmt.Errorf("failed to create validator client: %v", err)
	}

	subtreeStore := blob_memory.New()

	blockvalidationClient := blockvalidation.NewClient(context.Background(), ulogger.TestLogger{})
	subtreeValidationClient := subtreevalidation.NewClient(context.Background(), ulogger.TestLogger{})

	syncMgr, err := netsync.New(context.Background(),
		ulogger.TestLogger{},
		blockchainClient,
		validatorClient,
		utxoStore,
		subtreeStore,
		subtreeValidationClient,
		blockvalidationClient,
		&netsync.Config{
			PeerNotifier: peerNotifier,
			ChainParams:  ctx.cfg.chainParams,
			MaxPeers:     8,
		})
	if err != nil {
		return fmt.Errorf("failed to create SyncManager: %v", err)
	}

	ctx.syncManager = syncMgr
	ctx.peerNotifier = peerNotifier
	return nil
}

func (ctx *testContext) Teardown() {
}

// TestPeerConnections tests that the SyncManager tracks the set of connected
// peers.
func TestPeerConnections(t *testing.T) {
	chainParams := &chaincfg.MainNetParams

	var ctx testContext
	err := ctx.Setup(&testConfig{
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
	_, localNode1, err := MakeConnectedPeers(peerCfg, peerCfg, 0)
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
	case <-time.After(time.Second):
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
	_, localNode2, err := MakeConnectedPeers(peerCfg, peerCfg, 1)
	if err != nil {
		t.Fatal(err)
	}
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
	_, localNode3, err := MakeConnectedPeers(peerCfg, peerCfg, 2)
	if err != nil {
		t.Fatal(err)
	}
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

// Test blockchain syncing protocol. SyncManager should request, processes, and
// relay blocks to/from peers.
// TODO: Test is timing out, needs to be fixed.
func xTestBlockchainSync(t *testing.T) {
	chainParams := chaincfg.RegressionNetParams
	chainParams.CoinbaseMaturity = 1

	var ctx testContext
	err := ctx.Setup(&testConfig{
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

	_, localNode, err := MakeConnectedPeers(remotePeerCfg, localPeerCfg, 0)
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
	address, scriptSig, err := GenerateAnyoneCanSpendAddress(&chainParams)
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
		invMsg.AddInvVect(invVect)
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
		msg.AddInvVect(invVect)
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
