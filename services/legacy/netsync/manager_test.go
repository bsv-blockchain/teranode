// Copyright (c) 2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/blockchain"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg/chainhash"
	"github.com/bitcoin-sv/ubsv/services/legacy/database"
	_ "github.com/bitcoin-sv/ubsv/services/legacy/database/ffldb"
	"github.com/bitcoin-sv/ubsv/services/legacy/netsync"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/txscript"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

const (
	// testDbType is the database backend type to use for the tests.
	testDbType = "ffldb"

	// testDbRoot is the root directory used to create all test databases.
	testDbRoot = "testdbs"
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
	db           database.DB
	cfg          testConfig
	peerNotifier *MockPeerNotifier
	syncManager  *netsync.SyncManager
}

func (ctx *testContext) dbPath() string {
	return filepath.Join(testDbRoot, ctx.cfg.dbName)
}

func (ctx *testContext) Setup(config *testConfig) error {
	ctx.cfg = *config

	// Create the root directory for test database if it does not exist.
	if _, err := os.Stat(testDbRoot); os.IsNotExist(err) {
		if err = os.Mkdir(testDbRoot, 0700); err != nil {
			return fmt.Errorf("failed to create test db root: %v", err)
		}
	}

	// Create a new database to store the accepted blocks into.
	dbPath := ctx.dbPath()
	_ = os.RemoveAll(dbPath)
	db, err := database.Create(testDbType, dbPath, ctx.cfg.chainParams.Net)
	if err != nil {
		return fmt.Errorf("failed to create db: %v", err)
	}

	chain, err := blockchain.New(&blockchain.Config{
		DB:                 db,
		ChainParams:        ctx.cfg.chainParams,
		TimeSource:         blockchain.NewMedianTime(),
		ExcessiveBlockSize: 1000000,
	})
	if err != nil {
		return fmt.Errorf("failed to create blockchain: %v", err)
	}

	peerNotifier := NewMockPeerNotifier()

	syncMgr, err := netsync.New(&netsync.Config{
		PeerNotifier: peerNotifier,
		Chain:        chain,
		ChainParams:  ctx.cfg.chainParams,
		MaxPeers:     8,
	})
	if err != nil {
		return fmt.Errorf("failed to create SyncManager: %v", err)
	}

	ctx.db = db
	ctx.syncManager = syncMgr
	ctx.peerNotifier = peerNotifier
	return nil
}

func (ctx *testContext) Teardown() {
	ctx.db.Close()
	os.RemoveAll(testDbRoot)
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
