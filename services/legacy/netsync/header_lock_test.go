package netsync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// linkedHeaders builds a headers message whose headers chain from prev, and
// returns the hashes in order. nonce is bumped per header so every hash is
// distinct even when two batches are built in the same second.
func linkedHeaders(prev chainhash.Hash, n int, nonce *uint32) (*wire.MsgHeaders, []chainhash.Hash) {
	msg := wire.NewMsgHeaders()
	hashes := make([]chainhash.Hash, 0, n)
	cur := prev

	for i := 0; i < n; i++ {
		*nonce++
		bh := wire.NewBlockHeader(1, &cur, &chainhash.Hash{}, 0x1d00ffff, *nonce)
		_ = msg.AddBlockHeader(bh)
		cur = bh.BlockHash()
		hashes = append(hashes, cur)
	}

	return msg, hashes
}

// newHeaderLockManager builds a SyncManager wired for the header-list paths:
// headers-first mode on, a far-away next checkpoint so no batch ever trips the
// checkpoint branch, and a blockchain mock that never claims to already have a
// block (so fetchHeaderBlocks always requests what it walks over).
func newHeaderLockManager(t *testing.T, gate chan struct{}, entered chan struct{}) *SyncManager {
	t.Helper()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("not found"))

	best := blockchainClient.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)

	if gate != nil {
		var once sync.Once

		best.Run(func(mock.Arguments) {
			if entered != nil {
				once.Do(func() { close(entered) })
			}

			<-gate
		})
	}

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient
	sm.blockSizeTracker = newBlockSizeTracker(10)

	// A checkpoint far above anything these tests generate, so the checkpoint
	// branches never fire and the plain push/remove paths are what is exercised.
	checkpointHash := chainhash.Hash{0xcc}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1_000_000, Hash: &checkpointHash}

	return sm
}

// TestHeaderList_ConcurrentHeadersAndBlocksDoNotCorruptTheList drives the three
// goroutines that reach the header list in a running node at the same time: the
// per-message headers handler (dispatched on its own goroutine by blockHandler),
// the block-queue consumer running handleBlockMsg, and fetchHeaderBlocks. All
// three walk and mutate the same container/list, which is not goroutine-safe.
//
// Concurrency is the subject here, so goroutines are the point and t.Parallel()
// is still not used.
func TestHeaderList_ConcurrentHeadersAndBlocksDoNotCorruptTheList(t *testing.T) {
	const (
		batches  = 200
		perBatch = 8
	)

	sm := newHeaderLockManager(t, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 30, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// Seed the list with an anchor the first batch of headers can link to.
	// resetHeaderState turns headers-first mode off, so turn it back on.
	anchor := chainhash.Hash{0xa0}
	sm.resetHeaderState(&anchor, 10)
	sm.headersFirstMode.Store(true)

	// Hashes are handed to the block goroutine in list order, so it removes
	// from the front exactly as a real node does, without the test itself
	// having to read the list. One tick is released per headers batch, so the
	// block goroutine removes at most one header for every eight pushed and the
	// list can never drain — an empty list would send the headers handler down
	// the recovery path and disconnect an honest peer, which is a harness
	// artefact, not the behaviour under test.
	hashCh := make(chan chainhash.Hash, batches*perBatch+1)
	hashCh <- anchor
	tick := make(chan struct{}, batches)

	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()

		var nonce uint32

		tip := anchor

		for i := 0; i < batches; i++ {
			msg, hashes := linkedHeaders(tip, perBatch, &nonce)
			tip = hashes[len(hashes)-1]

			sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

			for _, h := range hashes {
				hashCh <- h
			}

			tick <- struct{}{}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < batches; i++ {
			<-tick

			h := <-hashCh
			sm.blockDownloads.Add(syncPeer, h)
			// The message carries no block, so handleBlockMsg returns straight
			// after the header-list bookkeeping that is under test.
			_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: h, peer: syncPeer})
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < batches; i++ {
			sm.fetchHeaderBlocks()
		}
	}()

	wg.Wait()

	require.True(t, syncPeer.Connected(), "no honest-peer disconnect should have been provoked")
}

// TestHandleHeadersMsg_RecoveryDoesNotWipeHeadersAddedWhileWeWereWaiting pins the
// re-validation the empty-list recovery needs. GetBestBlockHeader can block for
// minutes during initial sync, so the header lock cannot be held across it — and
// once it is dropped, whatever was read before the call can no longer be trusted.
// A handler that comes back and resets unconditionally throws away every header
// another goroutine added while it was waiting.
func TestHandleHeadersMsg_RecoveryDoesNotWipeHeadersAddedWhileWeWereWaiting(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})

	sm := newHeaderLockManager(t, gate, entered)

	peer, _, _ := connectRacePeer(t, 31, 1000)
	registerRacePeer(sm, peer)
	sm.storeSyncPeer(peer, &syncPeerState{})

	// The list starts empty, so the first headers message takes the recovery
	// path and parks inside GetBestBlockHeader.
	var nonceA uint32

	msgA, _ := linkedHeaders(chainhash.Hash{0xde}, 2, &nonceA)

	done := make(chan struct{})

	go func() {
		defer close(done)

		sm.handleHeadersMsg(&headersMsg{headers: msgA, peer: peer})
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("the headers handler never reached GetBestBlockHeader")
	}

	// Someone else recovers the state and lands two headers while the first
	// handler is still parked.
	anchor := chainhash.Hash{0xb0}
	sm.resetHeaderState(&anchor, 100)
	sm.headersFirstMode.Store(true)

	var nonceB uint32

	msgB, hashesB := linkedHeaders(anchor, 2, &nonceB)
	sm.handleHeadersMsg(&headersMsg{headers: msgB, peer: peer})

	require.Equal(t, 3, sm.headerList.Len(), "the second handler should have added two headers to the anchor")

	close(gate)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the parked headers handler never returned")
	}

	require.Equal(t, 3, sm.headerList.Len(),
		"the recovering handler must not wipe headers added while it was waiting")
	require.NotNil(t, sm.startHeader, "startHeader must survive the recovery")

	startNode, ok := sm.startHeader.Value.(*headerNode)
	require.True(t, ok)
	require.Equal(t, hashesB[0], *startNode.hash, "startHeader must still point at the first unfetched header")
}

// TestHandleHeadersMsg_DoesNotHoldTheHeaderLockAcrossGetBestBlockHeader pins the
// scoping rule that makes the above safe: the header lock is dropped around the
// blockchain call, so a second goroutine can still read the header list while
// the first is parked. During initial sync that call can block for minutes, and
// a lock held across it would stall every other header-list user behind it.
func TestHandleHeadersMsg_DoesNotHoldTheHeaderLockAcrossGetBestBlockHeader(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})

	sm := newHeaderLockManager(t, gate, entered)

	peer, _, _ := connectRacePeer(t, 32, 1000)
	registerRacePeer(sm, peer)
	sm.storeSyncPeer(peer, &syncPeerState{})

	var nonce uint32

	msg, _ := linkedHeaders(chainhash.Hash{0xef}, 2, &nonce)

	done := make(chan struct{})

	go func() {
		defer close(done)

		sm.handleHeadersMsg(&headersMsg{headers: msg, peer: peer})
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("the headers handler never reached GetBestBlockHeader")
	}

	read := make(chan int, 1)

	go func() { read <- sm.headerListLen() }()

	select {
	case <-read:
	case <-time.After(2 * time.Second):
		close(gate)
		t.Fatal("reading the header list blocked while the headers handler waited on GetBestBlockHeader")
	}

	close(gate)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the parked headers handler never returned")
	}
}
