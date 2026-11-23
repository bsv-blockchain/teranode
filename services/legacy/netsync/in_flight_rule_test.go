package netsync

import (
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// SV Node never asks for a block that is in flight. On 2026-09-24 mainnet received block 734,077
// seventeen times in thirteen seconds: each drained duplicate let every peer off the block while
// the real copy was still converting, the arrival refilled the peer at once, and the download pass,
// which checked the ledger but not what was arriving, asked for the block again. 72 duplicate
// copies of 41 blocks in two hours, with no slow peer and no race.

func TestABlockArrivingNowIsNotAskedForAgain(t *testing.T) {
	sm, _ := orderManager(t)
	sm.streams = newStreamRegistry()
	hash := chainhash.Hash{0x71}

	sm.streams.start(hash, 100, newTestPeer(t, "10.0.0.1:8333"), 50<<20, time.Now())

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 100, hash: hash}}),
		"its bytes are arriving, whatever the ledger says")
}

func TestABlockBeingConvertedIsNotAskedForAgain(t *testing.T) {
	sm, _ := orderManager(t)
	hash := chainhash.Hash{0x72}

	sm.inFlightBlocksMu.Lock()
	sm.inFlightBlocks = map[chainhash.Hash]*inFlightBlock{hash: {}}
	sm.inFlightBlocksMu.Unlock()

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 100, hash: hash}}))
}

func TestADrainedDuplicateReleasesOnlyItsOwnPeer(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)

	header := wire.BlockHeader{Version: 1, PrevBlock: h.blocks[1].MsgBlock().BlockHash()}
	hash := header.BlockHash()

	converting := newTestPeer(t, "10.0.0.2:8333")
	require.True(t, h.sm.blockDownloads.Add(converting, hash))
	require.True(t, h.sm.blockDownloads.Add(h.peer, hash))

	h.sm.noteDrainedDuplicate(hash)
	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: peerpkg.BlockBody{Header: header, Hash: hash, TxCount: 1, Size: 4096}, peer: h.peer})

	require.False(t, h.sm.blockDownloads.HasOwner(h.peer, hash), "the peer that sent the duplicate is let off")
	require.True(t, h.sm.blockDownloads.RequestedWithin(hash, blockRequestRetryInterval),
		"the peer whose copy is still converting still owes it, so nobody is asked again")
}

// Download passes run one at a time. Commits, arrivals, header replies and the park sweep each
// start one, and two running together could both find the same block unowned and both ask for
// it: the ledger lets a block have several owners, so neither noticed the other. On mainnet on
// 2026-09-24 five duplicate copies arrived in ten minutes with no quiet peer, race or departure
// behind any of them.
func TestConcurrentDownloadPassesAskForEachBlockOnce(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0x73}
	msg, hashes := linkedHeaders(anchor, 8, &nonce)

	sm := schedulerManager(t)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 16

	a, aRec := schedulerPeer(t, sm, 140, 1000)
	sm.storeSyncPeer(a, &syncPeerState{})

	b, bRec := schedulerPeer(t, sm, 141, 1000)
	wireStreamingPath(sm, a, b)

	seedFetchHeaders(t, sm, a, anchor, msg)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			sm.fetchHeaderBlocks()
		}()
	}

	wg.Wait()

	require.True(t, WaitUntil(func() bool { return aRec.count()+bRec.count() >= len(hashes) }, 5*time.Second))
	require.Never(t, func() bool { return aRec.count()+bRec.count() > len(hashes) }, 300*time.Millisecond, 10*time.Millisecond,
		"each block is asked for once, however many passes run at once")
}
