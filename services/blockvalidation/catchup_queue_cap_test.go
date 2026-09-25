package blockvalidation

import (
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

func TestCatchupQueueBoundsDistinctPeerAlternatives(t *testing.T) {
	server := &Server{
		catchupCh:           make(chan processBlockCatchup, 1),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}
	block := testhelpers.CreateTestBlockChain(t, 2)[1]
	require.True(t, server.enqueueCatchup(processBlockCatchup{block: block, peerID: "primary"}))
	for i := range maxCatchupAlternatives + 10 {
		require.True(t, server.enqueueCatchup(processBlockCatchup{
			block: block, peerID: fmt.Sprintf("alternative-%d", i), baseURL: fmt.Sprintf("http://peer-%d", i),
		}))
	}
	alternatives := server.catchupAlternatives.Get(*block.Hash())
	require.NotNil(t, alternatives)
	require.Len(t, alternatives.Value(), maxCatchupAlternatives, "paused targets must not retain unbounded block copies from distinct announcements")
	for i, alternative := range alternatives.Value() {
		require.Equal(t, fmt.Sprintf("alternative-%d", i), alternative.peerID)
	}
	require.Len(t, server.catchupQueued, 1)
	require.Len(t, server.catchupCh, 1)
	require.NotNil(t, server.processBlockNotify.Get(*block.Hash()))
}

func TestCatchupQueueFullPreservesPriorAlternatives(t *testing.T) {
	server := &Server{
		catchupCh:           make(chan processBlockCatchup, 1),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}
	blocks := testhelpers.CreateTestBlockChain(t, 3)
	require.True(t, server.enqueueCatchup(processBlockCatchup{block: blocks[1], peerID: "queued"}))
	hash := *blocks[2].Hash()
	server.catchupAlternatives.Set(hash, []processBlockCatchup{{block: blocks[2], peerID: "prior"}}, time.Hour)
	before := server.catchupAlternatives.Get(hash).ExpiresAt()
	require.False(t, server.enqueueCatchup(processBlockCatchup{block: blocks[2], peerID: "rejected"}))
	entry := server.catchupAlternatives.Get(hash)
	require.NotNil(t, entry)
	require.Equal(t, "prior", entry.Value()[0].peerID)
	require.WithinDuration(t, before, entry.ExpiresAt(), time.Second)
	require.NotContains(t, server.catchupQueued, hash)
}

func TestCatchupTerminalReleaseCannotEraseReannouncement(t *testing.T) {
	for _, policyDeclined := range []bool{false, true} {
		server := &Server{
			catchupCh:           make(chan processBlockCatchup, 2),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		}
		block := testhelpers.CreateTestBlockChain(t, 2)[1]
		hash := *block.Hash()
		require.True(t, server.enqueueCatchup(processBlockCatchup{block: block, peerID: "old"}))
		old := <-server.catchupCh
		server.catchupAlternatives.Set(hash, []processBlockCatchup{{block: block, peerID: "prior"}}, ttlcache.NoTTL)
		if policyDeclined {
			server.finishPolicyDeclinedTarget(old)
			require.NotNil(t, server.catchupAlternatives.Get(hash))
		} else {
			server.finishCatchupTarget(old)
			require.Nil(t, server.catchupAlternatives.Get(hash))
		}
		// A failure callback synchronously delivers a new announcement before
		// the old worker's deferred ownership release runs.
		require.True(t, server.enqueueCatchup(processBlockCatchup{block: block, peerID: "new"}))
		server.releaseCatchupOwnership(old)
		require.Equal(t, "new", server.catchupQueued[hash].peerID)
		require.Equal(t, "new", (<-server.catchupCh).peerID)
	}
}
