package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// seedFetchHeaders lands a prebuilt headers batch behind anchor, at height 10,
// exactly the state a real node is in when a headers batch has just arrived.
// The batch is passed in rather than built here because a caller may need the
// hashes before the manager exists.
//
// This used to hand the batch to handleHeadersMsg and let it splice the batch
// onto a header list. That list is gone: this seeds the header cache directly,
// which is the only thing assignWantedBlocks (via fetchHeaderBlocks) or
// wantedBlocks ever reads.
func seedFetchHeaders(t *testing.T, sm *SyncManager, _ *peerpkg.Peer, anchor chainhash.Hash, msg *wire.MsgHeaders) {
	t.Helper()

	sm.headersFirstMode.Store(true)

	// assignWantedBlocks reads the committed height from the chain now, so the
	// fixture's anchor height has to be reflected there too, or every
	// ceiling-bearing test in this package would see a committed height of zero
	// against headers seeded at 11 and up — an absolute ceiling below every
	// header it seeded, rather than the depth relative to height 10 these tests
	// are written against. The mocked tip's own hash need not equal anchor: the
	// header cache below is filled directly, not through fillHeaderCache, so
	// nothing here checks the two against each other.
	mockCommittedTip(t, sm, 10, 0)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 11, msg.Headers),
		"the seeded headers should all link onto the anchor")
}
