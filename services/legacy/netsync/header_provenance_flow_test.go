package netsync

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// newHeaderProvenanceManager builds a SyncManager whose header provenance comes
// from the real path: a real SQL blockchain client (so the committed tip the
// header cache anchors on is a genuine chain read), a header cache wired to the
// chain's checkpoints, and headers-first mode on.
//
// It replaces upstream's version, which seeded a header list and a stored
// nextCheckpoint. Neither exists on this branch: a headers batch is judged
// against the node's committed tip and against the pinned checkpoint hashes, and
// there is nothing else to seed.
func newHeaderProvenanceManager(t *testing.T) (*SyncManager, *peer.Peer, *peerSyncState) {
	t.Helper()

	tSettings, params := newOutpointOnlySettings(t, true, true, 33_333)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	p := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})
	state := &peerSyncState{requestedTxns: expiringmap.New[chainhash.Hash, struct{}](time.Hour)}
	t.Cleanup(state.requestedTxns.Stop)

	sm := &SyncManager{
		ctx: context.Background(), logger: ulogger.TestLogger{}, settings: tSettings,
		chainParams: params, blockchainClient: client,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		blockDownloads:   newBlockDownloadTracker(blockRequestAssignmentTTL),
		blockSizeTracker: newBlockSizeTracker(10),
		headerCache:      newHeaderCache().WithCheckpoints(params.Checkpoints),
	}
	sm.peerStates.Set(p, state)
	sm.storeSyncPeer(p, &syncPeerState{})
	sm.headersFirstMode.Store(true)

	return sm, p, state
}

// pinCheckpoint points the manager's checkpoint list, and the cache that reads
// it, at one (height, hash) pair. Both have to move together: the cache holds its
// own copy of the list so Fill can judge a run under the lock that installs it.
func pinCheckpoint(sm *SyncManager, height int32, hash *chainhash.Hash) {
	sm.chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: height, Hash: hash}}
	sm.headerCache = newHeaderCache().WithCheckpoints(sm.chainParams.Checkpoints)
}

// proveBlockOrigin installs a GENUINE header-cache proof that blk belongs to the
// checkpoint-certified chain: it pins a checkpoint naming blk's own (height, hash)
// and fills the cache with a run ending at blk's own header, rooted at whatever
// blk.MsgBlock().Header.PrevBlock already names. headerCache.Fill does the real
// linkage and hash-comparison work here — nothing about the proof is asserted
// directly, only arranged so the real check can succeed honestly.
//
// blk's height must already be set (bsvutil.Block.SetHeight) and its PrevBlock
// must already be final: both are read here, and pipelineBlockSink and
// prepareSubtrees both call blk.Hash() before this could still change it safely.
//
// Used by every fixed-round test that needs a below-checkpoint block to take the
// quick-validation / outpoint-only / unified fast paths, which all gate on
// blockOrigin(hash).headerProven (GHSA-gggq-8f59-4jm9) — a merged-in requirement
// this package's pipeline tests predate.
func proveBlockOrigin(t *testing.T, sm *SyncManager, blk *bsvutil.Block) {
	t.Helper()

	height := blk.Height()
	require.Greater(t, height, int32(0), "genesis cannot be proven; every gate already treats it as not fast-pathable")

	hash := *blk.Hash()
	sm.chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: height, Hash: &hash}}
	sm.headerCache = newHeaderCache().WithCheckpoints(sm.chainParams.Checkpoints)

	require.True(t, sm.headerCache.Fill(blk.MsgBlock().Header.PrevBlock, height, []*wire.BlockHeader{&blk.MsgBlock().Header}),
		"the one-header run ending at blk must link and agree with the checkpoint it was just given, or this helper's own construction is broken")
	require.True(t, sm.headerCache.Proven(hash), "sanity: the installed run must actually prove this hash")
}

// The whole point of the provenance gate, driven through the production handler:
// a headers batch that matches the pinned checkpoint hash grants the fast path to
// the run at or below it, and to nothing above it.
func TestHeaderProvenance_MatchedCheckpointGrantsOnlyItsPrefix(t *testing.T) {
	sm, p, _ := newHeaderProvenanceManager(t)

	genesis := *sm.chainParams.GenesisHash
	headers, hashes := linkedRun(genesis, 6)

	// Height 3 of the run, which starts one above the committed genesis tip.
	pinCheckpoint(sm, 3, &hashes[2])

	msg := wire.NewMsgHeaders()
	for _, header := range headers {
		require.NoError(t, msg.AddBlockHeader(header))
	}

	require.True(t, sm.fillHeaderCache(p, msg))

	require.True(t, sm.blockOrigin(hashes[0]).headerProven, "below the matched checkpoint is committed by linkage to it")
	require.True(t, sm.blockOrigin(hashes[2]).headerProven, "the checkpoint height itself is committed by the hash match")
	require.False(t, sm.blockOrigin(hashes[3]).headerProven, "above the matched checkpoint is committed by nothing")

	require.True(t, sm.quickValidationAllowed(sm.blockOrigin(hashes[0]), 1))
	require.False(t, sm.quickValidationAllowed(sm.blockOrigin(hashes[3]), 4))
}

// A run the node cannot tie to any pinned hash is kept — it still names heights,
// which is what the cache is for — but it grants nothing.
//
// This is the case mainnet is almost always in, because the checkpoints are tens
// of thousands of blocks apart and one reply covers two thousand. It is the
// reason this merge is a speed regression and not merely a tightening.
func TestHeaderProvenance_UnmatchedRunIsUsableButNeverProven(t *testing.T) {
	sm, p, _ := newHeaderProvenanceManager(t)

	genesis := *sm.chainParams.GenesisHash
	headers, hashes := linkedRun(genesis, 4)

	pinCheckpoint(sm, 11_111, &chainhash.Hash{0x7f})

	msg := wire.NewMsgHeaders()
	for _, header := range headers {
		require.NoError(t, msg.AddBlockHeader(header))
	}

	require.True(t, sm.fillHeaderCache(p, msg), "an unproven run is still the answer to where the chain goes next")
	require.Equal(t, 4, sm.headerCache.Len())

	for _, hash := range hashes {
		require.False(t, sm.blockOrigin(hash).headerProven)
	}

	require.False(t, sm.quickValidationAllowed(sm.blockOrigin(hashes[0]), 1))
}

// A batch that reaches a checkpoint height with the wrong hash costs the sender
// its connection and leaves the cache alone.
//
// Upstream did this in handleHeadersMsg against the header list; the branch had
// dropped the comparison entirely, so this is the defence being put back.
func TestHeaderProvenance_ContradictedCheckpointDisconnects(t *testing.T) {
	sm, p, _ := newHeaderProvenanceManager(t)

	genesis := *sm.chainParams.GenesisHash
	_, honest := linkedRun(genesis, 4)

	pinCheckpoint(sm, 2, &honest[1])

	forged, _ := forgedRun(genesis, 4)

	msg := wire.NewMsgHeaders()
	for _, header := range forged {
		require.NoError(t, msg.AddBlockHeader(header))
	}

	require.False(t, sm.fillHeaderCache(p, msg))
	require.Zero(t, sm.headerCache.Len(), "a refused batch must not be cached")
	require.NotNil(t, sm.contradictedCheckpoint(1, genesis, forged), "the refusal must be attributable to the checkpoint, not to linkage")
}
