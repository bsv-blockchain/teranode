package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// TestNoteCommittedParkedBlock_NilMetaKeepsHeightUnknown: a block recovered with no
// height (entry.height 0) reads its committed height back from the blockchain
// client. A client answering with a nil meta and a nil error must leave the height
// unknown, so the delivering peer's heights are not touched, rather than panic the
// park drain on meta.Height. The real sqlitememory store never answers that way, so
// the client is nilMetaClient (pipeline_parent_height_test.go), which overrides only
// GetBlockHeader on the real client.
func TestNoteCommittedParkedBlock_NilMetaKeepsHeightUnknown(t *testing.T) {
	sm := newPipelineManager(t, memory.New(), 8)
	sm.blockchainClient = nilMetaClient{ClientI: sm.blockchainClient}
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](10)
	sm.peerStates = txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState]()

	p := newTestPeer(t, "localhost:18491")
	state := &peerSyncState{requestedTxns: expiringmap.New[chainhash.Hash, struct{}](time.Hour)}
	sm.peerStates.Set(p, state)

	entry := parkedBlock{hash: chainhash.HashH([]byte("nil-meta-parked")), height: 0, peer: p}

	require.NotPanics(t, func() { sm.noteCommittedParkedBlock(entry) })
	require.Zero(t, p.LastBlock(), "an unknown committed height must not be credited to the peer")
	require.Zero(t, state.bestKnownHeight.Load(), "an unknown committed height must not raise the peer's best known height")
}
