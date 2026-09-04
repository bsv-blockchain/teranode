package protocol

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestBlockDoneCompactStatusDecidesThePeersFate covers the override
// ProcessBlockTxnMessage's two failure statuses reach BlockDone through
// (manager.go, compactblock.go fillStatus).
//
// A compact block's reconstruction can only fail WHILE the assembled stream is
// read, so the ingestor reports both statuses the same way: a stream fault the
// pipeline attributes to the peer (outcome.PeerFault). The partial block's own
// status is the only thing that separates them, and the two must NOT reach the
// same peer-visible outcome:
//
//   - readInvalid — net_processing.cpp:3610-3616, Misbehaving(pfrom, 100,
//     "invalid-cmpctblk-txns"). The peer supplied bytes that cannot be what it
//     was asked for.
//   - readFailed — net_processing.cpp:3618-3623, "Might have collided, fall
//     back to getdata now". A short ID is 48 bits, so an honest peer's
//     transaction can hash onto the slot we asked about. SVNode does not
//     Misbehaving here at all; the block goes back on offer and the ordinary
//     getdata path fetches it.
//
// The PeerFault flag is the pipeline's verdict on the BYTES, and on the failed
// branch those bytes were assembled by us, from our own index, against our own
// short IDs. It cannot stand as a verdict on the peer.
func TestBlockDoneCompactStatusDecidesThePeersFate(t *testing.T) {
	genesis := syncGenesis()
	chain := minedRun(genesis, 1, 8)

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	connected, err := idx.AddHeader(chain[0])
	require.NoError(t, err)
	require.True(t, connected)

	block, ok := idx.Lookup(chain[0].BlockHash())
	require.True(t, ok)

	fault := errors.New(errors.ERR_ERROR, "svp2p: compact block reconstruction failed")

	for _, tc := range []struct {
		name   string
		status readStatus
		delta  int
		drops  bool
	}{
		{name: "readInvalid is the peer's fault", status: readInvalid, delta: scoreInvalidBlock, drops: true},
		{name: "readFailed is nobody's fault", status: readFailed, delta: 0, drops: false},
		{name: "readOK leaves the ingest verdict alone", status: readOK, delta: scoreInvalidBlock, drops: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := syncTestManager(t, idx, &recordingIngestor{})

			peer := NewSyncPeer("1.2.3.4:8333", wire.SFNodeNetwork, newPeerSyncState())

			m.syncMu.Lock()
			require.True(t, m.blockDownloader.MarkBlockAsInFlight(peer, block, testNow))
			peer.State.compactIngest = &compactState{hash: block.Hash, status: tc.status}
			m.syncMu.Unlock()

			delta, err := m.BlockDone(peer, block.Hash, IngestOutcome{Err: fault, PeerFault: true})

			require.Equal(t, tc.delta, delta, "the partial block's status decides whether the peer is scored")

			if tc.drops {
				require.Error(t, err, "a peer that supplied invalid transactions must lose its connection")
			} else {
				require.NoError(t, err, "a possible short ID collision must not cost an honest peer its connection")
			}

			// Whatever the verdict on the peer, the block itself goes back on
			// offer: MarkBlockAsFailed runs on both branches.
			m.syncMu.Lock()
			inFlight := m.blockDownloader.IsInFlight(block.Hash)
			m.syncMu.Unlock()

			require.False(t, inFlight, "the block must be released for another peer to serve")
		})
	}
}
