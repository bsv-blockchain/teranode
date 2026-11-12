package netsync

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// These tests pin SV Node's CheckIndexAgainstCheckpoint (validation.cpp:5748-5775)
// for headers: a header at a checkpoint height must carry the pinned hash
// ("checkpoint mismatch"), and once the node holds a checkpoint, a header below
// it that is not on that checkpoint's chain is "bad-fork-prior-to-checkpoint".
// Both are DoS 100, which here costs the sender its connection.

// forkCheckpoints pins the honest chain at 20 and puts a later checkpoint at 100.
func forkCheckpoints(honestHashes []chainhash.Hash) []chaincfg.Checkpoint {
	return []chaincfg.Checkpoint{
		{Height: 20, Hash: &honestHashes[19]},
		{Height: 100, Hash: &chainhash.Hash{0x77}},
	}
}

// Before the checkpoint is held a fork below it is only an unproven branch; once
// a branch holds the checkpoint, the same fork is refused whole, wherever below
// the checkpoint it forks from, and the honest branch is untouched. A header at
// or above the checkpoint height may fork: SV Node's rule is strictly below.
func TestHeaderFork_AForkBelowAHeldCheckpointIsRefused(t *testing.T) {
	tip := chainhash.Hash{0xc0}
	honest, honestHashes := linkedRun(tip, 25)

	before := newHeaderCache().WithCheckpoints(forkCheckpoints(honestHashes))
	require.True(t, before.FillFrom("honest", tip, 1, honest[:15]).accepted)

	early, _ := forgedRun(tip, 10)
	require.True(t, before.FillFrom("other", tip, 1, early).accepted, "with no checkpoint held, a fork is just another branch")

	cache := newHeaderCache().WithCheckpoints(forkCheckpoints(honestHashes))
	require.True(t, cache.FillFrom("honest", tip, 1, honest).accepted)
	require.Equal(t, int32(20), cache.ProvenTo())

	cases := []struct {
		name   string
		parent chainhash.Hash
		at     int32
	}{
		{"from the committed tip", tip, 1},
		{"from a held header below the checkpoint", honestHashes[14], 16},
		{"from the header just below the checkpoint", honestHashes[18], 20},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fork, _ := forgedRun(tc.parent, 3)
			result := cache.FillFrom("attacker", tip, 1, fork)

			if tc.at == 20 {
				// Height 20 is the checkpoint itself: the pinned hash decides.
				require.Equal(t, rejectCheckpointMismatch, result.rejection)
			} else {
				require.Equal(t, rejectForkBeforeCheckpoint, result.rejection)
			}

			require.Equal(t, tc.at, result.rejectedHeight)
			require.True(t, result.rejection.disconnects())
			require.False(t, result.accepted)

			_, _, ok := cache.PeerTop("attacker")
			require.False(t, ok, "nothing from the refused batch is held")
		})
	}

	fromCheckpoint, _ := forgedRun(honestHashes[19], 3)
	result := cache.FillFrom("above", tip, 1, fromCheckpoint)
	require.Equal(t, headerAccepted, result.rejection, "a fork from the checkpoint itself is above it")
	require.True(t, result.accepted)

	duplicate := cache.FillFrom("second-honest", tip, 1, honest[:18])
	require.Equal(t, headerAccepted, duplicate.rejection, "honest headers below the checkpoint that the tree holds are not a fork")
	require.True(t, duplicate.accepted)

	for h := int32(1); h <= 25; h++ {
		got, ok := cache.At(h)
		require.True(t, ok)
		require.Equal(t, honestHashes[h-1], got, "height %d names the honest branch", h)
	}
}

// testnetTrunk is a sqlitememory store holding testnet's real headers 1 to 547
// as the committed chain above the store's own genesis, so the committed tip
// is 547 and the checkpoint at 546 is committed.
func testnetTrunk(t *testing.T) (*realTrunk, []*wire.BlockHeader) {
	t.Helper()

	data, err := os.ReadFile("../../blockchain/testdata/testnet_headers_0_547.bin")
	require.NoError(t, err)

	headers := make([]*wire.BlockHeader, 0, 548)

	for i := 0; i < len(data); i += 80 {
		var header wire.BlockHeader
		require.NoError(t, header.Deserialize(bytes.NewReader(data[i:i+80])))
		headers = append(headers, &header)
	}

	params := chaincfg.TestNetParams

	return newRealTrunk(t, &params, 1, headers[1:]), headers
}

// Through the manager over a real testnet trunk: a batch forking from the
// committed chain below its last checkpoint (546) is refused and costs the
// sender its connection, while a batch the store holds whole (a lagging honest
// peer) and a fork above that checkpoint are dropped without blame.
func TestHeaderFork_AForkFromTheCommittedChainBelowItsCheckpointDisconnects(t *testing.T) {
	cases := []struct {
		name       string
		idx        uint8
		batch      func(headers []*wire.BlockHeader) []*wire.BlockHeader
		disconnect bool
	}{
		{
			name: "fork from committed 100",
			idx:  244,
			batch: func(headers []*wire.BlockHeader) []*wire.BlockHeader {
				fork, _ := forgedRun(headers[100].BlockHash(), 5)

				return append(append([]*wire.BlockHeader{}, headers[95:101]...), fork...)
			},
			disconnect: true,
		},
		{
			name: "honest headers the store holds",
			idx:  245,
			batch: func(headers []*wire.BlockHeader) []*wire.BlockHeader {
				return headers[101:301]
			},
		},
		{
			name: "fork from the committed checkpoint 546",
			idx:  246,
			batch: func(headers []*wire.BlockHeader) []*wire.BlockHeader {
				fork, _ := forgedRun(headers[546].BlockHash(), 5)

				return fork
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trunk, headers := testnetTrunk(t)

			sm := newHeaderCacheManager(t)
			sm.blockchainClient = trunk.client
			sm.chainParams = trunk.params
			sm.headerCache = newHeaderCache().
				WithCheckpoints(trunk.params.Checkpoints).
				WithHeaderRules(trunk.rules(t, time.Now()))

			best, tipHash, ok := sm.committedTip()
			require.True(t, ok)
			require.Equal(t, int32(547), best, "sanity: the seeded testnet trunk is committed")
			require.Equal(t, headers[547].BlockHash(), tipHash)

			peer, _, _ := connectRacePeer(t, tc.idx, 1000)

			require.False(t, sm.fillHeaderCache(peer, headersMsgOf(t, tc.batch(headers))))
			require.Equal(t, !tc.disconnect, peer.Connected())
			require.Zero(t, sm.headerCache.heldHeaders())
		})
	}
}
