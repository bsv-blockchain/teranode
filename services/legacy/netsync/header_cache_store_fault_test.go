package netsync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// faultyTrunk is a real blockchain client with a hook in front of
// GetBlockHeader, so a test can make one lookup fail, or stall, the way a
// gRPC call to the blockchain service can. Every other call goes straight
// through.
type faultyTrunk struct {
	blockchain.HeaderSource

	mu     sync.Mutex
	before func(ctx context.Context, hash chainhash.Hash) error
	calls  int
}

func (f *faultyTrunk) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	f.mu.Lock()
	f.calls++
	before := f.before
	f.mu.Unlock()

	if before != nil {
		if err := before(ctx, *hash); err != nil {
			return nil, nil, err
		}
	}

	return f.HeaderSource.GetBlockHeader(ctx, hash)
}

// A batch the store holds whole, from an honest lagging peer, must not be read
// as a fork when one store lookup fails: only "not found" means not stored.
// The first lookup trunkFork makes is the batch's last header; failing it with
// a transient error used to read as "not stored", and the binary search then
// placed the fork at height 300, below the committed checkpoint at 546.
func TestHeaderFork_AFailedStoreLookupIsNotAFork(t *testing.T) {
	trunk, headers := testnetTrunk(t)

	failing := &faultyTrunk{HeaderSource: trunk.client}
	last := headers[300].BlockHash()

	var failed bool

	failing.before = func(_ context.Context, hash chainhash.Hash) error {
		if hash == last && !failed {
			failed = true

			return errors.NewServiceUnavailableError("blockchain service unavailable")
		}

		return nil
	}

	rules, err := newHeaderRules(ulogger.TestLogger{}, trunk.settings, trunk.params, failing)
	require.NoError(t, err)

	rules.now = time.Now

	sm := newHeaderCacheManager(t)
	sm.blockchainClient = trunk.client
	sm.chainParams = trunk.params
	sm.headerCache = newHeaderCache().
		WithCheckpoints(trunk.params.Checkpoints).
		WithHeaderRules(rules)

	peer, _, _ := connectRacePeer(t, 247, 1000)

	require.False(t, sm.fillHeaderCache(peer, headersMsgOf(t, headers[101:301])))
	require.True(t, failed, "sanity: the lookup was failed")
	require.True(t, peer.Connected(), "a failed store lookup says nothing about the peer")
	require.Zero(t, sm.headerCache.heldHeaders())

	// The same batch with the store answering is the honest lagging peer it is.
	require.False(t, sm.fillHeaderCache(peer, headersMsgOf(t, headers[101:301])))
	require.True(t, peer.Connected())
}

// A lookup that fails partway through the binary search abandons the batch
// too, rather than moving the search as if the header were missing.
func TestHeaderFork_AFailedLookupMidSearchIsNotAFork(t *testing.T) {
	trunk, headers := testnetTrunk(t)

	fork, _ := forgedRun(headers[546].BlockHash(), 5)
	batch := append(copyHeaders(headers[400:547]), fork...)

	failing := &faultyTrunk{HeaderSource: trunk.client}

	// The batch's last header is forged, so the first lookup misses honestly;
	// every lookup after it is part of the search, and the first is failed.
	failing.before = func(_ context.Context, _ chainhash.Hash) error {
		failing.mu.Lock()
		defer failing.mu.Unlock()

		if failing.calls == 2 {
			return errors.NewServiceUnavailableError("blockchain service unavailable")
		}

		return nil
	}

	cache := newHeaderCache().WithCheckpoints(trunk.params.Checkpoints)
	result := trunkFork(context.Background(), &headerRules{trunk: failing}, cache.checkpoints, 547, batch, mustLinked(t, batch))
	require.Equal(t, fillResult{}, result)
	require.Equal(t, 2, failing.calls, "the search stopped at the failed lookup")
}

func mustLinked(t *testing.T, headers []*wire.BlockHeader) []chainhash.Hash {
	t.Helper()

	hashes, ok := linkedHashes(headers, nil)
	require.True(t, ok)

	return hashes
}
