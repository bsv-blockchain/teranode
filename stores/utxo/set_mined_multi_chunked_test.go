package utxo

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// chunkSpyStore records every SetMinedMulti call's chunk size and hashes.
// Embeds Store as a nil interface: any OTHER method call panics, proving the
// helper touches nothing but SetMinedMulti.
type chunkSpyStore struct {
	Store
	mu     sync.Mutex
	calls  [][]*chainhash.Hash
	info   []MinedBlockInfo
	failOn int // 1-based call number to fail on; 0 = never fail
}

func (s *chunkSpyStore) SetMinedMulti(_ context.Context, hashes []*chainhash.Hash, info MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, hashes)
	s.info = append(s.info, info)
	if s.failOn > 0 && len(s.calls) == s.failOn {
		return nil, errors.NewStorageError("boom")
	}
	return nil, nil
}

func makeHashes(n int) []*chainhash.Hash {
	out := make([]*chainhash.Hash, n)
	for i := 0; i < n; i++ {
		h := chainhash.Hash{}
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		out[i] = &h
	}
	return out
}

func TestSetMinedMultiChunked(t *testing.T) {
	info := MinedBlockInfo{BlockID: 42, BlockHeight: 100, OnLongestChain: true}

	t.Run("empty input: no calls, no error", func(t *testing.T) {
		spy := &chunkSpyStore{}
		require.NoError(t, SetMinedMultiChunked(context.Background(), ulogger.TestLogger{}, spy, nil, info, 10, 4))
		require.Empty(t, spy.calls)
	})

	t.Run("25 hashes, batch 10, workers 2: every hash exactly once, no chunk over 10", func(t *testing.T) {
		spy := &chunkSpyStore{}
		hashes := makeHashes(25)
		require.NoError(t, SetMinedMultiChunked(context.Background(), ulogger.TestLogger{}, spy, hashes, info, 10, 2))

		seen := map[chainhash.Hash]int{}
		for _, call := range spy.calls {
			require.LessOrEqual(t, len(call), 10, "chunk exceeds batchSize")
			require.NotEmpty(t, call)
			for _, h := range call {
				seen[*h]++
			}
		}
		require.Len(t, seen, 25)
		for h, n := range seen {
			require.Equal(t, 1, n, "hash %s sent %d times", h.String(), n)
		}
		// every call carries the caller's exact info (incl. OnLongestChain)
		for _, gotInfo := range spy.info {
			require.Equal(t, info, gotInfo)
		}
	})

	t.Run("floors: batchSize 0 and maxWorkers 0 behave as 1", func(t *testing.T) {
		spy := &chunkSpyStore{}
		hashes := makeHashes(3)
		require.NoError(t, SetMinedMultiChunked(context.Background(), ulogger.TestLogger{}, spy, hashes, info, 0, 0))
		require.Len(t, spy.calls, 3) // batchSize floored to 1 => one call per hash
	})

	t.Run("store error propagates wrapped", func(t *testing.T) {
		spy := &chunkSpyStore{failOn: 1}
		hashes := makeHashes(5)
		err := SetMinedMultiChunked(context.Background(), ulogger.TestLogger{}, spy, hashes, info, 2, 1)
		require.Error(t, err)
	})
}
