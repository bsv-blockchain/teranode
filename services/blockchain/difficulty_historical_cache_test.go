package blockchain

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

func historicalCacheChain(t *testing.T, height uint32) (*Difficulty, []*model.BlockHeader, *historicalHeaderReadStore) {
	t.Helper()
	d, headers := historicalDifficultyChain(t, chaincfg.TestNetParams, height, 600)
	d.settings.BlockAssembly.DifficultyCache = true
	reads := &historicalHeaderReadStore{Store: d.store}
	d.store = reads
	return d, headers, reads
}

func storeHistoricalCacheChild(t *testing.T, d *Difficulty, parent *model.BlockHeader, height uint32, bits model.NBit) *model.BlockHeader {
	t.Helper()
	genesis, err := model.NewBlockFromMsgBlock(d.settings.ChainCfgParams.GenesisBlock, nil)
	require.NoError(t, err)
	genesis.CoinbaseTx.LockTime = 1
	header := *parent
	header.HashPrevBlock = parent.Hash()
	header.Timestamp += 600
	header.Bits = bits
	_, _, err = d.store.StoreBlock(t.Context(), &model.Block{Header: &header, CoinbaseTx: genesis.CoinbaseTx, TransactionCount: 1, Height: height}, "test")
	require.NoError(t, err)
	return &header
}

func requireHistoricalCacheTarget(t *testing.T, d *Difficulty, parent *model.BlockHeader, height uint32, delay int64, want string) *model.NBit {
	t.Helper()
	bits, err := d.CalcNextWorkRequired(t.Context(), parent, height, int64(parent.Timestamp)+delay)
	require.NoError(t, err)
	require.NotNil(t, bits)
	require.Equal(t, want, bits.String())
	return bits
}

func TestDifficultyHistoricalCacheOwnsReturnedBits(t *testing.T) {
	d, headers, reads := historicalCacheChain(t, 20)
	parent := storeHistoricalCacheChild(t, d, headers[20], 21, *d.powLimitnBits)
	bits := requireHistoricalCacheTarget(t, d, parent, 21, 600, "1c0ffff0")
	queried := reads.requestedHeaders.Load()
	require.Positive(t, queried)
	// Neither a cold result nor a later cache hit may expose mutable cache state.
	*bits = *d.powLimitnBits
	bits = requireHistoricalCacheTarget(t, d, parent, 21, 600, "1c0ffff0")
	*bits = *d.powLimitnBits
	requireHistoricalCacheTarget(t, d, parent, 21, 600, "1c0ffff0")
	require.Equal(t, queried, reads.requestedHeaders.Load(), "repeated parent must use the saved restoration")
}

func TestDifficultyHistoricalCacheDelayedCandidates(t *testing.T) {
	d, headers, reads := historicalCacheChain(t, 20)
	parent := storeHistoricalCacheChild(t, d, headers[20], 21, *d.powLimitnBits)
	requireHistoricalCacheTarget(t, d, parent, 21, 1201, "1d00ffff")
	require.Zero(t, reads.requestedHeaders.Load(), "cold delayed candidate needs no ancestry")
	requireHistoricalCacheTarget(t, d, parent, 21, 1200, "1c0ffff0")
	queried := reads.requestedHeaders.Load()
	require.Positive(t, queried, "a delayed candidate must not seed the timely target")
	requireHistoricalCacheTarget(t, d, parent, 21, 1201, "1d00ffff")
	requireHistoricalCacheTarget(t, d, parent, 21, 1200, "1c0ffff0")
	require.Equal(t, queried, reads.requestedHeaders.Load(), "delayed candidate must preserve the warm restoration")
}

func TestDifficultyHistoricalCacheForkIsolation(t *testing.T) {
	d, headers, _ := historicalCacheChain(t, 20)
	otherBits, err := model.NewNBitFromString("1c07fff8")
	require.NoError(t, err)
	other := storeHistoricalCacheChild(t, d, headers[19], 20, *otherBits)
	tips := []*model.BlockHeader{
		storeHistoricalCacheChild(t, d, headers[20], 21, *d.powLimitnBits),
		storeHistoricalCacheChild(t, d, other, 21, *d.powLimitnBits),
	}
	wants := []string{"1c0ffff0", "1c07fff8"}
	for _, branch := range []int{0, 1, 0} {
		requireHistoricalCacheTarget(t, d, tips[branch], 21, 600, wants[branch])
	}
	type result struct {
		bits *model.NBit
		err  error
		want string
	}
	const queries = 16
	results := make(chan result, queries)
	start := make(chan struct{})
	ctx := t.Context()
	for i := range queries {
		go func(branch int) {
			<-start
			bits, err := d.CalcNextWorkRequired(ctx, tips[branch], 21, int64(tips[branch].Timestamp)+600)
			results <- result{bits: bits, err: err, want: wants[branch]}
		}(i % 2)
	}
	close(start)
	// Drain before assertions so a failure cannot close SQLite under active readers.
	completed := make([]result, queries)
	for i := range completed {
		completed[i] = <-results
	}
	for _, got := range completed {
		require.NoError(t, got.err)
		require.NotNil(t, got.bits)
		require.Equal(t, got.want, got.bits.String())
	}
}

func TestDifficultyHistoricalCacheRetargetBoundary(t *testing.T) {
	d, headers, _ := historicalCacheChain(t, 2013)
	parent := storeHistoricalCacheChild(t, d, headers[2013], 2014, *d.powLimitnBits)
	requireHistoricalCacheTarget(t, d, parent, 2014, 600, "1c0ffff0")
	parent = storeHistoricalCacheChild(t, d, parent, 2015, *d.powLimitnBits)
	// Retarget uses the minimum-difficulty parent's target and 2015 elapsed gaps.
	// It wins over both the old restoration cache and a delayed candidate.
	requireHistoricalCacheTarget(t, d, parent, 2015, 1201, "1d00ffde")
	boundaryBits, err := model.NewNBitFromString("1d00ffde")
	require.NoError(t, err)
	parent = storeHistoricalCacheChild(t, d, parent, 2016, *boundaryBits)
	requireHistoricalCacheTarget(t, d, parent, 2016, 600, "1d00ffde")
	parent = storeHistoricalCacheChild(t, d, parent, 2017, *d.powLimitnBits)
	requireHistoricalCacheTarget(t, d, parent, 2017, 600, "1d00ffde")
}

func TestDifficultyHistoricalCacheFailureAndDisable(t *testing.T) {
	d, headers, reads := historicalCacheChain(t, 20)
	parent := storeHistoricalCacheChild(t, d, headers[20], 21, *d.powLimitnBits)
	requireHistoricalCacheTarget(t, d, parent, 21, 600, "1c0ffff0")
	missing := *parent
	unknown := *parent.HashPrevBlock
	unknown[0] ^= 0xff
	missing.HashPrevBlock = &unknown
	bits, err := d.CalcNextWorkRequired(t.Context(), &missing, 21, int64(missing.Timestamp)+600)
	require.ErrorIs(t, err, errors.ErrNotFound)
	require.Nil(t, bits)
	queried := reads.requestedHeaders.Load()
	requireHistoricalCacheTarget(t, d, parent, 21, 600, "1c0ffff0")
	require.Equal(t, queried, reads.requestedHeaders.Load(), "missing ancestry must not replace the valid cached parent")
	d.settings.BlockAssembly.DifficultyCache = false
	for range 2 {
		requireHistoricalCacheTarget(t, d, parent, 21, 600, "1c0ffff0")
		require.Greater(t, reads.requestedHeaders.Load(), queried, "disabled cache must query ancestry each time")
		queried = reads.requestedHeaders.Load()
	}
}
