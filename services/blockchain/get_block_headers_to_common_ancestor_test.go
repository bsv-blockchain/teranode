package blockchain

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// countingStore wraps a real blockchain store and counts the header page reads
// performed by the common-ancestor walk. It is not a mock: every call is served
// by the underlying sqlitememory store.
type countingStore struct {
	blockchainstore.Store

	getBlockHeadersCalls atomic.Int64
}

func (c *countingStore) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.getBlockHeadersCalls.Add(1)

	return c.Store.GetBlockHeaders(ctx, blockHash, numberOfHeaders)
}

// buildCommonAncestorTestChain stores numBlocks blocks on top of genesis and
// returns the store plus the hashes of every block, genesis first.
func buildCommonAncestorTestChain(t *testing.T, numBlocks int) (*countingStore, []*chainhash.Hash) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	genesis, err := store.GetBlockByHeight(ctx, 0)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	hashes := make([]*chainhash.Hash, 0, numBlocks+1)
	hashes = append(hashes, genesis.Hash())

	prev := genesis.Hash()
	now := uint32(time.Now().Unix()) // nolint:gosec

	for i := 0; i < numBlocks; i++ {
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      now + uint32(i), // nolint:gosec
				Nonce:          uint32(i + 1),   // nolint:gosec
				Bits:           *bits,
				HashPrevBlock:  prev,
				HashMerkleRoot: &chainhash.Hash{},
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			SizeInBytes:      80,
		}

		_, _, err = store.StoreBlock(ctx, block, "")
		require.NoError(t, err)

		prev = block.Hash()
		hashes = append(hashes, prev)
	}

	return &countingStore{Store: store}, hashes
}

// TestGetBlockHeadersToCommonAncestorUnknownLocatorIsBounded asserts that a
// locator made entirely of valid-format but unknown hashes does not walk the
// chain. Before the locator pre-flight was added the walk read the whole chain
// one 1,000-header page at a time before reporting the same not-found error.
func TestGetBlockHeadersToCommonAncestorUnknownLocatorIsBounded(t *testing.T) {
	const numBlocks = 2_500

	store, _ := buildCommonAncestorTestChain(t, numBlocks)
	ctx := context.Background()

	tip, err := store.GetBlockByHeight(ctx, numBlocks)
	require.NoError(t, err)

	// A locator of syntactically valid hashes that are not in the store at all.
	locator := make([]*chainhash.Hash, 0, 32)
	for i := 0; i < 32; i++ {
		h := chainhash.HashH([]byte{byte(i), 0xde, 0xad, 0xbe, 0xef})
		locator = append(locator, &h)
	}

	_, _, err = getBlockHeadersToCommonAncestor(ctx, store, tip.Hash(), locator, 100, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound), "expected a not-found error, got %v", err)

	// The whole request must be answerable without paging through the chain.
	require.LessOrEqual(t, store.getBlockHeadersCalls.Load(), int64(1),
		"unknown locator must not page through the chain")
}

// TestGetBlockHeadersToCommonAncestorLegitimateLocator is the regression that
// matters: a normal ~32-entry locator resolving near the tip must return
// exactly the headers it returns today, with default settings.
func TestGetBlockHeadersToCommonAncestorLegitimateLocator(t *testing.T) {
	const numBlocks = 2_500

	store, hashes := buildCommonAncestorTestChain(t, numBlocks)
	ctx := context.Background()

	tipHash := hashes[numBlocks]

	// Build a real block locator: the tip, the previous 10, then exponentially
	// spaced heights back to genesis - the same shape getBlockLocator produces.
	heights := computeLocatorHeights(uint32(numBlocks)) // nolint:gosec

	locator := make([]*chainhash.Hash, 0, len(heights))
	for _, h := range heights {
		locator = append(locator, hashes[h])
	}

	require.GreaterOrEqual(t, len(locator), 12)

	headers, metas, err := getBlockHeadersToCommonAncestor(ctx, store, tipHash, locator, 100, 0)
	require.NoError(t, err)
	require.Len(t, headers, 1)
	require.Len(t, metas, 1)

	// The locator contains the tip itself, so the common ancestor is the tip.
	require.Equal(t, tipHash.String(), headers[0].Hash().String())
	require.Equal(t, uint32(numBlocks), metas[0].Height)

	// Now a locator that resolves a little below the tip.
	deeperHeights := computeLocatorHeights(uint32(numBlocks - 50)) // nolint:gosec

	deeper := make([]*chainhash.Hash, 0, len(deeperHeights))
	for _, h := range deeperHeights {
		deeper = append(deeper, hashes[h])
	}

	headers, metas, err = getBlockHeadersToCommonAncestor(ctx, store, tipHash, deeper, 100, 0)
	require.NoError(t, err)
	require.Len(t, headers, 51)
	require.Len(t, metas, 51)
	require.Equal(t, tipHash.String(), headers[0].Hash().String())
	require.Equal(t, hashes[numBlocks-50].String(), headers[len(headers)-1].Hash().String())
	require.Equal(t, uint32(numBlocks-50), metas[len(metas)-1].Height)
}

// TestGetBlockHeadersToCommonAncestorWalkDepthBudget covers the opt-in hard
// budget: a locator that does resolve, but only at genesis, is allowed to walk
// the whole chain by default and is cut short when a budget is configured.
func TestGetBlockHeadersToCommonAncestorWalkDepthBudget(t *testing.T) {
	const numBlocks = 2_500

	store, hashes := buildCommonAncestorTestChain(t, numBlocks)
	ctx := context.Background()

	tipHash := hashes[numBlocks]
	locator := []*chainhash.Hash{hashes[0]}

	// Default (0) - unlimited, the walk reaches genesis as it does today.
	headers, _, err := getBlockHeadersToCommonAncestor(ctx, store, tipHash, locator, 100, 0)
	require.NoError(t, err)
	require.Len(t, headers, 100)
	require.Equal(t, hashes[0].String(), headers[len(headers)-1].Hash().String())

	before := store.getBlockHeadersCalls.Load()

	// Budget smaller than the distance to genesis - the walk stops and reports
	// not found rather than burning the rest of the chain.
	_, _, err = getBlockHeadersToCommonAncestor(ctx, store, tipHash, locator, 100, 1_000)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound), "expected a not-found error, got %v", err)

	require.LessOrEqual(t, store.getBlockHeadersCalls.Load()-before, int64(2),
		"the budget must cap the number of header pages read")
}

// TestGetBlockHeadersToCommonAncestorMainChainDeepLocatorSkipsTheWalk is the
// perf regression: on default settings (asset_maxLocatorWalkDepth=0), a locator
// that resolves deep in the chain must be answered with a single bounded range
// read, not by paging backward from the tip one numberOfHeaders batch at a
// time. It must still return exactly what the walk returns.
func TestGetBlockHeadersToCommonAncestorMainChainDeepLocatorSkipsTheWalk(t *testing.T) {
	const numBlocks = 2_500

	store, hashes := buildCommonAncestorTestChain(t, numBlocks)
	ctx := context.Background()

	tipHash := hashes[numBlocks]
	// Ancestor is deep: height 5, far below the default page size (1,000) and
	// the requested maxHeaders (100).
	locator := []*chainhash.Hash{hashes[5]}

	headers, metas, err := getBlockHeadersToCommonAncestor(ctx, store, tipHash, locator, 100, 0)
	require.NoError(t, err)

	require.Equal(t, int64(0), store.getBlockHeadersCalls.Load(),
		"a main-chain target must be answered by a range read, not the paging walk")

	// Same result the walk produces: 100 headers, ending at the ancestor
	// (height 5, since 5+100-1=104 < tip), descending.
	require.Len(t, headers, 100)
	require.Len(t, metas, 100)
	require.Equal(t, hashes[104].String(), headers[0].Hash().String())
	require.Equal(t, hashes[5].String(), headers[len(headers)-1].Hash().String())
	require.Equal(t, uint32(104), metas[0].Height)
	require.Equal(t, uint32(5), metas[len(metas)-1].Height)
}

// TestGetBlockHeadersToCommonAncestorForkTargetUsesTheWalk asserts that a
// stale/fork tip target is never answered by the on_main_chain range read: the
// range read would silently substitute whichever main-chain block sits at the
// same heights, not the fork's own ancestors. It must fall back to the
// (correct) walk and return the fork's own headers.
func TestGetBlockHeadersToCommonAncestorForkTargetUsesTheWalk(t *testing.T) {
	store, hashes := buildCommonAncestorTestChain(t, 5)
	ctx := context.Background()

	// hashes[3] is the shared ancestor; fork off from there with two blocks
	// that never become the main chain (equal work, stored second).
	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	now := uint32(time.Now().Unix()) // nolint:gosec

	forkBlock4 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      now + 1_000,
			Nonce:          9001,
			Bits:           *bits,
			HashPrevBlock:  hashes[3],
			HashMerkleRoot: &chainhash.Hash{1},
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      80,
	}
	_, _, err = store.StoreBlock(ctx, forkBlock4, "")
	require.NoError(t, err)

	forkBlock5 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      now + 1_001,
			Nonce:          9002,
			Bits:           *bits,
			HashPrevBlock:  forkBlock4.Hash(),
			HashMerkleRoot: &chainhash.Hash{2},
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      80,
	}
	_, _, err = store.StoreBlock(ctx, forkBlock5, "")
	require.NoError(t, err)

	// hashes[5] (the real main chain tip) has strictly more work stored first,
	// so the fork stays a fork; forkBlock5 is a stale tip, not on_main_chain.
	locator := []*chainhash.Hash{hashes[3]}

	headers, metas, err := getBlockHeadersToCommonAncestor(ctx, store, forkBlock5.Hash(), locator, 100, 0)
	require.NoError(t, err)
	require.NotEmpty(t, store.getBlockHeadersCalls.Load(),
		"a fork target must fall back to the walk, not the main-chain range read")

	require.Len(t, headers, 3)
	require.Len(t, metas, 3)
	// Descending: forkBlock5 (tip) first, then forkBlock4, then the ancestor
	// itself (hashes[3], shared with the main chain) - the fork's own headers,
	// not whatever sits on the main chain at heights 4 and 5.
	require.Equal(t, forkBlock5.Hash().String(), headers[0].Hash().String())
	require.Equal(t, forkBlock4.Hash().String(), headers[1].Hash().String())
	require.Equal(t, hashes[3].String(), headers[2].Hash().String())
	require.NotEqual(t, hashes[5].String(), headers[0].Hash().String())
	require.NotEqual(t, hashes[4].String(), headers[1].Hash().String())
}
