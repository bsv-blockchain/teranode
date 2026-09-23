package blockchain

import (
	"container/ring"
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchain_sql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// getBlockHeadersToCommonAncestorByWalk is the implementation this package used before
// the indexed lookup: it reads the chain backwards in 1,000-header pages until a locator
// hash turns up, keeping the last maxHeaders headers in a ring. It is kept here as the
// oracle the new implementation is checked against, and as the thing whose store-read
// count the budget test contrasts with.
func getBlockHeadersToCommonAncestorByWalk(ctx context.Context, store blockchain_store.Store, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	const (
		numberOfHeaders = 1_000
		searchLimit     = 10_000
	)

	var commonAncestorMeta *model.BlockHeaderMeta

	blockLocatorMap := make(map[chainhash.Hash]struct{}, len(blockLocatorHashes))
	for _, hash := range blockLocatorHashes {
		blockLocatorMap[*hash] = struct{}{}
	}

	maxInt := int(maxHeaders)
	hashStart := hashTarget
	lastNHeaders := ring.New(maxInt)
	lastNMetas := ring.New(maxInt)

out:
	for searchCount := 0; searchCount < searchLimit; searchCount++ {
		headers, headerMetas, err := store.GetBlockHeaders(ctx, hashStart, numberOfHeaders)
		if err != nil {
			return nil, nil, errors.NewStorageError("failed to get block headers", err)
		}

		if len(headers) <= 1 {
			break
		}

		for idx, header := range headers {
			lastNHeaders.Value = header
			lastNHeaders = lastNHeaders.Next()
			lastNMetas.Value = headerMetas[idx]
			lastNMetas = lastNMetas.Next()

			if _, ok := blockLocatorMap[*header.Hash()]; ok {
				commonAncestorMeta = headerMetas[idx]
				break out
			}
		}

		hashStart = headers[len(headers)-1].HashPrevBlock
	}

	if commonAncestorMeta == nil {
		return nil, nil, errors.NewNotFoundError("common ancestor hash not found after scanning last %d headers", searchLimit*numberOfHeaders)
	}

	return ringToSlice[*model.BlockHeader](lastNHeaders), ringToSlice[*model.BlockHeaderMeta](lastNMetas), nil
}

func ringToSlice[T any](r *ring.Ring) []T {
	slice := make([]T, 0, r.Len())

	r.Do(func(value interface{}) {
		if value != nil {
			slice = append(slice, value.(T))
		}
	})

	return slice
}

// countingStore counts GetBlockHeaders calls and the headers they return, which is the
// work an unauthenticated caller of the asset service's common-ancestor route can buy.
type countingStore struct {
	blockchain_store.Store

	headerCalls   int
	headersServed int
	otherCalls    int
}

func (c *countingStore) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	headers, metas, err := c.Store.GetBlockHeaders(ctx, blockHash, numberOfHeaders)

	c.headerCalls++
	c.headersServed += len(headers)

	return headers, metas, err
}

func (c *countingStore) GetLatestBlockHeaderFromBlockLocator(ctx context.Context, bestBlockHash *chainhash.Hash, blockLocator []chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.otherCalls++
	return c.Store.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, blockLocator)
}

func (c *countingStore) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.otherCalls++
	return c.Store.GetBlockHeader(ctx, blockHash)
}

func (c *countingStore) GetBlockInChainByHeightHash(ctx context.Context, height uint32, startHash *chainhash.Hash) (*model.Block, bool, error) {
	c.otherCalls++
	return c.Store.GetBlockInChainByHeightHash(ctx, height, startHash)
}

// newChainStore returns a store holding a chain of numberOfBlocks blocks on top of
// genesis, and the blocks it stored.
func newChainStore(t *testing.T, numberOfBlocks int) (blockchain_store.Store, []*model.Block) {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchain_sql.New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	blocks := chainOfBlocks(t, tSettings.ChainCfgParams.GenesisHash, numberOfBlocks)
	for _, block := range blocks {
		_, _, err = store.StoreBlock(t.Context(), block, "")
		require.NoError(t, err)
	}

	return store, blocks
}

func chainOfBlocks(t *testing.T, genesisHash *chainhash.Hash, numberOfBlocks int) []*model.Block {
	t.Helper()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*coinbase.TxIDChainHash(), 0, 0))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	hashPrevBlock := genesisHash
	blocks := make([]*model.Block, 0, numberOfBlocks)

	for i := 0; i < numberOfBlocks; i++ {
		block := &model.Block{
			Header: &model.BlockHeader{
				Version: 1,
				// nolint:gosec // test heights are small.
				Timestamp:      1231469665 + uint32(i),
				Nonce:          2573394689,
				HashPrevBlock:  hashPrevBlock,
				HashMerkleRoot: subtree.RootHash(),
				Bits:           *nBits,
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		}

		blocks = append(blocks, block)
		hashPrevBlock = block.Hash()
	}

	return blocks
}

func headerHashes(t *testing.T, headers []*model.BlockHeader) []string {
	t.Helper()

	hashes := make([]string, len(headers))
	for i, header := range headers {
		hashes[i] = header.Hash().String()
	}

	return hashes
}

// TestGetBlockHeadersToCommonAncestor_MatchesTheWalk pins the indexed lookup against the
// backward walk it replaces: same headers, same order, same metas, across locator
// distances either side of the response cap.
func TestGetBlockHeadersToCommonAncestor_MatchesTheWalk(t *testing.T) {
	store, blocks := newChainStore(t, 60)
	target := blocks[len(blocks)-1].Hash()

	for _, tc := range []struct {
		name          string
		locatorHeight int // index into blocks
		maxHeaders    uint32
	}{
		{name: "ancestor is the target itself", locatorHeight: 59, maxHeaders: 10},
		{name: "ancestor one below the target", locatorHeight: 58, maxHeaders: 10},
		{name: "span shorter than the cap", locatorHeight: 55, maxHeaders: 10},
		{name: "span exactly the cap", locatorHeight: 50, maxHeaders: 10},
		{name: "span longer than the cap", locatorHeight: 10, maxHeaders: 10},
		{name: "cap of one", locatorHeight: 10, maxHeaders: 1},
		{name: "cap far beyond the span", locatorHeight: 0, maxHeaders: 5_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			locator := []*chainhash.Hash{blocks[tc.locatorHeight].Hash()}

			wantHeaders, wantMetas, wantErr := getBlockHeadersToCommonAncestorByWalk(t.Context(), store, target, locator, tc.maxHeaders)
			require.NoError(t, wantErr)

			gotHeaders, gotMetas, err := getBlockHeadersToCommonAncestor(t.Context(), store, target, locator, tc.maxHeaders)
			require.NoError(t, err)

			require.Equal(t, headerHashes(t, wantHeaders), headerHashes(t, gotHeaders))
			require.Equal(t, wantMetas, gotMetas)

			// Self-check on the oracle: the response ends at the common ancestor and is
			// ordered from the highest height down.
			require.Equal(t, blocks[tc.locatorHeight].Hash().String(), gotHeaders[len(gotHeaders)-1].Hash().String())
			require.Greater(t, gotMetas[0].Height, gotMetas[len(gotMetas)-1].Height-1)
		})
	}
}

// TestGetBlockHeadersToCommonAncestor_AbsentLocatorCostsAFixedNumberOfReads is the
// regression test for bitcoin-sv/teranode#4894. An absent locator used to walk the chain
// to its start, so the store work grew with chain height for a response of at most
// maxHeaders. The lookup must now cost a small fixed number of reads.
func TestGetBlockHeadersToCommonAncestor_AbsentLocatorCostsAFixedNumberOfReads(t *testing.T) {
	inner, blocks := newChainStore(t, 2_500)
	target := blocks[len(blocks)-1].Hash()

	absent, err := chainhash.NewHashFromStr("00000000000000000000000000000000000000000000000000000000deadbeef")
	require.NoError(t, err)

	locator := []*chainhash.Hash{absent}

	walked := &countingStore{Store: inner}
	_, _, err = getBlockHeadersToCommonAncestorByWalk(t.Context(), walked, target, locator, 10)
	require.Error(t, err, "an absent locator has no common ancestor")
	require.True(t, errors.Is(err, errors.ErrNotFound))

	indexed := &countingStore{Store: inner}
	_, _, err = getBlockHeadersToCommonAncestor(t.Context(), indexed, target, locator, 10)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound), "an absent locator must still be reported as not found")

	// The walk paid for the whole chain; the lookup must not.
	require.Greater(t, walked.headersServed, 2_000, "sanity: the old walk read the chain")
	require.Zero(t, indexed.headerCalls, "an absent locator must not read a single header page")
	require.LessOrEqual(t, indexed.otherCalls, 2)
}

// TestGetBlockHeadersToCommonAncestor_ReadBudgetIsIndependentOfDistance pins the same
// property on the success path: a locator matching at genesis costs the same number of
// reads as one matching next to the tip.
func TestGetBlockHeadersToCommonAncestor_ReadBudgetIsIndependentOfDistance(t *testing.T) {
	inner, blocks := newChainStore(t, 2_500)
	target := blocks[len(blocks)-1].Hash()

	for _, tc := range []struct {
		name    string
		locator *chainhash.Hash
	}{
		{name: "next to the tip", locator: blocks[len(blocks)-2].Hash()},
		{name: "start of the chain", locator: blocks[0].Hash()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counted := &countingStore{Store: inner}

			headers, _, err := getBlockHeadersToCommonAncestor(t.Context(), counted, target, []*chainhash.Hash{tc.locator}, 10)
			require.NoError(t, err)
			require.NotEmpty(t, headers)

			require.LessOrEqual(t, counted.headerCalls, 1, "one page read, whatever the distance")
			require.LessOrEqual(t, counted.headersServed, 10, "never more headers than the cap")
			require.LessOrEqual(t, counted.otherCalls, 3)
		})
	}
}

// TestGetBlockHeadersToCommonAncestor_RejectsEmptyInput covers the degenerate inputs the
// walk handled by accident: an empty locator scanned the whole chain and found nothing,
// and maxHeaders of 0 built a nil ring and panicked on first use.
func TestGetBlockHeadersToCommonAncestor_RejectsEmptyInput(t *testing.T) {
	store, blocks := newChainStore(t, 5)
	target := blocks[len(blocks)-1].Hash()

	t.Run("empty locator", func(t *testing.T) {
		_, _, err := getBlockHeadersToCommonAncestor(t.Context(), store, target, nil, 10)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("zero maxHeaders", func(t *testing.T) {
		require.NotPanics(t, func() {
			_, _, err := getBlockHeadersToCommonAncestor(t.Context(), store, target, []*chainhash.Hash{blocks[0].Hash()}, 0)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrNotFound))
		})
	})

	t.Run("unknown target", func(t *testing.T) {
		unknown, err := chainhash.NewHashFromStr("00000000000000000000000000000000000000000000000000000000c0ffee00")
		require.NoError(t, err)

		_, _, err = getBlockHeadersToCommonAncestor(t.Context(), store, unknown, []*chainhash.Hash{blocks[0].Hash()}, 10)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
	})
}
