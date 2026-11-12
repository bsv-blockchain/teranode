package model

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// dedupLoadFixture builds a two-subtree block served from a rawSubtreeStore,
// with a PoW-valid header bound to the body. When dup is non-nil it is the last
// leaf of both subtrees.
func dedupLoadFixture(t *testing.T, dup *chainhash.Hash) (*Block, *subtreepkg.Subtree, *subtreepkg.Subtree, SubtreeStore) {
	t.Helper()

	b, subtrees, store := dedupLoadFixtureN(t, dup, 2, nil)

	return b, subtrees[0], subtrees[1], store.inner
}

// dedupLoadFixtureN is dedupLoadFixture with n full 4-leaf subtrees and a
// read-counting store. headerRoot overrides the merkle root the header is
// mined over; nil binds it to the body.
func dedupLoadFixtureN(t *testing.T, dup *chainhash.Hash, n int, headerRoot *chainhash.Hash) (*Block, []*subtreepkg.Subtree, *countingSubtreeStore) {
	t.Helper()

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	store := &rawSubtreeStore{data: make(map[chainhash.Hash][]byte)}
	subtrees := make([]*subtreepkg.Subtree, n)
	hashes := make([]*chainhash.Hash, n)

	for s := 0; s < n; s++ {
		st, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)

		leaves := 4
		if s == 0 {
			require.NoError(t, st.AddCoinbaseNode())

			leaves = 3
		}

		for i := 0; i < leaves-1; i++ {
			require.NoError(t, st.AddNode(randCorruptTestHash(t), 1, 0))
		}

		if dup != nil && (s == 0 || s == n-1) {
			require.NoError(t, st.AddNode(*dup, 1, 0))
		} else {
			require.NoError(t, st.AddNode(randCorruptTestHash(t), 1, 0))
		}

		store.put(t, st)
		subtrees[s] = st
		hashes[s] = st.RootHash()
	}

	if headerRoot == nil {
		headerRoot = testBodyMerkleRoot(t, coinbase, hashes, subtrees[0])
	}

	header := minedHeaderOnParent(t, 1, headerRoot, regtestGenesisHeader(t).Hash())

	b, err := NewBlock(header, coinbase, hashes, uint64(4*n), 123, 0, 0) // nolint: gosec
	require.NoError(t, err)

	return b, subtrees, &countingSubtreeStore{inner: store}
}

// testBodyMerkleRoot computes the header merkle root of a body of full
// subtrees straight from go-subtree, independently of CheckMerkleRoot.
func testBodyMerkleRoot(t *testing.T, coinbase *bt.Tx, hashes []*chainhash.Hash, first *subtreepkg.Subtree) *chainhash.Hash {
	t.Helper()

	root0, err := first.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) // nolint: gosec
	require.NoError(t, err)

	if len(hashes) == 1 {
		return root0
	}

	top, err := subtreepkg.NewIncompleteTreeByLeafCount(len(hashes))
	require.NoError(t, err)
	require.NoError(t, top.AddNode(*root0, 1, 0))

	for _, h := range hashes[1:] {
		require.NoError(t, top.AddNode(*h, 1, 0))
	}

	return top.RootHash()
}

// countingSubtreeStore counts subtree fetches.
type countingSubtreeStore struct {
	inner SubtreeStore
	reads atomic.Int32
}

func (c *countingSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	c.reads.Add(1)

	return c.inner.GetIoReader(ctx, key, fileType, opts...)
}

// TestGetAndValidateSubtreesWithDedup_FillsTxMap pins that the duplicate check
// is done while the subtrees load: the txMap comes back filled with every
// non-coinbase leaf at the same index checkDuplicateTransactions would give it,
// sized from the body as len(Subtrees) x first subtree length.
func TestGetAndValidateSubtreesWithDedup_FillsTxMap(t *testing.T) {
	b, st0, st1, store := dedupLoadFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	defer b.releaseTxMap()

	deduped, err := b.getAndValidateSubtreesWithDedup(ctx, ulogger.TestLogger{}, store, 2)
	require.NoError(t, err)
	require.True(t, deduped)

	require.NotNil(t, b.txMap)
	require.Equal(t, uint64(2*st0.Length()), b.txMapCount)
	require.Equal(t, 7, b.txMap.Length(), "every leaf but the coinbase placeholder")

	subtreeSize := st0.Size()

	idx, ok := b.txMap.Get(st0.Nodes[1].Hash)
	require.True(t, ok)
	require.Equal(t, uint64(1), idx)

	idx, ok = b.txMap.Get(st1.Nodes[2].Hash)
	require.True(t, ok)
	require.Equal(t, uint64(subtreeSize+2), idx)

	_, ok = b.txMap.Get(st0.Nodes[0].Hash)
	require.False(t, ok, "the coinbase placeholder is not inserted")
}

// TestGetAndValidateSubtreesWithDedup_DuplicateAcrossSubtrees pins that a
// transaction repeated in a later subtree fails the load with the same
// corrupt (re-download, never poison) classification the separate pass gives.
func TestGetAndValidateSubtreesWithDedup_DuplicateAcrossSubtrees(t *testing.T) {
	dup := randCorruptTestHash(t)

	b, _, _, store := dedupLoadFixture(t, &dup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	defer b.releaseTxMap()

	_, err := b.getAndValidateSubtreesWithDedup(ctx, ulogger.TestLogger{}, store, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate transaction")
	require.True(t, errors.IsBlockCorrupt(err), "a duplicated body is corrupt, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "must not poison")
}

// TestGetAndValidateSubtreesWithDedup_AlreadyLoaded pins the fallback signal:
// when the subtrees are already in memory nothing is fetched, so no dedup ran
// and the caller must do the separate pass.
func TestGetAndValidateSubtreesWithDedup_AlreadyLoaded(t *testing.T) {
	b, st0, st1, store := dedupLoadFixture(t, nil)
	b.SubtreeSlices = []*subtreepkg.Subtree{st0, st1}

	defer b.releaseTxMap()

	deduped, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, 2)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Nil(t, b.txMap)
}

// TestBlock_Valid_DuplicateAcrossLoadedSubtrees drives a duplicated body
// through Block.Valid with the subtrees fetched from a store, for both txMap
// backings. DiskMapDirs is set explicitly on each case: a developer's
// settings_local.conf can set block_diskMapDirs, which would otherwise silently
// route every Valid-level test down the disk-backed (separate pass) branch.
//
// Both paths bind the body to the header before fetching past the first and
// last subtree, so they agree: a bound duplicated body is reported as the
// duplicate, an unbound one as the merkle mismatch, and both are BlockCorrupt.
func TestBlock_Valid_DuplicateAcrossLoadedSubtrees(t *testing.T) {
	backings := []struct {
		name string
		dirs func(t *testing.T) []string
	}{
		{"in_memory_during_load", func(*testing.T) []string { return nil }},
		{"disk_separate_pass", func(t *testing.T) []string { return []string{t.TempDir()} }},
	}

	bodies := []struct {
		name    string
		unbound bool
		message string
	}{
		{"bound", false, "duplicate transaction"},
		{"unbound", true, "merkle root does not match"},
	}

	for _, backing := range backings {
		for _, body := range bodies {
			t.Run(backing.name+"/"+body.name, func(t *testing.T) {
				tSettings := test.CreateBaseTestSettings(t)
				tSettings.Block.DiskMapDirs = backing.dirs(t)

				var headerRoot *chainhash.Hash

				if body.unbound {
					other := randCorruptTestHash(t)
					headerRoot = &other
				}

				dup := randCorruptTestHash(t)
				b, _, store := dedupLoadFixtureN(t, &dup, 3, headerRoot)

				currentChain := regtestGenesisParentChain(t, b.Header)
				currentChainIDs := make([]uint32, 11)

				for i := 0; i < 11; i++ {
					currentChainIDs[i] = uint32(i) // nolint:gosec
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				valid, err := b.Valid(ctx, ulogger.TestLogger{}, store, nil, txmap.NewSyncedMap[chainhash.Hash, []uint32](),
					currentChain, currentChainIDs, tSettings, nil)
				require.False(t, valid)
				require.Error(t, err)
				require.Contains(t, err.Error(), body.message)
				require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)
				require.False(t, errors.Is(err, errors.ErrBlockInvalid), "must not poison")
				require.Nil(t, b.txMap, "the txMap must be released on the error path")
			})
		}
	}
}

// TestPutSubtreeInTxMap_NilTxMapIsTransient pins that a missing txMap fails the
// insert instead of panicking. The load-time dedup only allocates the map in
// onFirst, which runs because getAndValidateSubtrees reloads every subtree; if
// that ever changed and subtree 0 were skipped, onLoaded would reach the insert
// with no map. That is a node-side fault, so it must be retryable, never a
// verdict on the block.
func TestPutSubtreeInTxMap_NilTxMapIsTransient(t *testing.T) {
	b, _, st1, _ := dedupLoadFixture(t, nil)
	require.Nil(t, b.txMap)

	var err error

	require.NotPanics(t, func() {
		err = b.putSubtreeInTxMap(b.Hash().String(), st1, 1, st1.Size())
	})
	requireTransientError(t, err)
}

// TestGetAndValidateSubtreesWithDedup_UnboundBodyFailsBeforeAllocating pins the
// bound on the pre-merkle txMap sizing: the body is checked against the
// header's merkle root as soon as the first and last subtrees are loaded, so an
// unbound subtree list can neither size the txMap nor make the rest of the body
// be fetched.
func TestGetAndValidateSubtreesWithDedup_UnboundBodyFailsBeforeAllocating(t *testing.T) {
	other := randCorruptTestHash(t)
	b, _, store := dedupLoadFixtureN(t, nil, 4, &other)

	_, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "merkle root does not match")
	require.True(t, errors.IsBlockCorrupt(err), "an unbound body is corrupt, got: %v", err)
	require.Nil(t, b.txMap, "no txMap may be allocated for an unbound body")
	require.Equal(t, int32(2), store.reads.Load(), "only the first and last subtrees may be fetched before the binding check")
}

// TestGetAndValidateSubtreesWithDedup_BoundBodyLoadsEverySubtree is the
// positive side: a bound body passes the early check and every subtree loads.
func TestGetAndValidateSubtreesWithDedup_BoundBodyLoadsEverySubtree(t *testing.T) {
	b, _, store := dedupLoadFixtureN(t, nil, 4, nil)

	defer b.releaseTxMap()

	deduped, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, 2)
	require.NoError(t, err)
	require.True(t, deduped)
	require.Equal(t, int32(4), store.reads.Load())
	require.Equal(t, 15, b.txMap.Length(), "every leaf but the coinbase placeholder")
}

// TestGetAndValidateSubtreesWithDedup_DuplicateSubtreeHash pins that a subtree
// listed twice is rejected before the txMap is sized from the inflated count.
func TestGetAndValidateSubtreesWithDedup_DuplicateSubtreeHash(t *testing.T) {
	b, subtrees, store := dedupLoadFixtureN(t, nil, 3, nil)
	b.Subtrees = append(b.Subtrees, subtrees[1].RootHash())

	_, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, 2)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "a repeated subtree is corrupt, got: %v", err)
	require.Nil(t, b.txMap, "no txMap may be allocated for a repeated subtree")
}

// TestGetAndValidateSubtreesBound_UnboundBodyFailsEarly pins the same early
// binding check on the disk-map path, which allocates nothing during the load
// but would otherwise fetch every listed subtree before CheckMerkleRoot.
func TestGetAndValidateSubtreesBound_UnboundBodyFailsEarly(t *testing.T) {
	other := randCorruptTestHash(t)
	b, _, store := dedupLoadFixtureN(t, nil, 4, &other)

	err := b.getAndValidateSubtreesBound(context.Background(), ulogger.TestLogger{}, store, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "merkle root does not match")
	require.True(t, errors.IsBlockCorrupt(err))
	require.Equal(t, int32(2), store.reads.Load())
}
