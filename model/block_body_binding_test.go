package model

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// flatMerkleRoot is the canonical bitcoin merkle root over txids, with the
// duplicate-when-odd rule, computed without go-subtree or CheckMerkleRoot.
func flatMerkleRoot(h []chainhash.Hash) chainhash.Hash {
	for len(h) > 1 {
		if len(h)%2 == 1 {
			h = append(h, h[len(h)-1])
		}

		next := make([]chainhash.Hash, len(h)/2)

		for i := range next {
			var buf [64]byte

			copy(buf[:32], h[2*i][:])
			copy(buf[32:], h[2*i+1][:])
			next[i] = chainhash.DoubleHashH(buf[:])
		}

		h = next
	}

	return h[0]
}

// bodyShapeFixture builds a block whose subtrees have the given capacities and
// leaf counts, served from a counting store, under a PoW-valid header mined
// over the canonical flat merkle root of its transactions. newBlock returns a
// fresh, unloaded copy each call.
func bodyShapeFixture(t *testing.T, caps, lens []int) (newBlock func() *Block, store *countingSubtreeStore) {
	t.Helper()

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	raw := &rawSubtreeStore{data: make(map[chainhash.Hash][]byte)}
	flat := []chainhash.Hash{*coinbase.TxIDChainHash()}

	hashes := make([]*chainhash.Hash, 0, len(lens))

	for s := range lens {
		st, err := subtreepkg.NewTreeByLeafCount(caps[s])
		require.NoError(t, err)

		n := lens[s]
		if s == 0 {
			require.NoError(t, st.AddCoinbaseNode())

			n--
		}

		for i := 0; i < n; i++ {
			h := randCorruptTestHash(t)
			require.NoError(t, st.AddNode(h, 1, 0))
			flat = append(flat, h)
		}

		raw.put(t, st)
		hashes = append(hashes, st.RootHash())
	}

	root := flatMerkleRoot(flat)
	header := minedHeaderOnParent(t, 1, &root, regtestGenesisHeader(t).Hash())

	newBlock = func() *Block {
		hs := make([]*chainhash.Hash, len(hashes))
		copy(hs, hashes)

		b, err := NewBlock(header, coinbase, hs, uint64(len(flat)), 123, 0, 0) // nolint: gosec
		require.NoError(t, err)

		return b
	}

	return newBlock, &countingSubtreeStore{inner: raw}
}

// verdictClass reduces an error to the classification callers act on.
func verdictClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.IsBlockCorrupt(err):
		return "corrupt"
	case errors.Is(err, errors.ErrBlockInvalid):
		return "invalid"
	case errors.Is(err, errors.ErrStorageError):
		return "storage"
	case errors.Is(err, errors.ErrProcessing):
		return "processing"
	default:
		return "other: " + err.Error()
	}
}

// TestEarlyBodyBindingMatchesCheckMerkleRoot pins that checking the body
// against the header from the first and last subtrees plus the subtree keys
// reaches the same verdict class as loading everything and running
// CheckMerkleRoot, on both load paths, across honest shapes (full, short final
// subtree, single subtree, last at index 1) and malformed ones.
func TestEarlyBodyBindingMatchesCheckMerkleRoot(t *testing.T) {
	cases := []struct {
		caps, lens []int
	}{
		{[]int{4}, []int{4}},
		{[]int{4}, []int{3}},
		{[]int{4}, []int{1}},
		{[]int{4, 4}, []int{4, 4}},
		{[]int{4, 4}, []int{4, 1}},
		{[]int{4, 4}, []int{4, 2}},
		{[]int{4, 4}, []int{4, 3}},
		{[]int{4, 4, 4}, []int{4, 4, 1}},
		{[]int{4, 4, 4}, []int{4, 4, 3}},
		{[]int{8, 8, 8}, []int{8, 8, 5}},
		{[]int{4, 4, 4, 4}, []int{4, 4, 4, 2}},
		{[]int{4, 4, 4, 4, 4}, []int{4, 4, 4, 4, 3}},
		// malformed: final longer than the first, non-power-of-two first,
		// short middle subtrees
		{[]int{4, 8}, []int{4, 5}},
		{[]int{4, 4}, []int{3, 3}},
		{[]int{4, 4, 4}, []int{4, 2, 4}},
		{[]int{4, 4, 4}, []int{4, 2, 2}},
		{[]int{2, 4}, []int{2, 4}},
	}

	for _, c := range cases {
		t.Run(fmt.Sprint(c.lens), func(t *testing.T) {
			newBlock, store := bodyShapeFixture(t, c.caps, c.lens)
			ctx := context.Background()

			ref := newBlock()

			refErr := ref.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, store, 2)
			if refErr == nil {
				refErr = ref.CheckMerkleRoot(ctx)
			}

			withDedup := newBlock()

			_, dedupErr := withDedup.getAndValidateSubtreesWithDedup(ctx, ulogger.TestLogger{}, store, 2)
			if dedupErr == nil {
				dedupErr = withDedup.CheckMerkleRoot(ctx)
			}

			withDedup.releaseTxMap()

			bound := newBlock()

			boundErr := bound.getAndValidateSubtreesBound(ctx, ulogger.TestLogger{}, store, 2)
			if boundErr == nil {
				boundErr = bound.CheckMerkleRoot(ctx)
			}

			require.Equal(t, verdictClass(refErr), verdictClass(dedupErr), "load-time dedup: ref=%v got=%v", refErr, dedupErr)
			require.Equal(t, verdictClass(refErr), verdictClass(boundErr), "bound load: ref=%v got=%v", refErr, boundErr)
		})
	}
}

// TestEarlyBodyBinding_NonPlaceholderFirstLeaf pins that the early check keeps
// Valid's own verdict for a first subtree that does not open with the coinbase
// placeholder, rather than reporting a merkle mismatch over it.
func TestEarlyBodyBinding_NonPlaceholderFirstLeaf(t *testing.T) {
	b, subtrees, _ := dedupLoadFixtureN(t, nil, 2, nil)

	notPlaceholder, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, notPlaceholder.AddNode(randCorruptTestHash(t), 1, 0))
	}

	err = b.checkBodyBoundToHeader(b.Hash().String(), notPlaceholder, subtrees[1])
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)
	require.Contains(t, err.Error(), "not a coinbase placeholder")
}

// failingLastStore serves every subtree except the one keyed last.
type failingLastStore struct {
	*countingSubtreeStore
	last chainhash.Hash
}

func (f *failingLastStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if chainhash.Hash(key) == f.last {
		f.reads.Add(1)

		return nil, errors.NewStorageError("injected failure")
	}

	return f.countingSubtreeStore.GetIoReader(ctx, key, fileType, opts...)
}

// TestGetAndValidateSubtreesWithDedup_LastSubtreeFetchFails pins the error
// path of the boundary load: a failed fetch of the last subtree is a storage
// error, nothing past the boundary subtrees is fetched, and no txMap is sized.
func TestGetAndValidateSubtreesWithDedup_LastSubtreeFetchFails(t *testing.T) {
	b, subtrees, store := dedupLoadFixtureN(t, nil, 4, nil)
	failing := &failingLastStore{countingSubtreeStore: store, last: *subtrees[3].RootHash()}

	_, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, failing, 2)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "got: %v", err)
	require.Nil(t, b.txMap, "no txMap may be sized before the body is bound")
	require.LessOrEqual(t, failing.reads.Load(), int32(1+4), "only the boundary subtrees are fetched (the failing one with its retries)")
}
