package blockassembly

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// sliceUnminedIterator replays fixed transactions in fixed batches, standing in
// for a store iterator so the load can be fed arbitrary CreatedAt values.
type sliceUnminedIterator struct {
	batches [][]*utxo.UnminedTransaction
}

func (s *sliceUnminedIterator) Next(context.Context) ([]*utxo.UnminedTransaction, error) {
	if len(s.batches) == 0 {
		return nil, nil
	}

	b := s.batches[0]
	s.batches = s.batches[1:]

	return b, nil
}

func (s *sliceUnminedIterator) Err() error   { return nil }
func (s *sliceUnminedIterator) Close() error { return nil }

func extsortHash(i int) chainhash.Hash {
	var h chainhash.Hash
	binary.LittleEndian.PutUint64(h[:], uint64(i)+1)
	h[31] = 0x5a

	return h
}

func unminedAt(i, createdAt int, parents ...chainhash.Hash) *utxo.UnminedTransaction {
	inpoints := subtreepkg.TxInpoints{}
	if len(parents) > 0 {
		vouts := make([]uint32, 0, 2*len(parents))
		for range parents {
			vouts = append(vouts, 1, 0)
		}

		inpoints = subtreepkg.NewTxInpointsFromPacked(parents, vouts)
	}

	return &utxo.UnminedTransaction{
		Node:         &subtreepkg.Node{Hash: extsortHash(i), Fee: uint64(100 + i), SizeInBytes: 250},
		TxInpoints:   &inpoints,
		CreatedAt:    createdAt,
		UnminedSince: 1,
	}
}

func inBatches(txs []*utxo.UnminedTransaction, size int) [][]*utxo.UnminedTransaction {
	var out [][]*utxo.UnminedTransaction

	for start := 0; start < len(txs); start += size {
		out = append(out, txs[start:min(start+size, len(txs))])
	}

	return out
}

// loadedHashes returns the tx hashes in the subtree processor in block order,
// without the coinbase placeholder.
func loadedHashes(t *testing.T, ba *BlockAssembler) []chainhash.Hash {
	t.Helper()

	var out []chainhash.Hash

	subtrees := append(ba.subtreeProcessor.GetChainedSubtrees(), ba.subtreeProcessor.GetCurrentSubtree())
	for si, st := range subtrees {
		for ni, n := range st.Nodes {
			if si == 0 && ni == 0 {
				continue // coinbase placeholder
			}

			out = append(out, n.Hash)
		}
	}

	return out
}

func TestLoadUnminedSorted_OrdersAndSpills(t *testing.T) {
	initPrometheusMetrics()

	ba, _, cleanup := setupDiskSortTest(t)
	defer cleanup()

	dir := t.TempDir()
	ba.settings.BlockAssembly.UnminedTxDiskSortPaths = []string{dir}
	ba.settings.BlockAssembly.UnminedTxSortBufferRecords = 50
	ba.settings.BlockAssembly.UnminedLoadingBatchSize = 64

	const n = 1000

	rng := rand.New(rand.NewPCG(7, 9))
	txs := make([]*utxo.UnminedTransaction, n)

	for i := range txs {
		txs[i] = unminedAt(i, 1_700_000_000_000+rng.IntN(200))
	}

	// Reference order: stable by CreatedAt, iterator order on ties.
	want := make([]*utxo.UnminedTransaction, n)
	copy(want, txs)
	stableSortUnminedByCreatedAt(want)

	err := ba.loadUnminedSorted(context.Background(), &sliceUnminedIterator{batches: inBatches(txs, 97)}, map[uint32]bool{})
	require.NoError(t, err)

	got := loadedHashes(t, ba)
	require.Len(t, got, n)

	for i := range want {
		require.Equal(t, want[i].Hash, got[i], "position %d", i)
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "run files left behind")
}

// With inpoints stored, a child that shares its parent's CreatedAt must still
// be placed after it even when the iterator yields the child first and the
// pair straddles a spilled run.
func TestLoadUnminedSorted_ParentsFirstOnTies(t *testing.T) {
	initPrometheusMetrics()

	ba, _, cleanup := setupDiskSortTest(t)
	defer cleanup()

	ba.settings.BlockAssembly.UnminedTxDiskSortPaths = []string{t.TempDir()}
	ba.settings.BlockAssembly.UnminedTxSortBufferRecords = 2
	ba.settings.BlockAssembly.UnminedLoadingBatchSize = 1
	ba.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true

	parent := unminedAt(1, 5000)
	child := unminedAt(2, 5000, parent.Hash)
	grandchild := unminedAt(3, 5000, child.Hash)
	early := unminedAt(4, 4000)

	iter := &sliceUnminedIterator{batches: [][]*utxo.UnminedTransaction{{grandchild, child}, {early, parent}}}

	require.NoError(t, ba.loadUnminedSorted(context.Background(), iter, map[uint32]bool{}))

	require.Equal(t, []chainhash.Hash{early.Hash, parent.Hash, child.Hash, grandchild.Hash}, loadedHashes(t, ba))
}

func TestLoadUnminedSorted_FiltersLikeTheInMemoryPath(t *testing.T) {
	initPrometheusMetrics()

	ba, _, cleanup := setupDiskSortTest(t)
	defer cleanup()

	ba.settings.BlockAssembly.UnminedTxDiskSortPaths = []string{t.TempDir()}
	ba.settings.BlockAssembly.UnminedTxSortBufferRecords = 2

	keep := unminedAt(1, 1000)
	coinbase := &utxo.UnminedTransaction{Skip: true}
	mined := unminedAt(2, 900)
	mined.BlockIDs = []uint32{7}
	onSideChain := unminedAt(3, 800)
	onSideChain.BlockIDs = []uint32{99}

	iter := &sliceUnminedIterator{batches: [][]*utxo.UnminedTransaction{{keep, coinbase, mined, onSideChain}}}

	require.NoError(t, ba.loadUnminedSorted(context.Background(), iter, map[uint32]bool{7: true}))

	require.Equal(t, []chainhash.Hash{onSideChain.Hash, keep.Hash}, loadedHashes(t, ba))
}

// Enabling disk sort without a path must still spill (to the OS temp dir, as
// the Badger implementation did) rather than silently sort everything in RAM.
func TestUnminedSortDirs_FallsBackToTempDir(t *testing.T) {
	require.Equal(t, []string{os.TempDir()}, unminedSortDirs(nil))
	require.Equal(t, []string{os.TempDir()}, unminedSortDirs([]string{"", " "}))
	require.Equal(t, []string{"/a", "/b"}, unminedSortDirs([]string{"/a", "", "/b"}))
}
