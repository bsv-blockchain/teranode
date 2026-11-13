package blockassembly

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// failingMarkStore reports every transaction as existing and records the chunks
// handed to MarkTransactionsOnLongestChain(false), failing the failOn-th call.
type failingMarkStore struct {
	utxo.Store

	mu     sync.Mutex
	chunks [][]chainhash.Hash
	calls  int
	failOn int
}

func (s *failingMarkStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	for _, it := range items {
		it.Err = nil
	}

	return nil
}

func (s *failingMarkStore) MarkTransactionsOnLongestChain(_ context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if onLongestChain {
		return nil
	}

	s.calls++
	if s.calls == s.failOn {
		return errors.NewStorageError("mark failed on call %d", s.calls)
	}

	s.chunks = append(s.chunks, append([]chainhash.Hash(nil), txHashes...))

	return nil
}

func setChunkSize(t *testing.T, n int) {
	t.Helper()

	old := resetNetUnminedChunkSize
	resetNetUnminedChunkSize = n

	t.Cleanup(func() { resetNetUnminedChunkSize = old })
}

func hashesNamed(names ...string) []chainhash.Hash {
	out := make([]chainhash.Hash, len(names))
	for i, n := range names {
		out[i] = chainhash.HashH([]byte(n))
	}

	return out
}

func validBlockMeta() *model.BlockHeaderMeta { return &model.BlockHeaderMeta{} }

// A failed mark stops the marking. The transactions marked unmined must be a
// prefix of the moveBack order: a hole, with later chunks marked and an earlier
// one not, would leave the unmined set out of order and nothing retries it.
func TestMarkNetUnminedMoveBackTxs_AFailedMarkStopsAtThatChunk(t *testing.T) {
	setChunkSize(t, 2)

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	store := &failingMarkStore{Store: items.utxoStore, failOn: 2}
	ba.utxoStore = store

	txs := hashesNamed("t0", "t1", "t2", "t3", "t4")
	back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, txs...)}}

	require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(),
		[]blockWithMeta{{block: back, meta: validBlockMeta()}}, nil))

	require.Equal(t, [][]chainhash.Hash{{txs[0], txs[1]}}, store.chunks, "only the chunk before the failure is marked")
	require.Equal(t, 2, store.calls, "no call after the failed one")
}

func TestMarkNetUnminedMoveBackTxs_MarksEveryChunkInMoveBackOrder(t *testing.T) {
	setChunkSize(t, 2)

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	store := &failingMarkStore{Store: items.utxoStore}
	ba.utxoStore = store

	first := hashesNamed("a0", "a1", "a2")
	second := hashesNamed("b0", "b1")
	inForward := hashesNamed("a1", "b0")

	back1 := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, first...)}}
	back2 := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, second...)}}
	forward := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, inForward...)}, TransactionCount: 2}

	require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(),
		[]blockWithMeta{{block: back1, meta: validBlockMeta()}, {block: back2, meta: validBlockMeta()}},
		[]blockWithMeta{{block: forward, meta: validBlockMeta()}}))

	var got []chainhash.Hash
	for _, c := range store.chunks {
		require.LessOrEqual(t, len(c), 2)

		got = append(got, c...)
	}

	require.Equal(t, hashesNamed("a0", "a2", "b1"), got)
}

func TestMarkNetUnminedMoveBackTxs_AnUnreadableForwardBlockMarksNothing(t *testing.T) {
	setChunkSize(t, 2)

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	store := &failingMarkStore{Store: items.utxoStore}
	ba.utxoStore = store

	back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("a0", "a1")...)}}
	missing := chainhash.HashH([]byte("a subtree nobody stored"))
	forward := &model.Block{Subtrees: []*chainhash.Hash{&missing}, TransactionCount: 1}

	require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(),
		[]blockWithMeta{{block: back, meta: validBlockMeta()}},
		[]blockWithMeta{{block: forward, meta: validBlockMeta()}}))

	require.Empty(t, store.chunks)
}

func TestMarkNetUnminedMoveBackTxs_SkipsInvalidBlocksAndLeavesCoinbaseOut(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	store := &failingMarkStore{Store: items.utxoStore}
	ba.utxoStore = store

	live := hashesNamed("x0", "x1")
	invalid := hashesNamed("bad0")

	liveBlock := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, live...)}}
	badBlock := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, invalid...)}}

	require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(), []blockWithMeta{
		{block: badBlock, meta: &model.BlockHeaderMeta{Invalid: true}},
		{block: liveBlock, meta: validBlockMeta()},
	}, nil))

	var got []chainhash.Hash
	for _, c := range store.chunks {
		got = append(got, c...)
	}

	require.Equal(t, live, got, "the coinbase placeholder and the invalid block's transactions are not marked")
}

func collectStream(t *testing.T, stream txHashStream, block int) []chainhash.Hash {
	t.Helper()

	var (
		mu  sync.Mutex
		out []chainhash.Hash
	)

	require.NoError(t, stream(t.Context(), block, func(h []chainhash.Hash) error {
		mu.Lock()
		out = append(out, h...)
		mu.Unlock()

		return nil
	}))

	return out
}

func TestNetUnminedBlockStream_SkipsTheCoinbasePlaceholderAndNilSubtrees(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	txs := hashesNamed("s0", "s1", "s2")
	blk := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, txs...), nil}}

	// seedSubtreeWithTxs puts the coinbase placeholder first
	got := collectStream(t, ba.netUnminedBlockStream([]*model.Block{blk}, 1), 0)
	require.Equal(t, txs, got)
	require.NotContains(t, got, subtreepkg.CoinbasePlaceholderHashValue)
}

func TestNetUnminedBlockStream_ReadsASubtreeThatIsStillToBeChecked(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	txs := hashesNamed("c0", "c1")
	for _, h := range txs {
		require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 1}))
	}

	b, err := st.Serialize()
	require.NoError(t, err)

	root := st.RootHash()
	require.NoError(t, items.blobStore.Set(t.Context(), root[:], fileformat.FileTypeSubtreeToCheck, b))

	blk := &model.Block{Subtrees: []*chainhash.Hash{root}}
	require.Equal(t, txs, collectStream(t, ba.netUnminedBlockStream([]*model.Block{blk}, 1), 0))
}

func TestNetUnminedBlockStream_ParallelReadGivesTheSameHashes(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	var (
		subtrees []*chainhash.Hash
		want     []chainhash.Hash
	)

	for i := 0; i < 6; i++ {
		txs := hashesNamed("p"+string(rune('a'+i))+"0", "p"+string(rune('a'+i))+"1")
		subtrees = append(subtrees, seedSubtreeWithTxs(t, items, txs...))
		want = append(want, txs...)
	}

	blk := &model.Block{Subtrees: subtrees}

	require.Equal(t, want, collectStream(t, ba.netUnminedBlockStream([]*model.Block{blk}, 1), 0), "sequential keeps subtree order")
	require.ElementsMatch(t, want, collectStream(t, ba.netUnminedBlockStream([]*model.Block{blk}, 4), 0))
}

func TestNetUnminedBlockStream_AMissingSubtreeIsAnError(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	missing := chainhash.HashH([]byte("never stored"))
	blk := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("m0")...), &missing}}

	for _, concurrency := range []int{1, 4} {
		err := ba.netUnminedBlockStream([]*model.Block{blk}, concurrency)(t.Context(), 0, func([]chainhash.Hash) error { return nil })
		require.Error(t, err)
	}
}
