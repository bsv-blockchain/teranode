package blockassembly

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobopts "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// failingMarkStore reports every transaction as existing and records the chunks
// handed to MarkTransactionsOnLongestChain, failing the failOn-th unmined mark.
type failingMarkStore struct {
	utxo.Store

	mu     sync.Mutex
	chunks [][]chainhash.Hash
	calls  int
	failOn int

	// onBestChain are the transactions whose record names blockID, a block of the
	// best chain: mined there since the reset began.
	onBestChain []chainhash.Hash
	blockID     uint32
	remarked    [][]chainhash.Hash
	// failBlockIDs fails the read of the block ids, which only the check against
	// the best chain asks for.
	failBlockIDs bool
	// duringMark runs inside every unmined mark, while the scratch files exist.
	duringMark func()
}

func (s *failingMarkStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, requested ...fields.FieldName) error {
	wantBlockIDs := slices.Contains(requested, fields.BlockIDs)
	if wantBlockIDs && s.failBlockIDs {
		return errors.NewStorageError("block ids cannot be read")
	}

	for _, it := range items {
		it.Err = nil

		if wantBlockIDs && slices.Contains(s.onBestChain, it.Hash) {
			it.Data = &meta.Data{BlockIDs: []uint32{s.blockID}}
		}
	}

	return nil
}

func (s *failingMarkStore) MarkTransactionsOnLongestChain(_ context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if onLongestChain {
		s.remarked = append(s.remarked, append([]chainhash.Hash(nil), txHashes...))

		return nil
	}

	if s.duringMark != nil {
		s.duringMark()
	}

	s.calls++
	if s.calls == s.failOn {
		return errors.NewStorageError("mark failed on call %d", s.calls)
	}

	s.chunks = append(s.chunks, append([]chainhash.Hash(nil), txHashes...))

	return nil
}

func (s *failingMarkStore) marked() []chainhash.Hash {
	var out []chainhash.Hash
	for _, c := range s.chunks {
		out = append(out, c...)
	}

	return out
}

func setChunkSize(t *testing.T, n int) {
	t.Helper()

	old := resetNetUnminedChunkSize
	resetNetUnminedChunkSize = n

	t.Cleanup(func() { resetNetUnminedChunkSize = old })
}

func setMaxForwardInMemory(t *testing.T, n uint64) {
	t.Helper()

	old := resetNetUnminedMaxForwardInMemory
	resetNetUnminedMaxForwardInMemory = n

	t.Cleanup(func() { resetNetUnminedMaxForwardInMemory = old })
}

func setSubtreeRetryBackoff(t *testing.T) {
	t.Helper()

	old := netUnminedSubtreeRetryBackoff
	netUnminedSubtreeRetryBackoff = time.Millisecond

	t.Cleanup(func() { netUnminedSubtreeRetryBackoff = old })
}

func hashesNamed(names ...string) []chainhash.Hash {
	out := make([]chainhash.Hash, len(names))
	for i, n := range names {
		out[i] = chainhash.HashH([]byte(n))
	}

	return out
}

func validBlockMeta() *model.BlockHeaderMeta { return &model.BlockHeaderMeta{} }

func markerWithStore(t *testing.T) (*BlockAssembler, *failingMarkStore, *baTestItems) {
	t.Helper()
	setSubtreeRetryBackoff(t)

	items := setupBlockAssemblyTest(t)
	store := &failingMarkStore{Store: items.utxoStore}
	items.blockAssembler.utxoStore = store

	return items.blockAssembler, store, items
}

// A failed mark is a failed write to the UTXO store, so the reset fails with it
// and is run again, before it has rotated the tx map or moved the tip. A reset
// that carried on would leave the rest flagged mined on a block that is off the
// chain, and no later reset has these blocks to move back from.
func TestMarkNetUnminedMoveBackTxs_AFailedMarkFailsTheReset(t *testing.T) {
	setChunkSize(t, 2)

	ba, store, items := markerWithStore(t)
	store.failOn = 2

	txs := hashesNamed("t0", "t1", "t2", "t3", "t4")
	back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, txs...)}}

	err := ba.markNetUnminedMoveBackTxs(t.Context(), []blockWithMeta{{block: back, meta: validBlockMeta()}}, nil)
	require.Error(t, err)
	require.Len(t, store.chunks, 1)
	require.Equal(t, 2, store.calls, "no call after the failed one")
}

// Every block has several subtrees, read in parallel, and the forward block has
// the middle transaction of each: what is marked must not depend on the order
// the subtrees arrive in.
func TestMarkNetUnminedMoveBackTxs_MarksTheNetUnminedOfEverySubtree(t *testing.T) {
	setChunkSize(t, 3)

	ba, store, items := markerWithStore(t)

	var (
		blocks []blockWithMeta
		inFwd  []chainhash.Hash
		want   []chainhash.Hash
	)

	for b := 0; b < 2; b++ {
		var subtrees []*chainhash.Hash

		for i := 0; i < 12; i++ {
			name := string(rune('a'+b)) + string(rune('a'+i))
			txs := hashesNamed(name+"0", name+"1", name+"2")
			subtrees = append(subtrees, seedSubtreeWithTxs(t, items, txs...))
			inFwd = append(inFwd, txs[1])
			want = append(want, txs[0], txs[2])
		}

		blocks = append(blocks, blockWithMeta{block: &model.Block{Subtrees: subtrees}, meta: validBlockMeta()})
	}

	forward := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, inFwd...)}, TransactionCount: uint64(len(inFwd))}

	for _, mode := range []struct {
		name        string
		maxInMemory uint64
	}{{"in memory", 1 << 30}, {"on disk", 5}} {
		t.Run(mode.name, func(t *testing.T) {
			setMaxForwardInMemory(t, mode.maxInMemory)

			store.chunks = nil

			require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(), blocks, []blockWithMeta{{block: forward, meta: validBlockMeta()}}))

			for _, c := range store.chunks {
				require.LessOrEqual(t, len(c), 3)
			}

			require.ElementsMatch(t, want, store.marked())
		})
	}
}

func TestMarkNetUnminedMoveBackTxs_AnUnreadableForwardBlockMarksNothing(t *testing.T) {
	setChunkSize(t, 2)

	ba, store, items := markerWithStore(t)

	back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("a0", "a1")...)}}
	missing := chainhash.HashH([]byte("a subtree nobody stored"))
	forward := &model.Block{Subtrees: []*chainhash.Hash{&missing}, TransactionCount: 1}

	require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(),
		[]blockWithMeta{{block: back, meta: validBlockMeta()}},
		[]blockWithMeta{{block: forward, meta: validBlockMeta()}}), "an unreadable block does not fail the reset")

	require.Empty(t, store.chunks)
}

func TestMarkNetUnminedMoveBackTxs_SkipsInvalidBlocksAndLeavesCoinbaseOut(t *testing.T) {
	ba, store, items := markerWithStore(t)

	live := hashesNamed("x0", "x1")
	invalid := hashesNamed("bad0")

	liveBlock := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, live...)}}
	badBlock := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, invalid...)}}

	// An invalid moveForward block is not part of the new chain, so a
	// transaction only it has is still net unmined.
	badForward := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, live[0])}, TransactionCount: 1}

	require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(), []blockWithMeta{
		{block: badBlock, meta: &model.BlockHeaderMeta{Invalid: true}},
		{block: liveBlock, meta: validBlockMeta()},
	}, []blockWithMeta{
		{block: badForward, meta: &model.BlockHeaderMeta{Invalid: true}},
	}))

	require.ElementsMatch(t, live, store.marked(), "the coinbase placeholder and the invalid blocks' transactions are not marked")
}

// A reset of ordinary size keeps the forward set in memory and writes nothing:
// it must not depend on a scratch directory.
func TestMarkNetUnminedMoveBackTxs_ASmallResetWritesNoScratch(t *testing.T) {
	ba, store, items := markerWithStore(t)

	dir := t.TempDir()
	ba.settings.BlockAssembly.TxMapDirs = []string{dir}

	entries := -1
	store.duringMark = func() {
		found, _ := os.ReadDir(dir)
		entries = len(found)
	}

	back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("a0", "a1")...)}}
	forward := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("a1")...)}, TransactionCount: 1}

	require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(),
		[]blockWithMeta{{block: back, meta: validBlockMeta()}},
		[]blockWithMeta{{block: forward, meta: validBlockMeta()}}))

	require.Equal(t, hashesNamed("a0"), store.marked())
	require.Zero(t, entries, "no scratch directory while marking")
}

// The memory bound: a forward set larger than fits in memory is split into
// ranges on disk, under the configured tx map directory.
func TestMarkNetUnminedMoveBackTxs_ALargeForwardSetIsSplitIntoRangesOnDisk(t *testing.T) {
	setMaxForwardInMemory(t, 2)

	ba, store, items := markerWithStore(t)

	dir := t.TempDir()
	ba.settings.BlockAssembly.TxMapDirs = []string{dir}

	ranges := -1
	store.duringMark = func() {
		found, _ := filepath.Glob(filepath.Join(dir, netUnminedScratchPrefix+"*", "forward-*"))
		ranges = len(found)
	}

	back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("a0", "a1")...)}}
	forward := &model.Block{
		Subtrees:         []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("a1", "f0", "f1", "f2", "f3")...)},
		TransactionCount: 5,
	}

	require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(),
		[]blockWithMeta{{block: back, meta: validBlockMeta()}},
		[]blockWithMeta{{block: forward, meta: validBlockMeta()}}))

	require.Equal(t, hashesNamed("a0"), store.marked())
	require.Equal(t, 4, ranges, "5 forward transactions at 2 a range need 4 ranges")

	left, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, left, "the scratch directory is removed")
}

// A scratch directory that cannot be made fails the reset: carrying on would
// commit it with nothing marked.
func TestMarkNetUnminedMoveBackTxs_AScratchDirectoryThatCannotBeMadeFailsTheReset(t *testing.T) {
	setMaxForwardInMemory(t, 1)

	ba, store, items := markerWithStore(t)
	ba.settings.BlockAssembly.TxMapDirs = []string{filepath.Join(t.TempDir(), "not", "there")}

	back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("a0")...)}}
	forward := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("f0", "f1")...)}, TransactionCount: 2}

	require.Error(t, ba.markNetUnminedMoveBackTxs(t.Context(),
		[]blockWithMeta{{block: back, meta: validBlockMeta()}},
		[]blockWithMeta{{block: forward, meta: validBlockMeta()}}))
	require.Empty(t, store.chunks)
}

// A block that extends the reset's target while the reset runs can mine a
// moveBack transaction: its setTxMined clears unmined_since and the mark, decided
// before that block existed, would set it again. Each chunk is checked against
// the best chain after its mark.
func TestMarkNetUnminedMoveBackTxs_RemarksWhatTheBestChainMinedMeanwhile(t *testing.T) {
	ba, store, items := markerWithStore(t)

	best, _, err := ba.blockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)

	_, bestMeta, err := ba.blockchainClient.GetBlockHeader(t.Context(), best.Hash())
	require.NoError(t, err)

	txs := hashesNamed("r0", "r1", "r2")
	store.blockID = bestMeta.ID
	store.onBestChain = []chainhash.Hash{txs[1]}

	back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, txs...)}}

	require.NoError(t, ba.markNetUnminedMoveBackTxs(t.Context(), []blockWithMeta{{block: back, meta: validBlockMeta()}}, nil))

	require.ElementsMatch(t, txs, store.marked())
	require.Equal(t, [][]chainhash.Hash{{txs[1]}}, store.remarked, "the one mined on the best chain is marked as on it again")
}

func TestMarkNetUnminedMoveBackTxs_AFailedCheckAgainstTheBestChainFailsTheReset(t *testing.T) {
	ba, store, items := markerWithStore(t)
	store.failBlockIDs = true

	back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("r0")...)}}

	require.Error(t, ba.markNetUnminedMoveBackTxs(t.Context(), []blockWithMeta{{block: back, meta: validBlockMeta()}}, nil))
}

func collectStream(t *testing.T, stream txHashStream, block int) []chainhash.Hash {
	t.Helper()

	var (
		mu  sync.Mutex
		out []chainhash.Hash
	)

	unreadable, err := stream(t.Context(), block, func(h []chainhash.Hash) error {
		mu.Lock()
		out = append(out, h...)
		mu.Unlock()

		return nil
	})
	require.NoError(t, err)
	require.NoError(t, unreadable)

	return out
}

func TestNetUnminedBlockStream_SkipsTheCoinbasePlaceholderAndNilSubtrees(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	txs := hashesNamed("s0", "s1", "s2")
	blk := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, txs...), nil}}

	// seedSubtreeWithTxs puts the coinbase placeholder first
	got := collectStream(t, ba.netUnminedBlockStream([]*model.Block{blk}), 0)
	require.Equal(t, txs, got)
	require.NotContains(t, got, subtreepkg.CoinbasePlaceholderHashValue)
}

func storeSubtreeToCheck(t *testing.T, items *baTestItems, txs ...chainhash.Hash) *chainhash.Hash {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for _, h := range txs {
		require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 1}))
	}

	b, err := st.Serialize()
	require.NoError(t, err)

	root := st.RootHash()
	require.NoError(t, items.blobStore.Set(t.Context(), root[:], fileformat.FileTypeSubtreeToCheck, b))

	return root
}

func TestNetUnminedBlockStream_ReadsASubtreeThatIsStillToBeChecked(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	txs := hashesNamed("c0", "c1")
	blk := &model.Block{Subtrees: []*chainhash.Hash{storeSubtreeToCheck(t, items, txs...)}}

	require.Equal(t, txs, collectStream(t, ba.netUnminedBlockStream([]*model.Block{blk}), 0))
}

func TestNetUnminedBlockStream_ReadsEverySubtreeOfABlock(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	var (
		subtrees []*chainhash.Hash
		want     []chainhash.Hash
	)

	for i := 0; i < 40; i++ {
		txs := hashesNamed("p"+string(rune('a'+i))+"0", "p"+string(rune('a'+i))+"1")
		subtrees = append(subtrees, seedSubtreeWithTxs(t, items, txs...))
		want = append(want, txs...)
	}

	blk := &model.Block{Subtrees: subtrees}

	require.ElementsMatch(t, want, collectStream(t, ba.netUnminedBlockStream([]*model.Block{blk}), 0))
}

func TestNetUnminedBlockStream_AMissingSubtreeMakesTheBlockUnreadable(t *testing.T) {
	setSubtreeRetryBackoff(t)

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	there := hashesNamed("m0", "m1")
	missing := chainhash.HashH([]byte("never stored"))
	blk := &model.Block{Subtrees: []*chainhash.Hash{&missing, seedSubtreeWithTxs(t, items, there...)}}

	store := &flakySubtreeStore{Store: items.blobStore}
	ba.subtreeStore = store

	var got []chainhash.Hash

	unreadable, err := ba.netUnminedBlockStream([]*model.Block{blk})(t.Context(), 0, func(h []chainhash.Hash) error {
		got = append(got, h...)
		return nil
	})
	require.NoError(t, err, "a file that is gone is not a failing store")
	require.ErrorIs(t, unreadable, errors.ErrNotFound)
	require.Equal(t, there, got, "the subtrees that are there are still read")
	require.Equal(t, 2, store.opens[fileformat.FileTypeSubtree], "one open a subtree: a file that is not there is not waited for")
}

// flakySubtreeStore fails the first failOpens opens of a subtree file with
// openErr, and counts the opens of each file type.
type flakySubtreeStore struct {
	blob.Store

	mu        sync.Mutex
	failOpens int
	openErr   error
	opens     map[fileformat.FileType]int
}

func (s *flakySubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (io.ReadCloser, error) {
	s.mu.Lock()

	if s.opens == nil {
		s.opens = make(map[fileformat.FileType]int)
	}

	s.opens[fileType]++
	fail := fileType == fileformat.FileTypeSubtree && s.opens[fileType] <= s.failOpens

	s.mu.Unlock()

	if fail {
		return nil, s.openErr
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

// The subtree store answers with a transient error while a subtree moves to
// permanent storage. One such error must not cost the reset its marker.
func TestNetUnminedBlockStream_RetriesAnOpenThatFails(t *testing.T) {
	setSubtreeRetryBackoff(t)

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	txs := hashesNamed("y0", "y1")
	blk := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, txs...)}}

	store := &flakySubtreeStore{Store: items.blobStore, failOpens: 3, openErr: io.ErrUnexpectedEOF}
	ba.subtreeStore = store

	require.Equal(t, txs, collectStream(t, ba.netUnminedBlockStream([]*model.Block{blk}), 0))
	require.Equal(t, 4, store.opens[fileformat.FileTypeSubtree], "three failures, then the open that works")

	// One failure more than the retries cover is an error, and it is the store's.
	store = &flakySubtreeStore{Store: items.blobStore, failOpens: 4, openErr: io.ErrUnexpectedEOF}
	ba.subtreeStore = store

	unreadable, err := ba.netUnminedBlockStream([]*model.Block{blk})(t.Context(), 0, func([]chainhash.Hash) error { return nil })
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NoError(t, unreadable, "a store that keeps failing is not a file that is gone")
	require.Equal(t, 4, store.opens[fileformat.FileTypeSubtree])
}

// The file still to be checked stands in for a subtree file that is not there.
// It is not an answer to a store that failed to read the subtree file.
func TestNetUnminedBlockStream_FallsBackOnlyWhenTheSubtreeFileIsNotFound(t *testing.T) {
	setSubtreeRetryBackoff(t)

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	blk := &model.Block{Subtrees: []*chainhash.Hash{storeSubtreeToCheck(t, items, hashesNamed("z0")...)}}

	store := &flakySubtreeStore{Store: items.blobStore, failOpens: 100, openErr: errors.NewStorageError("subtree store unreachable")}
	ba.subtreeStore = store

	unreadable, err := ba.netUnminedBlockStream([]*model.Block{blk})(t.Context(), 0, func([]chainhash.Hash) error { return nil })
	require.Error(t, err)
	require.NoError(t, unreadable)
	require.Zero(t, store.opens[fileformat.FileTypeSubtreeToCheck], "no fallback for an error that is not a missing file")
}

// failingToCheckStore fails the first failOpens opens of a file still to be
// checked with a storage error, and counts them.
type failingToCheckStore struct {
	blob.Store

	mu        sync.Mutex
	failOpens int
	opens     int
}

func (s *failingToCheckStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (io.ReadCloser, error) {
	if fileType == fileformat.FileTypeSubtreeToCheck {
		s.mu.Lock()
		s.opens++
		fail := s.opens <= s.failOpens
		s.mu.Unlock()

		if fail {
			return nil, errors.NewStorageError("subtree store unreachable")
		}
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

// A store that fails to open the file still to be checked has not said the file
// is gone. Taking it for gone would make the block unreadable, and an unreadable
// moveForward block commits the reset with nothing marked. It is tried again, and
// a store that keeps failing is an error.
func TestNetUnminedBlockStream_AFailedOpenOfTheFileToCheckIsNotAMissingFile(t *testing.T) {
	setSubtreeRetryBackoff(t)

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	txs := hashesNamed("q0", "q1")
	blk := &model.Block{Subtrees: []*chainhash.Hash{storeSubtreeToCheck(t, items, txs...)}}

	store := &failingToCheckStore{Store: items.blobStore, failOpens: 2}
	ba.subtreeStore = store

	require.Equal(t, txs, collectStream(t, ba.netUnminedBlockStream([]*model.Block{blk}), 0), "a failed open is tried again")
	require.Equal(t, 3, store.opens, "two failures, then the open that works")

	store = &failingToCheckStore{Store: items.blobStore, failOpens: 100}
	ba.subtreeStore = store

	unreadable, err := ba.netUnminedBlockStream([]*model.Block{blk})(t.Context(), 0, func([]chainhash.Hash) error { return nil })
	require.Error(t, err, "a store that keeps failing is a failing store")
	require.NoError(t, unreadable, "and not a file that is gone")
	require.Equal(t, 1+netUnminedSubtreeRetries, store.opens)
}

// A file under a key it does not hash to is the wrong file: its transactions are
// not this block's.
func TestNetUnminedBlockStream_RejectsAFileThatDoesNotMatchItsKey(t *testing.T) {
	setSubtreeRetryBackoff(t)

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	other := seedSubtreeWithTxs(t, items, hashesNamed("o0", "o1")...)

	b, err := items.blobStore.Get(t.Context(), other[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	key := chainhash.HashH([]byte("the key of another subtree"))
	require.NoError(t, items.blobStore.Set(t.Context(), key[:], fileformat.FileTypeSubtree, b))

	blk := &model.Block{Subtrees: []*chainhash.Hash{&key}}

	visited := false
	unreadable, err := ba.netUnminedBlockStream([]*model.Block{blk})(t.Context(), 0, func([]chainhash.Hash) error {
		visited = true
		return nil
	})
	require.NoError(t, err)
	require.Error(t, unreadable, "the wrong file is as good as none, and asking again does not change it")
	require.False(t, visited)
}

// A cancelled context stops the reads before any of them fails. That must come
// back as an error: a block read in part would otherwise look read in full.
func TestNetUnminedBlockStream_ACancelledContextIsAnError(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	blk := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("k0")...)}}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	visited := false
	unreadable, err := ba.netUnminedBlockStream([]*model.Block{blk})(ctx, 0, func([]chainhash.Hash) error {
		visited = true
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, unreadable)
	require.False(t, visited)
}

// The file still to be checked was never validated, so its own header is no
// proof of what it holds: its root is computed from its nodes.
func TestNetUnminedBlockStream_RejectsAFileToCheckWhoseNodesDoNotMatchItsKey(t *testing.T) {
	setSubtreeRetryBackoff(t)

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	claimed, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for _, h := range hashesNamed("h0", "h1") {
		require.NoError(t, claimed.AddSubtreeNode(subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 1}))
	}

	key := claimed.RootHash()

	// The same header, over another node: the claim matches the key and the nodes do not.
	claimed.Nodes[1].Hash = hashesNamed("swapped")[0]

	b, err := claimed.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(t.Context(), key[:], fileformat.FileTypeSubtreeToCheck, b))

	blk := &model.Block{Subtrees: []*chainhash.Hash{key}}

	unreadable, err := ba.netUnminedBlockStream([]*model.Block{blk})(t.Context(), 0, func([]chainhash.Hash) error { return nil })
	require.NoError(t, err)
	require.Error(t, unreadable)
}

// A subtree store that fails to answer is a failing store, not an expired file:
// skipping the marker for it, or leaving the block out, would commit the reset
// with moveBack transactions still flagged mined.
func TestMarkNetUnminedMoveBackTxs_ASubtreeStoreThatFailsFailsTheReset(t *testing.T) {
	for _, side := range []string{"moveForward", "moveBack"} {
		t.Run(side, func(t *testing.T) {
			ba, store, items := markerWithStore(t)

			back := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("a0", "a1")...)}}
			forward := &model.Block{Subtrees: []*chainhash.Hash{seedSubtreeWithTxs(t, items, hashesNamed("f0")...)}, TransactionCount: 1}

			failFor := forward.Subtrees[0]
			if side == "moveBack" {
				failFor = back.Subtrees[0]
			}

			ba.subtreeStore = &unreachableForSubtreeStore{Store: items.blobStore, key: *failFor}

			require.Error(t, ba.markNetUnminedMoveBackTxs(t.Context(),
				[]blockWithMeta{{block: back, meta: validBlockMeta()}},
				[]blockWithMeta{{block: forward, meta: validBlockMeta()}}))
			require.Empty(t, store.chunks)
		})
	}
}

// unreachableForSubtreeStore cannot be reached for the files of one subtree.
type unreachableForSubtreeStore struct {
	blob.Store
	key chainhash.Hash
}

func (s *unreachableForSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (io.ReadCloser, error) {
	if string(key) == string(s.key[:]) {
		return nil, errors.NewStorageError("subtree store unreachable")
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}
