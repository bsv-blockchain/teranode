package subtreeprocessor

import (
	"context"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The text of the refusal Reorg and MoveForwardBlock return while a failed fork
// reorg waits for a reset. The tests match on text, not on the error type, so
// they also build, and fail, on a tree without the refusal.
const refusedUntilResetText = "refused: a failed fork reorg left block assembly in a state only a reset may move from"

// failedReorgMarkStore records every MarkTransactionsOnLongestChain(false) call,
// and fails them while failNotOnLongestChain is set.
type failedReorgMarkStore struct {
	utxostore.Store

	mu         sync.Mutex
	falseMarks [][]chainhash.Hash

	failNotOnLongestChain atomic.Bool
}

func (s *failedReorgMarkStore) MarkTransactionsOnLongestChain(ctx context.Context, hashes []chainhash.Hash, onLongestChain bool) error {
	if !onLongestChain {
		if s.failNotOnLongestChain.Load() {
			return errors.NewStorageError("injected MarkTransactionsOnLongestChain(false) failure")
		}

		s.mu.Lock()
		s.falseMarks = append(s.falseMarks, slices.Clone(hashes))
		s.mu.Unlock()
	}

	return s.Store.MarkTransactionsOnLongestChain(ctx, hashes, onLongestChain)
}

// takeMarkedNotOnLongestChain returns every hash marked not on the longest
// chain since the last call, and forgets them.
func (s *failedReorgMarkStore) takeMarkedNotOnLongestChain() []chainhash.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []chainhash.Hash
	for _, call := range s.falseMarks {
		out = append(out, call...)
	}

	s.falseMarks = nil

	return out
}

// failedForkReorgFixture is a started, disk-tx-map-backed processor on tip A
// (parent P) holding the mempool transaction m1, ready for a fork reorg from A
// to F. a1 is mined in the losing block A, f1 in the winning block F. Block A2
// is a child of A holding x1, for a plain move forward on the losing side.
type failedForkReorgFixture struct {
	stp   *SubtreeProcessor
	store *failedReorgMarkStore
	blob  *blob_memory.Memory

	parent, tipA, forkF, childA2 *model.BlockHeader
	blockA, blockF, blockA2      *model.Block

	m1, a1, f1, x1 chainhash.Hash

	fSubtreeKey, a2SubtreeKey     *chainhash.Hash
	fSubtreeBytes, a2SubtreeBytes []byte

	hook atomic.Pointer[func(stp *SubtreeProcessor)]
}

func newFailedForkReorgFixture(t *testing.T) *failedForkReorgFixture {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	sqlStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	f := &failedForkReorgFixture{
		store: &failedReorgMarkStore{Store: sqlStore},
		blob:  blob_memory.New(),
		m1:    chainhash.HashH([]byte("failed-fork-reorg-m1")),
		a1:    chainhash.HashH([]byte("failed-fork-reorg-a1")),
		f1:    chainhash.HashH([]byte("failed-fork-reorg-f1")),
		x1:    chainhash.HashH([]byte("failed-fork-reorg-x1")),
	}

	aSubtreeKey := storeReorgSubtree(t, ctx, f.blob, []subtreepkg.Node{{Hash: f.a1, Fee: 100, SizeInBytes: 250}})
	f.fSubtreeKey = storeReorgSubtree(t, ctx, f.blob, []subtreepkg.Node{{Hash: f.f1, Fee: 100, SizeInBytes: 250}})
	f.a2SubtreeKey = storeReorgSubtree(t, ctx, f.blob, []subtreepkg.Node{{Hash: f.x1, Fee: 100, SizeInBytes: 250}})

	f.fSubtreeBytes, err = f.blob.Get(ctx, f.fSubtreeKey[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	f.a2SubtreeBytes, err = f.blob.Get(ctx, f.a2SubtreeKey[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	f.parent = &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2300000001, Nonce: 9301}
	f.tipA = &model.BlockHeader{Version: 1, HashPrevBlock: f.parent.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2300000002, Nonce: 9302}
	f.forkF = &model.BlockHeader{Version: 1, HashPrevBlock: f.parent.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2300000003, Nonce: 9303}
	f.childA2 = &model.BlockHeader{Version: 1, HashPrevBlock: f.tipA.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2300000004, Nonce: 9304}

	f.blockA = &model.Block{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{aSubtreeKey}, Header: f.tipA}
	f.blockF = &model.Block{Height: 2, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{f.fSubtreeKey}, Header: f.forkF}
	f.blockA2 = &model.Block{Height: 3, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{f.a2SubtreeKey}, Header: f.childA2}

	_, err = sqlStore.Create(ctx, coinbaseTx2, 2)
	require.NoError(t, err)
	_, err = sqlStore.Create(ctx, coinbaseTx3, 2)
	require.NoError(t, err)

	bc := &blockchain.Mock{}
	bc.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	bc.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	bc.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	for _, h := range []*model.BlockHeader{f.parent, f.tipA, f.forkF, f.childA2} {
		bc.On("GetBlockHeader", mock.Anything, h.Hash()).Return(h, &model.BlockHeaderMeta{}, nil)
	}

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	f.stp, err = NewSubtreeProcessor(ctx, logger, settings, f.blob, bc, f.store, newSubtreeChan,
		WithTxMapDirs([]string{t.TempDir(), t.TempDir()}))
	require.NoError(t, err)

	f.stp.InitCurrentBlockHeader(f.tipA)

	require.NoError(t, f.stp.AddDirectly(&subtreepkg.Node{Hash: f.m1, Fee: 50, SizeInBytes: 200}, &subtreepkg.TxInpoints{}, true))

	// The hook runs on the processor goroutine at each block movement step. An
	// installed action fires once, on the first forward step of a fork reorg:
	// the one where the header is already the moved-back block's parent.
	f.stp.SetProgressHook(func() {
		if !f.stp.currentBlockHeader.Load().Hash().IsEqual(f.parent.Hash()) {
			return
		}

		if fn := f.hook.Swap(nil); fn != nil {
			(*fn)(f.stp)
		}
	})

	f.stp.Start(ctx)
	t.Cleanup(func() { f.stp.Stop(context.Background()) })

	return f
}

func (f *failedForkReorgFixture) onFirstForwardStep(fn func(stp *SubtreeProcessor)) {
	f.hook.Store(&fn)
}

func (f *failedForkReorgFixture) assemblyHashes(t *testing.T) []chainhash.Hash {
	t.Helper()

	var out []chainhash.Hash

	for _, h := range f.stp.GetTransactionHashes(context.Background()) {
		if !h.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			out = append(out, h)
		}
	}

	return out
}

// failReorgAtFirstForwardStep runs the fork reorg from A to F and makes it fail
// at F's first subtree read, after the move-back of A. F's subtree file is put
// back afterwards, so a retry would succeed.
func (f *failedForkReorgFixture) failReorgAtFirstForwardStep(t *testing.T) {
	t.Helper()

	f.onFirstForwardStep(func(_ *SubtreeProcessor) {
		require.NoError(t, f.blob.Del(context.Background(), f.fSubtreeKey[:], fileformat.FileTypeSubtree))
	})

	err := f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF})
	require.Error(t, err)
	require.ErrorContains(t, err, f.fSubtreeKey.String(), "must fail on the deleted forward subtree, not elsewhere")

	require.NoError(t, f.blob.Set(context.Background(), f.fSubtreeKey[:], fileformat.FileTypeSubtree, f.fSubtreeBytes, options.WithAllowOverwrite(true)))
}

// processorState is what a refused call must leave as it was.
type processorState struct {
	header   chainhash.Hash
	assembly []chainhash.Hash
	mapHasA1 bool
	txCount  uint64
}

func (f *failedForkReorgFixture) state(t *testing.T) processorState {
	t.Helper()

	return processorState{
		header:   *f.stp.GetCurrentBlockHeader().Hash(),
		assembly: f.assemblyHashes(t),
		mapHasA1: f.stp.GetCurrentTxMap().Exists(f.a1),
		txCount:  f.stp.TxCount(),
	}
}

// resetToTipA runs the reset block assembly runs after a failed reorg, minus
// the reload: back on A, with nothing moved.
func (f *failedForkReorgFixture) resetToTipA(t *testing.T) {
	t.Helper()

	resp := f.stp.Reset(f.tipA, []*model.Block{}, []*model.Block{}, false, nil)
	require.NoError(t, resp.Err)
	require.True(t, resp.Rotated)
}

// requireReorgKeepsA1 runs the fork reorg from A to F and checks it moved a1
// back into block assembly and marked it not on the longest chain.
func (f *failedForkReorgFixture) requireReorgKeepsA1(t *testing.T) {
	t.Helper()

	f.store.takeMarkedNotOnLongestChain()

	require.NoError(t, f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF}))
	require.Equal(t, f.forkF.Hash(), f.stp.GetCurrentBlockHeader().Hash())

	held := f.assemblyHashes(t)
	require.Contains(t, held, f.a1, "a1 is back in block assembly once A is moved back")
	require.NotContains(t, held, f.f1, "f1 is mined by F")
	require.Contains(t, f.store.takeMarkedNotOnLongestChain(), f.a1, "a1 is marked unmined, so the reload finds it after any later reset")
	require.NoError(t, f.stp.CheckSubtreeProcessor())
}

// A retry of a failed fork reorg ran on the rolled-back state. The restored
// map already held a1, so the retry's move-back skipped it and a1 left block
// assembly with nothing marking it unmined. The retry is now refused until a
// reset, and the reorg after the reset keeps a1.
func TestFailedForkReorg_RetryIsRefusedUntilAReset(t *testing.T) {
	f := newFailedForkReorgFixture(t)
	f.failReorgAtFirstForwardStep(t)

	before := f.state(t)
	require.True(t, before.mapHasA1, "precondition: the rolled-back map holds a1, the subtrees do not")

	err := f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF})
	require.ErrorContains(t, err, refusedUntilResetText)
	require.Equal(t, before, f.state(t), "a refused reorg changes nothing")

	f.resetToTipA(t)
	f.requireReorgKeepsA1(t)
}

// After a failed fork reorg the chain can go back to the losing side, so the
// next call is a plain move forward onto A's child. That is refused too, and
// runs once a reset has rotated the state.
func TestFailedForkReorg_MoveForwardIsRefusedUntilAReset(t *testing.T) {
	f := newFailedForkReorgFixture(t)
	f.failReorgAtFirstForwardStep(t)

	before := f.state(t)

	err := f.stp.MoveForwardBlock(f.blockA2)
	require.ErrorContains(t, err, refusedUntilResetText)
	require.Equal(t, before, f.state(t), "a refused move forward changes nothing")

	f.resetToTipA(t)

	require.NoError(t, f.stp.MoveForwardBlock(f.blockA2))
	require.Equal(t, f.childA2.Hash(), f.stp.GetCurrentBlockHeader().Hash())
	require.NoError(t, f.stp.CheckSubtreeProcessor())
}

// The forward-only catch-up shape of Reorg moves forward on the same state, so
// it is refused as well.
func TestFailedForkReorg_CatchUpIsRefusedUntilAReset(t *testing.T) {
	f := newFailedForkReorgFixture(t)
	f.failReorgAtFirstForwardStep(t)

	before := f.state(t)

	err := f.stp.Reorg([]*model.Block{}, []*model.Block{f.blockA2})
	require.ErrorContains(t, err, refusedUntilResetText)
	require.Equal(t, before, f.state(t), "a refused catch-up changes nothing")

	f.resetToTipA(t)

	require.NoError(t, f.stp.Reorg([]*model.Block{}, []*model.Block{f.blockA2}))
	require.Equal(t, f.childA2.Hash(), f.stp.GetCurrentBlockHeader().Hash())
}

// A panic in the fork branch never sets the error its rollback tests, but it
// leaves the half-moved state all the same. The next reorg is refused.
func TestFailedForkReorg_PanicIsRefusedUntilAReset(t *testing.T) {
	f := newFailedForkReorgFixture(t)
	f.onFirstForwardStep(func(_ *SubtreeProcessor) { panic("injected") })

	err := f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF})
	require.ErrorContains(t, err, "panicked")

	before := f.state(t)

	err = f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF})
	require.ErrorContains(t, err, refusedUntilResetText)
	require.Equal(t, before, f.state(t))

	err = f.stp.MoveForwardBlock(f.blockA2)
	require.ErrorContains(t, err, refusedUntilResetText)
	require.Equal(t, before, f.state(t))

	f.resetToTipA(t)
	f.requireReorgKeepsA1(t)
}

// A reset that fails in its sweep, before the tx map rotation, leaves the
// processor intact and so still refusing. Only a reset past the rotation lets
// the reorg run again.
func TestFailedForkReorg_ResetFailingBeforeTheRotationKeepsRefusing(t *testing.T) {
	f := newFailedForkReorgFixture(t)
	f.failReorgAtFirstForwardStep(t)

	f.store.failNotOnLongestChain.Store(true)

	resp := f.stp.Reset(f.tipA, []*model.Block{}, []*model.Block{}, false, nil)
	require.ErrorContains(t, resp.Err, "error marking assembly txs as not on longest chain before reset")
	require.False(t, resp.Rotated, "the reset failed before the rotation")

	f.store.failNotOnLongestChain.Store(false)

	before := f.state(t)

	err := f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF})
	require.ErrorContains(t, err, refusedUntilResetText)
	require.Equal(t, before, f.state(t))

	f.resetToTipA(t)
	f.requireReorgKeepsA1(t)
}

// A failed forward-only catch-up rolls back completely and block assembly does
// not reset after it, so it must not leave the processor refusing: the next
// catch-up runs.
func TestFailedCatchUp_TheNextCatchUpRuns(t *testing.T) {
	f := newFailedForkReorgFixture(t)

	require.NoError(t, f.blob.Del(context.Background(), f.a2SubtreeKey[:], fileformat.FileTypeSubtree))

	err := f.stp.Reorg([]*model.Block{}, []*model.Block{f.blockA2})
	require.Error(t, err)
	require.ErrorContains(t, err, f.a2SubtreeKey.String(), "must fail on the deleted subtree, not elsewhere")
	require.Equal(t, f.tipA.Hash(), f.stp.GetCurrentBlockHeader().Hash(), "the catch-up rolled back")

	require.NoError(t, f.blob.Set(context.Background(), f.a2SubtreeKey[:], fileformat.FileTypeSubtree, f.a2SubtreeBytes, options.WithAllowOverwrite(true)))

	require.NoError(t, f.stp.Reorg([]*model.Block{}, []*model.Block{f.blockA2}), "no reset ran, and the next catch-up must still run")
	require.Equal(t, f.childA2.Hash(), f.stp.GetCurrentBlockHeader().Hash())
}
