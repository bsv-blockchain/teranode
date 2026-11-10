package subtreeprocessor

// A failed fork reorg no longer holds, and puts back, the tx map and subtrees from
// before the reorg. Each step puts back its own input if it fails part way, the
// reset that must follow sweeps whatever the last completed step left, and the
// pre-reorg map and subtrees are released as soon as the reorg has moved past them.

import (
	"context"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// stepRestoreStore records every MarkTransactionsOnLongestChain(false) call, and
// fails the coinbase SpendAndCreate of failCoinbaseOf while it is set.
type stepRestoreStore struct {
	utxostore.Store

	mu         sync.Mutex
	falseMarks []chainhash.Hash

	failCoinbaseOf atomic.Pointer[chainhash.Hash]
}

func (s *stepRestoreStore) MarkTransactionsOnLongestChain(ctx context.Context, hashes []chainhash.Hash, onLongestChain bool) error {
	if !onLongestChain {
		s.mu.Lock()
		s.falseMarks = append(s.falseMarks, hashes...)
		s.mu.Unlock()
	}

	return s.Store.MarkTransactionsOnLongestChain(ctx, hashes, onLongestChain)
}

func (s *stepRestoreStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	if failing := s.failCoinbaseOf.Load(); failing != nil && tx.TxIDChainHash().IsEqual(failing) {
		return nil, nil, errors.NewStorageError("injected coinbase SpendAndCreate failure")
	}

	return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

// takeMarkedNotOnLongestChain returns every hash marked not on the longest chain
// since the last call, and forgets them.
func (s *stepRestoreStore) takeMarkedNotOnLongestChain() []chainhash.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := s.falseMarks
	s.falseMarks = nil

	return out
}

// stepRestoreFixture is a started, disk-tx-map-backed processor on tip A (parent
// P). Before the reorg it holds the unmined transaction m1, plus unmined fillers
// when asked for, so that it holds complete chained subtrees. a1 is mined in the losing block A. The
// winning branch is F (f1) then F2 (f2). A2 (x1) and A3 (x2) extend A, for a
// catch-up. blockABadBuild is A with a second subtree that makes the move-back's
// bulk build fail after it has replaced the subtrees.
type stepRestoreFixture struct {
	stp   *SubtreeProcessor
	store *stepRestoreStore
	blob  *blob_memory.Memory

	parent, tipA, forkF, forkF2, childA2, childA3             *model.BlockHeader
	blockA, blockABadBuild, blockF, blockF2, blockA2, blockA3 *model.Block

	m1, a1, f1, f2 chainhash.Hash

	fSubtreeKey, a3SubtreeKey *chainhash.Hash

	hook atomic.Pointer[func(header *model.BlockHeader)]
}

type stepRestoreOptions struct {
	fillers  int
	opts     []Options
	settings func(*settings.Settings)
}

func newStepRestoreFixture(t *testing.T, o stepRestoreOptions) *stepRestoreFixture {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	if o.settings != nil {
		o.settings(tSettings)
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	sqlStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	f := &stepRestoreFixture{
		store: &stepRestoreStore{Store: sqlStore},
		blob:  blob_memory.New(),
		m1:    chainhash.HashH([]byte("step-restore-m1")),
		a1:    chainhash.HashH([]byte("step-restore-a1")),
		f1:    chainhash.HashH([]byte("step-restore-f1")),
		f2:    chainhash.HashH([]byte("step-restore-f2")),
	}

	x1 := chainhash.HashH([]byte("step-restore-x1"))
	x2 := chainhash.HashH([]byte("step-restore-x2"))
	y1 := chainhash.HashH([]byte("step-restore-y1"))

	aSubtreeKey := storeReorgSubtree(t, ctx, f.blob, []subtreepkg.Node{{Hash: f.a1, Fee: 100, SizeInBytes: 250}})
	f.fSubtreeKey = storeReorgSubtree(t, ctx, f.blob, []subtreepkg.Node{{Hash: f.f1, Fee: 100, SizeInBytes: 250}})
	f2SubtreeKey := storeReorgSubtree(t, ctx, f.blob, []subtreepkg.Node{{Hash: f.f2, Fee: 100, SizeInBytes: 250}})
	a2SubtreeKey := storeReorgSubtree(t, ctx, f.blob, []subtreepkg.Node{{Hash: x1, Fee: 100, SizeInBytes: 250}})
	f.a3SubtreeKey = storeReorgSubtree(t, ctx, f.blob, []subtreepkg.Node{{Hash: x2, Fee: 100, SizeInBytes: 250}})

	// A second subtree whose first node is the coinbase placeholder: the move-back
	// skips the placeholder only in a block's first subtree, so the bulk build's
	// AddNode refuses it, after the move-back has replaced the subtrees.
	badBuildKey := storeReorgSubtree(t, ctx, f.blob, []subtreepkg.Node{{Hash: y1, Fee: 100, SizeInBytes: 250}})

	header := func(prev *model.BlockHeader, nonce uint32) *model.BlockHeader {
		return &model.BlockHeader{Version: 1, HashPrevBlock: prev.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2300000000 + nonce, Nonce: nonce}
	}

	f.parent = &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2300000001, Nonce: 9401}
	f.tipA = header(f.parent, 9402)
	f.forkF = header(f.parent, 9403)
	f.forkF2 = header(f.forkF, 9404)
	f.childA2 = header(f.tipA, 9405)
	f.childA3 = header(f.childA2, 9406)

	f.blockA = &model.Block{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{aSubtreeKey}, Header: f.tipA}
	f.blockABadBuild = &model.Block{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{aSubtreeKey, badBuildKey}, Header: f.tipA}
	f.blockF = &model.Block{Height: 2, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{f.fSubtreeKey}, Header: f.forkF}
	f.blockF2 = &model.Block{Height: 3, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{f2SubtreeKey}, Header: f.forkF2}
	f.blockA2 = &model.Block{Height: 3, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{a2SubtreeKey}, Header: f.childA2}
	f.blockA3 = &model.Block{Height: 4, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{f.a3SubtreeKey}, Header: f.childA3}

	_, err = sqlStore.Create(ctx, coinbaseTx2, 2)
	require.NoError(t, err)
	_, err = sqlStore.Create(ctx, coinbaseTx3, 2)
	require.NoError(t, err)

	bc := &blockchain.Mock{}
	bc.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	bc.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	bc.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	for _, h := range []*model.BlockHeader{f.parent, f.tipA, f.forkF, f.forkF2, f.childA2, f.childA3} {
		bc.On("GetBlockHeader", mock.Anything, h.Hash()).Return(h, &model.BlockHeaderMeta{}, nil)
	}

	newSubtreeChan := make(chan NewSubtreeRequest, 64)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	f.stp, err = NewSubtreeProcessor(ctx, logger, tSettings, f.blob, bc, f.store, newSubtreeChan,
		append([]Options{WithTxMapDirs([]string{t.TempDir(), t.TempDir()})}, o.opts...)...)
	require.NoError(t, err)

	f.stp.InitCurrentBlockHeader(f.tipA)

	// The unmined transactions from before the reorg, added before the
	// processor goroutine starts.
	require.NoError(t, f.stp.AddDirectly(&subtreepkg.Node{Hash: f.m1, Fee: 50, SizeInBytes: 200}, &subtreepkg.TxInpoints{}, true))

	for i := range o.fillers {
		filler := chainhash.HashH([]byte{0xee, byte(i)})
		require.NoError(t, f.stp.AddDirectly(&subtreepkg.Node{Hash: filler, Fee: 50, SizeInBytes: 200}, &subtreepkg.TxInpoints{}, true))
	}

	// The hook runs on the processor goroutine at the top of every move-back and
	// move-forward step, with the header the step starts from. An installed
	// action runs at every step until it is replaced or cleared.
	f.stp.SetProgressHook(func() {
		if fn := f.hook.Load(); fn != nil {
			(*fn)(f.stp.currentBlockHeader.Load())
		}
	})

	f.stp.Start(ctx)
	t.Cleanup(func() { f.stp.Stop(context.Background()) })

	return f
}

// atStep installs fn to run once, at the top of the step that starts from header.
func (f *stepRestoreFixture) atStep(header *model.BlockHeader, fn func(stp *SubtreeProcessor)) {
	var fired atomic.Bool

	hook := func(h *model.BlockHeader) {
		if h.Hash().IsEqual(header.Hash()) && fired.CompareAndSwap(false, true) {
			fn(f.stp)
		}
	}

	f.hook.Store(&hook)
}

func (f *stepRestoreFixture) assemblyHashes(t *testing.T) []chainhash.Hash {
	t.Helper()

	var out []chainhash.Hash

	for _, h := range f.stp.GetTransactionHashes(context.Background()) {
		if !h.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			out = append(out, h)
		}
	}

	return out
}

// sweepAfterFailure runs the reset that has to follow a failed fork reorg,
// without a reload, and returns what its sweep marked not on the longest chain.
func (f *stepRestoreFixture) sweepAfterFailure(t *testing.T) []chainhash.Hash {
	t.Helper()

	f.store.takeMarkedNotOnLongestChain()

	resp := f.stp.Reset(f.tipA, []*model.Block{}, []*model.Block{}, false, nil)
	require.NoError(t, resp.Err)
	require.True(t, resp.Rotated)

	return f.store.takeMarkedNotOnLongestChain()
}

// failSequentialRemainderOn deletes hash from the map the step reads its
// remainder from, so the sequential remainder pass fails on it after
// resetSubtreeState has replaced the subtrees. hash must be first in the
// remainder, so the pass fails before it adds any node.
func failSequentialRemainderOn(t *testing.T, stp *SubtreeProcessor, hash chainhash.Hash) {
	t.Helper()

	diskMap, ok := stp.currentTxMap.(*DiskTxMap)
	require.True(t, ok)
	require.True(t, diskMap.Delete(hash))
}

func errText(text string) func(*stepRestoreFixture) string {
	return func(*stepRestoreFixture) string { return text }
}

// The reset after a failed fork reorg must sweep every unmined transaction from
// before the reorg, wherever the reorg failed. The sweep reads the subtrees the failed
// reorg left. A step that fails part way, after it replaced the subtrees, puts
// back its own input, so the sweep sees the last completed step's output, which
// still holds m1 and, once A has moved back, a1.
func TestFailedForkReorg_ResetSweepsThePreReorgUnminedTxsAtEveryFailurePoint(t *testing.T) {
	cases := []struct {
		name string
		// run fails the reorg and returns its error
		run        func(t *testing.T, f *stepRestoreFixture) error
		wantErr    func(f *stepRestoreFixture) string
		consistent bool // map and subtrees agree after the failure
	}{
		{
			name: "before the first forward step's resetSubtreeState",
			run: func(t *testing.T, f *stepRestoreFixture) error {
				f.atStep(f.parent, func(_ *SubtreeProcessor) {
					require.NoError(t, f.blob.Del(context.Background(), f.fSubtreeKey[:], fileformat.FileTypeSubtree))
				})

				return f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2})
			},
			wantErr:    func(f *stepRestoreFixture) string { return f.fSubtreeKey.String() },
			consistent: true,
		},
		{
			name: "inside the first forward step's remainder pass",
			run: func(t *testing.T, f *stepRestoreFixture) error {
				f.atStep(f.parent, func(stp *SubtreeProcessor) { failSequentialRemainderOn(t, stp, f.a1) })

				return f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2})
			},
			wantErr: errText("error getting node txInpoints from currentTxMap"),
		},
		{
			name: "inside the second forward step's remainder pass",
			run: func(t *testing.T, f *stepRestoreFixture) error {
				f.atStep(f.forkF, func(stp *SubtreeProcessor) { failSequentialRemainderOn(t, stp, f.a1) })

				return f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2})
			},
			wantErr: errText("error getting node txInpoints from currentTxMap"),
		},
		{
			name: "in the first forward step's coinbase processing",
			run: func(t *testing.T, f *stepRestoreFixture) error {
				f.store.failCoinbaseOf.Store(f.blockF.CoinbaseTx.TxIDChainHash())
				defer f.store.failCoinbaseOf.Store(nil)

				return f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2})
			},
			wantErr:    errText("error processing coinbase utxos"),
			consistent: true,
		},
		{
			name: "at the pre-commit disk tx map check of a move-back-only reorg",
			run: func(t *testing.T, f *stepRestoreFixture) error {
				failDiskTxMapLogs(f.stp.diskTxMap, alwaysFailWrites, nil)

				return f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{})
			},
			wantErr: errText("disk tx map storage error before committing reorg"),
		},
		{
			name: "at the first forward step's pre-commit check, on the move-back's failed writes",
			run: func(t *testing.T, f *stepRestoreFixture) error {
				// The move-back writes a1 into the pre-reorg map, which the first
				// forward step then reads from, so its pre-commit check must see
				// the failed write there.
				failDiskTxMapLogs(f.stp.diskTxMap, alwaysFailWrites, nil)

				return f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2})
			},
			wantErr: errText("disk tx map storage error before dequeue"),
		},
		{
			name: "inside the move-back's bulk build",
			run: func(t *testing.T, f *stepRestoreFixture) error {
				return f.stp.Reorg([]*model.Block{f.blockABadBuild}, []*model.Block{f.blockF, f.blockF2})
			},
			wantErr: errText("error bulk building subtrees"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newStepRestoreFixture(t, stepRestoreOptions{})

			err := tc.run(t, f)
			require.ErrorContains(t, err, tc.wantErr(f), "the reorg must fail at the injected point, not elsewhere")
			require.Equal(t, f.tipA.Hash(), f.stp.GetCurrentBlockHeader().Hash(), "the header goes back to the losing tip")
			require.Nil(t, f.stp.GetPrecomputedMiningData(), "no mining data is served from a failed fork reorg's state")
			require.Nil(t, f.stp.GetIncompleteSubtreeMiningData(context.Background()), "not even an incomplete-subtree snapshot")

			held := f.assemblyHashes(t)
			require.Contains(t, held, f.m1, "the failed reorg's subtrees still hold m1, unmined before the reorg")

			if tc.consistent {
				require.NoError(t, f.stp.CheckSubtreeProcessor(), "the map agrees with the subtrees left in place")
			}

			swept := f.sweepAfterFailure(t)
			require.Contains(t, swept, f.m1, "the reset's sweep must mark m1")

			if slices.Contains(held, f.a1) {
				require.Contains(t, swept, f.a1, "and the moved-back a1 it holds")
			}
		})
	}
}

// A fork reorg releases the pre-reorg disk tx map at the first forward step's
// commit, and the pre-reorg subtrees once the move-back is done, instead of
// holding both to the end of the reorg.
func TestForkReorg_ReleasesThePreReorgMapAndSubtreesBeforeItEnds(t *testing.T) {
	// 4 leaves per subtree and 6 fillers give block assembly two complete
	// chained subtrees before the reorg, which the mining snapshot holds.
	f := newStepRestoreFixture(t, stepRestoreOptions{
		fillers:  6,
		settings: func(s *settings.Settings) { s.BlockAssembly.InitialMerkleItemsPerSubtree = 4 },
	})

	preReorgMap, ok := f.stp.GetCurrentTxMap().(*DiskTxMap)
	require.True(t, ok)

	// Read through the processor goroutine, which owns these fields.
	var (
		preReorgSubtrees []*subtreepkg.Subtree
		preReorgSnapshot *PrecomputedMiningData
	)

	// What the steps saw, written on the processor goroutine and read after
	// Reorg has answered.
	type observed struct {
		installed      []*subtreepkg.Subtree
		snapshot       *PrecomputedMiningData
		mapOpen        bool
		mapIsCurrent   bool
		anchorAtStep1  *DiskTxMap
		mapOpenAtStep2 bool
		anchorAtStep2  *DiskTxMap
	}

	var seen observed

	// The hook runs at the move-back's own step, which starts from A, and at
	// each forward step.
	hook := func(h *model.BlockHeader) {
		switch {
		case h.Hash().IsEqual(f.tipA.Hash()) && preReorgSubtrees == nil:
			preReorgSubtrees = append(slices.Clone(f.stp.chainedSubtrees), f.stp.currentSubtree.Load())
			preReorgSnapshot = f.stp.precomputedMiningData.Load()
		case h.Hash().IsEqual(f.parent.Hash()):
			seen.installed = append(slices.Clone(f.stp.chainedSubtrees), f.stp.currentSubtree.Load())
			seen.snapshot = f.stp.precomputedMiningData.Load()
			seen.mapOpen = preReorgMap.gen.Load() != nil
			seen.mapIsCurrent = f.stp.currentTxMap == TxInpointsMap(preReorgMap)
			seen.anchorAtStep1 = f.stp.diskTxMapAnchor
		case h.Hash().IsEqual(f.forkF.Hash()):
			seen.mapOpenAtStep2 = preReorgMap.gen.Load() != nil
			seen.anchorAtStep2 = f.stp.diskTxMapAnchor
		}
	}
	f.hook.Store(&hook)

	require.NoError(t, f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2}))
	require.Equal(t, f.forkF2.Hash(), f.stp.GetCurrentBlockHeader().Hash())

	require.Len(t, preReorgSubtrees, 3, "precondition: two chained subtrees and the current one before the reorg")
	require.NotNil(t, preReorgSnapshot)
	require.Len(t, preReorgSnapshot.Subtrees, 2, "precondition: the mining snapshot holds the two chained subtrees")

	// After the move-back, before the first forward step.
	for _, st := range preReorgSubtrees {
		require.NotContains(t, seen.installed, st, "the move-back replaced every pre-reorg subtree")
		require.NotContains(t, seen.snapshot.Subtrees, st, "and the mining snapshot no longer points at any of them")
	}

	require.NotSame(t, preReorgSnapshot, seen.snapshot, "the move-back refreshed the mining snapshot")
	require.Equal(t, f.parent.Hash(), seen.snapshot.PreviousHeader.Hash(), "on the parent the moved-back subtrees are valid on")

	require.True(t, seen.mapOpen, "the pre-reorg map is the first forward step's source, so it is open then")
	require.True(t, seen.mapIsCurrent)
	require.Nil(t, seen.anchorAtStep1, "and not yet the anchor: the first forward step's reset makes it that")

	// At the top of the second forward step, after the first one committed.
	require.False(t, seen.mapOpenAtStep2, "the first forward step's commit closed the pre-reorg map")
	require.Nil(t, seen.anchorAtStep2, "and released it as the anchor")

	require.Nil(t, preReorgMap.gen.Load(), "the pre-reorg map stays closed")
	require.Nil(t, f.stp.diskTxMapAnchor)
	require.NoError(t, f.stp.CheckSubtreeProcessor())
}

// A failed second forward step puts back the first step's output map, which the
// first step's commit left open, and not the closed pre-reorg map.
func TestForkReorg_AFailedLaterStepPutsBackThePreviousStepsMap(t *testing.T) {
	f := newStepRestoreFixture(t, stepRestoreOptions{})

	preReorgMap, ok := f.stp.GetCurrentTxMap().(*DiskTxMap)
	require.True(t, ok)

	var stepOneOutput TxInpointsMap

	f.atStep(f.forkF, func(stp *SubtreeProcessor) {
		stepOneOutput = stp.currentTxMap
		failSequentialRemainderOn(t, stp, f.a1)
	})

	err := f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2})
	require.ErrorContains(t, err, "error getting node txInpoints from currentTxMap")

	require.NotNil(t, stepOneOutput)
	require.NotSame(t, preReorgMap, stepOneOutput)
	require.Same(t, stepOneOutput, f.stp.GetCurrentTxMap(), "the failed step put back its input map")
	require.NotNil(t, stepOneOutput.(*DiskTxMap).gen.Load(), "which is still open")
	require.Same(t, stepOneOutput, f.stp.diskTxMap, "and stays the processor's disk map once the reorg has finished")
	require.Nil(t, preReorgMap.gen.Load(), "the pre-reorg map was closed at the first step's commit")

	require.ElementsMatch(t, []chainhash.Hash{f.m1, f.a1}, f.assemblyHashes(t), "the subtrees are the first step's output")
}

// A catch-up's rollback puts back the map from before the catch-up after any
// block fails, so the catch-up pins that map until it ends. Here the second block
// fails, after the first one committed.
func TestCatchUp_KeepsThePreCatchUpMapPinnedUntilItEnds(t *testing.T) {
	f := newStepRestoreFixture(t, stepRestoreOptions{})

	preCatchUpMap, ok := f.stp.GetCurrentTxMap().(*DiskTxMap)
	require.True(t, ok)

	var (
		openAtStep2   bool
		anchorAtStep2 *DiskTxMap
	)

	f.atStep(f.childA2, func(stp *SubtreeProcessor) {
		openAtStep2 = preCatchUpMap.gen.Load() != nil
		anchorAtStep2 = stp.diskTxMapAnchor

		require.NoError(t, f.blob.Del(context.Background(), f.a3SubtreeKey[:], fileformat.FileTypeSubtree))
	})

	err := f.stp.Reorg([]*model.Block{}, []*model.Block{f.blockA2, f.blockA3})
	require.ErrorContains(t, err, f.a3SubtreeKey.String(), "the catch-up must fail at A3, after A2 committed")

	require.True(t, openAtStep2, "the first block's commit must not close the pre-catch-up map")
	require.Same(t, preCatchUpMap, anchorAtStep2, "it stays pinned as the anchor")

	require.Equal(t, f.tipA.Hash(), f.stp.GetCurrentBlockHeader().Hash(), "the catch-up rolled back to A")
	require.Same(t, preCatchUpMap, f.stp.GetCurrentTxMap(), "to the pinned pre-catch-up map")
	require.NotNil(t, preCatchUpMap.gen.Load(), "which is open")
	require.True(t, preCatchUpMap.Exists(f.m1))
	require.ElementsMatch(t, []chainhash.Hash{f.m1}, f.assemblyHashes(t))
	require.NoError(t, f.stp.CheckSubtreeProcessor())
	require.Nil(t, f.stp.diskTxMapAnchor, "the catch-up has ended, so nothing is pinned")
}
