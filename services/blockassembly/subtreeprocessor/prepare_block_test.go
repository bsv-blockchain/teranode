package subtreeprocessor

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// gatedStore is a blob store whose reads can be held open: while held, GetIoReader blocks until
// the gate is released or the caller's context is done.
type gatedStore struct {
	blob.Store
	gate  atomic.Pointer[chan struct{}]
	reads atomic.Int32
}

func (g *gatedStore) hold() {
	ch := make(chan struct{})
	g.gate.Store(&ch)
}

func (g *gatedStore) release() {
	if ch := g.gate.Swap(nil); ch != nil {
		close(*ch)
	}
}

func (g *gatedStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	g.reads.Add(1)

	if ch := g.gate.Load(); ch != nil {
		select {
		case <-*ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return g.Store.GetIoReader(ctx, key, fileType, opts...)
}

// prepareFixture is a subtree processor on the genesis block and, in its subtree store, the
// two full subtrees of a block on top of genesis whose header commits to them, plus a spare
// subtree that is in no block.
type prepareFixture struct {
	stp      *SubtreeProcessor
	store    blob.Store
	gated    *gatedStore
	block    *model.Block
	subtrees []*subtreepkg.Subtree
	spare    *subtreepkg.Subtree
}

func newPrepareFixture(t *testing.T) *prepareFixture {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///prepare")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 32

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	f := &prepareFixture{gated: &gatedStore{Store: blob_memory.New()}}
	f.store = f.gated

	f.stp, err = NewSubtreeProcessor(ctx, logger, tSettings, f.store, blockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	f.stp.currentBlockHeader.Store(model.GenesisBlockHeader)

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	for s := 0; s < 3; s++ {
		st, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)

		for i := 0; i < 4; i++ {
			if s == 0 && i == 0 {
				require.NoError(t, st.AddCoinbaseNode())
				continue
			}

			require.NoError(t, st.AddNode(chainhash.HashH([]byte(fmt.Sprintf("prepare-%d-%d", s, i))), 1, 1))
		}

		b, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, f.store.Set(ctx, st.RootHash().CloneBytes(), fileformat.FileTypeSubtree, b))

		if s < 2 {
			f.subtrees = append(f.subtrees, st)
		} else {
			f.spare = st
		}
	}

	root0, err := f.subtrees[0].RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))
	require.NoError(t, err)

	merkleRoot := chainhash.DoubleHashH(append(root0.CloneBytes(), f.subtrees[1].RootHash().CloneBytes()...))

	f.block, err = model.NewBlock(&model.BlockHeader{
		Version:        1,
		HashPrevBlock:  model.GenesisBlockHeader.Hash(),
		HashMerkleRoot: &merkleRoot,
		Bits:           model.GenesisBlockHeader.Bits,
	}, coinbase, []*chainhash.Hash{f.subtrees[0].RootHash(), f.subtrees[1].RootHash()}, 8, 1000, 1, 0)
	require.NoError(t, err)

	return f
}

// withSubtrees returns a copy of the block with the same header, so the same hash, and the
// given subtree list.
func (f *prepareFixture) withSubtrees(t *testing.T, subtrees ...*chainhash.Hash) *model.Block {
	t.Helper()

	b, err := model.NewBlock(f.block.Header, f.block.CoinbaseTx, subtrees, f.block.TransactionCount, f.block.SizeInBytes, 1, 0)
	require.NoError(t, err)
	require.True(t, b.Hash().IsEqual(f.block.Hash()))

	return b
}

func (f *prepareFixture) delSubtrees(t *testing.T) {
	t.Helper()

	for _, st := range append(f.subtrees, f.spare) {
		require.NoError(t, f.store.Del(context.Background(), st.RootHash().CloneBytes(), fileformat.FileTypeSubtree))
	}
}

// waitPrepared waits until the map being prepared for hash is done and returns it, or nil if
// none is being prepared for hash.
func waitPrepared(t *testing.T, stp *SubtreeProcessor, hash *chainhash.Hash) *preparedTxMap {
	t.Helper()

	stp.preparedMu.Lock()
	p := stp.prepared
	stp.preparedMu.Unlock()

	if p == nil || !p.hash.IsEqual(hash) {
		return nil
	}

	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatal("prepared transaction map not built in time")
	}

	return p
}

func (f *prepareFixture) takeMap(t *testing.T, block *model.Block) (*SortedHashSet, error) {
	t.Helper()

	blockSubtreesMap, _ := f.stp.processBlockSubtrees(block)
	m, _, err := f.stp.createTransactionMapIfNeeded(context.Background(), block, blockSubtreesMap)

	return m, err
}

// A map prepared for a block is the one moveForwardBlock uses for it: once prepared, the block's
// subtrees are not read again.
func TestPrepareBlock_MoveForwardUsesPreparedMap(t *testing.T) {
	f := newPrepareFixture(t)

	want, err := f.takeMap(t, f.block)
	require.NoError(t, err)
	require.Equal(t, 8, want.Length())

	f.stp.PrepareBlock(f.block)
	p := waitPrepared(t, f.stp, f.block.Hash())
	require.NotNil(t, p)
	require.NoError(t, p.err)

	f.delSubtrees(t)

	got, err := f.takeMap(t, f.block)
	require.NoError(t, err)
	require.Equal(t, want.Length(), got.Length())

	f.stp.preparedMu.Lock()
	defer f.stp.preparedMu.Unlock()
	require.Nil(t, f.stp.prepared, "a used map is handed over, not kept")
}

// The map is sized from the body bound to the header, not from the transaction count the block
// claims, which is not committed to by the header.
func TestPrepareBlock_SizedFromBoundBody(t *testing.T) {
	f := newPrepareFixture(t)

	f.block.TransactionCount = 1 << 40

	f.stp.PrepareBlock(f.block)
	p := waitPrepared(t, f.stp, f.block.Hash())
	require.NotNil(t, p)
	require.NoError(t, p.err)
	require.Equal(t, uint64(8), p.txCount)
	require.Equal(t, 8, p.txMap.Length())
}

// A subtree list the header does not commit to is not prepared, even under the header of a
// real block, and the real block's list replaces it.
func TestPrepareBlock_UnboundSubtreeListIsNotPrepared(t *testing.T) {
	f := newPrepareFixture(t)

	doctored := f.withSubtrees(t, f.subtrees[0].RootHash(), f.spare.RootHash())

	f.stp.PrepareBlock(doctored)
	p := waitPrepared(t, f.stp, f.block.Hash())
	require.NotNil(t, p)
	require.ErrorContains(t, p.err, "merkle root does not match")
	require.Nil(t, p.txMap)

	f.stp.PrepareBlock(f.block)
	p = waitPrepared(t, f.stp, f.block.Hash())
	require.NotNil(t, p)
	require.NoError(t, p.err, "the bound list replaces the unbound one")

	f.delSubtrees(t)

	got, err := f.takeMap(t, f.block)
	require.NoError(t, err)
	require.Equal(t, 8, got.Length())
}

// A map prepared for a block's subtree list is not used for the same block hash with another
// list.
func TestPrepareBlock_NotUsedForAnotherSubtreeList(t *testing.T) {
	f := newPrepareFixture(t)

	f.stp.PrepareBlock(f.block)
	p := waitPrepared(t, f.stp, f.block.Hash())
	require.NotNil(t, p)
	require.NoError(t, p.err)

	f.delSubtrees(t)

	_, err := f.takeMap(t, f.withSubtrees(t, f.subtrees[0].RootHash(), f.spare.RootHash()))
	require.Error(t, err, "the other list's subtrees must be read, and they are gone")
}

// A subtree whose file does not hash to its key does not bind the list.
func TestPrepareBlock_SubtreeNotMatchingItsKeyIsNotPrepared(t *testing.T) {
	f := newPrepareFixture(t)

	b, err := f.spare.Serialize()
	require.NoError(t, err)
	require.NoError(t, f.store.Del(context.Background(), f.subtrees[1].RootHash().CloneBytes(), fileformat.FileTypeSubtree))
	require.NoError(t, f.store.Set(context.Background(), f.subtrees[1].RootHash().CloneBytes(), fileformat.FileTypeSubtree, b))

	f.stp.PrepareBlock(f.block)
	p := waitPrepared(t, f.stp, f.block.Hash())
	require.NotNil(t, p)
	require.ErrorContains(t, p.err, "does not hash to its key")
}

// Only a block on the current tip is prepared.
func TestPrepareBlock_IgnoresBlockNotOnTip(t *testing.T) {
	f := newPrepareFixture(t)

	f.block.Header.HashPrevBlock = &chainhash.Hash{1}
	f.block.GetHash()
	f.stp.PrepareBlock(f.block)

	f.stp.preparedMu.Lock()
	defer f.stp.preparedMu.Unlock()
	require.Nil(t, f.stp.prepared)
}

// A map prepared for one block is never used for another, and is dropped when another block
// moves the tip.
func TestPrepareBlock_OtherBlockDoesNotUsePreparedMap(t *testing.T) {
	f := newPrepareFixture(t)

	f.stp.PrepareBlock(f.block)
	require.NotNil(t, waitPrepared(t, f.stp, f.block.Hash()))

	f.delSubtrees(t)

	other, err := model.NewBlock(&model.BlockHeader{
		Version:        f.block.Header.Version,
		HashPrevBlock:  f.block.Header.HashPrevBlock,
		HashMerkleRoot: f.block.Header.HashMerkleRoot,
		Bits:           f.block.Header.Bits,
		Nonce:          f.block.Header.Nonce + 1,
	}, f.block.CoinbaseTx, f.block.Subtrees, 8, 1000, 1, 0)
	require.NoError(t, err)
	require.False(t, other.Hash().IsEqual(f.block.Hash()))

	_, err = f.takeMap(t, other)
	require.Error(t, err, "the other block's subtrees must be read, and they are gone")

	f.stp.preparedMu.Lock()
	defer f.stp.preparedMu.Unlock()
	require.Nil(t, f.stp.prepared)
}

// A prepare that failed leaves moveForwardBlock to build the map itself.
func TestPrepareBlock_FailedPrepareFallsBack(t *testing.T) {
	f := newPrepareFixture(t)

	require.NoError(t, f.store.Del(context.Background(), f.subtrees[1].RootHash().CloneBytes(), fileformat.FileTypeSubtree))

	f.stp.PrepareBlock(f.block)
	p := waitPrepared(t, f.stp, f.block.Hash())
	require.NotNil(t, p)
	require.Error(t, p.err)

	b, err := f.subtrees[1].Serialize()
	require.NoError(t, err)
	require.NoError(t, f.store.Set(context.Background(), f.subtrees[1].RootHash().CloneBytes(), fileformat.FileTypeSubtree, b))

	got, err := f.takeMap(t, f.block)
	require.NoError(t, err)
	require.Equal(t, 8, got.Length())
}

// A map prepared over all of a block's subtrees is not used when some of them are this node's
// own, which moveForwardBlock leaves out of the map.
func TestPrepareBlock_NotUsedWhenOwnSubtreesInBlock(t *testing.T) {
	f := newPrepareFixture(t)

	f.stp.PrepareBlock(f.block)
	require.NotNil(t, waitPrepared(t, f.stp, f.block.Hash()))

	f.delSubtrees(t)

	// as if the second subtree were this node's own: only the first is mapped
	blockSubtreesMap := map[chainhash.Hash]int{*f.block.Subtrees[0]: 0}

	_, _, err := f.stp.createTransactionMapIfNeeded(context.Background(), f.block, blockSubtreesMap)
	require.Error(t, err, "the first subtree must be read, and it is gone")
}

// takeMap waits for a map that is still being built, instead of building it again: the hint
// usually arrives early, but moveForwardBlock can run while the map is being built.
func TestPrepareBlock_TakeWaitsForMapBeingBuilt(t *testing.T) {
	f := newPrepareFixture(t)

	f.gated.hold()
	t.Cleanup(f.gated.release)

	f.stp.PrepareBlock(f.block)

	type taken struct {
		m  *SortedHashSet
		ok bool
	}

	res := make(chan taken, 1)

	go func() {
		m, _, ok := f.stp.takePreparedTxMap(context.Background(), f.block, len(f.block.Subtrees))
		res <- taken{m, ok}
	}()

	select {
	case <-res:
		t.Fatal("the map was taken before it was built")
	case <-time.After(100 * time.Millisecond):
	}

	f.gated.release()

	select {
	case r := <-res:
		require.True(t, r.ok)
		require.Equal(t, 8, r.m.Length())
	case <-time.After(10 * time.Second):
		t.Fatal("the map was not handed over once built")
	}
}

// A moveForwardBlock that is cancelled while the map is being built does not wait for it, and
// stops the build.
func TestPrepareBlock_TakeStopsWaitingWhenContextDone(t *testing.T) {
	f := newPrepareFixture(t)

	f.gated.hold()
	t.Cleanup(f.gated.release)

	f.stp.PrepareBlock(f.block)

	f.stp.preparedMu.Lock()
	p := f.stp.prepared
	f.stp.preparedMu.Unlock()
	require.NotNil(t, p)

	ctx, cancel := context.WithCancel(context.Background())

	res := make(chan bool, 1)

	go func() {
		_, _, ok := f.stp.takePreparedTxMap(ctx, f.block, len(f.block.Subtrees))
		res <- ok
	}()

	cancel()

	select {
	case ok := <-res:
		require.False(t, ok)
	case <-time.After(10 * time.Second):
		t.Fatal("take did not stop waiting when its context was done")
	}

	// the build was cancelled with it, and ends without a map
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled build did not end")
	}

	require.Error(t, p.err)
	require.Nil(t, p.txMap)
}

// A prepare that failed is retried by the next hint for the block, which block validation sends
// again when it retries.
func TestPrepareBlock_FailedPrepareIsRetried(t *testing.T) {
	f := newPrepareFixture(t)

	require.NoError(t, f.store.Del(context.Background(), f.subtrees[1].RootHash().CloneBytes(), fileformat.FileTypeSubtree))

	f.stp.PrepareBlock(f.block)
	failed := waitPrepared(t, f.stp, f.block.Hash())
	require.NotNil(t, failed)
	require.Error(t, failed.err)

	b, err := f.subtrees[1].Serialize()
	require.NoError(t, err)
	require.NoError(t, f.store.Set(context.Background(), f.subtrees[1].RootHash().CloneBytes(), fileformat.FileTypeSubtree, b))

	f.stp.PrepareBlock(f.block)
	retried := waitPrepared(t, f.stp, f.block.Hash())
	require.NotNil(t, retried)
	require.NotSame(t, failed, retried)
	require.NoError(t, retried.err)
	require.Equal(t, 8, retried.txMap.Length())
}

// The same block hash with another subtree list, which a peer can relay under the honest
// header, does not cancel the honest prepare: it only replaces it once its list is bound.
func TestPrepareBlock_OtherListDoesNotCancelBoundPrepare(t *testing.T) {
	f := newPrepareFixture(t)

	f.gated.hold()
	t.Cleanup(f.gated.release)

	f.stp.PrepareBlock(f.block)

	f.stp.preparedMu.Lock()
	honest := f.stp.prepared
	f.stp.preparedMu.Unlock()
	require.NotNil(t, honest)

	f.stp.PrepareBlock(f.withSubtrees(t, f.subtrees[0].RootHash(), f.spare.RootHash()))

	// both are held on their first subtree read
	require.Eventually(t, func() bool { return f.gated.reads.Load() == 2 }, 10*time.Second, time.Millisecond)

	f.gated.release()

	require.NotNil(t, waitPrepared(t, f.stp, f.block.Hash()))
	require.NoError(t, honest.err, "the honest prepare was not cancelled")
	require.Equal(t, 8, honest.txMap.Length())

	f.stp.preparedMu.Lock()
	defer f.stp.preparedMu.Unlock()
	require.Same(t, honest, f.stp.prepared)
}

// The other list replaces the prepared one once it binds to the header, as when the earlier
// one was not the block's.
func TestPrepareBlock_BoundListReplacesUnboundPrepare(t *testing.T) {
	f := newPrepareFixture(t)

	f.gated.hold()
	t.Cleanup(f.gated.release)

	doctored := f.withSubtrees(t, f.subtrees[0].RootHash(), f.spare.RootHash())

	f.stp.PrepareBlock(doctored)
	f.stp.PrepareBlock(f.block)
	f.gated.release()

	require.Eventually(t, func() bool {
		f.stp.preparedMu.Lock()
		p := f.stp.prepared
		f.stp.preparedMu.Unlock()

		if p == nil || !sameSubtrees(p.subtrees, f.block.Subtrees) {
			return false
		}

		select {
		case <-p.done:
			return p.err == nil && p.txMap != nil
		default:
			return false
		}
	}, 10*time.Second, time.Millisecond)
}

// A map finished after the tip moved past its block is dropped, not held until the next block.
func TestPrepareBlock_DroppedWhenTipMovedPastBlock(t *testing.T) {
	f := newPrepareFixture(t)

	f.gated.hold()
	t.Cleanup(f.gated.release)

	f.stp.PrepareBlock(f.block)

	f.stp.preparedMu.Lock()
	p := f.stp.prepared
	f.stp.preparedMu.Unlock()
	require.NotNil(t, p)

	// the block was added without taking the map, as when the hint arrived late
	f.stp.currentBlockHeader.Store(f.block.Header)
	f.gated.release()

	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatal("prepare did not end")
	}

	require.Nil(t, p.txMap)

	f.stp.preparedMu.Lock()
	defer f.stp.preparedMu.Unlock()
	require.Nil(t, f.stp.prepared)
}
