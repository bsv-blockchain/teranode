package blockassembly

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxostoresql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// locatorFailingClient fails the next GetBlockLocator call once armed. One pass
// of moveToBlock calls getReorgBlocks up to three times (moveToBlock, handleReorg,
// then the reset), so the test arms it after the first two have run, from inside
// the reorg, and the call it fails is the reset's.
type locatorFailingClient struct {
	blockchain.ClientI

	failNext atomic.Bool
}

func (c *locatorFailingClient) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	if c.failNext.CompareAndSwap(true, false) {
		return nil, errors.NewServiceError("injected GetBlockLocator failure")
	}

	return c.ClientI.GetBlockLocator(ctx, blockHeaderHash, blockHeaderHeight)
}

// failedForkReorgBA is a block assembler on tip A (parent P), whose subtree
// processor reads real subtrees from a blob store and real records from a
// sqlitememory UTXO store. a1 is mined in A. The winning branch is F (f1) then
// F2. A2 and A3 extend the losing side from A.
type failedForkReorgBA struct {
	ba     *BlockAssembler
	stp    *subtreeprocessor.SubtreeProcessor
	client *locatorFailingClient
	utxo   utxo.Store
	blob   *blob_memory.Memory

	parent, tipA, forkF, forkF2, childA2, childA3 *model.BlockHeader

	a1Parent, a1, f1 *bt.Tx

	fSubtreeKey   *chainhash.Hash
	fSubtreeBytes []byte

	hook atomic.Pointer[func()]
}

// newTestTx builds a record-ready transaction, distinct per tag.
func newTestTx(t *testing.T, tag byte) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: uint32(tag), PreviousTxSatoshis: 5000000000, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{})}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{tag}))
	tx.Inputs = []*bt.Input{in}
	tx.Outputs = []*bt.Output{{Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, tag, 0x00, 0x00, 0x00, 0x00, 0x88, 0xac})}}

	return tx
}

// newTestChild builds a transaction spending output 0 of parent.
func newTestChild(t *testing.T, parent *bt.Tx, tag byte) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: parent.Outputs[0].Satoshis, PreviousTxScript: parent.Outputs[0].LockingScript, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{})}
	require.NoError(t, in.PreviousTxIDAdd(parent.TxIDChainHash()))
	tx.Inputs = []*bt.Input{in}
	tx.Outputs = []*bt.Output{{Satoshis: 900, LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, tag, 0x00, 0x00, 0x00, 0x00, 0x88, 0xac})}}

	return tx
}

// newTestCoinbase returns a coinbase transaction unique to tag.
func newTestCoinbase(t *testing.T, tag byte) *bt.Tx {
	t.Helper()

	cb, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	cb.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, tag, 0x01, 0x01})

	return cb
}

// storeSubtreeWithMeta stores a subtree of the given transactions, and its meta,
// under the subtree's root hash.
func storeSubtreeWithMeta(t *testing.T, blob *blob_memory.Memory, txs ...*bt.Tx) *chainhash.Hash {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(64)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, tx := range txs {
		require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: *tx.TxIDChainHash(), Fee: 100, SizeInBytes: uint64(tx.Size())}))
	}

	stBytes, err := st.Serialize()
	require.NoError(t, err)

	key := st.RootHash()
	require.NoError(t, blob.Set(t.Context(), key[:], fileformat.FileTypeSubtree, stBytes))

	meta := subtreepkg.NewSubtreeMeta(st)
	for i := range st.Nodes {
		require.NoError(t, meta.SetTxInpoints(i, subtreepkg.NewTxInpoints()))
	}

	metaBytes, err := meta.Serialize()
	require.NoError(t, err)
	require.NoError(t, blob.Set(t.Context(), key[:], fileformat.FileTypeSubtreeMeta, metaBytes))

	return key
}

func newFailedForkReorgBA(t *testing.T) *failedForkReorgBA {
	t.Helper()

	initPrometheusMetrics()

	ctx := t.Context()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := createTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := utxostoresql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	f := &failedForkReorgBA{
		client: &locatorFailingClient{ClientI: localClient},
		utxo:   utxoStore,
		blob:   blob_memory.New(),
		f1:     newTestTx(t, 0xf1),
	}

	// a1 spends a mined parent, so the reload's input check can accept it. a1
	// and f1 have records and are mined: a1 in A, f1 in F.
	a1Parent := newTestTx(t, 0x50)
	f.a1Parent = a1Parent
	f.a1 = newTestChild(t, a1Parent, 0xa1)

	for _, tx := range []*bt.Tx{a1Parent, f.f1} {
		_, _, err = utxoStore.SpendAndCreate(ctx, tx, 0, utxo.WithCreateOnly())
		require.NoError(t, err)
	}

	_, _, err = utxoStore.SpendAndCreate(ctx, f.a1, 2)
	require.NoError(t, err)

	aSubtreeKey := storeSubtreeWithMeta(t, f.blob, f.a1)
	f.fSubtreeKey = storeSubtreeWithMeta(t, f.blob, f.f1)

	f.fSubtreeBytes, err = f.blob.Get(ctx, f.fSubtreeKey[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	header := func(prev *model.BlockHeader, nonce uint32) *model.BlockHeader {
		return &model.BlockHeader{Version: 1, HashPrevBlock: prev.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 1700000000 + nonce, Nonce: nonce, Bits: *bits}
	}

	f.parent = blockHeader1
	f.tipA = header(f.parent, 7101)
	f.forkF = header(f.parent, 7201)
	f.forkF2 = header(f.forkF, 7202)
	f.childA2 = header(f.tipA, 7102)
	f.childA3 = header(f.childA2, 7103)

	addBlock := func(h *model.BlockHeader, tag byte, txCount uint64, subtrees ...*chainhash.Hash) {
		require.NoError(t, localClient.AddBlock(ctx, &model.Block{
			Header:           h,
			CoinbaseTx:       newTestCoinbase(t, tag),
			TransactionCount: txCount,
			Subtrees:         subtrees,
		}, "", blockchainoptions.WithMinedSet(true)))
	}

	addBlock(f.parent, 0x01, 1)
	addBlock(f.tipA, 0x02, 2, aSubtreeKey)
	addBlock(f.forkF, 0x03, 2, f.fSubtreeKey)
	addBlock(f.forkF2, 0x04, 1)
	addBlock(f.childA2, 0x05, 1)
	addBlock(f.childA3, 0x06, 1)

	// The parent is mined in P, a1 in A and f1 in F.
	for _, mined := range []struct {
		tx    *bt.Tx
		block *model.BlockHeader
	}{{a1Parent, f.parent}, {f.a1, f.tipA}, {f.f1, f.forkF}} {
		_, meta, err := localClient.GetBlockHeader(ctx, mined.block.Hash())
		require.NoError(t, err)

		_, err = utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{mined.tx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: meta.ID, BlockHeight: meta.Height, OnLongestChain: true})
		require.NoError(t, err)
	}

	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, 100)

	f.ba, err = NewBlockAssembler(ctx, ulogger.TestLogger{}, tSettings, gocore.NewStat("test"), utxoStore, f.blob, f.client, newSubtreeChan)
	require.NoError(t, err)

	var ok bool
	f.stp, ok = f.ba.subtreeProcessor.(*subtreeprocessor.SubtreeProcessor)
	require.True(t, ok)

	// Cleanups run last in, first out, so the processor stops before its
	// channel closes and the drain goroutine below ends.
	t.Cleanup(func() { close(newSubtreeChan) })
	t.Cleanup(func() { f.stp.Stop(context.Background()) })

	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	f.ba.setBestBlockHeader(f.tipA, 2)
	f.stp.InitCurrentBlockHeader(f.tipA)

	// Fires once, on the first forward step of the fork reorg: the one where the
	// processor's header is already the moved-back block's parent.
	f.stp.SetProgressHook(func() {
		if !f.stp.GetCurrentBlockHeader().Hash().IsEqual(f.parent.Hash()) {
			return
		}

		if fn := f.hook.Swap(nil); fn != nil {
			(*fn)()
		}
	})

	f.stp.Start(ctx)

	return f
}

func (f *failedForkReorgBA) meta(t *testing.T, h *model.BlockHeader) *model.BlockHeaderMeta {
	t.Helper()

	_, meta, err := f.client.GetBlockHeader(t.Context(), h.Hash())
	require.NoError(t, err)

	return meta
}

func (f *failedForkReorgBA) assemblyHas(t *testing.T, tx *bt.Tx) bool {
	t.Helper()

	for _, h := range f.stp.GetTransactionHashes(t.Context()) {
		if h.Equal(*tx.TxIDChainHash()) {
			return true
		}
	}

	return false
}

// failForkReorgThenItsReset runs one pass of moveToBlock to F2. The fork reorg
// from A fails at F's first forward step, after A has moved back, and the reset
// handleReorg falls back to fails at getReorgBlocks, before the subtree
// processor. Block assembly is left on A with the rolled-back processor state.
func (f *failedForkReorgBA) failForkReorgThenItsReset(t *testing.T) {
	t.Helper()

	f.hook.Store(func() *func() {
		fn := func() {
			require.NoError(t, f.blob.Del(context.Background(), f.fSubtreeKey[:], fileformat.FileTypeSubtree))
			f.client.failNext.Store(true)
		}

		return &fn
	}())

	err := f.ba.moveToBlock(t.Context(), f.forkF2, f.meta(t, f.forkF2))
	require.ErrorContains(t, err, "[Reset] error getting reorg blocks", "the reset must fail before it reaches the subtree processor")
	require.False(t, f.client.failNext.Load(), "the injected failure was consumed by the reset")

	tip, _ := f.ba.CurrentBlock()
	require.Equal(t, f.tipA.Hash(), tip.Hash(), "block assembly stays on A")
	require.Equal(t, f.tipA.Hash(), f.stp.GetCurrentBlockHeader().Hash(), "the processor rolled back to A")

	require.NoError(t, f.blob.Set(context.Background(), f.fSubtreeKey[:], fileformat.FileTypeSubtree, f.fSubtreeBytes, options.WithAllowOverwrite(true)))
}

// The next pass after a failed fork reorg and a failed reset retried the reorg
// on the rolled-back state, which dropped a1 from block assembly and never
// marked it unmined. It now resets, and a1 is back in block assembly.
func TestFailedForkReorgAndReset_NextPassResetsAndKeepsTheMovedBackTx(t *testing.T) {
	f := newFailedForkReorgBA(t)
	f.failForkReorgThenItsReset(t)

	err := f.ba.moveToBlock(t.Context(), f.forkF2, f.meta(t, f.forkF2))
	require.ErrorIs(t, err, errors.ErrBlockAssemblyReset, "the next pass resets instead of reorging the rolled-back state")

	tip, _ := f.ba.CurrentBlock()
	require.Equal(t, f.forkF2.Hash(), tip.Hash())
	require.Equal(t, f.forkF2.Hash(), f.stp.GetCurrentBlockHeader().Hash())

	require.True(t, f.assemblyHas(t, f.a1), "a1 is back in block assembly once A is off the chain")
	require.False(t, f.assemblyHas(t, f.f1), "f1 is mined by F")
	require.NotZero(t, unminedSinceOfTx(t, f.utxo, f.a1), "a1 is marked unmined")
	require.NoError(t, f.stp.CheckSubtreeProcessor())
}

// After the failed fork reorg the chain goes back to the losing side, so the
// next pass is a plain move forward to A's child A2. The processor refuses it,
// and block assembly must reset rather than be refused on every pass.
func TestFailedForkReorgAndReset_MoveForwardToTheLosingSideResets(t *testing.T) {
	f := newFailedForkReorgBA(t)
	f.failForkReorgThenItsReset(t)

	err := f.ba.moveToBlock(t.Context(), f.childA2, f.meta(t, f.childA2))
	require.ErrorIs(t, err, errors.ErrBlockAssemblyReset, "a refused move forward must lead to a reset")

	tip, _ := f.ba.CurrentBlock()
	require.Equal(t, f.childA2.Hash(), tip.Hash())
	require.Equal(t, f.childA2.Hash(), f.stp.GetCurrentBlockHeader().Hash())

	// a1 is mined in A, which is on the chain again, so it is rightly not in
	// block assembly. The reset left subtrees and map in agreement.
	require.False(t, f.assemblyHas(t, f.a1))
	require.NoError(t, f.stp.CheckSubtreeProcessor())

	require.NoError(t, f.ba.moveToBlock(t.Context(), f.childA3, f.meta(t, f.childA3)), "after the reset, block assembly moves forward again")
	require.Equal(t, f.childA3.Hash(), f.stp.GetCurrentBlockHeader().Hash())
}

// The reset after a refused move forward reloads the unmined transactions with
// their inputs checked, as handleReorg's reset after a failed reorg does. d1 is
// an unmined double spend of a1's input, and a1 is mined in A, which is on the
// chain again, so the reload must leave d1 out of block assembly.
func TestFailedForkReorgAndReset_ResetAfterARefusalChecksTheReloadedInputs(t *testing.T) {
	f := newFailedForkReorgBA(t)
	f.failForkReorgThenItsReset(t)

	d1 := newTestChild(t, f.a1Parent, 0xd1)
	_, _, err := f.utxo.SpendAndCreate(t.Context(), d1, 2, utxo.WithCreateOnly())
	require.NoError(t, err)
	require.NotZero(t, unminedSinceOfTx(t, f.utxo, d1), "precondition: d1 is unmined, so the reload reads it")

	err = f.ba.moveToBlock(t.Context(), f.childA2, f.meta(t, f.childA2))
	require.ErrorIs(t, err, errors.ErrBlockAssemblyReset, "a refused move forward must lead to a reset")
	require.Equal(t, f.childA2.Hash(), f.stp.GetCurrentBlockHeader().Hash())

	require.False(t, f.assemblyHas(t, d1), "the reload checks inputs and leaves out d1, which spends an output a1 already spends")
	require.NoError(t, f.stp.CheckSubtreeProcessor())
}

// A reset after a refused move forward can itself fail. When it fails before
// the processor's tx map rotation, as here in getReorgBlocks, block assembly
// stays on A, the processor keeps refusing, and the next pass resets again
// rather than stalling. A reset that fails after the rotation has already
// cleared the refusal.
func TestFailedForkReorgAndReset_FailedResetAfterARefusalIsRetriedNextPass(t *testing.T) {
	f := newFailedForkReorgBA(t)
	f.failForkReorgThenItsReset(t)

	// The move forward makes no getReorgBlocks call before MoveForwardBlock, so
	// the armed failure is consumed by the reset after the refusal.
	f.client.failNext.Store(true)

	err := f.ba.moveToBlock(t.Context(), f.childA2, f.meta(t, f.childA2))
	require.ErrorContains(t, err, "error resetting after the subtree processor refused to move")
	require.ErrorContains(t, err, "[Reset] error getting reorg blocks")
	require.False(t, f.client.failNext.Load(), "the injected failure was consumed by the reset")

	tip, _ := f.ba.CurrentBlock()
	require.Equal(t, f.tipA.Hash(), tip.Hash(), "block assembly stays on A")
	require.Equal(t, f.tipA.Hash(), f.stp.GetCurrentBlockHeader().Hash())

	err = f.ba.moveToBlock(t.Context(), f.childA2, f.meta(t, f.childA2))
	require.ErrorIs(t, err, errors.ErrBlockAssemblyReset, "the next pass is refused again and resets")

	tip, _ = f.ba.CurrentBlock()
	require.Equal(t, f.childA2.Hash(), tip.Hash())
	require.Equal(t, f.childA2.Hash(), f.stp.GetCurrentBlockHeader().Hash())
	require.NoError(t, f.stp.CheckSubtreeProcessor())
}

// The same as the move forward, but two blocks ahead on the losing side, so the
// next pass is a forward-only catch-up, which returns its failure without a
// reset of its own.
func TestFailedForkReorgAndReset_CatchUpOnTheLosingSideResets(t *testing.T) {
	f := newFailedForkReorgBA(t)
	f.failForkReorgThenItsReset(t)

	err := f.ba.moveToBlock(t.Context(), f.childA3, f.meta(t, f.childA3))
	require.ErrorIs(t, err, errors.ErrBlockAssemblyReset, "a refused catch-up must lead to a reset")

	tip, _ := f.ba.CurrentBlock()
	require.Equal(t, f.childA3.Hash(), tip.Hash())
	require.Equal(t, f.childA3.Hash(), f.stp.GetCurrentBlockHeader().Hash())
	require.NoError(t, f.stp.CheckSubtreeProcessor())
}

func unminedSinceOfTx(t *testing.T, store utxo.Store, tx *bt.Tx) uint32 {
	t.Helper()

	data, err := store.Get(t.Context(), tx.TxIDChainHash())
	require.NoError(t, err)

	return data.UnminedSince
}
