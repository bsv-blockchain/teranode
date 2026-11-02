package subtreeprocessor

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
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// countPendingScans wraps a real store and counts the pending-intent scans. failComplete makes every
// CompleteConflictIntent fail, failPending makes every scan fail, and failLoad makes every checklist
// load fail.
type countPendingScans struct {
	utxo.Store
	scans        atomic.Int32
	failComplete atomic.Bool
	failPending  atomic.Bool
	failLoad     atomic.Bool
}

func (s *countPendingScans) LoadConflictRecovery(ctx context.Context, id chainhash.Hash) (*utxo.ConflictRecovery, error) {
	if s.failLoad.Load() {
		return nil, errors.NewStorageError("injected checklist load failure")
	}

	return s.Store.LoadConflictRecovery(ctx, id)
}

func (s *countPendingScans) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	if s.failComplete.Load() {
		return errors.NewStorageError("injected WAL completion failure")
	}

	return s.Store.CompleteConflictIntent(ctx, intentID)
}

func (s *countPendingScans) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	s.scans.Add(1)

	if s.failPending.Load() {
		return nil, errors.NewStorageError("injected WAL scan failure")
	}

	return s.Store.PendingConflictIntents(ctx)
}

// walTestChain is a real sqlitememory blockchain: genesis and two blocks on top of it, stored with
// their mined flag set. moveForward holds the two blocks for a reset or a moveForwardBlock.
type walTestChain struct {
	genesis     *model.BlockHeader
	moveForward []*model.Block
}

func newWALTestChain(t *testing.T) (blockchain.ClientI, *walTestChain) {
	t.Helper()

	ctx := t.Context()
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	genesis, _, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	first := childHeader(genesis, 301)
	chain := &walTestChain{
		genesis: genesis,
		moveForward: []*model.Block{
			{Height: 1, CoinbaseTx: coinbaseTx, TransactionCount: 1, Subtrees: []*chainhash.Hash{}, Header: first},
			{Height: 2, CoinbaseTx: coinbaseTx3, TransactionCount: 1, Subtrees: []*chainhash.Hash{}, Header: childHeader(first, 302)},
		},
	}

	for _, block := range chain.moveForward {
		require.NoError(t, client.AddBlock(ctx, block, "", blockchainoptions.WithMinedSet(true)))
	}

	return client, chain
}

func newResetWALTestProcessor(t *testing.T, start bool) (*SubtreeProcessor, *countPendingScans, *blob_memory.Memory, *walTestChain) {
	t.Helper()

	ctx := t.Context()
	blockchainClient, chain := newWALTestChain(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	sqlStore, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	store := &countPendingScans{Store: sqlStore}
	subtreeStore := blob_memory.New()

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), subtreeStore, blockchainClient, store, newSubtreeChan)
	require.NoError(t, err)

	if start {
		stp.Start(ctx)
	}

	t.Cleanup(func() {
		stp.Stop(context.Background())
		close(newSubtreeChan)
	})

	stp.InitCurrentBlockHeader(chain.genesis)

	return stp, store, subtreeStore, chain
}

func resetWALSpendTx(t *testing.T, parent *bt.Tx, satoshis uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	tx.Outputs = []*bt.Output{{Satoshis: satoshis, LockingScript: bscript.NewFromBytes([]byte{0x52})}}

	return tx
}

// A reset computes the forward intent ID of each moveForward block, so it never scans the pending intents.
func TestReset_DoesNotScanPendingIntents(t *testing.T) {
	stp, store, _, chain := newResetWALTestProcessor(t, true)
	moveForward := chain.moveForward

	response := stp.Reset(chain.genesis, nil, moveForward, false, func() error { return nil })
	require.NoError(t, response.Err)
	require.Equal(t, moveForward[1].Header.Hash(), stp.GetCurrentBlockHeader().Hash(), "reset must land on the last moveForward block")

	require.Zero(t, store.scans.Load(), "a reset must not scan the pending intents")
}

// createResetWALConflict stores a parent, a loser that spends it and a conflicting winner that
// spends the same output, and returns the loser and the winner.
func createResetWALConflict(t *testing.T, store utxo.Store, seed byte) (*bt.Tx, *bt.Tx) {
	t.Helper()

	ctx := t.Context()

	parent := bt.NewTx()
	parentIn := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 200000, SequenceNumber: 0xFFFFFFFF, UnlockingScript: bscript.NewFromBytes([]byte{})}
	_ = parentIn.PreviousTxIDAdd(&chainhash.Hash{9, 9, seed})
	parent.Inputs = []*bt.Input{parentIn}
	parent.Outputs = []*bt.Output{{Satoshis: 100000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)

	loser := resetWALSpendTx(t, parent, 90000)
	_, _, err = store.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)

	winner := resetWALSpendTx(t, parent, 80000)
	_, _, err = store.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	return loser, winner
}

// A deferred drain whose acknowledgement failed leaves a finished forward intent pending. The
// follow-up reset may take the fast-forward branch, and it must still complete that intent.
func TestReset_FastForwardCompletesWALIntent(t *testing.T) {
	ctx := t.Context()
	stp, store, _, chain := newResetWALTestProcessor(t, true)
	moveForward := chain.moveForward

	_, winner := createResetWALConflict(t, store, 1)
	_, _, err := utxo.ProcessConflicting(ctx, store, 1, *moveForward[0].Hash(), []chainhash.Hash{*winner.TxIDChainHash()}, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	otherBlock := chainhash.HashH([]byte("reset-wal-other-block"))
	_, otherWinner := createResetWALConflict(t, store, 2)
	_, _, err = utxo.ProcessConflicting(ctx, store, 1, otherBlock, []chainhash.Hash{*otherWinner.TxIDChainHash()}, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 2)

	store.scans.Store(0)

	response := stp.Reset(chain.genesis, nil, moveForward, true, func() error { return nil })
	require.NoError(t, response.Err)
	require.Equal(t, moveForward[1].Header.Hash(), stp.GetCurrentBlockHeader().Hash(), "reset must land on the last moveForward block")

	scans := store.scans.Load()

	pending, err = store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the fast-forward reset must complete the intent of its moveForward block")
	require.Equal(t, otherBlock, pending[0].BlockHash, "an intent for a block outside the reset must stay pending")

	require.Zero(t, scans, "a reset must not scan the pending intents")
}

// A reset over a block with a conflicting node runs ProcessConflicting with deferred completion. The
// reset must still complete that block's WAL intent after the block is applied.
func TestReset_ConflictingNodeCompletesWALIntent(t *testing.T) {
	ctx := t.Context()
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, true)

	loser, winner := createResetWALConflict(t, store, 9)

	moveForward := chain.moveForward
	moveForward[0].Subtrees = []*chainhash.Hash{storeConflictSubtree(t, subtreeStore, winner)}

	response := stp.Reset(chain.genesis, nil, moveForward, false, func() error { return nil })
	require.NoError(t, response.Err)

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending, "the reset must complete the WAL intent of the conflicting block")

	spend, err := utxo.GetSpends(winner)
	require.NoError(t, err)

	owner, err := store.GetSpend(ctx, spend[0])
	require.NoError(t, err)
	require.Equal(t, winner.TxIDChainHash(), owner.SpendingData.TxID)

	m, err := store.Get(ctx, loser.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, m.Conflicting)
}

// storeConflictSubtree stores a subtree that lists winner as a conflicting node and returns its hash.
func storeConflictSubtree(t *testing.T, subtreeStore *blob_memory.Memory, winner *bt.Tx) *chainhash.Hash {
	t.Helper()

	return storeTestSubtree(t, subtreeStore, true, winner)
}

// storeTestSubtree stores a subtree with the given txs after the coinbase, with its meta, and returns
// its hash. With conflicting set, every tx is also a conflicting node.
func storeTestSubtree(t *testing.T, subtreeStore *blob_memory.Memory, conflicting bool, txs ...*bt.Tx) *chainhash.Hash {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, tx := range txs {
		require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 1, 1))

		if conflicting {
			require.NoError(t, st.AddConflictingNode(*tx.TxIDChainHash()))
		}
	}

	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	// A move back reads the subtree meta too.
	meta := subtreepkg.NewSubtreeMeta(st)
	for i := range st.Nodes {
		require.NoError(t, meta.SetTxInpoints(i, subtreepkg.NewTxInpoints()))
	}

	metaBytes, err := meta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeMeta, metaBytes))

	return st.RootHash()
}

// A failed WAL acknowledgement after moveForwardBlock's commit point must not fail the call. The
// call's swap-back would install the cleared shadow over the applied block. A reset would acknowledge
// nothing, because block assembly is already at the block, so the intent stays for startup recovery.
func TestMoveForwardBlock_AckFailureAfterCommitLeavesIntent(t *testing.T) {
	ctx := t.Context()
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)

	loser, winner := createResetWALConflict(t, store, 8)

	block := chain.moveForward[0]
	block.Subtrees = []*chainhash.Hash{storeConflictSubtree(t, subtreeStore, winner)}

	store.failComplete.Store(true)

	_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, true, false)
	require.NoError(t, err, "the block is applied; a failed acknowledgement must not fail the call")
	require.False(t, stp.TakeDrainResetRequested(), "a reset cannot acknowledge the intent, so none is requested")

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the unacknowledged intent stays pending")

	m, err := store.Get(ctx, loser.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, m.Conflicting)
}

// The deferred drain acknowledges the block's forward intent because the block has conflicting
// nodes, even when the operation demoted no transaction.
func TestRunDeferredBlockDrain_AcknowledgesWithoutDemotedTxs(t *testing.T) {
	ctx := t.Context()
	stp, store, _, chain := newResetWALTestProcessor(t, false)

	_, winner := createResetWALConflict(t, store, 7)

	block := chain.moveForward[0]

	_, _, err := utxo.ProcessConflicting(ctx, store, 1, *block.Hash(), []chainhash.Hash{*winner.TxIDChainHash()}, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	stp.runDeferredBlockDrain(ctx, block, &deferredBlockDrain{ackConflictIntents: true})

	require.False(t, stp.TakeDrainResetRequested())

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending, "the drain must acknowledge the intent of a block with conflicting nodes")
}

// The deferred drain has the same policy as moveForwardBlock: a failed acknowledgement after the
// commit is only reported, and the intent stays pending.
func TestRunDeferredBlockDrain_AckFailureLeavesIntent(t *testing.T) {
	ctx := t.Context()
	stp, store, _, chain := newResetWALTestProcessor(t, false)

	_, winner := createResetWALConflict(t, store, 9)

	block := chain.moveForward[0]

	_, _, err := utxo.ProcessConflicting(ctx, store, 1, *block.Hash(), []chainhash.Hash{*winner.TxIDChainHash()}, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	store.failComplete.Store(true)

	stp.runDeferredBlockDrain(ctx, block, &deferredBlockDrain{ackConflictIntents: true})

	require.False(t, stp.TakeDrainResetRequested(), "a reset cannot acknowledge the intent, so none is requested")

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the unacknowledged intent stays pending")
}

// A reset has no rollback, so a failed WAL acknowledgement after it committed is only reported. The
// reset succeeds and the intent stays pending for startup recovery.
func TestReset_AckFailureAfterCommitDoesNotFailReset(t *testing.T) {
	ctx := t.Context()
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, true)

	_, winner := createResetWALConflict(t, store, 6)

	moveForward := chain.moveForward
	moveForward[0].Subtrees = []*chainhash.Hash{storeConflictSubtree(t, subtreeStore, winner)}

	store.failComplete.Store(true)

	response := stp.Reset(chain.genesis, nil, moveForward, false, func() error { return nil })
	require.NoError(t, response.Err, "the reset committed; a failed acknowledgement must not fail it")
	require.Equal(t, moveForward[1].Header.Hash(), stp.GetCurrentBlockHeader().Hash())

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the unacknowledged intent stays pending")
}

// A reorg that moves back a block with conflicting nodes reverses its forward operation and retires
// the forward checklist without a WAL scan. The reverse has committed, so a failed retirement is only
// logged: the reorg succeeds, and a re-add of the block or the next startup retires the checklist.
func TestReorgBlocks_RetiresReversedForwardChecklist(t *testing.T) {
	for _, failRetire := range []bool{false, true} {
		t.Run(map[bool]string{false: "retired", true: "retire fails"}[failRetire], func(t *testing.T) {
			ctx := t.Context()
			stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)

			loser, winner := createResetWALConflict(t, store, 20)

			block := chain.moveForward[0]
			block.Subtrees = []*chainhash.Hash{storeConflictSubtree(t, subtreeStore, winner)}

			// Leave the forward checklist pending, as a failed acknowledgement does.
			store.failComplete.Store(true)
			_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, true, false)
			require.NoError(t, err)
			store.failComplete.Store(false)

			forward := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: block.Height, BlockHash: *block.Hash(), TxHashes: []chainhash.Hash{*winner.TxIDChainHash()}}
			r, err := store.LoadConflictRecovery(ctx, forward.IntentID())
			require.NoError(t, err)
			require.Equal(t, utxo.ConflictRecoveryFinished, r.Phase)

			side := &model.Block{Height: 1, CoinbaseTx: coinbaseTx2, TransactionCount: 1, Subtrees: []*chainhash.Hash{}, Header: childHeader(chain.genesis, 401)}
			require.NoError(t, stp.blockchainClient.AddBlock(ctx, side, "", blockchainoptions.WithMinedSet(true)))

			store.scans.Store(0)
			store.failLoad.Store(failRetire)

			require.NoError(t, stp.reorgBlocks(ctx, []*model.Block{block}, []*model.Block{side}), "the reverse committed, so the reorg must succeed")
			require.Zero(t, store.scans.Load(), "the reorg retires the reversed blocks without a scan")

			m, err := store.Get(ctx, winner.TxIDChainHash(), fields.Conflicting)
			require.NoError(t, err)
			require.True(t, m.Conflicting, "the reverse demotes the winner")
			m, err = store.Get(ctx, loser.TxIDChainHash(), fields.Conflicting)
			require.NoError(t, err)
			require.False(t, m.Conflicting, "the reverse restores the loser")

			store.failLoad.Store(false)

			r, err = store.LoadConflictRecovery(ctx, forward.IntentID())
			require.NoError(t, err)
			if failRetire {
				require.NotNil(t, r, "a failed retirement leaves the checklist for later")
				return
			}
			require.Nil(t, r, "the reorg retires the forward checklist of the reversed block")
		})
	}
}

// A partial re-mine, end to end. Block B applied two winners, W1 and W2. A reorg replaced B with a side
// block that mines W1 only. Startup heals the stale forward intent of B: the current chain confirms
// W1, so it keeps the applied state and retires the intent. Block assembly's catch-up reorg then moves
// back B. W1 is re-mined in the side block, so the reverse skips it and undoes only W2.
func TestReorgBlocks_PartialRemineAfterAcceptedHeal(t *testing.T) {
	ctx := t.Context()
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)

	loser1, winner1 := createResetWALConflict(t, store, 30)
	loser2, winner2 := createResetWALConflict(t, store, 31)

	block := chain.moveForward[0]
	block.Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, true, winner1, winner2)}

	// Leave the forward intent pending, as a crash before the acknowledgement does.
	store.failComplete.Store(true)
	_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, true, false)
	require.NoError(t, err)
	store.failComplete.Store(false)

	side := &model.Block{Height: 1, CoinbaseTx: coinbaseTx2, TransactionCount: 2, Header: childHeader(chain.genesis, 402),
		Subtrees: []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, winner1)}}
	require.NoError(t, stp.blockchainClient.AddBlock(ctx, side, "", blockchainoptions.WithMinedSet(true)))

	_, sideMeta, err := stp.blockchainClient.GetBlockHeader(ctx, side.Hash())
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{winner1.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: sideMeta.ID, BlockHeight: 1})
	require.NoError(t, err)

	forward := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: block.Height, BlockHash: *block.Hash(),
		TxHashes: []chainhash.Hash{*winner1.TxIDChainHash(), *winner2.TxIDChainHash()}}
	guard := utxo.NewCurrentChainGuard(func(_ context.Context, ids []uint32) (bool, error) {
		for _, id := range ids {
			if id == sideMeta.ID {
				return true, nil
			}
		}
		return false, nil
	})

	// Block assembly applied B, so its catch-up moves B back and the heal leaves W2 to that reverse.
	mayBeMarked, err := utxo.HealStaleForwardIntent(ctx, store, forward, guard, func(context.Context) (bool, error) { return true, nil })
	require.NoError(t, err)
	require.Empty(t, mayBeMarked)

	r, err := store.LoadConflictRecovery(ctx, forward.IntentID())
	require.NoError(t, err)
	require.Nil(t, r, "the heal accepts the partial re-mine and retires the intent")

	require.NoError(t, stp.reorgBlocks(ctx, []*model.Block{block}, []*model.Block{side}))

	owner := func(tx *bt.Tx) *chainhash.Hash {
		spends, err := utxo.GetSpends(tx)
		require.NoError(t, err)

		response, err := store.GetSpend(ctx, spends[0])
		require.NoError(t, err)

		return response.SpendingData.TxID
	}
	conflicting := func(tx *bt.Tx) bool {
		m, err := store.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)

		return m.Conflicting
	}

	require.Equal(t, winner1.TxIDChainHash(), owner(winner1), "W1 is re-mined, so it keeps its input")
	require.False(t, conflicting(winner1))
	require.True(t, conflicting(loser1))

	require.Equal(t, loser2.TxIDChainHash(), owner(winner2), "the reverse gives W2's input back to L2")
	require.True(t, conflicting(winner2), "the reverse demotes W2")
	require.False(t, conflicting(loser2))
}

// A reset does not reverse the blocks it moves back, but it retires their forward checklists, as
// reorgBlocks does. Otherwise a checklist left by a failed acknowledgement would make the next startup
// roll back an operation that has stayed live since. The applied state stays as it is.
func TestReset_RetiresMovedBackForwardChecklist(t *testing.T) {
	ctx := t.Context()
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)

	loser, winner := createResetWALConflict(t, store, 40)

	block := chain.moveForward[0]
	block.Subtrees = []*chainhash.Hash{storeConflictSubtree(t, subtreeStore, winner)}

	// Leave the forward checklist pending, as a failed acknowledgement does.
	store.failComplete.Store(true)
	_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, true, false)
	require.NoError(t, err)
	store.failComplete.Store(false)

	id := utxo.ForwardConflictIntentID(*block.Hash())
	r, err := store.LoadConflictRecovery(ctx, id)
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryFinished, r.Phase)

	side := &model.Block{Height: 1, CoinbaseTx: coinbaseTx2, TransactionCount: 1, Subtrees: []*chainhash.Hash{}, Header: childHeader(chain.genesis, 403)}
	require.NoError(t, stp.blockchainClient.AddBlock(ctx, side, "", blockchainoptions.WithMinedSet(true)))

	store.scans.Store(0)

	require.NoError(t, stp.reset(chain.genesis, []*model.Block{block}, []*model.Block{side}, false, func() error { return nil }))
	require.Zero(t, store.scans.Load(), "the reset retires the moved-back blocks without a scan")

	r, err = store.LoadConflictRecovery(ctx, id)
	require.NoError(t, err)
	require.Nil(t, r, "the reset retires the forward checklist of the moved-back block")

	spends, err := utxo.GetSpends(winner)
	require.NoError(t, err)
	owner, err := store.GetSpend(ctx, spends[0])
	require.NoError(t, err)
	require.Equal(t, winner.TxIDChainHash(), owner.SpendingData.TxID, "a reset leaves the applied state as it is")

	m, err := store.Get(ctx, loser.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, m.Conflicting)
}
