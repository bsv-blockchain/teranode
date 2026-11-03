package subtreeprocessor

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

func spendingTx(t *testing.T, parent *bt.Tx, vout uint32, satoshis uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From(parent.TxIDChainHash().String(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
	require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	return tx
}

// createCoinbaseChain stores the block's coinbase and a chain of two txs on it: first spends the
// coinbase, second spends first.
func createCoinbaseChain(t *testing.T, ctx context.Context, stp *SubtreeProcessor, block *model.Block) (first, second *bt.Tx) {
	t.Helper()

	require.NoError(t, stp.processCoinbaseUtxos(ctx, block))

	spendHeight := block.Height + uint32(stp.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // small chain constant

	first = spendingTx(t, block.CoinbaseTx, 0, block.CoinbaseTx.Outputs[0].Satoshis/2)
	_, _, err := stp.utxoStore.SpendAndCreate(ctx, first, spendHeight)
	require.NoError(t, err)

	second = spendingTx(t, first, 0, first.Outputs[0].Satoshis/2)
	_, _, err = stp.utxoStore.SpendAndCreate(ctx, second, spendHeight)
	require.NoError(t, err)

	return first, second
}

func requireRecreatedChain(t *testing.T, ctx context.Context, stp *SubtreeProcessor, first, second *bt.Tx) {
	t.Helper()

	for _, tx := range []*bt.Tx{first, second} {
		_, err := stp.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
		require.NoError(t, err, "tx %s must exist again after the block is moved forward", tx.TxIDChainHash())
	}

	spent, err := stp.utxoStore.GetSpend(ctx, spendOf(t, second))
	require.NoError(t, err)
	require.NotNil(t, spent.SpendingData, "the first tx's output is spent by the second again")
	require.Equal(t, second.TxIDChainHash().String(), spent.SpendingData.TxID.String())
}

func spendOf(t *testing.T, spender *bt.Tx) *utxostore.Spend {
	t.Helper()

	utxoHash, err := util.UTXOHashFromInput(spender.Inputs[0])
	require.NoError(t, err)

	return &utxostore.Spend{TxID: spender.Inputs[0].PreviousTxIDChainHash(), Vout: spender.Inputs[0].PreviousTxOutIndex, UTXOHash: utxoHash}
}

// A block that holds subtrees from another node is applied through the foreign-block path of
// moveForwardBlock. Moving it forward again after a move back must recreate the chain of txs that
// spent its coinbase, in parent-before-child order.
func TestMoveForwardBlockRecreatesDeletedChainForABlockWithForeignSubtrees(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	require.NotEmpty(t, block.Subtrees, "precondition: the block has subtrees, so the empty-block branch is not used")

	first, second := createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	for _, tx := range []*bt.Tx{first, second, block.CoinbaseTx} {
		_, err := stp.utxoStore.Get(ctx, tx.TxIDChainHash())
		require.ErrorIs(t, err, errors.ErrTxNotFound, "precondition: the move back deleted %s", tx.TxIDChainHash())
	}

	_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, false, true)
	require.NoError(t, err)

	_, err = stp.utxoStore.Get(ctx, block.CoinbaseTx.TxIDChainHash())
	require.NoError(t, err, "the coinbase is recreated by the move forward")

	requireRecreatedChain(t, ctx, stp, first, second)
}

// The own-block path of moveForwardBlock applies a block whose subtrees block assembly built itself.
func TestMoveForwardBlockRecreatesDeletedChainForAnOwnBlock(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	for i := 0; i < 40; i++ {
		n := subtreepkg.Node{Hash: chainhash.HashH([]byte{byte(i), 9}), Fee: 1, SizeInBytes: 100}
		require.NoError(t, stp.AddDirectly(&n, &subtreepkg.TxInpoints{}, true))
	}

	require.NotEmpty(t, stp.chainedSubtrees, "precondition: at least one chained subtree")

	block.Subtrees = []*chainhash.Hash{stp.chainedSubtrees[0].RootHash()}

	first, second := createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, false, true)
	require.NoError(t, err)

	requireRecreatedChain(t, ctx, stp, first, second)
}

// A saved tx whose input another tx now spends cannot be recreated, and neither can the saved txs
// that spend it. Every retry would fail the same way, so the move forward must carry on without them.
func TestMoveForwardBlockSkipsASavedTxWhoseInputIsNowSpent(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, second := createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	rival := spendingTx(t, block.CoinbaseTx, 0, first.Outputs[0].Satoshis-1)
	require.NoError(t, stp.processCoinbaseUtxos(ctx, block))

	spendHeight := block.Height + uint32(stp.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // small chain constant
	_, _, err := stp.utxoStore.SpendAndCreate(ctx, rival, spendHeight)
	require.NoError(t, err, "a rival already spends the coinbase output the saved tx needs")

	_, _, err = stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, false, true)
	require.NoError(t, err, "a saved tx that can never be recreated must not block the move forward")

	for _, tx := range []*bt.Tx{first, second} {
		_, getErr := stp.utxoStore.Get(ctx, tx.TxIDChainHash())
		require.ErrorIs(t, getErr, errors.ErrTxNotFound, "tx %s is skipped", tx.TxIDChainHash())
	}

	_, err = stp.utxoStore.Get(ctx, rival.TxIDChainHash())
	require.NoError(t, err, "the rival stays")

	spent, err := stp.utxoStore.GetSpend(ctx, spendOf(t, rival))
	require.NoError(t, err)
	require.NotNil(t, spent.SpendingData)
	require.Equal(t, rival.TxIDChainHash().String(), spent.SpendingData.TxID.String(), "the rival still owns the coinbase output")

	_, err = stp.subtreeStore.Get(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.ErrorIs(t, err, errors.ErrNotFound, "a retry would get the same result, so the saved file is deleted")
}

// Any failure other than a spent input still fails the move forward.
func TestMoveForwardBlockFailsWhenTheSavedFileIsUnreadable(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))
	require.NoError(t, stp.subtreeStore.Set(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs, []byte{0, 9, 9}, options.WithAllowOverwrite(true)))

	_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, false, true)
	require.Error(t, err, "saved txs that cannot be read must not be passed over")
}

func TestParentsFirstOrdersAChainWhateverTheInputOrder(t *testing.T) {
	root := bt.NewTx()
	require.NoError(t, root.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
	require.NoError(t, root.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 9000))

	a := spendingTx(t, root, 0, 8000)
	b := spendingTx(t, a, 0, 7000)
	c := spendingTx(t, b, 0, 6000)

	got := parentsFirst([]*reorgDeletedTx{{tx: c}, {tx: a}, {tx: b}})

	require.Equal(t, []string{a.TxIDChainHash().String(), b.TxIDChainHash().String(), c.TxIDChainHash().String()},
		[]string{got[0].tx.TxIDChainHash().String(), got[1].tx.TxIDChainHash().String(), got[2].tx.TxIDChainHash().String()})
}

// A reset that moves a block back and then forward again, whether through the per-block loop or
// the checkpoint-trusted fast-forward branch, must recreate the txs it deleted with the block's
// coinbase.
func TestResetRecreatesChainDeletedWhenTheSameBlockIsMovedBackAndForward(t *testing.T) {
	for _, fastForward := range []bool{false, true} {
		t.Run(fmt.Sprintf("fastForward=%v", fastForward), func(t *testing.T) {
			ctx := context.Background()
			stp, _ := newProgressTestProcessor(t, resetBlockchainMock())

			block := &model.Block{
				Height:     1,
				CoinbaseTx: coinbaseTx,
				Subtrees:   []*chainhash.Hash{},
				Header:     childHeader(prevBlockHeader, 401),
			}

			first, second := createCoinbaseChain(t, ctx, stp, block)

			stp.InitCurrentBlockHeader(block.Header)

			response := stp.Reset(block.Header, []*model.Block{block}, []*model.Block{block}, fastForward, func() error { return nil })
			require.NoError(t, response.Err)

			requireRecreatedChain(t, ctx, stp, first, second)
		})
	}
}

// A reset that moves a block back and forward again must not fail for a saved tx that another tx
// now conflicts with.
func TestResetSkipsASavedTxWhoseInputIsNowSpent(t *testing.T) {
	for _, fastForward := range []bool{false, true} {
		t.Run(fmt.Sprintf("fastForward=%v", fastForward), func(t *testing.T) {
			ctx := context.Background()
			stp, _ := newProgressTestProcessor(t, resetBlockchainMock())

			block := &model.Block{
				Height:     1,
				CoinbaseTx: coinbaseTx,
				Subtrees:   []*chainhash.Hash{},
				Header:     childHeader(prevBlockHeader, 402),
			}

			first, _ := createCoinbaseChain(t, ctx, stp, block)

			require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))
			require.NoError(t, stp.processCoinbaseUtxos(ctx, block))

			rival := spendingTx(t, block.CoinbaseTx, 0, first.Outputs[0].Satoshis-1)
			_, _, err := stp.utxoStore.SpendAndCreate(ctx, rival, block.Height+uint32(stp.settings.ChainCfgParams.CoinbaseMaturity)) //nolint:gosec // small chain constant
			require.NoError(t, err, "a rival already spends the coinbase output the saved tx needs")

			stp.InitCurrentBlockHeader(block.Header)

			response := stp.Reset(block.Header, nil, []*model.Block{block}, fastForward, func() error { return nil })
			require.NoError(t, response.Err)

			_, err = stp.utxoStore.Get(ctx, first.TxIDChainHash())
			require.ErrorIs(t, err, errors.ErrTxNotFound, "the saved tx is skipped")
		})
	}
}

// A reset that cannot read the saved txs must fail rather than report success with records missing.
func TestResetFailsWhenTheSavedFileIsUnreadable(t *testing.T) {
	for _, fastForward := range []bool{false, true} {
		t.Run(fmt.Sprintf("fastForward=%v", fastForward), func(t *testing.T) {
			ctx := context.Background()
			stp, _ := newProgressTestProcessor(t, resetBlockchainMock())

			block := &model.Block{
				Height:     1,
				CoinbaseTx: coinbaseTx,
				Subtrees:   []*chainhash.Hash{},
				Header:     childHeader(prevBlockHeader, 403),
			}
			blockHash := block.Header.Hash()

			createCoinbaseChain(t, ctx, stp, block)

			require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))
			require.NoError(t, stp.subtreeStore.Set(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs, []byte{0, 9, 9}, options.WithAllowOverwrite(true)))
			require.NoError(t, stp.processCoinbaseUtxos(ctx, block))

			stp.InitCurrentBlockHeader(block.Header)

			response := stp.Reset(block.Header, nil, []*model.Block{block}, fastForward, func() error { return nil })
			require.Error(t, response.Err)
		})
	}
}

func TestReorgDeletedTxsWithoutASubtreeStore(t *testing.T) {
	ctx := context.Background()
	f := newCoinbaseChildFixture(t, func(s blob.Store) blob.Store { return s })
	f.stp.subtreeStore = nil

	require.NoError(t, f.stp.restoreReorgDeletedTxs(ctx, f.block), "nothing could have been saved, so there is nothing to restore")

	require.Error(t, f.stp.removeCoinbaseUtxos(ctx, f.block), "children must not be deleted when there is nowhere to save them")

	_, err := f.utxoStore.Get(ctx, f.child.TxIDChainHash())
	require.NoError(t, err, "the child must survive")
}

func TestReorgDeletedTxsEncodingRoundTripsMinedState(t *testing.T) {
	root := bt.NewTx()
	require.NoError(t, root.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
	require.NoError(t, root.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 9000))

	mined := spendingTx(t, root, 0, 8000)
	unminedTx := spendingTx(t, mined, 0, 7000)
	neverMined := spendingTx(t, unminedTx, 0, 6000)

	entries := []*reorgDeletedTx{
		{tx: mined, mined: []utxostore.MinedBlockInfo{{BlockID: 7, BlockHeight: 101, SubtreeIdx: 2, OnLongestChain: true}, {BlockID: 9, BlockHeight: 103, SubtreeIdx: 0, OnLongestChain: true}}},
		{tx: unminedTx, unmined: true, mined: []utxostore.MinedBlockInfo{{BlockID: 8, BlockHeight: 102, SubtreeIdx: 1}}},
		{tx: neverMined, unmined: true},
	}

	got, err := decodeReorgDeletedTxs(encodeReorgDeletedTxs(entries))
	require.NoError(t, err)
	require.Len(t, got, len(entries))

	for i := range entries {
		require.Equal(t, entries[i].tx.TxIDChainHash().String(), got[i].tx.TxIDChainHash().String())
		require.Equal(t, entries[i].unmined, got[i].unmined)

		if len(entries[i].mined) == 0 {
			require.Empty(t, got[i].mined)
		} else {
			require.Equal(t, entries[i].mined, got[i].mined)
		}
	}
}

func TestDecodeReorgDeletedTxsRejectsTruncatedData(t *testing.T) {
	_, err := decodeReorgDeletedTxs([]byte{0, 1, 0, 0, 0})
	require.Error(t, err, "an entry that claims a block info the data does not hold is truncated")

	_, err = decodeReorgDeletedTxs([]byte{0, 1})
	require.Error(t, err, "a header cut short is truncated")
}

// A tx that was mined when it was deleted must come back with its block IDs and without unmined_since,
// or block assembly would take it for an unmined tx and mine it again.
func TestRestoreReorgDeletedTxsGivesBackTheMinedState(t *testing.T) {
	for _, offChain := range []bool{false, true} {
		t.Run(fmt.Sprintf("offLongestChain=%v", offChain), func(t *testing.T) {
			ctx := context.Background()
			stp, block := newMoveForwardCommitBoundaryProcessor(t)

			require.NoError(t, stp.processCoinbaseUtxos(ctx, block))

			spendHeight := block.Height + uint32(stp.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // small chain constant
			child := spendingTx(t, block.CoinbaseTx, 0, block.CoinbaseTx.Outputs[0].Satoshis/2)

			_, _, err := stp.utxoStore.SpendAndCreate(ctx, child, spendHeight, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 41, BlockHeight: 1234, SubtreeIdx: 3, OnLongestChain: true}))
			require.NoError(t, err)

			if offChain {
				require.NoError(t, stp.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*child.TxIDChainHash()}, false))
			}

			before, err := stp.utxoStore.Get(ctx, child.TxIDChainHash(), fields.UnminedSince, fields.BlockIDs)
			require.NoError(t, err)
			require.Equal(t, []uint32{41}, before.BlockIDs)
			require.Equal(t, offChain, before.UnminedSince != 0, "precondition: the tx has the chain state under test")

			require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))
			require.NoError(t, stp.processCoinbaseUtxos(ctx, block))
			require.NoError(t, stp.restoreReorgDeletedTxs(ctx, block))

			after, err := stp.utxoStore.Get(ctx, child.TxIDChainHash(), fields.UnminedSince, fields.BlockIDs, fields.BlockHeights, fields.SubtreeIdxs)
			require.NoError(t, err)
			require.Equal(t, []uint32{41}, after.BlockIDs, "the block IDs must come back")
			require.Equal(t, []uint32{1234}, after.BlockHeights)
			require.Equal(t, []int{3}, after.SubtreeIdxs)
			require.Equal(t, offChain, after.UnminedSince != 0, "the tx must be unmined only if it was unmined when deleted")
		})
	}
}

// A tx that spends outputs descended from the coinbases of two different blocks is deleted with the
// block moved back first, so it is saved in that block's file. Moving the blocks forward in order
// must recreate both parents before it.
func TestMoveForwardBlocksRecreateATxThatSpendsDescendantsOfTwoCoinbases(t *testing.T) {
	ctx := context.Background()
	stp, earlier := newMoveForwardCommitBoundaryProcessor(t)

	later := &model.Block{
		Height:     earlier.Height + 1,
		CoinbaseTx: coinbaseTx2,
		Subtrees:   []*chainhash.Hash{},
		Header:     childHeader(earlier.Header, 777),
	}

	require.NoError(t, stp.processCoinbaseUtxos(ctx, earlier))
	require.NoError(t, stp.processCoinbaseUtxos(ctx, later))

	spendHeight := later.Height + uint32(stp.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // small chain constant

	parentOfEarlier := spendingTx(t, earlier.CoinbaseTx, 0, earlier.CoinbaseTx.Outputs[0].Satoshis/2)
	_, _, err := stp.utxoStore.SpendAndCreate(ctx, parentOfEarlier, spendHeight)
	require.NoError(t, err)

	parentOfLater := spendingTx(t, later.CoinbaseTx, 0, later.CoinbaseTx.Outputs[0].Satoshis/2)
	_, _, err = stp.utxoStore.SpendAndCreate(ctx, parentOfLater, spendHeight)
	require.NoError(t, err)

	both := bt.NewTx()
	require.NoError(t, both.From(parentOfEarlier.TxIDChainHash().String(), 0, parentOfEarlier.Outputs[0].LockingScript.String(), parentOfEarlier.Outputs[0].Satoshis))
	require.NoError(t, both.From(parentOfLater.TxIDChainHash().String(), 0, parentOfLater.Outputs[0].LockingScript.String(), parentOfLater.Outputs[0].Satoshis))
	require.NoError(t, both.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parentOfEarlier.Outputs[0].Satoshis))

	for _, in := range both.Inputs {
		in.UnlockingScript = bscript.NewFromBytes([]byte{})
	}

	_, _, err = stp.utxoStore.SpendAndCreate(ctx, both, spendHeight)
	require.NoError(t, err)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, later))
	require.NoError(t, stp.removeCoinbaseUtxos(ctx, earlier))

	for name, tx := range map[string]*bt.Tx{"parentOfEarlier": parentOfEarlier, "parentOfLater": parentOfLater, "both": both} {
		_, getErr := stp.utxoStore.Get(ctx, tx.TxIDChainHash())
		require.ErrorIs(t, getErr, errors.ErrTxNotFound, "precondition: %s %s was deleted", name, tx.TxIDChainHash())
	}

	for _, block := range []*model.Block{earlier, later} {
		_, _, err = stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, false, true)
		require.NoError(t, err)

		// reorgBlocks advances the current header after each block; an empty block does not do it itself
		stp.currentBlockHeader.Store(block.Header)
	}

	for _, tx := range []*bt.Tx{parentOfEarlier, parentOfLater, both} {
		_, getErr := stp.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
		require.NoError(t, getErr, "tx %s must exist again", tx.TxIDChainHash())
	}

	for _, in := range both.Inputs {
		utxoHash, hashErr := util.UTXOHashFromInput(in)
		require.NoError(t, hashErr)

		spent, spendErr := stp.utxoStore.GetSpend(ctx, &utxostore.Spend{TxID: in.PreviousTxIDChainHash(), Vout: in.PreviousTxOutIndex, UTXOHash: utxoHash})
		require.NoError(t, spendErr)
		require.NotNil(t, spent.SpendingData)
		require.Equal(t, both.TxIDChainHash().String(), spent.SpendingData.TxID.String())
	}
}

func TestMoveForwardBlockDeletesTheSavedFileAfterARestore(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, second := createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	_, err := stp.subtreeStore.Get(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err, "precondition: the move back saved the txs")

	_, _, err = stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, false, true)
	require.NoError(t, err)

	requireRecreatedChain(t, ctx, stp, first, second)

	_, err = stp.subtreeStore.Get(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.ErrorIs(t, err, errors.ErrNotFound, "a restored block must not leave its saved file behind")
}

// A tx restored once and then changed in the store must be saved with its current state the next
// time the block is moved back, not with the state of the earlier save.
func TestKeepReorgDeletedTxsSavesTheCurrentStateOfARestoredTx(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, second := createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))
	require.NoError(t, stp.processCoinbaseUtxos(ctx, block))
	require.NoError(t, stp.restoreReorgDeletedTxs(ctx, block))

	requireRecreatedChain(t, ctx, stp, first, second)

	_, err := stp.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{first.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: 42, BlockHeight: 1234, SubtreeIdx: 0, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	saved, err := stp.readReorgDeletedTxs(ctx, blockHash)
	require.NoError(t, err)

	var entry *reorgDeletedTx

	for _, e := range saved {
		if e.tx.TxIDChainHash().IsEqual(first.TxIDChainHash()) {
			entry = e
		}
	}

	require.NotNil(t, entry, "the first tx is saved again")
	require.Len(t, entry.mined, 1)
	require.Equal(t, uint32(42), entry.mined[0].BlockID)
	require.False(t, entry.unmined)
}

// A saved tx that is absent from the store keeps its saved entry when the move back is retried.
func TestKeepReorgDeletedTxsKeepsTheSavedEntryOfATxNoLongerInTheStore(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, second := createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	hashes := []chainhash.Hash{*first.TxIDChainHash(), *second.TxIDChainHash()}
	require.NoError(t, stp.keepReorgDeletedTxs(ctx, block, hashes))

	saved, err := stp.readReorgDeletedTxs(ctx, blockHash)
	require.NoError(t, err)
	require.Len(t, saved, 2, "the saved entries of txs absent from the store survive a retry")
}
