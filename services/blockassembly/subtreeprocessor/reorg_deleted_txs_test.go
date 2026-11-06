package subtreeprocessor

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
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

	_, restoreErr := f.stp.restoreReorgDeletedTxs(ctx, f.block)
	require.NoError(t, restoreErr, "nothing could have been saved, so there is nothing to restore")

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

			stp.blockchainClient.(*blockchain.Mock).On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(!offChain, nil)

			require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))
			require.NoError(t, stp.processCoinbaseUtxos(ctx, block))
			_, restoreErr := stp.restoreReorgDeletedTxs(ctx, block)
			require.NoError(t, restoreErr)

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
	_, restoreErr := stp.restoreReorgDeletedTxs(ctx, block)
	require.NoError(t, restoreErr)

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
	_, err := stp.keepReorgDeletedTxs(ctx, block, hashes)
	require.NoError(t, err)

	saved, err := stp.readReorgDeletedTxs(ctx, blockHash)
	require.NoError(t, err)
	require.Len(t, saved, 2, "the saved entries of txs absent from the store survive a retry")
}

// A saved tx whose parent was skipped when an earlier block was restored has a missing input. It is
// skipped like a spent input, or every later restore of its block would fail the same way.
func TestRestoreReorgDeletedTxsSkipsASavedTxWhoseParentIsMissing(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	first, second := createCoinbaseChain(t, ctx, stp, block)

	block2 := &model.Block{
		Height:     block.Height,
		CoinbaseTx: block.CoinbaseTx,
		Subtrees:   []*chainhash.Hash{},
		Header:     childHeader(block.Header, 901),
	}
	block2Hash := block2.Header.Hash()
	require.NotEqual(t, block.Header.Hash().String(), block2Hash.String())

	extended := func(tx *bt.Tx) *bt.Tx {
		meta, err := stp.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
		require.NoError(t, err)
		require.NoError(t, stp.utxoStore.PreviousOutputsDecorate(ctx, meta.Tx))

		return meta.Tx
	}

	file := encodeReorgDeletedTxs([]*reorgDeletedTx{{tx: extended(second), unmined: true}})
	require.NoError(t, stp.subtreeStore.Set(ctx, block2Hash[:], fileformat.FileTypeReorgDeletedTxs, file))

	require.NoError(t, utxostore.DeleteWithParents(ctx, stp.utxoStore, second.TxIDChainHash()))
	require.NoError(t, utxostore.DeleteWithParents(ctx, stp.utxoStore, first.TxIDChainHash()))

	_, restoreErr := stp.restoreReorgDeletedTxs(ctx, block2)
	require.NoError(t, restoreErr, "a saved tx whose parent is gone must be skipped, not fail the restore")

	_, err := stp.utxoStore.Get(ctx, second.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the tx is skipped")

	_, err = stp.subtreeStore.Get(ctx, block2Hash[:], fileformat.FileTypeReorgDeletedTxs)
	require.ErrorIs(t, err, errors.ErrNotFound, "a retry would get the same result, so the saved file is deleted")
}

// The chain state of a restored tx comes from the chain as it is now, not from the state the tx had
// when it was deleted: a block that has left the chain no longer mines the tx.
func TestRestoreReorgDeletedTxsRecalculatesTheChainState(t *testing.T) {
	for _, blockOnChain := range []bool{false, true} {
		t.Run(fmt.Sprintf("blockOnChain=%v", blockOnChain), func(t *testing.T) {
			ctx := context.Background()
			stp, block := newMoveForwardCommitBoundaryProcessor(t)

			first, _ := createCoinbaseChain(t, ctx, stp, block)

			_, err := stp.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{first.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: 42, BlockHeight: 1234, SubtreeIdx: 0, OnLongestChain: true})
			require.NoError(t, err)

			before, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.UnminedSince)
			require.NoError(t, err)
			require.Zero(t, before.UnminedSince, "precondition: the tx was saved as mined on the longest chain")

			require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

			stp.blockchainClient.(*blockchain.Mock).On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{42}).Return(blockOnChain, nil)

			require.NoError(t, stp.processCoinbaseUtxos(ctx, block))
			_, restoreErr := stp.restoreReorgDeletedTxs(ctx, block)
			require.NoError(t, restoreErr)

			after, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.UnminedSince, fields.BlockIDs)
			require.NoError(t, err)
			require.Equal(t, []uint32{42}, after.BlockIDs, "the block ID stays")

			if blockOnChain {
				require.Zero(t, after.UnminedSince, "a tx in a block on the chain is on the longest chain")
			} else {
				require.NotZero(t, after.UnminedSince, "a tx in a block off the chain must be offered to block assembly again")
			}
		})
	}
}

// removeCoinbaseUtxos locks the coinbase and its descendants. When the txs cannot be saved it stops
// before it deletes anything, so it must not leave them locked.
func TestRemoveCoinbaseUtxosUnlocksTheTxsWhenTheSaveFails(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, second := createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.subtreeStore.Set(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs, []byte{0, 9, 9}, options.WithAllowOverwrite(true)))

	require.Error(t, stp.removeCoinbaseUtxos(ctx, block), "the saved file is unreadable, so the save fails")

	for name, tx := range map[string]*bt.Tx{"coinbase": block.CoinbaseTx, "first": first, "second": second} {
		txMeta, err := stp.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Locked)
		require.NoError(t, err, "%s was not deleted", name)
		require.False(t, txMeta.Locked, "%s must not stay locked", name)
	}
}

type deleteFailsOnce struct {
	utxostore.Store
	target chainhash.Hash
	failed bool
}

func (s *deleteFailsOnce) Delete(ctx context.Context, hash *chainhash.Hash) error {
	if !s.failed && *hash == s.target {
		s.failed = true

		return errors.NewStorageError("injected")
	}

	return s.Store.Delete(ctx, hash)
}

// DeleteWithParents clears the parent references of a tx before it deletes the tx. If the delete
// fails, the coinbase output no longer names the tx, so a retry does not walk to it. The saved file
// still lists it, and the retry must delete what the file lists.
func TestRemoveCoinbaseUtxosRetryDeletesATxWhoseParentRefsWereCleared(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	first, second := createCoinbaseChain(t, ctx, stp, block)

	stp.utxoStore = &deleteFailsOnce{Store: stp.utxoStore, target: *first.TxIDChainHash()}

	require.Error(t, stp.removeCoinbaseUtxos(ctx, block), "the first attempt fails on the delete of the first tx")
	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block), "the retry must complete")

	for name, tx := range map[string]*bt.Tx{"first": first, "second": second, "coinbase": block.CoinbaseTx} {
		_, err := stp.utxoStore.Get(ctx, tx.TxIDChainHash())
		require.ErrorIs(t, err, errors.ErrTxNotFound, "%s must be deleted by the retry", name)
	}
}

// A retry that finds an old saved file and a tx still in the store saves the live state of the tx,
// not the state in the old file.
func TestKeepReorgDeletedTxsPrefersTheLiveStateOverAnOldSavedFile(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, second := createCoinbaseChain(t, ctx, stp, block)

	_, err := stp.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{first.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: 42, BlockHeight: 1234, SubtreeIdx: 0, OnLongestChain: true})
	require.NoError(t, err)

	extended := func(tx *bt.Tx) *bt.Tx {
		txMeta, getErr := stp.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
		require.NoError(t, getErr)
		require.NoError(t, stp.utxoStore.PreviousOutputsDecorate(ctx, txMeta.Tx))

		return txMeta.Tx
	}

	stale := encodeReorgDeletedTxs([]*reorgDeletedTx{
		{tx: extended(first), mined: []utxostore.MinedBlockInfo{{BlockID: 7, BlockHeight: 99, SubtreeIdx: 1, OnLongestChain: true}}},
		{tx: extended(second), unmined: true},
	})
	require.NoError(t, stp.subtreeStore.Set(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs, stale, options.WithAllowOverwrite(true)))

	_, err = stp.keepReorgDeletedTxs(ctx, block, []chainhash.Hash{*first.TxIDChainHash(), *second.TxIDChainHash()})
	require.NoError(t, err)

	saved, err := stp.readReorgDeletedTxs(ctx, blockHash)
	require.NoError(t, err)
	require.Len(t, saved, 2)

	for _, entry := range saved {
		if entry.tx.TxIDChainHash().IsEqual(first.TxIDChainHash()) {
			require.Len(t, entry.mined, 1)
			require.Equal(t, uint32(42), entry.mined[0].BlockID, "the live state wins over the old file")
			require.Equal(t, uint32(1234), entry.mined[0].BlockHeight)
		}
	}
}

func addToAssembly(t *testing.T, stp *SubtreeProcessor, tx *bt.Tx) {
	t.Helper()

	inpoints, err := subtreepkg.NewTxInpointsFromTx(tx)
	require.NoError(t, err)

	node := &subtreepkg.Node{Hash: *tx.TxIDChainHash(), Fee: tx.TotalInputSatoshis() - tx.TotalOutputSatoshis(), SizeInBytes: uint64(tx.Size())} //nolint:gosec // size is non-negative
	require.NoError(t, stp.AddDirectly(node, &inpoints, true))
}

// A restored tx that is not on the longest chain is in the store again, and block assembly must hold
// it again too: removeCoinbaseUtxos took it out of the assembly and left its hash in the remove map.
func TestMoveForwardBlockPutsARestoredUnminedTxBackInBlockAssembly(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	first, _ := createCoinbaseChain(t, ctx, stp, block)
	addToAssembly(t, stp, first)

	require.True(t, stp.currentTxMap.Exists(*first.TxIDChainHash()), "precondition: the tx is in block assembly")

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	require.False(t, stp.currentTxMap.Exists(*first.TxIDChainHash()), "precondition: the move back took the tx out of block assembly")
	require.True(t, stp.removeMap.Exists(*first.TxIDChainHash()), "precondition: the move back marked the tx for removal")

	_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, false, true)
	require.NoError(t, err)

	_, err = stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.Tx)
	require.NoError(t, err, "the tx is recreated")

	require.True(t, stp.currentTxMap.Exists(*first.TxIDChainHash()), "the recreated tx is back in block assembly")
	require.False(t, stp.removeMap.Exists(*first.TxIDChainHash()), "the tx is no longer marked for removal")
}

// A reset reloads the unmined txs from the store after it applies the blocks, so a restored tx must not
// also be added to block assembly by the apply itself.
func TestApplyCoinbaseUtxosWithoutRequeueLeavesRestoredTxsOutOfBlockAssembly(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	first, _ := createCoinbaseChain(t, ctx, stp, block)
	addToAssembly(t, stp, first)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	require.NoError(t, stp.applyCoinbaseUtxos(ctx, block, false, true))

	_, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.Tx)
	require.NoError(t, err, "the tx is recreated")

	require.False(t, stp.currentTxMap.Exists(*first.TxIDChainHash()), "the reset reloads it from the store, so the apply must not add it")
}

type delFailsOnceBlobStore struct {
	blob.Store
	failed bool
}

func (s *delFailsOnceBlobStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	if !s.failed && fileType == fileformat.FileTypeReorgDeletedTxs {
		s.failed = true

		return errors.NewStorageError("injected")
	}

	return s.Store.Del(ctx, key, fileType, opts...)
}

// When the saved file cannot be deleted after the txs were recreated, the retry finds every tx
// already in the store. It must still hand the off-chain txs back to block assembly.
func TestApplyCoinbaseUtxosRetryRequeuesATxRecreatedByAFailedAttempt(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, _ := createCoinbaseChain(t, ctx, stp, block)
	addToAssembly(t, stp, first)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	require.False(t, stp.currentTxMap.Exists(*first.TxIDChainHash()), "precondition: the move back took the tx out of block assembly")
	require.True(t, stp.removeMap.Exists(*first.TxIDChainHash()), "precondition: the move back marked the tx for removal")

	realStore := stp.subtreeStore
	stp.subtreeStore = &delFailsOnceBlobStore{Store: realStore}

	require.Error(t, stp.applyCoinbaseUtxos(ctx, block, true, true), "the first attempt fails on the delete of the saved file")

	_, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.Tx)
	require.NoError(t, err, "precondition: the first attempt recreated the tx")

	stp.subtreeStore = realStore

	require.NoError(t, stp.applyCoinbaseUtxos(ctx, block, true, true), "the retry must complete")

	require.True(t, stp.currentTxMap.Exists(*first.TxIDChainHash()), "the retry puts the tx back in block assembly")
	require.False(t, stp.removeMap.Exists(*first.TxIDChainHash()), "the tx is no longer marked for removal")

	exists, err := stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err)
	require.False(t, exists, "the saved file is deleted by the retry")
}

func containsTx(txs []*bt.Tx, want *bt.Tx) bool {
	for _, tx := range txs {
		if tx.TxIDChainHash().IsEqual(want.TxIDChainHash()) {
			return true
		}
	}

	return false
}

func recreateWithFailedSavedFileDelete(t *testing.T, ctx context.Context, stp *SubtreeProcessor, block *model.Block, first *bt.Tx, oldBlockID uint32) {
	t.Helper()

	_, err := stp.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{first.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: oldBlockID, BlockHeight: 1234, SubtreeIdx: 0, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))
	require.NoError(t, stp.processCoinbaseUtxos(ctx, block))

	chainMock := stp.blockchainClient.(*blockchain.Mock)
	chainMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{oldBlockID}).Return(false, nil)

	realStore := stp.subtreeStore
	stp.subtreeStore = &delFailsOnceBlobStore{Store: realStore}

	_, restoreErr := stp.restoreReorgDeletedTxs(ctx, block)
	require.Error(t, restoreErr, "the first attempt fails on the delete of the saved file")

	stp.subtreeStore = realStore

	_, err = stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.Tx)
	require.NoError(t, err, "precondition: the first attempt recreated the tx")
}

// A retry after the saved file could not be deleted finds the tx in the store. Block validation may
// have mined it in a current-chain block since, so the live record decides, not the saved block IDs.
func TestRestoreReorgDeletedTxsLeavesAnExistingTxMinedOnTheCurrentChainAlone(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, _ := createCoinbaseChain(t, ctx, stp, block)

	recreateWithFailedSavedFileDelete(t, ctx, stp, block, first, 42)

	_, err := stp.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{first.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: 99, BlockHeight: 1300, SubtreeIdx: 0, OnLongestChain: true})
	require.NoError(t, err)

	live, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.Contains(t, live.BlockIDs, uint32(99), "precondition: the live record is mined in block 99")

	stp.blockchainClient.(*blockchain.Mock).On("CheckBlockIsInCurrentChain", mock.Anything, live.BlockIDs).Return(true, nil)

	unminedBefore := live.UnminedSince

	unmined, err := stp.restoreReorgDeletedTxs(ctx, block)
	require.NoError(t, err)
	require.False(t, containsTx(unmined, first), "a tx mined on the current chain is not put back in block assembly")

	after, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, live.BlockIDs, after.BlockIDs)
	require.Equal(t, unminedBefore, after.UnminedSince, "the retry must not change the chain state of the live record")

	exists, err := stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err)
	require.False(t, exists, "the saved file is deleted by the retry")
}

func TestRestoreReorgDeletedTxsRequeuesAnExistingTxWhoseLiveBlocksLeftTheChain(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, _ := createCoinbaseChain(t, ctx, stp, block)

	recreateWithFailedSavedFileDelete(t, ctx, stp, block, first, 42)

	live, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, live.UnminedSince, "precondition: the live record already carries the unmined marker")

	unmined, err := stp.restoreReorgDeletedTxs(ctx, block)
	require.NoError(t, err)
	require.True(t, containsTx(unmined, first), "a tx whose blocks left the chain goes back in block assembly")

	after, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, live.BlockIDs, after.BlockIDs)
	require.Equal(t, live.UnminedSince, after.UnminedSince, "the retry must not change the chain state of the live record")

	exists, err := stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err)
	require.False(t, exists, "the saved file is deleted by the retry")
}

func clearUnminedMarker(t *testing.T, ctx context.Context, stp *SubtreeProcessor, tx *bt.Tx) {
	t.Helper()

	require.NoError(t, stp.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true))

	live, err := stp.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.Zero(t, live.UnminedSince, "precondition: the record looks mined")
	require.NotEmpty(t, live.BlockIDs, "precondition: the record carries block IDs")
}

// An earlier attempt creates the tx with block IDs and fails before it marks the tx as not on the
// longest chain. The retry finds a record that looks mined and must repair it before the saved file
// is deleted, whether or not the caller requeues.
func TestRestoreReorgDeletedTxsRepairsAnExistingTxLeftMinedByAFailedAttempt(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, _ := createCoinbaseChain(t, ctx, stp, block)

	recreateWithFailedSavedFileDelete(t, ctx, stp, block, first, 42)
	clearUnminedMarker(t, ctx, stp, first)

	unmined, err := stp.restoreReorgDeletedTxs(ctx, block)
	require.NoError(t, err)
	require.True(t, containsTx(unmined, first), "the repaired tx goes back in block assembly")

	after, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, after.UnminedSince, "the retry marks the tx as not on the longest chain")
	require.Equal(t, []uint32{42}, after.BlockIDs)

	exists, err := stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err)
	require.False(t, exists, "the saved file is deleted by the retry")
}

func answerChainChecksByBlockID(stp *SubtreeProcessor, currentChainBlockID uint32) {
	chainMock := stp.blockchainClient.(*blockchain.Mock)
	chainMock.ExpectedCalls = nil

	chainMock.On("CheckBlockIsInCurrentChain", mock.Anything, mock.MatchedBy(func(blockIDs []uint32) bool {
		for _, id := range blockIDs {
			if id == currentChainBlockID {
				return true
			}
		}

		return false
	})).Return(true, nil)
	chainMock.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(false, nil)
}

func mineOnCurrentChainBeforeTheMark(t *testing.T, ctx context.Context, store utxostore.Store, tx *bt.Tx, blockID uint32) func(onLongestChain bool) {
	t.Helper()

	return func(onLongestChain bool) {
		if onLongestChain {
			return
		}

		_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: blockID, BlockHeight: 1300, SubtreeIdx: 0, OnLongestChain: true})
		require.NoError(t, err)
	}
}

// Block validation mines the tx on the current chain after the first read and before the mark. The
// store holds block 42 at the first read and blocks 42 and 99 at the re-read. The blockchain client
// answers by block ID, so only a second check that uses the re-read block IDs sees block 99 and marks
// the tx back on the longest chain.
func TestRestoreReorgDeletedTxsKeepsAnExistingTxMinedBetweenTheReadAndTheMark(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, _ := createCoinbaseChain(t, ctx, stp, block)

	recreateWithFailedSavedFileDelete(t, ctx, stp, block, first, 42)
	clearUnminedMarker(t, ctx, stp, first)

	answerChainChecksByBlockID(stp, 99)

	realStore := stp.utxoStore
	stp.utxoStore = &faultyUtxoStore{Store: realStore, beforeMark: mineOnCurrentChainBeforeTheMark(t, ctx, realStore, first, 99)}

	unmined, err := stp.restoreReorgDeletedTxs(ctx, block)
	require.NoError(t, err)
	require.False(t, containsTx(unmined, first), "a tx mined on the current chain is not put back in block assembly")

	after, err := realStore.Get(ctx, first.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.Contains(t, after.BlockIDs, uint32(99), "precondition: the tx was mined in block 99 between the read and the mark")
	require.Zero(t, after.UnminedSince, "the tx is marked back on the longest chain")

	exists, err := stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err)
	require.False(t, exists, "the saved file is deleted by the retry")
}

// An attempt marks the tx as not on the longest chain, finds it mined on the current chain, and fails
// on the re-read, so the marker stays on a tx that is mined on the current chain. The saved file must
// stay, and the next attempt must clear the marker before it deletes the file.
func TestRestoreReorgDeletedTxsClearsAMarkerLeftByAnInterruptedRepair(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, _ := createCoinbaseChain(t, ctx, stp, block)

	recreateWithFailedSavedFileDelete(t, ctx, stp, block, first, 42)
	clearUnminedMarker(t, ctx, stp, first)

	answerChainChecksByBlockID(stp, 99)

	realStore := stp.utxoStore
	stp.utxoStore = &faultyUtxoStore{
		Store:      realStore,
		beforeMark: mineOnCurrentChainBeforeTheMark(t, ctx, realStore, first, 99),
		getErr: func(call int) error {
			if call == 2 {
				return errors.NewStorageError("injected")
			}

			return nil
		},
	}

	_, err := stp.restoreReorgDeletedTxs(ctx, block)
	require.Error(t, err, "the first attempt fails on the re-read")

	exists, err := stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err)
	require.True(t, exists, "a failed restore keeps the saved file")

	interrupted, err := realStore.Get(ctx, first.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.Contains(t, interrupted.BlockIDs, uint32(99), "precondition: the live record is mined on the current chain")
	require.NotZero(t, interrupted.UnminedSince, "precondition: the interrupted repair left the marker")

	stp.utxoStore = realStore

	unmined, err := stp.restoreReorgDeletedTxs(ctx, block)
	require.NoError(t, err)
	require.False(t, containsTx(unmined, first), "a tx mined on the current chain is not put back in block assembly")

	after, err := realStore.Get(ctx, first.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.Zero(t, after.UnminedSince, "the retry clears the marker")

	exists, err = stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err)
	require.False(t, exists, "the saved file is deleted only once the status is right")
}

type faultyUtxoStore struct {
	utxostore.Store
	getErr     func(call int) error
	markErr    func(onLongestChain bool) error
	beforeMark func(onLongestChain bool)
	gets       int
}

func (s *faultyUtxoStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	s.gets++

	if s.getErr != nil {
		if err := s.getErr(s.gets); err != nil {
			return nil, err
		}
	}

	return s.Store.Get(ctx, hash, f...)
}

func (s *faultyUtxoStore) MarkTransactionsOnLongestChain(ctx context.Context, hashes []chainhash.Hash, onLongestChain bool) error {
	if s.beforeMark != nil {
		s.beforeMark(onLongestChain)
	}

	if s.markErr != nil {
		if err := s.markErr(onLongestChain); err != nil {
			return err
		}
	}

	return s.Store.MarkTransactionsOnLongestChain(ctx, hashes, onLongestChain)
}

type chainAnswer struct {
	onChain bool
	err     error
}

func answerChainChecks(stp *SubtreeProcessor, blockIDs []uint32, answers ...chainAnswer) {
	chainMock := stp.blockchainClient.(*blockchain.Mock)
	chainMock.ExpectedCalls = nil

	for _, a := range answers {
		chainMock.On("CheckBlockIsInCurrentChain", mock.Anything, blockIDs).Return(a.onChain, a.err).Once()
	}
}

func TestRestoreReorgDeletedTxsFailsWhenTheRepairOfAnExistingTxFails(t *testing.T) {
	injected := errors.NewStorageError("injected")

	tests := []struct {
		name        string
		answers     []chainAnswer
		getErr      func(call int) error
		markErr     func(onLongestChain bool) error
		skipped     bool
		wantErr     bool
		wantMessage string
	}{
		{
			name:        "the first read fails",
			answers:     []chainAnswer{{}},
			getErr:      func(call int) error { return injected },
			wantErr:     true,
			wantMessage: "error reading the existing record",
		},
		{
			name:    "the record is gone when it is read",
			answers: []chainAnswer{{}},
			getErr:  func(call int) error { return errors.NewTxNotFoundError("gone") },
			skipped: true,
		},
		{
			name:        "the first chain check fails",
			answers:     []chainAnswer{{}, {err: injected}},
			wantErr:     true,
			wantMessage: "error reading the existing record",
		},
		{
			name:        "the mark fails",
			answers:     []chainAnswer{{}, {}},
			markErr:     func(onLongestChain bool) error { return injected },
			wantErr:     true,
			wantMessage: "error marking tx",
		},
		{
			name:    "the re-read fails",
			answers: []chainAnswer{{}, {}},
			getErr: func(call int) error {
				if call == 2 {
					return injected
				}

				return nil
			},
			wantErr:     true,
			wantMessage: "error reading the existing record",
		},
		{
			name:        "the second chain check fails",
			answers:     []chainAnswer{{}, {}, {err: injected}},
			wantErr:     true,
			wantMessage: "error reading the existing record",
		},
		{
			name:    "the mark back on the longest chain fails",
			answers: []chainAnswer{{}, {}, {onChain: true}},
			markErr: func(onLongestChain bool) error {
				if onLongestChain {
					return injected
				}

				return nil
			},
			wantErr:     true,
			wantMessage: "error marking tx",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stp, block := newMoveForwardCommitBoundaryProcessor(t)
			blockHash := block.Header.Hash()

			first, _ := createCoinbaseChain(t, ctx, stp, block)

			recreateWithFailedSavedFileDelete(t, ctx, stp, block, first, 42)
			clearUnminedMarker(t, ctx, stp, first)

			answerChainChecks(stp, []uint32{42}, tc.answers...)

			stp.utxoStore = &faultyUtxoStore{Store: stp.utxoStore, getErr: tc.getErr, markErr: tc.markErr}

			unmined, err := stp.restoreReorgDeletedTxs(ctx, block)

			exists, existsErr := stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
			require.NoError(t, existsErr)

			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantMessage)
				require.True(t, exists, "a failed restore keeps the saved file for the next retry")

				return
			}

			require.NoError(t, err)
			require.False(t, containsTx(unmined, first))
			require.True(t, tc.skipped)
			require.False(t, exists, "a skipped tx does not keep the saved file")
		})
	}
}

func prepareFreshRestoreOfAMinedTx(t *testing.T, ctx context.Context, stp *SubtreeProcessor, block *model.Block, first *bt.Tx) {
	t.Helper()

	_, err := stp.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{first.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: 42, BlockHeight: 1234, SubtreeIdx: 0, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))
	require.NoError(t, stp.processCoinbaseUtxos(ctx, block))
}

func TestRestoreReorgDeletedTxsFailsWhenTheChainCheckOfASavedTxFails(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, _ := createCoinbaseChain(t, ctx, stp, block)

	prepareFreshRestoreOfAMinedTx(t, ctx, stp, block, first)
	answerChainChecks(stp, []uint32{42}, chainAnswer{err: errors.NewStorageError("injected")})

	_, err := stp.restoreReorgDeletedTxs(ctx, block)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error checking whether the blocks of tx")

	exists, err := stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err)
	require.True(t, exists, "a failed restore keeps the saved file for the next retry")
}

func TestRestoreReorgDeletedTxsFailsWhenTheBatchedMarkFails(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	first, _ := createCoinbaseChain(t, ctx, stp, block)

	prepareFreshRestoreOfAMinedTx(t, ctx, stp, block, first)
	answerChainChecks(stp, []uint32{42}, chainAnswer{})

	stp.utxoStore = &faultyUtxoStore{Store: stp.utxoStore, markErr: func(bool) error { return errors.NewStorageError("injected") }}

	_, err := stp.restoreReorgDeletedTxs(ctx, block)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error marking 1 recreated txs")

	exists, err := stp.subtreeStore.Exists(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.NoError(t, err)
	require.True(t, exists, "a failed restore keeps the saved file for the next retry")
}

type setLockedHonoursCtx struct {
	utxostore.Store
}

type cancelOnGetBlobStore struct {
	blob.Store
	cancel context.CancelFunc
}

func (s *cancelOnGetBlobStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	if fileType == fileformat.FileTypeReorgDeletedTxs {
		s.cancel()

		return nil, errors.NewStorageError("injected", ctx.Err())
	}

	return s.Store.Get(ctx, key, fileType, opts...)
}

func (s *setLockedHonoursCtx) SetLocked(ctx context.Context, hashes []chainhash.Hash, locked bool) error {
	if err := ctx.Err(); err != nil && !locked {
		return errors.NewStorageError("injected", err)
	}

	return s.Store.SetLocked(ctx, hashes, locked)
}

// The unlock after a failed save must still run when the reorg context is cancelled by then.
func TestRemoveCoinbaseUtxosUnlocksTheTxsWhenTheSaveFailsOnACancelledContext(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	first, second := createCoinbaseChain(t, ctx, stp, block)

	realStore := stp.utxoStore
	stp.utxoStore = &setLockedHonoursCtx{Store: realStore}

	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()

	stp.subtreeStore = &cancelOnGetBlobStore{Store: stp.subtreeStore, cancel: cancel}

	require.Error(t, stp.removeCoinbaseUtxos(cancelled, block), "the save fails")
	require.Error(t, cancelled.Err(), "precondition: the reorg context was cancelled while the txs were saved")

	for name, tx := range map[string]*bt.Tx{"coinbase": block.CoinbaseTx, "first": first, "second": second} {
		txMeta, err := realStore.Get(ctx, tx.TxIDChainHash(), fields.Locked)
		require.NoError(t, err, "%s was not deleted", name)
		require.False(t, txMeta.Locked, "%s must not stay locked", name)
	}
}

type setLockedFailsOnUnlock struct {
	utxostore.Store
}

func (s *setLockedFailsOnUnlock) SetLocked(ctx context.Context, hashes []chainhash.Hash, locked bool) error {
	if !locked {
		return errors.NewStorageError("injected")
	}

	return s.Store.SetLocked(ctx, hashes, locked)
}

func TestRemoveCoinbaseUtxosReturnsTheSaveErrorWhenTheUnlockAlsoFails(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	blockHash := block.Header.Hash()

	createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.subtreeStore.Set(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs, []byte{0, 9, 9}, options.WithAllowOverwrite(true)))

	stp.utxoStore = &setLockedFailsOnUnlock{Store: stp.utxoStore}

	err := stp.removeCoinbaseUtxos(ctx, block)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error saving child spends")
}

func TestRemoveCoinbaseUtxosFailsWhenTheCoinbaseCannotBeDeleted(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	first, second := createCoinbaseChain(t, ctx, stp, block)

	realStore := stp.utxoStore
	stp.utxoStore = &deleteFailsOnce{Store: realStore, target: *block.CoinbaseTx.TxIDChainHash()}

	err := stp.removeCoinbaseUtxos(ctx, block)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error deleting utxos for tx")

	for _, tx := range []*bt.Tx{first, second} {
		_, getErr := realStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
		require.ErrorIs(t, getErr, errors.ErrTxNotFound, "the children were deleted before the coinbase failed")
	}
}

// errorCapturingLogger records the error-level lines. The processors in these tests use a logger that
// fails the test on an error line, which is what a move-back that skips a tx is meant to write.
type errorCapturingLogger struct {
	ulogger.TestLogger
	mu     sync.Mutex
	errors []string
}

func (l *errorCapturingLogger) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}

func (l *errorCapturingLogger) errorLines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.errors...)
}

func containsAll(line string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(line, part) {
			return false
		}
	}

	return true
}

// A parent's spending data can name a child whose record is gone. The move back must skip that child
// and still save and delete every descendant the walk reaches through a record that exists, not report
// success and leave the coinbase and its other descendants in the store. A descendant reachable only
// through the missing child is not reached in the store, so its record stays. Block assembly can still
// hold the missing child and its descendants, so the move back takes them all out of the subtrees and
// marks them for removal, and leaves an unrelated tx in place.
func TestRemoveCoinbaseUtxosSkipsADescendantWhoseRecordIsGone(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	logger := &errorCapturingLogger{}
	stp.logger = logger

	require.NoError(t, stp.processCoinbaseUtxos(ctx, block))

	spendHeight := block.Height + uint32(stp.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // small chain constant

	// first spends the coinbase and has two outputs: gone spends the first, kept spends the second
	first := bt.NewTx()
	require.NoError(t, first.From(block.CoinbaseTx.TxIDChainHash().String(), 0, block.CoinbaseTx.Outputs[0].LockingScript.String(), block.CoinbaseTx.Outputs[0].Satoshis))
	require.NoError(t, first.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", block.CoinbaseTx.Outputs[0].Satoshis/4))
	require.NoError(t, first.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", block.CoinbaseTx.Outputs[0].Satoshis/4))
	first.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	_, _, err := stp.utxoStore.SpendAndCreate(ctx, first, spendHeight)
	require.NoError(t, err)

	gone := spendingTx(t, first, 0, first.Outputs[0].Satoshis/2)
	_, _, err = stp.utxoStore.SpendAndCreate(ctx, gone, spendHeight)
	require.NoError(t, err)

	kept := spendingTx(t, first, 1, first.Outputs[1].Satoshis/2)
	_, _, err = stp.utxoStore.SpendAndCreate(ctx, kept, spendHeight)
	require.NoError(t, err)

	// only spends gone and has no other parent, grand spends only: neither is reachable in the store
	// once the record of gone is removed
	only := spendingTx(t, gone, 0, gone.Outputs[0].Satoshis/2)
	_, _, err = stp.utxoStore.SpendAndCreate(ctx, only, spendHeight)
	require.NoError(t, err)

	grand := spendingTx(t, only, 0, only.Outputs[0].Satoshis/2)
	_, _, err = stp.utxoStore.SpendAndCreate(ctx, grand, spendHeight)
	require.NoError(t, err)

	// unrelated spends a tx outside the coinbase's descendants, so the move back must leave it
	unrelatedParent := bt.NewTx()
	require.NoError(t, unrelatedParent.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	unrelated := spendingTx(t, unrelatedParent, 0, 500)

	for _, tx := range []*bt.Tx{gone, only, grand, unrelated} {
		addToAssembly(t, stp, tx)
		require.True(t, stp.currentTxMap.Exists(*tx.TxIDChainHash()), "precondition: block assembly holds %s", tx.TxIDChainHash())
	}

	// the record of gone is removed, and the output of first that gone spent still names it
	require.NoError(t, stp.utxoStore.Delete(ctx, gone.TxIDChainHash()))

	firstMeta, err := stp.utxoStore.Get(ctx, first.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)

	stale := false

	for _, spendingData := range firstMeta.SpendingDatas {
		if spendingData != nil && spendingData.TxID.IsEqual(gone.TxIDChainHash()) {
			stale = true
		}
	}

	require.True(t, stale, "precondition: the first tx still names the missing tx as the spender of its output")

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	for name, tx := range map[string]*bt.Tx{"coinbase": block.CoinbaseTx, "first": first, "kept": kept} {
		_, getErr := stp.utxoStore.Get(ctx, tx.TxIDChainHash())
		require.ErrorIs(t, getErr, errors.ErrTxNotFound, "%s must be deleted by the move back", name)

		require.True(t, stp.removeMap.Exists(*tx.TxIDChainHash()) || tx == block.CoinbaseTx, "%s must be in the remove map", name)
	}

	require.False(t, stp.currentTxMap.Exists(*gone.TxIDChainHash()), "the missing tx must be taken out of block assembly")
	require.True(t, stp.removeMap.Exists(*gone.TxIDChainHash()), "the missing tx must be in the remove map")

	foundIndex, _, _ := stp.locateTxInSubtrees(*gone.TxIDChainHash())
	require.Equal(t, -1, foundIndex, "the missing tx must not be in any subtree")

	for name, tx := range map[string]*bt.Tx{"only": only, "grand": grand} {
		require.False(t, stp.currentTxMap.Exists(*tx.TxIDChainHash()), "%s descends from the missing tx and must be taken out of block assembly", name)
		require.True(t, stp.removeMap.Exists(*tx.TxIDChainHash()), "%s must be in the remove map", name)

		foundIndex, _, _ = stp.locateTxInSubtrees(*tx.TxIDChainHash())
		require.Equal(t, -1, foundIndex, "%s must not be in any subtree", name)

		// the walk cannot reach it in the store, so its record stays
		_, getErr := stp.utxoStore.Get(ctx, tx.TxIDChainHash())
		require.NoError(t, getErr, "the record of %s is not reached by the walk", name)
	}

	require.True(t, stp.currentTxMap.Exists(*unrelated.TxIDChainHash()), "an unrelated tx stays in block assembly")
	require.False(t, stp.removeMap.Exists(*unrelated.TxIDChainHash()), "an unrelated tx is not marked for removal")

	foundIndex, _, _ = stp.locateTxInSubtrees(*unrelated.TxIDChainHash())
	require.GreaterOrEqual(t, foundIndex, 0, "an unrelated tx stays in the subtrees")

	saved, err := stp.readReorgDeletedTxs(ctx, block.Header.Hash())
	require.NoError(t, err)

	savedHashes := make(map[chainhash.Hash]struct{}, len(saved))
	for _, entry := range saved {
		savedHashes[*entry.tx.TxIDChainHash()] = struct{}{}
	}

	require.Len(t, savedHashes, 2, "the surviving descendants are saved, the missing one is not")
	require.Contains(t, savedHashes, *first.TxIDChainHash())
	require.Contains(t, savedHashes, *kept.TxIDChainHash())

	skipLines := 0

	for _, line := range logger.errorLines() {
		if containsAll(line, "skipping tx", gone.TxIDChainHash().String(), block.CoinbaseTx.TxIDChainHash().String()) {
			skipLines++
		}
	}

	require.Equal(t, 1, skipLines, "the missing descendant is named once in an error line: %v", logger.errorLines())
}

// When the coinbase itself is not in the store there is nothing to walk from, so the move back has
// nothing to do and is not an error.
func TestRemoveCoinbaseUtxosWithAnAbsentCoinbaseDoesNothing(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	logger := &errorCapturingLogger{}
	stp.logger = logger

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))
	require.Empty(t, logger.errorLines())

	blockHash := block.Header.Hash()
	_, err := stp.subtreeStore.Get(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	require.ErrorIs(t, err, errors.ErrNotFound, "nothing is saved")
}

// A tx the walk locked can vanish before it is read back for the save. It cannot be saved, and failing
// would gain nothing: a retry would skip the record as missing, so the tx would still be absent from the
// file. The save goes on without it, and says so.
func TestKeepReorgDeletedTxsCountsATxThatVanishedAfterItWasLocked(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)
	logger := &errorCapturingLogger{}
	stp.logger = logger

	first, second := createCoinbaseChain(t, ctx, stp, block)
	blockHash := block.Header.Hash()

	before := testutil.ToFloat64(prometheusReorgDeletedTxsVanished)

	// second vanishes between the lock and the read, first is still there
	require.NoError(t, stp.utxoStore.Delete(ctx, second.TxIDChainHash()))

	kept, err := stp.keepReorgDeletedTxs(ctx, block, []chainhash.Hash{*first.TxIDChainHash(), *second.TxIDChainHash()})
	require.NoError(t, err, "a vanished tx must not fail the save")
	require.Equal(t, []chainhash.Hash{*first.TxIDChainHash()}, kept)

	require.Equal(t, before+1, testutil.ToFloat64(prometheusReorgDeletedTxsVanished))

	vanishedLines := 0

	for _, line := range logger.errorLines() {
		if containsAll(line, "vanished", second.TxIDChainHash().String(), block.String()) {
			vanishedLines++
		}
	}

	require.Equal(t, 1, vanishedLines, "the vanished tx is named once in an error line: %v", logger.errorLines())

	saved, err := stp.readReorgDeletedTxs(ctx, blockHash)
	require.NoError(t, err)
	require.Len(t, saved, 1)

	// a tx with a copy in the saved file is not vanished: the copy is kept, and nothing is counted
	_, err = stp.keepReorgDeletedTxs(ctx, block, []chainhash.Hash{*first.TxIDChainHash()})
	require.NoError(t, err)
	require.NoError(t, stp.utxoStore.Delete(ctx, first.TxIDChainHash()))

	kept, err = stp.keepReorgDeletedTxs(ctx, block, []chainhash.Hash{*first.TxIDChainHash()})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*first.TxIDChainHash()}, kept)
	require.Equal(t, before+1, testutil.ToFloat64(prometheusReorgDeletedTxsVanished), "a tx with a saved copy is not counted")
}

// vanishOnSaveRead deletes the target record just before the move back reads it back for the save,
// so it is gone after the walk locked it and before keepReorgDeletedTxs reads its tx.
type vanishOnSaveRead struct {
	utxostore.Store
	target chainhash.Hash
}

func (s *vanishOnSaveRead) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	if hash.Equal(s.target) && slices.Contains(f, fields.Tx) {
		if err := s.Store.Delete(ctx, hash); err != nil && !errors.Is(err, errors.ErrTxNotFound) {
			return nil, err
		}
	}

	return s.Store.Get(ctx, hash, f...)
}

// A tx the walk locked that vanishes before the save is not in the saved file and has no record to
// delete, but block assembly can still hold it and its descendants. The move back takes them out of
// the subtrees and marks them for removal, as it does for a missing tx.
func TestRemoveCoinbaseUtxosTakesAVanishedTxOutOfBlockAssembly(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	first, second := createCoinbaseChain(t, ctx, stp, block)

	// third spends second and is only in block assembly, so the walk never reaches it
	third := spendingTx(t, second, 0, second.Outputs[0].Satoshis/2)

	for _, tx := range []*bt.Tx{first, second, third} {
		addToAssembly(t, stp, tx)
		require.True(t, stp.currentTxMap.Exists(*tx.TxIDChainHash()), "precondition: block assembly holds %s", tx.TxIDChainHash())
	}

	before := testutil.ToFloat64(prometheusReorgDeletedTxsVanished)

	stp.utxoStore = &vanishOnSaveRead{Store: stp.utxoStore, target: *second.TxIDChainHash()}

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	require.Equal(t, before+1, testutil.ToFloat64(prometheusReorgDeletedTxsVanished), "precondition: second vanished after the lock")

	for name, tx := range map[string]*bt.Tx{"first": first, "second": second, "third": third} {
		require.False(t, stp.currentTxMap.Exists(*tx.TxIDChainHash()), "%s must be taken out of block assembly", name)
		require.True(t, stp.removeMap.Exists(*tx.TxIDChainHash()), "%s must be in the remove map", name)

		foundIndex, _, _ := stp.locateTxInSubtrees(*tx.TxIDChainHash())
		require.Equal(t, -1, foundIndex, "%s must not be in any subtree", name)
	}
}
