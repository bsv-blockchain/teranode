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
	"github.com/bsv-blockchain/teranode/stores/blob"
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

// A move forward that cannot recreate a saved tx must fail, not hand the block on with records missing.
func TestMoveForwardBlockFailsWhenASavedTxCannotBeRecreated(t *testing.T) {
	ctx := context.Background()
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	first, _ := createCoinbaseChain(t, ctx, stp, block)

	require.NoError(t, stp.removeCoinbaseUtxos(ctx, block))

	rival := spendingTx(t, block.CoinbaseTx, 0, first.Outputs[0].Satoshis-1)
	require.NoError(t, stp.processCoinbaseUtxos(ctx, block))

	spendHeight := block.Height + uint32(stp.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // small chain constant
	_, _, err := stp.utxoStore.SpendAndCreate(ctx, rival, spendHeight)
	require.NoError(t, err, "a rival already spends the coinbase output the saved tx needs")

	_, _, err = stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, false, true)
	require.Error(t, err, "the saved tx cannot be spent again, so the block must not be applied")
}

func TestParentsFirstOrdersAChainWhateverTheInputOrder(t *testing.T) {
	root := bt.NewTx()
	require.NoError(t, root.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
	require.NoError(t, root.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 9000))

	a := spendingTx(t, root, 0, 8000)
	b := spendingTx(t, a, 0, 7000)
	c := spendingTx(t, b, 0, 6000)

	byHash := map[chainhash.Hash]*bt.Tx{}
	for _, tx := range []*bt.Tx{a, b, c} {
		byHash[*tx.TxIDChainHash()] = tx
	}

	got := parentsFirst([]chainhash.Hash{*c.TxIDChainHash(), *a.TxIDChainHash(), *b.TxIDChainHash()}, byHash)

	require.Equal(t, []string{a.TxIDChainHash().String(), b.TxIDChainHash().String(), c.TxIDChainHash().String()},
		[]string{got[0].TxIDChainHash().String(), got[1].TxIDChainHash().String(), got[2].TxIDChainHash().String()})
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

// A reset that moves a block back and does not move it forward leaves the saved txs unused, and a
// reset that cannot recreate a saved tx must fail rather than report success with records missing.
func TestResetFailsWhenASavedTxCannotBeRecreated(t *testing.T) {
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
			require.Error(t, response.Err, "the saved tx cannot be spent again, so the reset must not report success")
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
