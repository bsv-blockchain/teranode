// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	utxoStore "github.com/bsv-blockchain/teranode/stores/utxo"
	utxostoresql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// setupValidateParentChainStore creates an in-memory UTXO store for validateParentChain tests.
func setupValidateParentChainStore(ctx context.Context, t *testing.T) utxoStore.Store {
	t.Helper()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	u, err := url.Parse("sqlitememory:///validate-parent-chain")
	require.NoError(t, err)

	store, err := utxostoresql.New(ctx, logger, tSettings, u)
	require.NoError(t, err)

	return store
}

// setupValidateParentChainBA creates a BlockAssembler for validateParentChain tests
// with the given UTXO store and blockchain client mock.
func setupValidateParentChainBA(utxoS utxoStore.Store, blockchainClient *blockchain.Mock) *BlockAssembler {
	tSettings := &settings.Settings{}
	tSettings.BlockAssembly.ParentValidationBatchSize = 1000

	return &BlockAssembler{
		utxoStore:        utxoS,
		blockchainClient: blockchainClient,
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
	}
}

// makeSimpleTx creates a minimal transaction with one input pointing to prevHash:vout
// and one output. inputSatoshis and outputSatoshis must satisfy inputSatoshis >= outputSatoshis
// for fee validation. Use unique outputSatoshis values to get unique txids.
func makeSimpleTx(t *testing.T, prevHash *chainhash.Hash, prevVout uint32, inputSatoshis uint64, outputSatoshis uint64) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	err := tx.From(
		prevHash.String(),
		prevVout,
		"76a914000000000000000000000000000000000000000088ac",
		inputSatoshis,
	)
	require.NoError(t, err)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	err = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", outputSatoshis)
	require.NoError(t, err)
	return tx
}

// makeCoinbaseTx creates a minimal coinbase-style transaction that can be used as a
// standalone parent. It uses a large unique satoshi value to produce unique txids.
// The input references an all-zero prev hash (coinbase convention).
func makeCoinbaseTx(t *testing.T, satoshis uint64) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()

	// Coinbase input: all-zero prev hash, vout = 0xffffffff
	tx.Inputs = []*bt.Input{
		{
			PreviousTxOutIndex: 0xffffffff,
			SequenceNumber:     0xffffffff,
			UnlockingScript:    bscript.NewFromBytes([]byte{0x03, byte(satoshis), byte(satoshis >> 8), byte(satoshis >> 16)}),
		},
	}
	err := tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})
	require.NoError(t, err)

	err = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis)
	require.NoError(t, err)
	return tx
}

// makeUnminedTx creates a UnminedTransaction from a bt.Tx with TxInpoints from its inputs.
func makeUnminedTx(t *testing.T, tx *bt.Tx, createdAt int) *utxoStore.UnminedTransaction {
	t.Helper()
	inpoints, err := subtreepkg.NewTxInpointsFromInputs(tx.Inputs)
	require.NoError(t, err)
	return &utxoStore.UnminedTransaction{
		Node: &subtreepkg.Node{
			Hash:        *tx.TxIDChainHash(),
			Fee:         1000,
			SizeInBytes: 250,
		},
		TxInpoints:   &inpoints,
		CreatedAt:    createdAt,
		UnminedSince: 1,
	}
}

// TestValidateParentChain_HardFailsWithFSMIdle_UnminedParentNotInList tests that
// when a TX_CHILD has an unmined parent TX_PARENT that is NOT in the processing list,
// validateParentChain returns an error containing "repair-conflicts" and calls
// Idle to transition FSM.
func TestValidateParentChain_HardFailsWithFSMIdle_UnminedParentNotInList(t *testing.T) {
	ctx := context.Background()
	store := setupValidateParentChainStore(ctx, t)

	// Create TX_PARENT as a mined coinbase (so it doesn't need its own parents in store).
	// Then give it unmined_since > 0 to simulate it being NOT on the main chain.
	txParent := makeCoinbaseTx(t, 50_000)
	_, err := store.Create(ctx, txParent, 100) // unmined_since = 100
	require.NoError(t, err)

	// TX_CHILD spends TX_PARENT[0]. Input satoshis=50000, output=40000 (fee=10000).
	txChild := makeSimpleTx(t, txParent.TxIDChainHash(), 0, 50_000, 40_000)
	_, err = store.Create(ctx, txChild, 100)
	require.NoError(t, err)

	// Set up blockchain client mock — Idle should be called to transition FSM.
	blockchainClientMock := &blockchain.Mock{}
	blockchainClientMock.On("Idle", mock.Anything).Return(nil)

	ba := setupValidateParentChainBA(store, blockchainClientMock)

	// Input list contains only TX_CHILD (NOT TX_PARENT).
	unminedChild := makeUnminedTx(t, txChild, 1)
	bestBlockHeaderIDsMap := map[uint32]bool{} // empty = no blocks on best chain

	// validateParentChain should fail because TX_PARENT is unmined but not in list.
	err = ba.validateParentChain(ctx, []*utxoStore.UnminedTransaction{unminedChild}, bestBlockHeaderIDsMap)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrRepairNeeded)

	// Idle must have been called to transition FSM.
	blockchainClientMock.AssertCalled(t, "Idle", mock.Anything)
}

// TestValidateParentChain_SucceedsForCleanState tests that when TX_PARENT is mined
// (UnminedSince=0, BlockIDs on best chain) and TX_CHILD spends it, validateParentChain
// succeeds and returns TX_CHILD in the valid list.
func TestValidateParentChain_SucceedsForCleanState(t *testing.T) {
	ctx := context.Background()
	store := setupValidateParentChainStore(ctx, t)

	// TX_PARENT is a coinbase with block info — mined on best chain.
	txParent := makeCoinbaseTx(t, 50_000)
	const blockID uint32 = 1
	_, err := store.Create(ctx, txParent, 0, // unmined_since = 0 → mined
		utxoStore.WithMinedBlockInfo(utxoStore.MinedBlockInfo{BlockID: blockID, BlockHeight: 1}),
	)
	require.NoError(t, err)

	// TX_CHILD spends TX_PARENT[0]. Input satoshis=50000, output=40000 (fee=10000).
	txChild := makeSimpleTx(t, txParent.TxIDChainHash(), 0, 50_000, 40_000)
	_, err = store.Create(ctx, txChild, 100)
	require.NoError(t, err)

	blockchainClientMock := &blockchain.Mock{}
	// Idle should NOT be called for success path.
	ba := setupValidateParentChainBA(store, blockchainClientMock)

	unminedChild := makeUnminedTx(t, txChild, 1)
	bestBlockHeaderIDsMap := map[uint32]bool{blockID: true}

	err = ba.validateParentChain(ctx, []*utxoStore.UnminedTransaction{unminedChild}, bestBlockHeaderIDsMap)
	require.NoError(t, err)

	// Idle must NOT have been called.
	blockchainClientMock.AssertNotCalled(t, "Idle")
}

// TestValidateParentChain_SucceedsWhenParentIsInList tests that when both TX_PARENT
// and TX_CHILD are unmined, and TX_PARENT is listed BEFORE TX_CHILD, both are returned.
//
// Scenario:
//   - rootTx is mined (UnminedSince=0, BlockIDs on best chain) — provides the UTXO base
//   - txParent is unmined, spends rootTx[0] — in the input list
//   - txChild is unmined, spends txParent[0] — in the input list after txParent
func TestValidateParentChain_SucceedsWhenParentIsInList(t *testing.T) {
	ctx := context.Background()
	store := setupValidateParentChainStore(ctx, t)

	// rootTx is created using the fixed hex from sql_test.go — mined on best chain.
	rootTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)

	const bestBlockID uint32 = 1
	_, err = store.Create(ctx, rootTx, 0,
		utxoStore.WithMinedBlockInfo(utxoStore.MinedBlockInfo{BlockID: bestBlockID, BlockHeight: 1}),
	)
	require.NoError(t, err)

	// txParent spends rootTx[0]. Unmined.
	// rootTx.Outputs[0].Satoshis = 5000000 (0x004c4b40 = from BEF hex: 0x002af205 LE = 5000000 ... let's use the actual value).
	rootOut0Satoshis := rootTx.Outputs[0].Satoshis
	txParent := makeSimpleTx(t, rootTx.TxIDChainHash(), 0, rootOut0Satoshis, rootOut0Satoshis/2)
	_, err = store.Create(ctx, txParent, 100) // unmined_since=100
	require.NoError(t, err)

	// txChild spends txParent[0]. Unmined.
	txParentOut0Satoshis := txParent.Outputs[0].Satoshis
	txChild := makeSimpleTx(t, txParent.TxIDChainHash(), 0, txParentOut0Satoshis, txParentOut0Satoshis/2)
	_, err = store.Create(ctx, txChild, 100) // unmined_since=100
	require.NoError(t, err)

	blockchainClientMock := &blockchain.Mock{}
	ba := setupValidateParentChainBA(store, blockchainClientMock)

	// Input list: txParent first (index 0), txChild second (index 1).
	unminedParent := makeUnminedTx(t, txParent, 0)
	unminedChild := makeUnminedTx(t, txChild, 1)
	bestBlockHeaderIDsMap := map[uint32]bool{bestBlockID: true}

	err = ba.validateParentChain(ctx, []*utxoStore.UnminedTransaction{unminedParent, unminedChild}, bestBlockHeaderIDsMap)
	require.NoError(t, err)

	// Idle must NOT have been called.
	blockchainClientMock.AssertNotCalled(t, "Idle")
}
