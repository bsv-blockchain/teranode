package smoke

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

const (
	// Use smaller values for faster test execution
	testCoinbaseMaturity             = 2
	testReassignedUtxoSpendableAfter = 5
)

func TestReassignSQLiteEnforcesMaturityAndRejectsUnstoredScript(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// SystemTestSettings selects SQLite, which honors the shortened maturity
	// setting. This test does not exercise the Aerospike reassignment backend.
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				// Reduce coinbase maturity for faster test
				s.ChainCfgParams.CoinbaseMaturity = testCoinbaseMaturity
				s.Validator.UseLocalValidator = true
				// Reduce reassigned UTXO spendable blocks for faster test
				s.UtxoStore.ReAssignedUtxoSpendableAfterBlocks = testReassignedUtxoSpendableAfter
			},
		),
	})

	defer td.Stop(t)
	require.Equal(t, "sqlite", td.Settings.UtxoStore.UtxoStore.Scheme)

	// Public ingress removes the validator's error chain. Check that ingress
	// reaches validation, then pin the cause with the daemon's local validator.
	requireRejected := func(tx *bt.Tx, spend *utxo.Spend, cause error, detail string) {
		t.Helper()
		require.ErrorContains(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx), "failed to validate transaction")
		// Re-extension mutates inputs; preserve the fixture for subsequent probes.
		// Height 0 selects tip+1, exactly as propagation does.
		_, err := td.ValidatorClient.ValidateWithOptions(td.Ctx, tx.Clone(), 0,
			&validator.Options{AddTXToBlockAssembly: false})
		require.Error(t, err)
		if cause != nil {
			require.ErrorIs(t, err, cause)
		}
		if detail != "" {
			require.ErrorContains(t, err, detail)
		}
		status, err := td.UtxoStore.GetSpend(td.Ctx, spend)
		require.NoError(t, err)
		require.Nil(t, status.SpendingData, "every rejected spend must leave the output unspent")
	}

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks (coinbase maturity + 1)
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{testCoinbaseMaturity + 1})
	require.NoError(t, err)

	// Generate private keys and addresses for Alice, Bob, and Charles
	alicePrivateKey := td.GetPrivateKey(t)

	bobPrivateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	bob := bobPrivateKey.PubKey()

	charlesPrivatekey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	charles := charlesPrivatekey.PubKey()

	// Get coinbase transaction from block 1
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create parent transaction with outputs to Alice
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 1)
	require.NoError(t, err)

	aliceToBobTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0, alicePrivateKey),
		transactions.WithP2PKHOutputs(2, 10000, bob),
	)

	// Send Alice to Bob transaction
	err = td.PropagationClient.ProcessTransaction(td.Ctx, aliceToBobTx)
	require.NoError(t, err)

	// Wait for the transaction to be processed by block assembly before mining
	td.WaitForBlockAssemblyToProcessTx(t, aliceToBobTx.TxIDChainHash().String())

	// Mine a block and wait for processing
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err)

	throwawayTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0, alicePrivateKey),
		transactions.WithP2PKHOutputs(1, 10000, charles),
	)

	// Freeze UTXO of Alice-Bob transaction
	aliceBobUtxoHash, err := util.UTXOHashFromOutput(aliceToBobTx.TxIDChainHash(), aliceToBobTx.Outputs[0], 0)
	require.NoError(t, err)

	spend := &utxo.Spend{
		TxID:     aliceToBobTx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: aliceBobUtxoHash,
	}

	// Keep a second output assigned to its stored owner to test the maturity
	// gate independently of changing the locking script.
	sameOwnerHash, err := util.UTXOHashFromOutput(aliceToBobTx.TxIDChainHash(), aliceToBobTx.Outputs[1], 1)
	require.NoError(t, err)
	sameOwnerSpend := &utxo.Spend{TxID: aliceToBobTx.TxIDChainHash(), Vout: 1, UTXOHash: sameOwnerHash}

	err = td.UtxoStore.FreezeUTXOs(td.Ctx, []*utxo.Spend{spend, sameOwnerSpend}, td.Settings)
	require.NoError(t, err)

	amendedOutputScript := &bt.Output{
		Satoshis:      aliceToBobTx.Outputs[0].Satoshis,
		LockingScript: throwawayTx.Outputs[0].LockingScript,
	}

	// Reassign the UTXO to Charles
	reassignUtxoHash, err := util.UTXOHashFromOutput(aliceToBobTx.TxIDChainHash(), amendedOutputScript, 0)
	require.NoError(t, err)

	newSpend := &utxo.Spend{
		TxID:     aliceToBobTx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: reassignUtxoHash,
	}

	// Reassignment computes maturity from the store's height, which updates
	// asynchronously. Pin the starting height before either commitment changes.
	reassignHeight := uint32(testCoinbaseMaturity + 2)
	td.WaitForUtxoStoreHeight(t, reassignHeight)
	require.Equal(t, reassignHeight, td.UtxoStore.GetBlockHeight())

	err = td.UtxoStore.ReAssignUTXO(td.Ctx, spend, newSpend, td.Settings)
	require.NoError(t, err)

	require.NoError(t, td.UtxoStore.ReAssignUTXO(td.Ctx, sameOwnerSpend, sameOwnerSpend, td.Settings))
	bobSpendingTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(aliceToBobTx, 1, bobPrivateKey),
		transactions.WithP2PKHOutputs(1, 100, charles),
	)
	status, err := td.UtxoStore.GetSpend(td.Ctx, sameOwnerSpend)
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_IMMATURE), status.Status)
	requireRejected(bobSpendingTx, sameOwnerSpend, errors.ErrTxLocked, "")

	// ReAssignUTXO changes the commitment, not the stored locking script.
	// The validator must not accept Charles's replacement script supplied in
	// extended transaction bytes, before or after the maturity height.
	charlesSpendingTx := bt.NewTx()
	charlesUtxo := &bt.UTXO{
		TxIDHash:      aliceToBobTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: throwawayTx.Outputs[0].LockingScript,
		Satoshis:      aliceToBobTx.Outputs[0].Satoshis,
	}

	err = charlesSpendingTx.FromUTXOs(charlesUtxo)
	require.NoError(t, err)

	err = charlesSpendingTx.AddP2PKHOutputFromPubKeyBytes(bob.Compressed(), 100)
	require.NoError(t, err)

	err = charlesSpendingTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: charlesPrivatekey})
	require.NoError(t, err)

	requireRejected(charlesSpendingTx, newSpend, nil, "OP_EQUALVERIFY")

	// Mine to the exact maturity height and wait for the asynchronous store
	// update, retaining coverage of an off-by-one error in the gate.
	td.MineAndWait(t, testReassignedUtxoSpendableAfter)
	maturityHeight := reassignHeight + testReassignedUtxoSpendableAfter
	td.WaitForUtxoStoreHeight(t, maturityHeight)
	require.Equal(t, maturityHeight, td.UtxoStore.GetBlockHeight())
	for _, maturedSpend := range []*utxo.Spend{newSpend, sameOwnerSpend} {
		status, err := td.UtxoStore.GetSpend(td.Ctx, maturedSpend)
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_OK), status.Status)
	}

	// The changed commitment has matured, but it does not authorize trusting
	// the submitter's replacement script. Ownership-changing reassignment
	// needs an authoritative script source in addition to ReAssignUTXO.
	requireRejected(charlesSpendingTx, newSpend, nil, "OP_EQUALVERIFY")

	// The original owner is locked out too: its signature matches the stored
	// script, but its commitment no longer matches the reassigned hash.
	originalOwnerSpendingTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(aliceToBobTx, 0, bobPrivateKey),
		transactions.WithP2PKHOutputs(1, 100, charles),
	)
	requireRejected(originalOwnerSpendingTx, newSpend, errors.ErrUtxoHashMismatch, "")

	// Self-reassignment is only a maturity control, not a working confiscation.
	// The same-owner output now passes both script validation and the height gate.
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, bobSpendingTx))
}
