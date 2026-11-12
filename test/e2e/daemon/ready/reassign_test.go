package smoke

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
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
				// Reduce reassigned UTXO spendable blocks for faster test
				s.UtxoStore.ReAssignedUtxoSpendableAfterBlocks = testReassignedUtxoSpendableAfter
			},
		),
	})

	defer td.Stop(t)
	require.Equal(t, "sqlite", td.Settings.UtxoStore.UtxoStore.Scheme)

	// Public ingress redacts some validation errors. Pair every rejection
	// there with its exact cause from a real validator over the same stores.
	// Nil Kafka producers and a disabled assembly handoff keep this probe local.
	v, err := validator.New(td.Ctx, td.Logger, td.Settings, td.UtxoStore,
		nil, nil, nil, nil, td.BlockchainClient)
	require.NoError(t, err)
	requireRejected := func(tx *bt.Tx, reason string) {
		t.Helper()
		// Snapshot before either path runs, and decode separate requests.
		// Keep tx untouched so retries still supply the original extended fields.
		submittedBytes := tx.ExtendedBytes()
		ingress, err := bt.NewTxFromBytes(submittedBytes)
		require.NoError(t, err)
		require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, ingress))
		probe, err := bt.NewTxFromBytes(submittedBytes)
		require.NoError(t, err)
		_, err = v.ValidateWithOptions(td.Ctx, probe, td.UtxoStore.GetBlockHeight(),
			&validator.Options{AddTXToBlockAssembly: false})
		require.ErrorContains(t, err, reason)
	}

	// Set run state
	err = td.BlockchainClient.Run(td.Ctx, "test")
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
	requireRejected(bobSpendingTx, "not spendable until")
	status, err = td.UtxoStore.GetSpend(td.Ctx, sameOwnerSpend)
	require.NoError(t, err)
	require.Nil(t, status.SpendingData, "the immature spend must leave the output unspent")

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

	requireRejected(charlesSpendingTx, "OP_EQUALVERIFY")

	// Mine beyond the boundary, then wait for the UTXO store's asynchronous
	// height update before attributing any rejection to the locking script.
	td.MineAndWait(t, testReassignedUtxoSpendableAfter+1)
	require.Eventually(t, func() bool {
		for _, maturedSpend := range []*utxo.Spend{newSpend, sameOwnerSpend} {
			status, err := td.UtxoStore.GetSpend(td.Ctx, maturedSpend)
			if err != nil || status == nil || status.Status != int(utxo.Status_OK) {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "both outputs must mature in the UTXO store")

	// The changed commitment has matured, but it does not authorize trusting
	// the submitter's replacement script. Ownership-changing reassignment
	// needs an authoritative script source in addition to ReAssignUTXO.
	requireRejected(charlesSpendingTx, "OP_EQUALVERIFY")
	status, err = td.UtxoStore.GetSpend(td.Ctx, newSpend)
	require.NoError(t, err)
	require.Nil(t, status.SpendingData, "the rejected transaction must leave the output unspent")

	// The original owner is locked out too: its signature matches the stored
	// script, but its commitment no longer matches the reassigned hash.
	originalOwnerSpendingTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(aliceToBobTx, 0, bobPrivateKey),
		transactions.WithP2PKHOutputs(1, 100, charles),
	)
	requireRejected(originalOwnerSpendingTx, "UTXO_MISMATCH")
	status, err = td.UtxoStore.GetSpend(td.Ctx, newSpend)
	require.NoError(t, err)
	require.Nil(t, status.SpendingData, "the original owner's rejected spend must leave the output unspent")

	// Self-reassignment is only a maturity control, not a working confiscation.
	// The same-owner output now passes both script validation and the height gate.
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, bobSpendingTx))
}
