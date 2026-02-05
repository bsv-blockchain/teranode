package smoke

import (
	"encoding/hex"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFreezeAndUnfreezeUtxos(t *testing.T) {
	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableValidator:      true,
		SettingsOverrideFunc: test.SystemTestSettings(),
		// EnableFullLogging: true,
	})

	defer td.Stop(t, true)

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	td.MineAndWait(t, 101)

	privateKey1, err := bec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")

	address1, err := bscript.NewAddressFromPublicKey(privateKey1.PubKey(), true)
	require.NoError(t, err, "Failed to create address")

	// Get coinbase transaction from block 1
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	coinbaseTx := block1.CoinbaseTx

	parentTx, err := td.CreateParentTransactionWithNOutputs(t, coinbaseTx, 10)
	require.NoError(t, err, "Failed to create parent transaction")

	// Split outputs into two parts
	firstSet := len(parentTx.Outputs) - 2

	getSpends := func(outputs []*bt.Output, startIndex int) []*utxo.Spend {
		td.Logger.Infof("Creating spends with %d outputs", len(outputs))

		spends := make([]*utxo.Spend, 0)

		const maxUint32 = 1<<32 - 1

		for i, output := range outputs {
			idx := i + startIndex
			if idx < 0 || idx > maxUint32 {
				return nil
			}

			utxoHash, _ := util.UTXOHashFromOutput(parentTx.TxIDChainHash(), output, uint32(idx))
			spend := &utxo.Spend{
				TxID:     parentTx.TxIDChainHash(),
				Vout:     uint32(idx),
				UTXOHash: utxoHash,
			}
			spends = append(spends, spend)
		}

		return spends
	}

	createAndSendTx := func(outputs []*bt.Output, startIndex int) (*bt.Tx, error) {
		td.Logger.Infof("Creating and sending transaction with %d outputs", len(outputs))

		spendingTx := bt.NewTx()
		totalSatoshis := uint64(0)
		utxos := make([]*bt.UTXO, 0)

		const maxUint32 = 1<<32 - 1

		for i, output := range outputs {
			idx := i + startIndex
			if idx < 0 || idx > maxUint32 {
				return nil, nil
			}

			utxo := &bt.UTXO{
				TxIDHash:      parentTx.TxIDChainHash(),
				Vout:          uint32(idx),
				LockingScript: output.LockingScript,
				Satoshis:      output.Satoshis,
			}
			utxos = append(utxos, utxo)
			totalSatoshis += output.Satoshis
			td.Logger.Infof("totalSatoshis: %d", totalSatoshis)
		}

		err := spendingTx.FromUTXOs(utxos...)
		require.NoError(t, err, "Error adding UTXOs to transaction")

		fee := uint64(1000)
		amountToSend := totalSatoshis - fee

		err = spendingTx.AddP2PKHOutputFromAddress(address1.AddressString, amountToSend)
		require.NoError(t, err, "Error adding output to transaction")

		err = spendingTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: td.GetPrivateKey(t)})
		require.NoError(t, err, "Error filling transaction inputs")

		td.Logger.Infof("Transaction created: %s", spendingTx.String())

		err = td.PropagationClient.ProcessTransaction(td.Ctx, spendingTx)

		return spendingTx, err
	}

	// Get spends for the first set of outputs
	spends := getSpends(parentTx.Outputs[:firstSet], 0)
	err = td.UtxoStore.FreezeUTXOs(td.Ctx, spends, td.Settings)
	require.NoError(t, err, "Failed to freeze UTXOs")

	// Create and send transaction with the frozen spends - should fail
	_, err = createAndSendTx(parentTx.Outputs[:firstSet], 0)
	require.Error(t, err, "Should not allow to spend frozen UTXOs")

	// Unfreeze the UTXOs
	err = td.UtxoStore.UnFreezeUTXOs(td.Ctx, spends, td.Settings)
	require.NoError(t, err, "Failed to unfreeze UTXOs")

	// Create and send transaction with the unfrozen spends - should succeed
	spendingTx, err := createAndSendTx(parentTx.Outputs[:firstSet], 0)
	require.NoError(t, err, "Failed to create and send transaction after unfreezing")

	// Mine a block
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to generate block")

	// Verify transaction is in block
	block102, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	err = block102.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	err = block102.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)

	subtree, err := block102.GetSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	blFound := false

	for i := 0; i < len(subtree); i++ {
		st := subtree[i]
		for _, node := range st.Nodes {
			t.Logf("node.Hash: %s", node.Hash.String())
			t.Logf("tx.TxIDChainHash().String(): %s", spendingTx.TxIDChainHash().String())

			if node.Hash.String() == spendingTx.TxIDChainHash().String() {
				blFound = true
				break
			}
		}
	}

	assert.True(t, blFound, "Transaction not found in block")
}

func TestDeleteAtHeightHappyPath(t *testing.T) {
	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(settings *settings.Settings) {
				settings.GlobalBlockHeightRetention = 1
			},
		),
	})

	defer td.Stop(t, true)

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{2})
	require.NoError(t, err)

	// Get coinbase transaction from block 1
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	coinbaseTx := block1.CoinbaseTx

	// Create a transaction that spends the coinbase
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, coinbaseTx, 2)
	require.NoError(t, err, "Failed to create parent transaction")

	// Send the parent transaction
	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err, "Failed to send parent transaction")

	// Generate a block to confirm the parent transaction
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err)

	// Create a transaction that spends all outputs from parent transaction
	// This should trigger delete at height once confirmed
	spendingTx := bt.NewTx()
	totalSatoshis := uint64(0)
	utxos := make([]*bt.UTXO, 0)

	for i, output := range parentTx.Outputs {
		utxo := &bt.UTXO{
			TxIDHash:      parentTx.TxIDChainHash(),
			Vout:          uint32(i),
			LockingScript: output.LockingScript,
			Satoshis:      output.Satoshis,
		}
		utxos = append(utxos, utxo)
		totalSatoshis += output.Satoshis
	}

	err = spendingTx.FromUTXOs(utxos...)
	require.NoError(t, err, "Error adding UTXOs to transaction")

	// Create output to a new address
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	fee := uint64(1000)
	amountToSend := totalSatoshis - fee
	err = spendingTx.AddP2PKHOutputFromAddress(address.AddressString, amountToSend)
	require.NoError(t, err)

	// Sign and send the spending transaction
	err = spendingTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: td.GetPrivateKey(t)})
	require.NoError(t, err)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, spendingTx)
	require.NoError(t, err)

	// Generate a block to confirm the spending transaction
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err)

	// Verify the parent transaction is marked for deletion
	txMeta, err := td.UtxoStore.Get(td.Ctx, parentTx.TxIDChainHash())
	require.NoError(t, err)
	require.NotNil(t, txMeta)

	// Generate blocks until just before deletion height
	blocksToGenerate := td.Settings.GlobalBlockHeightRetention
	_, err = td.CallRPC(td.Ctx, "generate", []any{blocksToGenerate + 1})
	require.NoError(t, err)

	time.Sleep(10 * time.Second)

	// Verify transaction still exists
	meta, err := td.UtxoStore.Get(td.Ctx, parentTx.TxIDChainHash())

	if err == nil && meta == nil {
		t.Fatal("Get returned no error and nil meta — this is an invalid state")
	}

	//require.Error(t, err)
	// require.Nil(t, meta)
}

func TestSubtreeBlockHeightRetention(t *testing.T) {
	t.Skip()
	const cleanerInterval = 1 * time.Second

	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(settings *settings.Settings) {
				settings.GlobalBlockHeightRetention = 10
			},
		),
	})

	defer td.Stop(t, true)

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	td.MineAndWait(t, 101)

	// Get coinbase transaction from block 1
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	coinbaseTx := block1.CoinbaseTx

	// Create a transaction that spends the coinbase
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, coinbaseTx, 2)
	require.NoError(t, err, "Failed to create parent transaction")

	// Send the parent transaction
	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err, "Failed to send parent transaction")

	// Generate a block to confirm the parent transaction
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err)

	// Get current block height
	currentHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)

	// Create a spending transaction
	spendingTx := bt.NewTx()
	totalSatoshis := uint64(0)
	utxos := make([]*bt.UTXO, 0)

	for i, output := range parentTx.Outputs {
		utxo := &bt.UTXO{
			TxIDHash:      parentTx.TxIDChainHash(),
			Vout:          uint32(i),
			LockingScript: output.LockingScript,
			Satoshis:      output.Satoshis,
		}
		utxos = append(utxos, utxo)
		totalSatoshis += output.Satoshis
	}

	err = spendingTx.FromUTXOs(utxos...)
	require.NoError(t, err, "Error adding UTXOs to transaction")

	// Create output to a new address
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	fee := uint64(1000)
	amountToSend := totalSatoshis - fee
	err = spendingTx.AddP2PKHOutputFromAddress(address.AddressString, amountToSend)
	require.NoError(t, err)

	// Sign and send the spending transaction
	err = spendingTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: td.GetPrivateKey(t)})
	require.NoError(t, err)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, spendingTx)
	require.NoError(t, err)

	// make sure the tx is processed by blockassembly
	delay := td.Settings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		time.Sleep(delay)
	}

	// Generate a block to confirm the spending transaction
	_, err = td.CallRPC(td.Ctx, "generate", []any{1})
	require.NoError(t, err)

	// Verify subtree exists for the block containing our transaction
	block, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, currentHeight+1)
	require.NoError(t, err)

	// can't use block.GetAndValidateSubtrees() as .subtree files have their DAH removed when they are mined
	// err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, nil)

	subtreeHash := block.Subtrees[0]

	// Calculate when subtree should be deleted
	retentionHeight := currentHeight + td.Settings.GlobalBlockHeightRetention - 1

	// Generate blocks until just before retention height
	blocksToGenerate := retentionHeight - currentHeight - 1
	_, err = td.CallRPC(td.Ctx, "generate", []any{blocksToGenerate})
	require.NoError(t, err)

	time.Sleep(cleanerInterval)

	// Verify subtree still exists

	// can't use block.GetAndValidateSubtrees() as .subtree files have their DAH removed when they are mined
	// err = block.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, nil)

	_, err = td.SubtreeStore.Get(td.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)

	// Generate one more block to reach retention height
	td.MineAndWait(t, 300)

	time.Sleep(cleanerInterval)

	// Verify subtree is deleted
	_, err = td.SubtreeStore.Get(td.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.Error(t, err)
}

func TestDeleteAtHeightHappyPath2(t *testing.T) {
	t.Skip()
	// Initialize test daemon with required services
	// init aerospike
	utxoStoreURL, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err, "Failed to setup Aerospike container")
	parsedURL, err := url.Parse(utxoStoreURL)
	require.NoError(t, err, "Failed to parse UTXO store URL")
	t.Cleanup(func() {
		_ = teardown()
	})
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(settings *settings.Settings) {
				settings.GlobalBlockHeightRetention = 1
				settings.UtxoStore.UtxoStore = parsedURL
				settings.GlobalBlockHeightRetention = 1
			},
		),
	})

	defer td.Stop(t, true)

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 5e8),
	)

	childTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 10000),
	)

	parentTxBytes := hex.EncodeToString(parentTx.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []any{parentTxBytes})
	require.NoError(t, err)

	childTxBytes := hex.EncodeToString(childTx.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []any{childTxBytes})
	require.NoError(t, err)

	td.WaitForBlockAssemblyToProcessTx(t, parentTx.TxIDChainHash().String())
	td.WaitForBlockAssemblyToProcessTx(t, childTx.TxIDChainHash().String())

	td.MineAndWait(t, 1)

	_, err = td.UtxoStore.Get(td.Ctx, childTx.TxIDChainHash())
	require.NoError(t, err)

	rawTx := GetRawTx(t, td.UtxoStore, *parentTx.TxIDChainHash(), fields.DeleteAtHeight.String(), fields.BlockHeights.String(), string(fields.Utxos))
	require.NotNil(t, rawTx)

	PrintRawTx(t, "Raw parentTx", rawTx.(map[string]interface{}))

	require.Equal(t, 5, rawTx.(map[string]interface{})[fields.DeleteAtHeight.String()])
}
