package utils

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/ordishs/gocore"
)

// Transactions for spammer
// GenerateNewValidSingleInputTransaction generates a new valid transaction with a single input and output
// GenerateNewValidMultiInputOutputTransaction generates a new valid transaction with multiple inputs and outputs
// GenerateDoubleSpendTransactions generates two transactions spending same input, i.e. double spend transaction
// GenerateNewManyInputsSingleOutputTransaction generates a new valid transaction with many inputs and a single output
// GenerateNewSingleInputManyOutputsTransaction generates a new valid transaction with a single input and many outputs
// GenerateInvalidTransactionMissingInput generates an invalid transaction by not adding an input
// GenerateInvalidTransactionMissingOutput generates an invalid transaction by not adding an output
// GenerateVeryLargeTransaction generates a very large transaction with 10000 inputs and 10000 outputs

// TxBuildingKeys is a struct that holds the relevant data for generating transactions
type TxBuildingKeys struct {
	coinbaseAddr *bscript.Address
	privateKey   *bec.PrivateKey
	address      *bscript.Address
}

// GetTxBuildingKeysFromConfig is a helper function that gets the transaction building keys from the config
func GetTxBuildingKeysFromConfig(key string) (*TxBuildingKeys, error) {
	var txBuildingKeys TxBuildingKeys

	coinbasePrivKey, _ := gocore.Config().Get(key)

	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to decode Coinbase private key: %v", err)
	}

	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	txBuildingKeys.coinbaseAddr = coinbaseAddr

	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		return nil, errors.NewProcessingError("Failed to generate private key: %v", err)
	}

	txBuildingKeys.privateKey = privateKey

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to create address: %v", err)
	}

	txBuildingKeys.address = address

	return &txBuildingKeys, nil
}

// RequestUtxosForTransaction is a helper function that requests utxos for a transaction
func RequestUtxosForTransaction(ctx context.Context, node *TeranodeTestClient, address *bscript.Address, noOfUtxos int) ([]*bt.UTXO, error) {
	inputUtxos := make([]*bt.UTXO, noOfUtxos)

	requestFundsTx, err := node.CoinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to request funds: %v", err)
	}

	txDistributor := node.DistributorClient

	_, err = txDistributor.SendTransaction(ctx, requestFundsTx)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to send transaction: %v", err)
	}

	for i := 0; i < noOfUtxos; i++ {
		output := requestFundsTx.Outputs[0]

		utxo := &bt.UTXO{
			TxIDHash:      requestFundsTx.TxIDChainHash(),
			Vout:          uint32(0),
			LockingScript: output.LockingScript,
			Satoshis:      output.Satoshis,
		}

		inputUtxos[i] = utxo
	}

	return inputUtxos, nil
}

// GenerateUtxoInputsForTransactions is a helper function that generates utxos for transactions
func GenerateUtxoInputsForTransactions(ctx context.Context, node *TeranodeTestClient, address *bscript.Address, noOfUtxos int) ([]*bt.UTXO, error) {
	// each request will return 100 utxos
	numberOfRequestsToMake := noOfUtxos / 100
	utxos := make([]*bt.UTXO, 0)

	for i := 0; i < numberOfRequestsToMake; i++ {
		utxosForTransaction, err := RequestUtxosForTransaction(ctx, node, address, 100)
		if err != nil {
			return nil, errors.NewProcessingError("Failed to request utxos for transaction: %v", err)
		}

		utxos = append(utxos, utxosForTransaction...)
	}

	return utxos, nil
}

// GenerateNewValidSingleInputTransaction generates a new valid transaction with a single input and output
func GenerateNewValidSingleInputTransaction(node *TeranodeTestClient) (*bt.Tx, error) {
	txBuildingKeys, err := GetTxBuildingKeysFromConfig("coinbase_wallet_private_key")
	if err != nil {
		return nil, errors.NewProcessingError("Failed to get transaction building keys: %v", err)
	}

	ctx := context.Background()

	utxo, err := GenerateUtxoInputsForTransactions(ctx, node, txBuildingKeys.address, 1)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to generate utxo inputs for transaction: %v", err)
	}

	tx := bt.NewTx()

	err = tx.FromUTXOs(utxo[0])
	if err != nil {
		return nil, errors.NewProcessingError("Error adding UTXO to transaction: %s\n", err)
	}

	err = tx.AddP2PKHOutputFromAddress(txBuildingKeys.coinbaseAddr.AddressString, utxo[0].Satoshis)
	if err != nil {
		return nil, errors.NewProcessingError("Error adding output to transaction: %v", err)
	}

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: txBuildingKeys.privateKey})
	if err != nil {
		return nil, errors.NewProcessingError("Error filling transaction inputs: %v", err)
	}

	return tx, nil
}

// GenerateNewValidMultiInputOutputTransaction generates a new valid transaction with 1000 inputs and 1000 outputs
func GenerateNewValidMultiInputOutputTransaction(node *TeranodeTestClient, numberOfInputsOutputs int) (*bt.Tx, error) {
	txBuildingKeys, err := GetTxBuildingKeysFromConfig("coinbase_wallet_private_key")
	if err != nil {
		return nil, errors.NewProcessingError("Failed to get transaction building keys: %v", err)
	}

	ctx := context.Background()

	utxo, err := GenerateUtxoInputsForTransactions(ctx, node, txBuildingKeys.address, numberOfInputsOutputs)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to generate utxo inputs for transaction: %v", err)
	}

	tx := bt.NewTx()

	err = tx.FromUTXOs(utxo...)
	if err != nil {
		return nil, errors.NewProcessingError("Error adding UTXOs to transaction: %s\n", err)
	}

	for i := 0; i < numberOfInputsOutputs; i++ {
		err = tx.PayToAddress(txBuildingKeys.address.AddressString, utxo[i].Satoshis)
		if err != nil {
			return nil, errors.NewProcessingError("Error adding output to transaction: %v", err)
		}
	}

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: txBuildingKeys.privateKey})
	if err != nil {
		return nil, errors.NewProcessingError("Error filling transaction inputs: %v", err)
	}

	return tx, nil
}

// GenerateDoubleSpendTransactions generates two transactions spending same input, i.e. double spend transaction
func GenerateDoubleSpendTransactions(node *TeranodeTestClient) (*[]bt.Tx, error) {
	transactions := make([]bt.Tx, 2)

	txBuildingKeys, err := GetTxBuildingKeysFromConfig("coinbase_wallet_private_key")
	if err != nil {
		return nil, errors.NewProcessingError("Failed to get transaction building keys: %v", err)
	}

	ctx := context.Background()

	utxo, err := GenerateUtxoInputsForTransactions(ctx, node, txBuildingKeys.address, 1)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to generate utxo inputs for transaction: %v", err)
	}

	tx := bt.NewTx()

	err = tx.FromUTXOs(utxo[0])
	if err != nil {
		return nil, errors.NewProcessingError("Error adding UTXO to transaction: %s\n", err)
	}

	err = tx.AddP2PKHOutputFromAddress(txBuildingKeys.coinbaseAddr.AddressString, utxo[0].Satoshis)
	if err != nil {
		return nil, errors.NewProcessingError("Error adding output to transaction: %v", err)
	}

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: txBuildingKeys.privateKey})
	if err != nil {
		return nil, errors.NewProcessingError("Error filling transaction inputs: %v", err)
	}

	txDoubleSpend := bt.NewTx()

	err = txDoubleSpend.FromUTXOs(utxo[0])
	if err != nil {
		return nil, errors.NewProcessingError("Error adding UTXO to transaction: %s\n", err)
	}

	err = txDoubleSpend.AddP2PKHOutputFromAddress(txBuildingKeys.coinbaseAddr.AddressString, utxo[0].Satoshis)
	if err != nil {
		return nil, errors.NewProcessingError("Error adding output to transaction: %v", err)
	}

	err = txDoubleSpend.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: txBuildingKeys.privateKey})
	if err != nil {
		return nil, errors.NewProcessingError("Error filling transaction inputs: %v", err)
	}

	transactions[0] = *tx            //nolint:govet // this needs to be refactored to avoid this
	transactions[1] = *txDoubleSpend //nolint:govet // this needs to be refactored to avoid this

	return &transactions, nil
}

// GenerateNewManyInputsSingleOutputTransaction generates a new valid transaction with many inputs and a single output
func GenerateNewManyInputsSingleOutputTransaction(node *TeranodeTestClient, numberOfInputs int) (*bt.Tx, error) {
	txBuildingKeys, err := GetTxBuildingKeysFromConfig("coinbase_wallet_private_key")
	if err != nil {
		return nil, errors.NewProcessingError("Failed to get transaction building keys: %v", err)
	}

	ctx := context.Background()

	utxo, err := GenerateUtxoInputsForTransactions(ctx, node, txBuildingKeys.address, numberOfInputs)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to generate utxo inputs for transaction: %v", err)
	}

	tx := bt.NewTx()

	err = tx.FromUTXOs(utxo...)
	if err != nil {
		return nil, errors.NewProcessingError("Error adding UTXOs to transaction: %s\n", err)
	}

	// get sum of the all input satoshis
	var totalSatoshis uint64
	for i := 0; i < numberOfInputs; i++ {
		totalSatoshis += utxo[i].Satoshis
	}

	err = tx.PayToAddress(txBuildingKeys.address.AddressString, totalSatoshis)
	if err != nil {
		return nil, errors.NewProcessingError("Error adding output to transaction: %v", err)
	}

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: txBuildingKeys.privateKey})
	if err != nil {
		return nil, errors.NewProcessingError("Error filling transaction inputs: %v", err)
	}

	return tx, nil
}

// GenerateNewSingleInputManyOutputsTransaction generates a new valid transaction with a single input and many outputs
func GenerateNewSingleInputManyOutputsTransaction(node *TeranodeTestClient, numberOfOutputs int) (*bt.Tx, error) {
	tx := bt.NewTx()

	txBuildingKeys, err := GetTxBuildingKeysFromConfig("coinbase_wallet_private_key")
	if err != nil {
		return nil, errors.NewProcessingError("Failed to get transaction building keys: %v", err)
	}

	ctx := context.Background()

	utxo, err := GenerateUtxoInputsForTransactions(ctx, node, txBuildingKeys.address, 1)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to generate utxo inputs for transaction: %v", err)
	}

	err = tx.FromUTXOs(utxo[0])
	if err != nil {
		return nil, errors.NewProcessingError("Error adding UTXO to transaction: %s\n", err)
	}

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: txBuildingKeys.privateKey})
	if err != nil {
		return nil, errors.NewProcessingError("Error filling transaction inputs: %v", err)
	}

	if numberOfOutputs <= 0 {
		return nil, errors.NewProcessingError("Number of outputs must be greater than zero")
	}

	// Safely convert numberOfOutputs to uint64
	numberOfOutputsUint64 := uint64(numberOfOutputs)

	for i := 0; i < numberOfOutputs; i++ {
		err = tx.PayToAddress(txBuildingKeys.address.AddressString, utxo[0].Satoshis/numberOfOutputsUint64)
		if err != nil {
			return nil, errors.NewProcessingError("Error adding output to transaction: %v", err)
		}
	}

	return tx, nil
}

// GenerateInvalidTransactionMissingInput generates an invalid transaction by not adding an input
func GenerateInvalidTransactionMissingInput(node *TeranodeTestClient) (*bt.Tx, error) {
	txBuildingKeys, err := GetTxBuildingKeysFromConfig("coinbase_wallet_private_key")
	if err != nil {
		return nil, errors.NewProcessingError("Failed to get transaction building keys: %v", err)
	}

	tx := bt.NewTx()

	err = tx.AddP2PKHOutputFromAddress(txBuildingKeys.coinbaseAddr.AddressString, 10000)
	if err != nil {
		return nil, errors.NewProcessingError("Error adding output to transaction: %v", err)
	}

	return tx, nil
}

// GenerateInvalidTransactionMissingOutput generates an invalid transaction by not adding an output
func GenerateInvalidTransactionMissingOutput(node *TeranodeTestClient) (*bt.Tx, error) {
	tx := bt.NewTx()

	txBuildingKeys, err := GetTxBuildingKeysFromConfig("coinbase_wallet_private_key")
	if err != nil {
		return nil, errors.NewProcessingError("Failed to get transaction building keys: %v", err)
	}

	ctx := context.Background()

	utxo, err := GenerateUtxoInputsForTransactions(ctx, node, txBuildingKeys.address, 1)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to generate utxo inputs for transaction: %v", err)
	}

	err = tx.FromUTXOs(utxo[0])
	if err != nil {
		return nil, errors.NewProcessingError("Error adding UTXO to transaction: %s\n", err)
	}

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: txBuildingKeys.privateKey})
	if err != nil {
		return nil, errors.NewProcessingError("Error filling transaction inputs: %v", err)
	}

	return tx, nil
}

// GenerateVeryLargeTransaction generates a very large transaction with 10000 inputs and 10000 outputs
func GenerateVeryLargeTransaction(node *TeranodeTestClient) (*bt.Tx, error) {
	tx := bt.NewTx()

	txBuildingKeys, err := GetTxBuildingKeysFromConfig("coinbase_wallet_private_key")
	if err != nil {
		return nil, errors.NewProcessingError("Failed to get transaction building keys: %v", err)
	}

	ctx := context.Background()

	utxo, err := GenerateUtxoInputsForTransactions(ctx, node, txBuildingKeys.address, 10000)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to generate utxo inputs for transaction: %v", err)
	}

	err = tx.FromUTXOs(utxo...)
	if err != nil {
		return nil, errors.NewProcessingError("Error adding UTXOs to transaction: %s\n", err)
	}

	for i := 0; i < 10000; i++ {
		err = tx.PayToAddress(txBuildingKeys.address.AddressString, utxo[i].Satoshis)
		if err != nil {
			return nil, errors.NewProcessingError("Error adding output to transaction: %v", err)
		}
	}

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: txBuildingKeys.privateKey})
	if err != nil {
		return nil, errors.NewProcessingError("Error filling transaction inputs: %v", err)
	}

	return tx, nil
}

// GenerateValidTransaction generates a valid transaction
func GenerateValidTransactionAddToSubtree(hash chainhash.Hash, subtree *subtree.Subtree, fee uint64, sizeInBytes uint64) error {
	if err := subtree.AddNode(hash, fee, sizeInBytes); err != nil {
		return err
	}

	return nil
}

// GenerateDoubleSpendTransaction generates a double spend transaction by adding the same transaction twice
func GenerateDoubleSpendTransactionAddToSubtree(hash chainhash.Hash, subtree *subtree.Subtree, fee uint64, sizeInBytes uint64) error {
	if err := subtree.AddNode(hash, fee, sizeInBytes); err != nil {
		return err
	}

	if err := subtree.AddNode(hash, fee, sizeInBytes); err != nil {
		return err
	}

	return nil
}
