// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
package utxo

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/bitcoin-sv/teranode/util"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
)

// CalculateUtxoStatus determines the status of a UTXO based on its spending state
// and coinbase maturity requirements.
//
// Parameters:
//   - spendingTxID: The ID of the transaction that spends this UTXO, or nil if unspent
//   - coinbaseSpendingHeight: The height at which a coinbase UTXO becomes spendable
//   - blockHeight: The current block height
//
// Returns:
//   - Status: The calculated UTXO status
func CalculateUtxoStatus(spendingData *spend.SpendingData, coinbaseSpendingHeight uint32, blockHeight uint32) Status {
	status := Status_OK

	if spendingData != nil {
		status = Status_SPENT
	} else if coinbaseSpendingHeight > 0 && coinbaseSpendingHeight > blockHeight {
		status = Status_LOCKED
	}

	return status
}

// CalculateUtxoStatus2 is a simplified version of CalculateUtxoStatus that only considers
// the spending state of a UTXO, ignoring coinbase maturity.
func CalculateUtxoStatus2(spendingData *spend.SpendingData) Status {
	status := Status_OK

	if spendingData != nil {
		status = Status_SPENT
	}

	return status
}

// GetFeesAndUtxoHashes calculates the total fees and generates UTXO hashes for a transaction.
// Returns an error if the transaction is not properly extended with input values.
//
// Parameters:
//   - ctx: Context for cancellation
//   - tx: The transaction to analyze
//   - blockHeight: Current block height
//
// Returns:
//   - uint64: Total transaction fees
//   - []*chainhash.Hash: UTXO hashes for each output
//   - error: Any error encountered during processing
func GetFeesAndUtxoHashes(ctx context.Context, tx *bt.Tx, blockHeight uint32) (uint64, []*chainhash.Hash, error) {
	if !tx.IsExtended() && !tx.IsCoinbase() {
		return 0, nil, errors.NewProcessingError("tx is not extended")
	}

	var fees uint64

	utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))

	if !tx.IsCoinbase() {
		for _, input := range tx.Inputs {
			fees += input.PreviousTxSatoshis
		}
	}

	txid := tx.TxIDChainHash()

	for i, output := range tx.Outputs {
		select {
		case <-ctx.Done():
			return fees, utxoHashes, errors.NewProcessingError("[GetFeesAndUtxoHashes] timeout - managed to prepare %d of %d", i, len(tx.Outputs))
		default:
			if !tx.IsCoinbase() {
				fees -= output.Satoshis
			}

			iUint32, err := safeconversion.IntToUint32(i)
			if err != nil {
				return 0, nil, errors.NewProcessingError("failed to convert i", err)
			}

			utxoHash, utxoErr := util.UTXOHashFromOutput(txid, output, iUint32)
			if utxoErr != nil {
				return 0, nil, errors.NewProcessingError("error getting output utxo hash: %s", utxoErr)
			}

			utxoHashes[i] = utxoHash
		}
	}

	return fees, utxoHashes, nil
}

// GetUtxoHashes returns the UTXO hashes for the outputs of a transaction.
// If a txHash is provided, it will be used instead of calculating the transaction's hash.
//
// Parameters:
//   - tx: The transaction to analyze
//   - txHash: Optional pre-calculated transaction hash to use instead of computing one
//
// Returns:
//   - []*chainhash.Hash: Array of UTXO hashes, one for each output
//   - error: Any error encountered during processing
func GetUtxoHashes(tx *bt.Tx, txHash ...*chainhash.Hash) ([]*chainhash.Hash, error) {
	var txChainHash *chainhash.Hash
	if len(txHash) > 0 {
		txChainHash = txHash[0]
	} else {
		txChainHash = tx.TxIDChainHash()
	}

	utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))

	for i, output := range tx.Outputs {
		if output != nil {
			iUint32, err := safeconversion.IntToUint32(i)
			if err != nil {
				return nil, errors.NewProcessingError("failed to convert i", err)
			}

			utxoHash, utxoErr := util.UTXOHashFromOutput(txChainHash, output, iUint32)
			if utxoErr != nil {
				return nil, errors.NewProcessingError("error getting output utxo hash: %s", utxoErr)
			}

			utxoHashes[i] = utxoHash
		}
	}

	return utxoHashes, nil
}

// ShouldStoreOutputAsUTXO determines if a transaction output should be stored as a UTXO.
// Returns true if the output has a non-zero value or is not an OP_RETURN output.
func ShouldStoreOutputAsUTXO(isCoinbase bool, output *bt.Output, blockHeight uint32) bool {
	if output.Satoshis > 0 {
		// if the output has a non-zero satoshis value, it should be stored as a UTXO
		return true
	}

	// we only store outputs with a zero satoshis value if they are not an OP_RETURN or OP_FALSE OP_RETURN
	b := []byte(*output.LockingScript)
	opReturn := len(b) > 0 && b[0] == bscript.OpRETURN
	opFalseOpReturn := len(b) > 1 && b[0] == bscript.OpFALSE && b[1] == bscript.OpRETURN

	return !(opReturn || opFalseOpReturn)
}

// GetSpends creates Spend objects for all inputs of a transaction.
// Each Spend represents a UTXO being consumed by the transaction.
//
// Parameters:
//   - tx: The transaction to analyze for input spending
//
// Returns:
//   - []*Spend: Array of Spend objects, one for each transaction input
//   - error: Any error encountered during processing
func GetSpends(tx *bt.Tx) (spends []*Spend, err error) {
	var (
		txIDChainHash = tx.TxIDChainHash()
		hash          *chainhash.Hash
	)

	spends = make([]*Spend, 0, len(tx.Inputs))

	for i, input := range tx.Inputs {
		hash, err = util.UTXOHashFromInput(input)
		if err != nil {
			return nil, errors.NewProcessingError("error getting input utxo hash", err)
		}

		// v.logger.Debugf("spending utxo %s:%d -> %s", input.PreviousTxIDChainHash().String(), input.PreviousTxOutIndex, hash.String())
		spends = append(spends, &Spend{
			TxID:         input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     hash,
			SpendingData: spend.NewSpendingData(txIDChainHash, i), // Use the current index as the Vin
		})
	}

	return spends, nil
}
