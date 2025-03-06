package utxo

import (
	crand "crypto/rand"
	"math/rand/v2"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func CreateTransaction(numberOfOutputs uint64) (*bt.Tx, error) {
	// Create a random 32 byte hash
	randomBytes := make([]byte, 32)
	_, _ = crand.Read(randomBytes)

	hash, err := chainhash.NewHash(randomBytes)
	if err != nil {
		return nil, err
	}

	var (
		inputSatoshis  uint64
		outputSatoshis uint64
	)

	// Loop around until we get a valid input and output amount
	for {
		inputSatoshis = rand.Uint64() //nolint:gosec

		outputSatoshis = inputSatoshis / numberOfOutputs
		if outputSatoshis > 0 {
			break
		}
	}

	newTx := bt.NewTx()

	if err := newTx.From(hash.String(), 0, "", inputSatoshis); err != nil {
		return nil, err
	}

	for i := uint64(0); i < numberOfOutputs; i++ {
		if err := newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", outputSatoshis); err != nil {
			return nil, err
		}
	}

	return newTx, nil
}

func GetSpendingTx(tx *bt.Tx, vOut ...uint32) *bt.Tx {
	newTx := bt.NewTx()

	for _, outIdx := range vOut {
		_ = newTx.From(tx.TxIDChainHash().String(), outIdx, tx.Outputs[outIdx].LockingScript.String(), tx.Outputs[outIdx].Satoshis)
	}

	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64())
	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64())
	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64())
	_ = newTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", &bt.FeeQuote{})

	return newTx
}
