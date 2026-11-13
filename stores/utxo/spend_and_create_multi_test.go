package utxo

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// DefaultSpendAndCreateMulti's contract tests run on real stores, from the
// shared suite in stores/utxo/tests (spend_and_create_multi.go and
// spend_and_create_multi_contract.go), wired into the sqlitememory, Postgres and
// Aerospike packages. What stays here is store-free: checkSpendAndCreateMultiList's
// parent bookkeeping, which no store outcome can observe.

// multiTx builds a transaction spending the given outpoints, with nOutputs
// outputs. Each call with a distinct seed gives a distinct txid.
func multiTx(t *testing.T, seed uint32, nOutputs int, spends ...Outpoint) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = seed

	for _, op := range spends {
		in := &bt.Input{PreviousTxOutIndex: op.Vout, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
		txid := op.TxID
		require.NoError(t, in.PreviousTxIDAdd(&txid))
		tx.Inputs = append(tx.Inputs, in)
	}

	for i := 0; i < nOutputs; i++ {
		tx.AddOutput(&bt.Output{Satoshis: uint64(1000 + i), LockingScript: bscript.NewFromBytes([]byte{0x51})})
	}

	return tx
}

func outsideOutpoint(n byte) Outpoint {
	return Outpoint{TxID: chainhash.Hash{0xee, n}, Vout: 0}
}

func outOf(tx *bt.Tx, vout uint32) Outpoint {
	return Outpoint{TxID: *tx.TxIDChainHash(), Vout: vout}
}
