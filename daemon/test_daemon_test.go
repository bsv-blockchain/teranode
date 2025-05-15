package daemon

import (
	"testing"

	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/ordishs/go-utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTransaction(t *testing.T) {
	privKey, pubKey := bec.PrivKeyFromBytes(bec.S256(), []byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	td := TestDaemon{privKey: privKey}

	baseTx1 := td.CreateTransactionWithOptions(t, WithP2PKHOutputs(1, 1000), WithSkipCheck())
	baseTx2 := td.CreateTransactionWithOptions(t, WithP2PKHOutputs(1, 2000), WithSkipCheck())

	t.Run("WithSingleInputSingleOutput", func(t *testing.T) {
		tx := td.CreateTransactionWithOptions(t,
			WithInput(baseTx1, 0),
			WithP2PKHOutputs(1, 1000),
		)

		assert.Equal(t, 1, len(tx.Inputs))
		assert.Equal(t, baseTx1.TxID(), utils.ReverseAndHexEncodeSlice(tx.Inputs[0].PreviousTxID()))
		assert.Equal(t, 1, len(tx.Outputs))
		assert.Equal(t, uint64(1000), tx.Outputs[0].Satoshis)
	})

	t.Run("WithSingleInputMultipleOutputs", func(t *testing.T) {
		tx := td.CreateTransactionWithOptions(t,
			WithInput(baseTx1, 0),
			WithP2PKHOutputs(1, 1000),
			WithOpReturnData([]byte("test")),
			WithP2PKHOutputs(1, 1000),
			WithP2PKHOutputs(2, 500),
		)

		assert.Equal(t, 1, len(tx.Inputs))
		assert.Equal(t, baseTx1.TxID(), utils.ReverseAndHexEncodeSlice(tx.Inputs[0].PreviousTxID()))
		assert.Equal(t, 5, len(tx.Outputs))
		assert.Equal(t, uint64(1000), tx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(0), tx.Outputs[1].Satoshis)
		assert.Equal(t, uint64(1000), tx.Outputs[2].Satoshis)
		assert.Equal(t, uint64(500), tx.Outputs[3].Satoshis)
		assert.Equal(t, uint64(500), tx.Outputs[4].Satoshis)
	})

	t.Run("WithMultipleInputsMultipleOutputs", func(t *testing.T) {
		tx := td.CreateTransactionWithOptions(t,
			WithInput(baseTx1, 0),
			WithInput(baseTx2, 0),
			WithP2PKHOutputs(1, 1000),
			WithOpReturnData([]byte("test")),
		)

		assert.Equal(t, 2, len(tx.Inputs))
		assert.Equal(t, baseTx1.TxID(), utils.ReverseAndHexEncodeSlice(tx.Inputs[0].PreviousTxID()))
		assert.Equal(t, baseTx2.TxID(), utils.ReverseAndHexEncodeSlice(tx.Inputs[1].PreviousTxID()))
		assert.Equal(t, 2, len(tx.Outputs))
		assert.Equal(t, uint64(1000), tx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(0), tx.Outputs[1].Satoshis)
	})

	t.Run("OversizedScript", func(t *testing.T) {
		maxScriptSize := 10000

		tx := td.CreateTransactionWithOptions(t,
			WithInput(baseTx1, 0),
			WithP2PKHOutputs(1, 10000, pubKey),
			WithOpReturnSize(maxScriptSize),
		)

		assert.Equal(t, 1, len(tx.Inputs))
		assert.Equal(t, baseTx1.TxID(), utils.ReverseAndHexEncodeSlice(tx.Inputs[0].PreviousTxID()))
		assert.Equal(t, 2, len(tx.Outputs))
		assert.Equal(t, uint64(10000), tx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(0), tx.Outputs[1].Satoshis)
		assert.Greater(t, len(tx.Outputs[1].LockingScript.Bytes()), maxScriptSize)
	})

	t.Run("WithCustomOutputs", func(t *testing.T) {
		customPrivKey, err := bec.NewPrivateKey(bec.S256())
		require.NoError(t, err)

		customPubKey := customPrivKey.PubKey()

		parentTx := td.CreateTransactionWithOptions(t,
			WithInput(baseTx1, 0),
			WithP2PKHOutputs(2, 2000, customPubKey))

		assert.Equal(t, 2, len(parentTx.Outputs))
		assert.Equal(t, uint64(2000), parentTx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(2000), parentTx.Outputs[1].Satoshis)

		childTx := td.CreateTransactionWithOptions(t,
			WithInput(parentTx, 0, customPrivKey),
			WithP2PKHOutputs(2, 2000, customPubKey))

		assert.Equal(t, 2, len(childTx.Outputs))
		assert.Equal(t, uint64(2000), childTx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(2000), childTx.Outputs[1].Satoshis)
	})

	t.Run("WithCustomScript", func(t *testing.T) {
		customScript, _ := bscript.NewFromASM("OP_RETURN 48656C6C6F20576F726C64")

		tx := td.CreateTransactionWithOptions(t,
			WithInput(baseTx1, 0),
			WithOutput(0, customScript),
			WithOpReturnData([]byte("test")),
		)

		assert.Equal(t, 2, len(tx.Outputs))
		assert.Equal(t, uint64(0), tx.Outputs[0].Satoshis)
		assert.Equal(t, customScript, tx.Outputs[0].LockingScript)
		assert.Equal(t, uint64(0), tx.Outputs[1].Satoshis)
		// script should be 0x06(script size) + OP_FALSE + OP_RETURN + <test>
		assert.Equal(t, []byte{0x06, 0x00, 0x6a, 0x74, 0x65, 0x73, 0x74}, tx.Outputs[1].LockingScript.Bytes())
	})
}
