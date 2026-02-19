package transactions

import (
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCoinbase(t *testing.T) {
	_, pubKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	tx := Create(t,
		WithCoinbaseData(100, "/Test miner/"),
		WithP2PKHOutputs(1, 50*100_000_000, pubKey),
	)

	assert.Equal(t, 1, len(tx.Inputs))
	assert.Equal(t, 1, len(tx.Outputs))
	assert.Equal(t, uint64(50*100_000_000), tx.Outputs[0].Satoshis)

	assert.Equal(t, "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff10036400002f54657374206d696e65722fffffffff0100f2052a010000001976a91433bce29781f73091a159baa346334ab17bb35cd288ac00000000", hex.EncodeToString(tx.Bytes()))
}

func TestCreateTransaction(t *testing.T) {
	privKey, pubKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	baseTx1 := Create(t, WithCoinbaseData(100, "/Test miner/"), WithP2PKHOutputs(1, 1e8, pubKey))
	baseTx2 := Create(t, WithCoinbaseData(101, "/Test miner/"), WithP2PKHOutputs(1, 2e8, pubKey))

	t.Run("WithSingleInputSingleOutput", func(t *testing.T) {
		tx := Create(t,
			WithPrivateKey(privKey),
			WithInput(baseTx1, 0),
			WithP2PKHOutputs(1, 1000),
		)

		assert.Equal(t, 1, len(tx.Inputs))
		assert.Equal(t, baseTx1.TxID(), util.ReverseAndHexEncodeSlice(tx.Inputs[0].PreviousTxID()))
		assert.Equal(t, 1, len(tx.Outputs))
		assert.Equal(t, uint64(1000), tx.Outputs[0].Satoshis)
	})

	t.Run("WithSingleInputMultipleOutputs", func(t *testing.T) {
		tx := Create(t,
			WithPrivateKey(privKey),
			WithInput(baseTx1, 0),
			WithP2PKHOutputs(1, 1000),
			WithOpReturnData([]byte("test")),
			WithP2PKHOutputs(1, 1000),
			WithP2PKHOutputs(2, 500),
		)

		assert.Equal(t, 1, len(tx.Inputs))
		assert.Equal(t, baseTx1.TxID(), util.ReverseAndHexEncodeSlice(tx.Inputs[0].PreviousTxID()))
		assert.Equal(t, 5, len(tx.Outputs))
		assert.Equal(t, uint64(1000), tx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(0), tx.Outputs[1].Satoshis)
		assert.Equal(t, uint64(1000), tx.Outputs[2].Satoshis)
		assert.Equal(t, uint64(500), tx.Outputs[3].Satoshis)
		assert.Equal(t, uint64(500), tx.Outputs[4].Satoshis)
	})

	t.Run("WithMultipleInputsMultipleOutputs", func(t *testing.T) {
		tx := Create(t,
			WithPrivateKey(privKey),
			WithInput(baseTx1, 0),
			WithInput(baseTx2, 0),
			WithP2PKHOutputs(1, 1000),
			WithOpReturnData([]byte("test")),
		)

		assert.Equal(t, 2, len(tx.Inputs))
		assert.Equal(t, baseTx1.TxID(), util.ReverseAndHexEncodeSlice(tx.Inputs[0].PreviousTxID()))
		assert.Equal(t, baseTx2.TxID(), util.ReverseAndHexEncodeSlice(tx.Inputs[1].PreviousTxID()))
		assert.Equal(t, 2, len(tx.Outputs))
		assert.Equal(t, uint64(1000), tx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(0), tx.Outputs[1].Satoshis)
	})

	t.Run("OversizedScript", func(t *testing.T) {
		maxScriptSize := 10000

		tx := Create(t,
			WithPrivateKey(privKey),
			WithInput(baseTx1, 0),
			WithP2PKHOutputs(1, 10000),
			WithOpReturnSize(maxScriptSize),
		)

		assert.Equal(t, 1, len(tx.Inputs))
		assert.Equal(t, baseTx1.TxID(), util.ReverseAndHexEncodeSlice(tx.Inputs[0].PreviousTxID()))
		assert.Equal(t, 2, len(tx.Outputs))
		assert.Equal(t, uint64(10000), tx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(0), tx.Outputs[1].Satoshis)
		assert.Greater(t, len(tx.Outputs[1].LockingScript.Bytes()), maxScriptSize)
	})

	t.Run("WithCustomOutputs", func(t *testing.T) {
		customPrivKey, err := bec.NewPrivateKey()
		require.NoError(t, err)

		customPubKey := customPrivKey.PubKey()

		parentTx := Create(t,
			WithPrivateKey(customPrivKey),
			WithInput(baseTx1, 0),
			WithP2PKHOutputs(2, 10_000, customPubKey))

		assert.Equal(t, 2, len(parentTx.Outputs))
		assert.Equal(t, uint64(10_000), parentTx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(10_000), parentTx.Outputs[1].Satoshis)

		childTx := Create(t,
			WithPrivateKey(customPrivKey),
			WithInput(parentTx, 0),
			WithP2PKHOutputs(2, 2000, customPubKey))

		assert.Equal(t, 2, len(childTx.Outputs))
		assert.Equal(t, uint64(2000), childTx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(2000), childTx.Outputs[1].Satoshis)
	})

	t.Run("WithCustomScript", func(t *testing.T) {
		customScript, _ := bscript.NewFromASM("OP_RETURN 48656C6C6F20576F726C64")

		tx := Create(t,
			WithPrivateKey(privKey),
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
