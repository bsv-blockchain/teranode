package daemon

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateTransaction tests the CreateTransaction method of the TestDaemon struct.
func TestCreateTransaction(t *testing.T) {
	privKey, pubKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	td := TestDaemon{privKey: privKey}

	baseTx1 := td.CreateTransactionWithOptions(t, transactions.WithCoinbaseData(100, "/Test miner/"), transactions.WithP2PKHOutputs(1, 1e8))
	baseTx2 := td.CreateTransactionWithOptions(t, transactions.WithCoinbaseData(101, "/Test miner/"), transactions.WithP2PKHOutputs(1, 2e8))

	t.Run("WithSingleInputSingleOutput", func(t *testing.T) {
		tx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(baseTx1, 0),
			transactions.WithP2PKHOutputs(1, 1000),
		)

		assert.Equal(t, 1, len(tx.Inputs))
		assert.Equal(t, baseTx1.TxID(), util.ReverseAndHexEncodeSlice(tx.Inputs[0].PreviousTxID()))
		assert.Equal(t, 1, len(tx.Outputs))
		assert.Equal(t, uint64(1000), tx.Outputs[0].Satoshis)
	})

	t.Run("WithSingleInputMultipleOutputs", func(t *testing.T) {
		tx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(baseTx1, 0),
			transactions.WithP2PKHOutputs(1, 1000),
			transactions.WithOpReturnData([]byte("test")),
			transactions.WithP2PKHOutputs(1, 1000),
			transactions.WithP2PKHOutputs(2, 500),
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
		tx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(baseTx1, 0),
			transactions.WithInput(baseTx2, 0),
			transactions.WithP2PKHOutputs(1, 1000),
			transactions.WithOpReturnData([]byte("test")),
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

		tx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(baseTx1, 0),
			transactions.WithP2PKHOutputs(1, 10000, pubKey),
			transactions.WithOpReturnSize(maxScriptSize),
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

		parentTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(baseTx1, 0),
			transactions.WithP2PKHOutputs(2, 100_000, customPubKey))

		assert.Equal(t, 2, len(parentTx.Outputs))
		assert.Equal(t, uint64(100_000), parentTx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(100_000), parentTx.Outputs[1].Satoshis)

		childTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(parentTx, 0, customPrivKey),
			transactions.WithP2PKHOutputs(2, 2000, customPubKey))

		assert.Equal(t, 2, len(childTx.Outputs))
		assert.Equal(t, uint64(2000), childTx.Outputs[0].Satoshis)
		assert.Equal(t, uint64(2000), childTx.Outputs[1].Satoshis)
	})

	t.Run("WithCustomScript", func(t *testing.T) {
		customScript, _ := bscript.NewFromASM("OP_RETURN 48656C6C6F20576F726C64")

		tx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(baseTx1, 0),
			transactions.WithOutput(0, customScript),
			transactions.WithOpReturnData([]byte("test")),
		)

		assert.Equal(t, 2, len(tx.Outputs))
		assert.Equal(t, uint64(0), tx.Outputs[0].Satoshis)
		assert.Equal(t, customScript, tx.Outputs[0].LockingScript)
		assert.Equal(t, uint64(0), tx.Outputs[1].Satoshis)
		// script should be 0x06(script size) + OP_FALSE + OP_RETURN + <test>
		assert.Equal(t, []byte{0x06, 0x00, 0x6a, 0x74, 0x65, 0x73, 0x74}, tx.Outputs[1].LockingScript.Bytes())
	})
}

// TestGetPortFromString tests the getPortFromString function.
func TestGetPortFromString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{
			name:     "empty string",
			input:    "",
			expected: 0,
		},
		{
			name:     "missing colon",
			input:    "localhost",
			expected: 0,
		},
		{
			name:     "non-numeric port",
			input:    "localhost:abc",
			expected: 0,
		},
		{
			name:     "valid port",
			input:    "localhost:8080",
			expected: 8080,
		},
		{
			name:     "colon with empty port",
			input:    "localhost:",
			expected: 0,
		},
		{
			name:     "multiple colons - IPv6",
			input:    "[::1]:443",
			expected: 443,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := getPortFromString(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

// TestGetPortFromURL tests the getPortFromURL function.
func TestGetPortFromURL(t *testing.T) {
	tests := []struct {
		name     string
		inputURL string
		expected int
	}{
		{
			name:     "no port in URL",
			inputURL: "http://localhost",
			expected: 0,
		},
		{
			name:     "valid numeric port",
			inputURL: "http://localhost:8080",
			expected: 8080,
		},
		{
			name:     "empty port",
			inputURL: "http://localhost:",
			expected: 0,
		},
		{
			name:     "IPv6 with valid port",
			inputURL: "http://[::1]:443",
			expected: 443,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(tc.inputURL)
			require.NoError(t, err)

			result := getPortFromURL(u)
			require.Equal(t, tc.expected, result)
		})
	}
}

// TestLogJSON tests the LogJSON method of the TestDaemon struct.
func TestLogJSON(t *testing.T) {
	td := &TestDaemon{} // create a new TestDaemon instance

	t.Run("valid_data", func(t *testing.T) {
		data := map[string]interface{}{
			"foo": "bar",
			"baz": 123,
		}
		td.LogJSON(t, "SampleData", data)
	})
}

// TestGetPrivateKey tests the GetPrivateKey method of the TestDaemon struct.
func TestGetPrivateKey(t *testing.T) {
	expected := &bec.PrivateKey{} // this would typically be initialized with a real key in practice
	td := &TestDaemon{privKey: expected}

	actual := td.GetPrivateKey(t)
	require.Equal(t, expected, actual)
}

// TestJSONError_Error tests the Error method of the JSONError struct.
func TestJSONError_Error(t *testing.T) {
	je := &JSONError{
		Code:    -32601,
		Message: "Method not found",
	}

	expected := "code: -32601, message: Method not found"
	actual := je.Error()

	require.Equal(t, expected, actual)
}
