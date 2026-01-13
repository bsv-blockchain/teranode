package smoke

import (
	"fmt"
	"testing"

	aerospikeclient "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	aerospikestore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// GetRawTx retrieves a raw transaction from the UTXO store (Aerospike)
// If specific fields are provided and only one field is requested, returns the value of that field directly
// Otherwise returns a map of all requested fields
func GetRawTx(t *testing.T, store utxo.Store, txID chainhash.Hash, fields ...string) interface{} {
	switch store.(type) {
	case *aerospikestore.Store:
		aeroStore := store.(*aerospikestore.Store)
		namespace := aeroStore.GetNamespace()
		setName := aeroStore.GetName()
		key, err := aerospikeclient.NewKey(namespace, setName, txID.CloneBytes())
		require.NoError(t, err)

		record, err := aeroStore.GetClient().Get(nil, key, fields...)
		require.NoError(t, err)
		require.NotNil(t, record)

		// If only one field was requested, return its value directly
		if len(fields) == 1 {
			return record.Bins[fields[0]]
		}

		// Otherwise return the full map
		m := make(map[string]interface{})
		for k, v := range record.Bins {
			m[k] = v
		}
		return m
	default:
		t.Logf("Unsupported store type: %T", store)
		t.FailNow()
	}
	return nil
}

// PrintRawTx prints a raw transaction in a readable format
func PrintRawTx(t *testing.T, label string, rawTx map[string]interface{}) {
	fmt.Printf("%s:\n", label)
	for k, v := range rawTx {
		switch k {
		case fields.Utxos.String():
			printUtxos(v)
		case fields.TxID.String():
			printTxID(v)
		default:
			var b []byte
			if arr, ok := v.([]interface{}); ok {
				printArray(k, arr)
			} else if b, ok = v.([]byte); ok {
				fmt.Printf("%-14s: %x\n", k, b)
			} else {
				fmt.Printf("%-14s: %v\n", k, v)
			}
		}
	}
}

func printArray(name string, value interface{}) {
	fmt.Printf("%-14s:", name)

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	arr, ok := value.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(arr) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range arr {
		if b, found := item.([]byte); found {
			if i == 0 {
				fmt.Printf(" %-5d : %x\n", i, b)
			} else {
				fmt.Printf("              : %-5d : %x\n", i, b)
			}
		} else {
			if i == 0 {
				fmt.Printf(" %-5d : %v\n", i, item)
			} else {
				fmt.Printf("              : %-5d : %v\n", i, item)
			}
		}
	}
}

func printUtxos(value interface{}) {
	fmt.Printf("%-14s:", fields.Utxos.String())

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	arr, ok := value.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(arr) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range arr {
		if b, found := item.([]byte); found {
			// Format the hex string with spaces after byte 32 and 64
			hexStr := formatUtxoHex(b)
			if i == 0 {
				fmt.Printf(" %-5d : %s\n", i, hexStr)
			} else {
				fmt.Printf("              : %-5d : %s\n", i, hexStr)
			}
		} else {
			if i == 0 {
				fmt.Printf(" %-5d : %v\n", i, item)
			} else {
				fmt.Printf("              : %-5d : %v\n", i, item)
			}
		}
	}
}

func formatUtxoHex(b []byte) string {
	if len(b) == 32 {
		// Reverse 32-byte hash from little-endian to big-endian
		reversed := make([]byte, 32)
		for i := 0; i < 32; i++ {
			reversed[i] = b[31-i]
		}
		return fmt.Sprintf("%x", reversed)
	}

	if len(b) < 32 {
		return fmt.Sprintf("%x", b)
	}

	// For values > 32 bytes, reverse the first 32 bytes (hash part)
	reversed := make([]byte, 32)
	for i := 0; i < 32; i++ {
		reversed[i] = b[31-i]
	}
	result := fmt.Sprintf("%x", reversed)

	if len(b) > 32 && len(b) <= 64 {
		result += " " + fmt.Sprintf("%x", b[32:])
	} else if len(b) > 64 {
		result += " " + fmt.Sprintf("%x", b[32:64]) + " " + fmt.Sprintf("%x", b[64:])
	}

	return result
}

func printTxID(value interface{}) {
	fmt.Printf("%-14s:", fields.TxID.String())

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	b, ok := value.([]byte)
	if !ok {
		fmt.Printf(" <not bytes>\n")
		return
	}

	if len(b) != 32 {
		fmt.Printf(" %x (invalid length: %d bytes)\n", b, len(b))
		return
	}

	// Reverse the 32-byte hash from little-endian to big-endian
	reversed := make([]byte, 32)
	for i := 0; i < 32; i++ {
		reversed[i] = b[31-i]
	}
	fmt.Printf(" %x\n", reversed)
}
