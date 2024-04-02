//go:build manual_tests

package aerospike

import (
	"net/url"
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchRetry(t *testing.T) {

	logger := ulogger.New("test")

	storeUrl, err := url.Parse("aerospike://localhost:3000/test?timeout=1s")
	require.NoError(t, err)

	utxoStore, err := New(logger, storeUrl)
	require.NoError(t, err)

	lockingScript, err := bscript.NewFromASM("OP_RETURN 48656c6c6f20576f726c64")
	require.NoError(t, err)

	tx := bt.NewTx()

	for i := uint64(0); i < 10; i++ {
		tx.AddOutput(&bt.Output{
			Satoshis:      1000 + i,
			LockingScript: lockingScript,
		})
	}

	// Add a duplicate output
	tx.AddOutput(&bt.Output{
		Satoshis:      1001,
		LockingScript: lockingScript,
	})

	err = utxoStore.Store(nil, tx)
	require.NoError(t, err)

}

func TestCollision(t *testing.T) {
	//utxohash_test.go:133: utxoHash1: 75a4d690b598ba9ee2ef43852630fb35d4901b25ec65eb0e765cb9350d87d4f0
	//utxohash_test.go:134: utxoHash2: 625015ff5c2e912b61f1a2174fd3e02ad80d511ada4645bb191f431137efe4f4
	hash1, err := chainhash.NewHashFromStr("75a4d690b598ba9ee2ef43852630fb35d4901b25ec65eb0e765cb9350d87d4f0")
	require.NoError(t, err)
	hash2, err := chainhash.NewHashFromStr("625015ff5c2e912b61f1a2174fd3e02ad80d511ada4645bb191f431137efe4f4")
	require.NoError(t, err)

	key1, err := aerospike.NewKey("utxo-store", "utxo", hash1[:])
	require.NoError(t, err)
	key2, err := aerospike.NewKey("utxo-store", "utxo", hash2[:])
	require.NoError(t, err)

	assert.NotEqualf(t, key1.String(), key2.String(), "keys should not be equal")
}
