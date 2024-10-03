package aerospike

import (
	"context"
	"os"
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_getBinsToStore(t *testing.T) {
	s := &Store{
		utxoBatchSize: 100,
	}

	t.Run("TestStore_getBinsToStore empty", func(t *testing.T) {
		tx := &bt.Tx{}
		bins, err := s.getBinsToStore(tx, 0, nil, false, tx.TxIDChainHash(), false)
		require.Error(t, err)
		require.Nil(t, bins)
	})

	t.Run("TestStore_getBinsToStore", func(t *testing.T) {
		initPrometheusMetrics()

		// read hex file from os
		txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		bins, err := s.getBinsToStore(tx, 0, nil, false, tx.TxIDChainHash(), false)
		require.NoError(t, err)
		require.NotNil(t, bins)

		// check the bins
		require.Equal(t, 1, len(bins))

		utxos, _ := utxo.GetUtxoHashes(tx)

		var blockIDs []uint32

		expectedBinValues := map[string]aerospike.Value{
			"version":      aerospike.NewIntegerValue(int(tx.Version)),
			"locktime":     aerospike.NewIntegerValue(int(tx.LockTime)),
			"fee":          aerospike.NewIntegerValue(187),
			"sizeInBytes":  aerospike.NewIntegerValue(tx.Size()),
			"extendedSize": aerospike.NewIntegerValue(len(tx.ExtendedBytes())),
			"spentUtxos":   aerospike.NewIntegerValue(0),
			"nrUtxos":      aerospike.NewIntegerValue(2),
			"nrRecords":    aerospike.NewIntegerValue(1),
			"isCoinbase":   aerospike.BoolValue(false),
			"utxos": aerospike.NewListValue([]interface{}{
				aerospike.BytesValue(utxos[0].CloneBytes()),
				aerospike.BytesValue(utxos[1].CloneBytes()),
			}),
			"inputs": aerospike.NewListValue([]interface{}{
				tx.Inputs[0].ExtendedBytes(false),
				tx.Inputs[1].ExtendedBytes(false),
			}),
			"outputs": aerospike.NewListValue([]interface{}{
				tx.Outputs[0].Bytes(),
				tx.Outputs[1].Bytes(),
			}),
			"blockIDs": aerospike.NewValue(blockIDs),
		}

		// check the bin values
		for _, v := range bins[0] {
			if _, ok := expectedBinValues[v.Name]; ok {
				assert.Equal(t, expectedBinValues[v.Name], v.Value)
			} else {
				t.Errorf("unexpected bin name: %s", v.Name)
			}
		}
	})

	// to run this HUGE tx test, download the tx and put it in the testdata folder
	t.Run("TestStore_getBinsToStore very large", func(t *testing.T) {
		t.Skip("Skipping test with missing tx.")

		// read hex file from os
		txHex, err := os.ReadFile("testdata/337e211af7bcf90470ead4f92910b2990b635dcab8414bf5849f3b1e25800b0c_extended.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		// external should be set by the aerospike create function for huge txs
		external := len(tx.ExtendedBytes()) > MaxTxSizeInStoreInBytes

		bins, err := s.getBinsToStore(tx, 0, nil, external, tx.TxIDChainHash(), false)
		require.NoError(t, err)
		require.NotNil(t, bins)
	})
}

func BenchmarkStore_Create(b *testing.B) {
	initPrometheusMetrics()

	// read hex file from os
	txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
	require.NoError(b, err)

	tx, err := bt.NewTxFromString(string(txHex))
	require.NoError(b, err)

	s := &Store{
		utxoBatchSize: 100,
	}

	sendStoreBatch := func(batch []*batchStoreItem) {
		// do nothing
		for _, item := range batch {
			item.done <- nil
		}
	}
	s.storeBatcher = batcher.New[batchStoreItem](100, 1, sendStoreBatch, true)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = s.Create(context.Background(), tx, 0)
		require.NoError(b, err)
	}
}
