//go:build test_all || test_stores || test_utxo || test_aerospike

package aerospike

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	teranode_aerospike "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// go test -v -tags test_aerospike ./test/...

func TestStore_GetTxFromExternalStore(t *testing.T) {
	ctx := context.Background()

	client, _, _, deferFn := initAerospike(t)
	defer deferFn()

	t.Run("TestStore_GetTxFromExternalStore", func(t *testing.T) {
		s := &teranode_aerospike.Store{}
		s.SetExternalStore(memory.New())
		s.SetClient(client)
		s.SetNamespace(aerospikeNamespace)
		s.SetName(aerospikeSet)
		s.SetExternalTxCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](1 * time.Minute))

		// read a sample transaction from testdata and store it in the external store
		f, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		txFromFile, err := bt.NewTxFromString(string(f))
		require.NoError(t, err)

		txHash := txFromFile.TxIDChainHash()
		txBytes := txFromFile.Bytes()

		err = s.GetExternalStore().Set(ctx, txHash.CloneBytes(), txBytes, options.WithFileExtension("tx"))
		require.NoError(t, err)

		// Test fetching the transaction from the external store
		fetchedTx, err := s.GetTxFromExternalStore(ctx, *txHash)
		require.NoError(t, err)
		require.NotNil(t, fetchedTx)
		require.Equal(t, txFromFile.Version, fetchedTx.Version)
		require.Equal(t, txFromFile.LockTime, fetchedTx.LockTime)
		require.Equal(t, len(txFromFile.Inputs), len(fetchedTx.Inputs))
		require.Equal(t, len(txFromFile.Outputs), len(fetchedTx.Outputs))
		require.Equal(t, txFromFile.Outputs[0].Satoshis, fetchedTx.Outputs[0].Satoshis)
		require.Equal(t, txFromFile.Outputs[0].LockingScript, fetchedTx.Outputs[0].LockingScript)
	})

	t.Run("TestStore_GetTxFromExternalStore concurrent", func(t *testing.T) {
		s := &teranode_aerospike.Store{}
		s.SetExternalStore(memory.New())
		s.SetClient(client)
		s.SetNamespace(aerospikeNamespace)
		s.SetName(aerospikeSet)
		s.SetExternalTxCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](1 * time.Minute))

		// read a sample transaction from testdata and store it in the external store
		f, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		txFromFile, err := bt.NewTxFromString(string(f))
		require.NoError(t, err)

		txHash := txFromFile.TxIDChainHash()
		txBytes := txFromFile.Bytes()

		err = s.GetExternalStore().Set(ctx, txHash.CloneBytes(), txBytes, options.WithFileExtension("tx"))
		require.NoError(t, err)

		// Test fetching the transaction from the external store concurrently
		g := errgroup.Group{}
		for i := 0; i < 100; i++ {
			g.Go(func() error {
				fetchedTx, err := s.GetTxFromExternalStore(ctx, *txHash)
				if err != nil {
					return err
				}

				require.NotNil(t, fetchedTx)

				return nil
			})
		}

		err = g.Wait()
		require.NoError(t, err)

		// check how often the external store was accessed
		memStore, ok := s.GetExternalStore().(*memory.Memory)
		require.True(t, ok)
		assert.Equal(t, memStore.Counters["set"], 1)
		assert.Equal(t, memStore.Counters["get"], 1)
	})
}
