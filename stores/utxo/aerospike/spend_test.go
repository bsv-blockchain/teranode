package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_SpendMultiRecord(t *testing.T) {
	client, db, _, deferFn := initAerospike(t)
	defer deferFn()

	ctx := context.Background()

	t.Run("SpendMultiRecord LUA", func(t *testing.T) {
		db.utxoBatchSize = 1

		// clean up the externalStore, if needed
		_ = db.externalStore.Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// create a tx
		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		// mine the tx
		err = db.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, 101)
		require.NoError(t, err)

		utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))
		for vOut, txOut := range tx.Outputs {
			//nolint:gosec
			utxoHashes[vOut], err = util.UTXOHashFromOutput(tx.TxIDChainHash(), txOut, uint32(vOut))
			require.NoError(t, err)

			//nolint:gosec
			keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(vOut/db.utxoBatchSize))
			key, err := aerospike.NewKey(db.namespace, db.setName, keySource)
			require.NoError(t, err)

			// check we created 5 records in aerospike properly
			resp, err := client.Get(nil, key)
			require.NoError(t, err)

			assert.Equal(t, 1, resp.Bins["nrUtxos"])

			if vOut == 0 {
				assert.Equal(t, true, resp.Bins["external"])
				assert.Equal(t, 5, resp.Bins["nrRecords"])
			} else {
				_, ok := resp.Bins["external"]
				require.False(t, ok)
			}
		}

		// check we created the tx in the external store
		exists, err := db.externalStore.Exists(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		require.True(t, exists)

		// check that the TTL is not set on the external store
		ttl, err := db.externalStore.GetTTL(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), ttl)

		keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(0))
		mainRecordKey, err := aerospike.NewKey(db.namespace, db.setName, keySource)
		require.NoError(t, err)

		// spend the utxos, one by one, checking the return values from the lua script
		nrRecords := 5

		for i := 4; i >= 0; i-- {
			//nolint:gosec
			err = db.Spend(ctx, []*utxostore.Spend{{TxID: tx.TxIDChainHash(), Vout: uint32(i), UTXOHash: utxoHashes[i], SpendingTxID: txID}}, 102)
			require.NoError(t, err)

			// give the db time to update the main record
			time.Sleep(100 * time.Millisecond)

			// get nrRecords from main record
			resp, err := client.Get(nil, mainRecordKey)
			require.NoError(t, err)

			nrRecords--
			if nrRecords == 0 {
				// main record check
				assert.Greater(t, resp.Expiration, uint32(0)) // expiration has been set
				assert.Equal(t, 1, resp.Bins["nrRecords"])

				// check the external file ttl has been set
				ttl, err := db.externalStore.GetTTL(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
				require.NoError(t, err)
				assert.Greater(t, ttl, time.Duration(0))
			} else {
				assert.Equal(t, resp.Expiration, uint32(aerospike.TTLDontExpire)) // expiration has been set
				assert.Equal(t, nrRecords, resp.Bins["nrRecords"])
			}
		}
	})
}
