package pruner

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

// TestPrunerSkipsUnminedRecords verifies the pruner never deletes a record that is
// currently unmined (unminedSince set) unless it is conflicting, including the
// pagination records of a protected large transaction.
func TestPrunerSkipsUnminedRecords(t *testing.T) {
	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	test.SkipIfContainerUnavailable(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	namespace, set := "test", "test"

	service, err := NewService(createTestSettings(), Options{
		Logger:        ulogger.New("test"),
		Client:        client,
		ExternalStore: memory.New(),
		Namespace:     namespace,
		Set:           set,
		IndexWaiter:   &MockIndexWaiter{Client: client, Namespace: namespace, Set: set},
	})
	require.NoError(t, err)

	service.Start(ctx)
	time.Sleep(2 * time.Second) // wait for the delete_at_height index

	const pruneHeight = 5

	writePolicy := aerospike.NewWritePolicy(0, 0)

	put := func(txID chainhash.Hash, num uint32, bins aerospike.BinMap) *aerospike.Key {
		key, kerr := aerospike.NewKey(namespace, set, uaerospike.CalculateKeySourceInternal(&txID, num))
		require.NoError(t, kerr)

		bins[fields.TxID.String()] = txID.CloneBytes()
		bins[fields.DeleteAtHeight.String()] = pruneHeight
		require.NoError(t, client.Put(writePolicy, key, bins))

		return key
	}

	exists := func(key *aerospike.Key) bool {
		ok, eerr := client.Exists(nil, key)
		require.NoError(t, eerr)

		return ok
	}

	unmined := fields.UnminedSince.String()
	conflicting := fields.Conflicting.String()
	totalExtra := fields.TotalExtraRecs.String()

	unminedKey := put(chainhash.HashH([]byte("unmined")), 0, aerospike.BinMap{unmined: 3, totalExtra: 0})
	conflictingKey := put(chainhash.HashH([]byte("conflicting")), 0, aerospike.BinMap{unmined: 3, conflicting: true, totalExtra: 0})
	minedKey := put(chainhash.HashH([]byte("mined")), 0, aerospike.BinMap{totalExtra: 0})

	// a large tx: master plus one pagination record, which carries its own DAH but no unminedSince
	unminedLarge := chainhash.HashH([]byte("unmined-large"))
	unminedLargeMaster := put(unminedLarge, 0, aerospike.BinMap{unmined: 3, totalExtra: 1})
	unminedLargePage := put(unminedLarge, 1, aerospike.BinMap{})

	minedLarge := chainhash.HashH([]byte("mined-large"))
	minedLargeMaster := put(minedLarge, 0, aerospike.BinMap{totalExtra: 1})
	minedLargePage := put(minedLarge, 1, aerospike.BinMap{})

	pruneCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err = service.Prune(pruneCtx, pruneHeight, "<test-hash>")
	require.NoError(t, err)

	require.True(t, exists(unminedKey), "unmined non-conflicting record must survive")
	require.False(t, exists(conflictingKey), "unmined conflicting record must be deleted")
	require.False(t, exists(minedKey), "mined record must be deleted")
	require.True(t, exists(unminedLargeMaster), "unmined large tx master must survive")
	require.True(t, exists(unminedLargePage), "pagination record of an unmined large tx must survive")
	require.False(t, exists(minedLargeMaster), "mined large tx master must be deleted")
	require.False(t, exists(minedLargePage), "pagination record of a mined large tx must be deleted")
}
