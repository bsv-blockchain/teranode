package pruner

import (
	"context"
	"fmt"
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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestPrunerSkipsUnminedRecords verifies the pruner never deletes a record that is
// currently unmined (unminedSince set) unless it is conflicting, including the
// pagination records of a protected large transaction, and that it clears the DAH of
// the records it skips so they leave the DAH index.
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

	unmined := fields.UnminedSince.String()
	conflicting := fields.Conflicting.String()
	totalExtra := fields.TotalExtraRecs.String()
	dahBin := fields.DeleteAtHeight.String()

	const pruneHeight = 5

	writePolicy := aerospike.NewWritePolicy(0, 0)

	// chunk size 1 puts a page and its master in different chunks
	for run, chunkSize := range []int{0, 1} {
		t.Run(fmt.Sprintf("chunkSize=%d", chunkSize), func(t *testing.T) {
			service, err := NewService(createTestSettings(), Options{
				Logger:        ulogger.New("test"),
				Client:        client,
				ExternalStore: memory.New(),
				Namespace:     namespace,
				Set:           set,
				IndexWaiter:   &MockIndexWaiter{Client: client, Namespace: namespace, Set: set},
			})
			require.NoError(t, err)

			if chunkSize > 0 {
				service.chunkSize = chunkSize
			}

			service.Start(ctx)
			require.Eventually(t, service.indexReady.Load, 30*time.Second, 100*time.Millisecond, "delete_at_height index not ready")

			tag := fmt.Sprintf("-%d", chunkSize)

			put := func(name string, num uint32, bins aerospike.BinMap) *aerospike.Key {
				txID := chainhash.HashH([]byte(name + tag))
				key, kerr := aerospike.NewKey(namespace, set, uaerospike.CalculateKeySourceInternal(&txID, num))
				require.NoError(t, kerr)

				bins[fields.TxID.String()] = txID.CloneBytes()
				bins[dahBin] = pruneHeight
				require.NoError(t, client.Put(writePolicy, key, bins))

				return key
			}

			exists := func(key *aerospike.Key) bool {
				ok, eerr := client.Exists(nil, key)
				require.NoError(t, eerr)

				return ok
			}

			hasDAH := func(key *aerospike.Key) bool {
				rec, gerr := client.Get(nil, key)
				require.NoError(t, gerr)

				_, ok := rec.Bins[dahBin]

				return ok
			}

			unminedKey := put("unmined", 0, aerospike.BinMap{unmined: 3, totalExtra: 0})
			conflictingKey := put("conflicting", 0, aerospike.BinMap{unmined: 3, conflicting: true, totalExtra: 0})
			minedKey := put("mined", 0, aerospike.BinMap{totalExtra: 0})

			// unmined but preserved: protected, and keeps its DAH
			preservedKey := put("preserved", 0, aerospike.BinMap{unmined: 3, totalExtra: 0, fields.PreserveUntil.String(): 500})

			// legacy masters written without totalExtraRecs
			legacyUnminedKey := put("legacy-unmined", 0, aerospike.BinMap{unmined: 3})
			legacyMinedKey := put("legacy-mined", 0, aerospike.BinMap{})

			// a large tx: master plus one pagination record, which carries its own DAH but no unminedSince
			unminedLargeMaster := put("unmined-large", 0, aerospike.BinMap{unmined: 3, totalExtra: 1})
			unminedLargePage := put("unmined-large", 1, aerospike.BinMap{})

			minedLargeMaster := put("mined-large", 0, aerospike.BinMap{totalExtra: 1})
			minedLargePage := put("mined-large", 1, aerospike.BinMap{})

			conflictingLargeMaster := put("conflicting-large", 0, aerospike.BinMap{unmined: 3, conflicting: true, totalExtra: 1})
			conflictingLargePage := put("conflicting-large", 1, aerospike.BinMap{})

			// a pagination record whose master does not exist
			orphanPage := put("orphan", 1, aerospike.BinMap{})

			pruneCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			_, err = service.Prune(pruneCtx, pruneHeight, "<test-hash>")
			require.NoError(t, err)

			require.True(t, exists(unminedKey), "unmined non-conflicting record must survive")
			require.False(t, exists(conflictingKey), "unmined conflicting record must be deleted")
			require.False(t, exists(minedKey), "mined record must be deleted")
			require.True(t, exists(preservedKey), "unmined preserved record must survive")
			require.True(t, exists(legacyUnminedKey), "unmined master without totalExtraRecs must survive")
			require.False(t, exists(legacyMinedKey), "mined master without totalExtraRecs must be deleted")
			require.True(t, exists(unminedLargeMaster), "unmined large tx master must survive")
			require.True(t, exists(unminedLargePage), "pagination record of an unmined large tx must survive")
			require.False(t, exists(minedLargeMaster), "mined large tx master must be deleted")
			require.False(t, exists(minedLargePage), "pagination record of a mined large tx must be deleted")
			require.False(t, exists(conflictingLargeMaster), "conflicting large tx master must be deleted")
			require.False(t, exists(conflictingLargePage), "pagination record of a conflicting large tx must be deleted")
			require.False(t, exists(orphanPage), "orphan pagination record must be deleted")

			// the DAH of a skipped record is cleared so it leaves the DAH index, its page included
			require.False(t, hasDAH(unminedKey), "skipped unmined master must have its DAH cleared")
			require.False(t, hasDAH(legacyUnminedKey), "skipped legacy unmined master must have its DAH cleared")
			require.False(t, hasDAH(unminedLargeMaster), "skipped unmined large master must have its DAH cleared")
			require.False(t, hasDAH(unminedLargePage), "page of a skipped master must have its DAH cleared")
			require.True(t, hasDAH(preservedKey), "preserved unmined master must keep its DAH")

			// a second cycle returns only the preserved records again (one per run so far)
			skippedBefore := testutil.ToFloat64(prometheusUtxoRecordsSkippedUnmined)

			_, err = service.Prune(pruneCtx, pruneHeight, "<test-hash>")
			require.NoError(t, err)

			require.Equal(t, skippedBefore+float64(run+1), testutil.ToFloat64(prometheusUtxoRecordsSkippedUnmined))

			// a record re-mined between the read and the clear keeps its DAH
			reminedKey := put("remined", 0, aerospike.BinMap{unmined: 3, totalExtra: 1})
			reminedPage := put("remined", 1, aerospike.BinMap{})

			_, err = client.Operate(writePolicy, reminedKey, aerospike.PutOp(aerospike.NewBin(unmined, nil)))
			require.NoError(t, err)

			require.Zero(t, service.clearUnminedDAH([]*dahClear{{masterKey: reminedKey, pageKeys: []*aerospike.Key{reminedPage}}}))
			require.True(t, hasDAH(reminedKey), "re-mined master must keep its DAH")
			require.True(t, hasDAH(reminedPage), "page of a re-mined master must keep its DAH")
		})
	}
}
