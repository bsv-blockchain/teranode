package aerospike_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// initAerospikeImage runs initAerospike against the given server image.
func initAerospikeImage(t *testing.T, image string, tSettings *settings.Settings) (*teranode_aerospike.Store, context.Context) {
	t.Helper()

	_, store, ctx := initAerospikeImageWith(t, image, tSettings, nil)

	return store, ctx
}

// initAerospikeImageWith runs initAerospikeWith against the given server image
// and closes the store at cleanup, which stops its background index self-test.
func initAerospikeImageWith(t *testing.T, image string, tSettings *settings.Settings, beforeStore func(*uaerospike.Client)) (*uaerospike.Client, *teranode_aerospike.Store, context.Context) {
	t.Helper()

	previous := aerospikeContainerImage
	aerospikeContainerImage = image

	t.Cleanup(func() { aerospikeContainerImage = previous })

	client, store, ctx, cleanup := initAerospikeWith(t, tSettings, ulogger.NewErrorTestLogger(t), beforeStore)
	t.Cleanup(cleanup)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, store.Close(closeCtx))
	})

	return client, store, ctx
}

// conflictingIndexTx builds a transaction spending the given outputs of parent,
// with outputs fresh outputs.
func conflictingIndexTx(t *testing.T, parent *bt.Tx, vouts []uint32, outputs int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for _, vout := range vouts {
		require.NoError(t, tx.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
		tx.Inputs[len(tx.Inputs)-1].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	}

	for i := 0; i < outputs; i++ {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(100+i)))
	}

	return tx
}

// conflictingIndexParent creates and stores a transaction with n outputs to spend.
func conflictingIndexParent(t *testing.T, ctx context.Context, store *teranode_aerospike.Store, seed, n int) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(fmt.Sprintf("%064x", seed), 0, "51", uint64(1_000_000*n)))

	for i := 0; i < n; i++ {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000_000))
	}

	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	return parent
}

// awaitConflictingIndex waits until the store's startup self-test has trusted
// the index; the index is created at startup and builds in the background.
// It deliberately does not call ConflictingTxHashes, so it also pins that the
// self-test runs without block validation asking for the index.
func awaitConflictingIndex(t *testing.T, store *teranode_aerospike.Store) {
	t.Helper()

	require.Eventually(t, func() bool {
		done, _ := store.ConflictingIndexVerdict()
		return done
	}, 30*time.Second, 100*time.Millisecond, "conflicting index self-test never finished")

	_, usable := store.ConflictingIndexVerdict()
	require.True(t, usable, "conflicting index self-test did not trust the index")
}

func conflictingSet(t *testing.T, ctx context.Context, store *teranode_aerospike.Store) map[chainhash.Hash]struct{} {
	t.Helper()

	hashes, indexed, err := store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.True(t, indexed)

	set := make(map[chainhash.Hash]struct{}, len(hashes))
	for _, h := range hashes {
		_, dup := set[h]
		require.False(t, dup, "transaction %s listed twice", h)

		set[h] = struct{}{}
	}

	return set
}

// TestConflictingTxHashes_TracksTheFlag pins that the index follows the
// conflicting flag through the store's own writers. It runs on a namespace with
// expiration disabled (nsup-period=0, and the default
// allow-ttl-without-nsup=false), where the server rejects any write carrying a
// TTL, so it also pins that the self-test works there and leaves no probe
// record behind.
func TestConflictingTxHashes_TracksTheFlag(t *testing.T) {
	client, store, ctx := initAerospikeImageWith(t, "aerospike/aerospike-server:8.1", test.CreateBaseTestSettings(t), func(client *uaerospike.Client) {
		setNamespaceConfig(t, client, "allow-ttl-without-nsup=false")
		setNamespaceConfig(t, client, "nsup-period=0")
	})

	awaitConflictingIndex(t, store)
	require.Zero(t, probeRecordCount(t, client))

	parent := conflictingIndexParent(t, ctx, store, 1, 2)

	child := conflictingIndexTx(t, parent, []uint32{0}, 1)
	_, err := store.Create(ctx, child, 100)
	require.NoError(t, err)

	require.Empty(t, conflictingSet(t, ctx, store))

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Equal(t, map[chainhash.Hash]struct{}{*child.TxIDChainHash(): {}}, conflictingSet(t, ctx, store))

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, false)
	require.NoError(t, err)
	require.Empty(t, conflictingSet(t, ctx, store))

	created := conflictingIndexTx(t, parent, []uint32{1}, 1)
	_, err = store.Create(ctx, created, 100, utxo.WithConflicting(true))
	require.NoError(t, err)
	require.Equal(t, map[chainhash.Hash]struct{}{*created.TxIDChainHash(): {}}, conflictingSet(t, ctx, store))
}

// TestConflictingTxHashes_MatchesTheFlagOnEveryRecord is the contract the
// already-validated subtree check relies on (issue 1742): the indexed set is
// exactly the set of transactions whose record reads Conflicting, including a
// paginated transaction, which is listed once.
func TestConflictingTxHashes_MatchesTheFlagOnEveryRecord(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2

	store, ctx := initAerospikeImage(t, "aerospike/aerospike-server:8.1", tSettings)
	awaitConflictingIndex(t, store)

	const children = 40

	parent := conflictingIndexParent(t, ctx, store, 2, children)
	r := rand.New(rand.NewSource(7))

	all := make([]chainhash.Hash, 0, children)

	for i := 0; i < children; i++ {
		// Every fifth child is paginated: 5 outputs at 2 per record.
		outputs := 1
		if i%5 == 0 {
			outputs = 5
		}

		child := conflictingIndexTx(t, parent, []uint32{uint32(i)}, outputs) //nolint:gosec // i < 40

		var opts []utxo.CreateOption
		if r.Intn(4) == 0 {
			opts = append(opts, utxo.WithConflicting(true))
		}

		_, err := store.Create(ctx, child, 100, opts...)
		require.NoError(t, err)

		all = append(all, *child.TxIDChainHash())
	}

	for round := 0; round < 3; round++ {
		var set, clear []chainhash.Hash

		for _, h := range all {
			switch r.Intn(3) {
			case 0:
				set = append(set, h)
			case 1:
				clear = append(clear, h)
			}
		}

		_, _, err := store.SetConflicting(ctx, set, true)
		require.NoError(t, err)

		_, _, err = store.SetConflicting(ctx, clear, false)
		require.NoError(t, err)

		items := make([]*utxo.UnresolvedMetaData, len(all))
		for i, h := range all {
			items[i] = &utxo.UnresolvedMetaData{Hash: h, Idx: i}
		}

		require.NoError(t, store.BatchDecorate(ctx, items, fields.Conflicting))

		want := make(map[chainhash.Hash]struct{})

		for _, item := range items {
			require.NoError(t, item.Err)

			if item.Data.Conflicting {
				want[item.Hash] = struct{}{}
			}
		}

		require.NotEmpty(t, want, "round %d flags nothing, so it proves nothing", round)
		require.Equal(t, want, conflictingSet(t, ctx, store), "round %d", round)
	}
}

// TestConflictingTxHashes_ServerWithoutExpressionIndexes pins the answer from a
// server that cannot hold the index: not indexed, no error, so the caller falls
// back instead of reading "nothing is conflicting".
func TestConflictingTxHashes_ServerWithoutExpressionIndexes(t *testing.T) {
	store, ctx := initAerospikeImage(t, "aerospike/aerospike-server:8.0", test.CreateBaseTestSettings(t))

	hashes, indexed, err := store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.False(t, indexed)
	require.Nil(t, hashes)
}

// setNamespaceConfig applies a dynamic namespace setting on every node.
func setNamespaceConfig(t *testing.T, client *uaerospike.Client, setting string) {
	t.Helper()

	command := fmt.Sprintf("set-config:context=namespace;id=%s;%s", aerospikeNamespace, setting)

	for _, node := range client.GetNodes() {
		info, err := node.RequestInfo(aerospike.NewInfoPolicy(), command)
		require.NoError(t, err)
		require.Equal(t, "ok", info[command], "%s on node %s", command, node.GetName())
	}
}

// probeRecordCount counts records left behind by the index self-test.
func probeRecordCount(t *testing.T, client *uaerospike.Client) int {
	t.Helper()

	recordset, err := client.ScanAll(aerospike.NewScanPolicy(), aerospikeNamespace, aerospikeSet)
	require.NoError(t, err)

	count := 0

	for res := range recordset.Results() {
		require.NoError(t, res.Err)

		if _, ok := res.Record.Bins["_cfIdxProbe"]; ok {
			count++
		}
	}

	return count
}

// TestConflictingIndex_NotTrustedWhenItMissesTheFlag pins the self-test against
// a readable index that gives wrong answers: an index under the store's name
// that never follows the conflicting bin. The store must report not indexed,
// so block validation falls back, rather than reading "nothing is conflicting".
func TestConflictingIndex_NotTrustedWhenItMissesTheFlag(t *testing.T) {
	_, store, ctx := initAerospikeImageWith(t, "aerospike/aerospike-server:8.1", test.CreateBaseTestSettings(t), func(client *uaerospike.Client) {
		wrong := aerospike.ExpCond(aerospike.ExpBoolBin("_neverWritten"), aerospike.ExpIntVal(1), aerospike.ExpUnknown())

		task, err := client.CreateIndexWithExpression(aerospike.NewWritePolicy(0, 0), aerospikeNamespace, aerospikeSet, "conflictingIndex", aerospike.NUMERIC, aerospike.ICT_DEFAULT, wrong)
		require.NoError(t, err)
		require.NoError(t, <-task.OnComplete())
	})

	require.Eventually(t, func() bool {
		done, _ := store.ConflictingIndexVerdict()
		return done
	}, 30*time.Second, 100*time.Millisecond, "conflicting index self-test never finished")

	_, usable := store.ConflictingIndexVerdict()
	require.False(t, usable, "a conflicting index that misses the flag was trusted")

	parent := conflictingIndexParent(t, ctx, store, 3, 1)
	child := conflictingIndexTx(t, parent, []uint32{0}, 1)
	_, err := store.Create(ctx, child, 100, utxo.WithConflicting(true))
	require.NoError(t, err)

	hashes, indexed, err := store.ConflictingTxHashes(ctx)
	require.NoError(t, err)
	require.False(t, indexed)
	require.Nil(t, hashes)
}
