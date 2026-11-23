package aerospike_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// floorLoseNextMasterUDFResponse lets the next UDF batch that touches the master of hash run on the
// server and then reports a timeout to the caller, so the write commits but its response is lost.
// It returns a counter of the responses lost so far.
func floorLoseNextMasterUDFResponse(t *testing.T, store *teranode_aerospike.Store, client *uaerospike.Client, masterKey *aerospike.Key) *atomic.Int64 {
	t.Helper()

	var (
		armed atomic.Bool
		lost  atomic.Int64
	)

	armed.Store(true)

	store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		err := client.BatchOperate(policy, records)

		for _, r := range records {
			if _, isUDF := r.(*aerospike.BatchUDF); isUDF && string(r.BatchRec().Key.Digest()) == string(masterKey.Digest()) && armed.CompareAndSwap(true, false) {
				lost.Add(1)

				return aerospike.ErrTimeout
			}
		}

		return err
	})

	t.Cleanup(func() { store.SetBatchOperateFn(nil) })

	return &lost
}

// A SetConflicting(false) whose master write commits but whose response is lost reaches the caller as an
// error. The retry finds the master already clear, so its response carries no signal. The pages must
// still be cleared, or the pruner deletes them at their old stamp while the master is live.
func TestSetConflictingRetryAfterLostResponseClearsPages(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	prunerSvc := floorPrunerService(ctx, t, store)
	stale := floorCachedTip + retention + floorStaleOffset

	parent := floorCreateThreePageParent(ctx, t, store, 60)
	hash := parent.TxIDChainHash()

	floorForceStaleStamps(t, client, store, hash, stale, true)

	lost := floorLoseNextMasterUDFResponse(t, store, client, floorKey(t, store, hash, 0))

	_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*hash}, false)
	require.Error(t, err, "the lost response must reach the caller as an error")
	require.Equal(t, int64(1), lost.Load(), "setup: the master write must have run and lost its response")

	_, masterStamped := readRecordDAH(t, client, store, hash, 0)
	require.False(t, masterStamped, "setup: the master committed the clear")
	floorRequirePagesAt(t, client, store, hash, int(stale))

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*hash}, false)
	require.NoError(t, err)

	floorRequirePagesAt(t, client, store, hash, 0)

	n, err := prunerSvc.PruneWithPartitions(ctx, stale, "lost-response-clear", 1)
	require.NoError(t, err)
	require.Zero(t, n, "the pruner must find nothing to delete")
	floorRequireRecordsExist(t, client, store, hash)
}

// floorMarkPagesVersioned stamps every page of hash with the given stamp and a master version far above
// any the master reports, so a versioned write from the master is refused as FILTERED_OUT.
func floorMarkPagesVersioned(t *testing.T, client *uaerospike.Client, keyFor func(page uint32) *aerospike.Key, stamp, version int) {
	t.Helper()

	for page := uint32(1); page < floorPaginatedOut; page++ {
		require.NoError(t, client.PutBins(nil, keyFor(page),
			aerospike.NewBin(fields.DeleteAtHeight.String(), stamp),
			aerospike.NewBin(fields.DAHVersion.String(), version),
		))
	}
}

// A page that already holds the master's version or a later one refuses the write as FILTERED_OUT.
// The setDAH batcher must count that as success and leave the page as it was.
func TestUnspendCountsAFilteredOutPageWriteAsSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	const pageVersion = 1000

	stamp := floorCachedTip + retention

	parent := floorCreateThreePageParent(ctx, t, store, 61)
	hash := parent.TxIDChainHash()

	spends := floorSpendFirstOutput(ctx, t, store, parent)

	floorForceStaleStamps(t, client, store, hash, stamp, false)
	floorMarkPagesVersioned(t, client, func(page uint32) *aerospike.Key { return floorKey(t, store, hash, page) }, int(stamp), pageVersion)

	require.NoError(t, store.Unspend(ctx, spends), "a page that is already current must not fail the unspend")

	_, masterStamped := readRecordDAH(t, client, store, hash, 0)
	require.False(t, masterStamped, "setup: the unspend cleared the master")
	floorRequirePagesAt(t, client, store, hash, int(stamp))

	for page := uint32(1); page < floorPaginatedOut; page++ {
		require.Equal(t, pageVersion, floorDAHVersion(t, client, store, hash, page), "page %d must keep its version", page)
	}
}

// The multi-record page write used by the lock path takes the same FILTERED_OUT refusal as success.
func TestSetLockedCountsAFilteredOutPageWriteAsSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	const pageVersion = 1000

	stamp := floorCachedTip + retention

	parent := floorCreateThreePageParent(ctx, t, store, 62)
	hash := parent.TxIDChainHash()

	floorForceStaleStamps(t, client, store, hash, stamp, false)
	floorMarkPagesVersioned(t, client, func(page uint32) *aerospike.Key { return floorKey(t, store, hash, page) }, int(stamp), pageVersion)

	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*hash}, true), "a page that is already current must not fail the lock")

	_, masterStamped := readRecordDAH(t, client, store, hash, 0)
	require.False(t, masterStamped, "setup: the lock cleared the master")
	floorRequirePagesAt(t, client, store, hash, 0)

	for page := uint32(1); page < floorPaginatedOut; page++ {
		require.Equal(t, pageVersion, floorDAHVersion(t, client, store, hash, page), "page %d must keep its version, which the refused write would have replaced", page)
	}
}
