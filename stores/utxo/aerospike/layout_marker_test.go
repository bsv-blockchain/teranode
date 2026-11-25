package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLayoutTestStore starts a throwaway Aerospike and opens a store with the
// given batch size, returning the client and settings for direct marker
// inspection.
func newLayoutTestStore(t *testing.T, batchSize int) (*Store, *aerospike.Client, *settings.Settings, string) {
	t.Helper()

	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = batchSize

	container, err := runAerospikeTestContainer(ctx)
	test.SkipIfContainerUnavailable(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	h, err := container.Host(ctx)
	require.NoError(t, err)

	p, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := aerospike.NewClient(h, p)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo&externalStore=file://./data/external-lm", h, p))
	require.NoError(t, err)

	store, newErr := New(ctx, logger, tSettings, aeroURL)
	if newErr != nil {
		_ = os.RemoveAll("./data/external-lm")
		return nil, client, tSettings, ""
	}
	t.Cleanup(func() { _ = os.RemoveAll("./data/external-lm") })

	return store, client, tSettings, fmt.Sprintf("%s:%d", h, p)
}

// A first open records the batch size it was configured with.
func TestLayoutMarker_FirstOpenRecordsBatchSize(t *testing.T) {
	store, client, _, _ := newLayoutTestStore(t, 128)
	if store == nil {
		t.Skip("store open failed")
	}

	key, err := aerospike.NewKey(store.namespace, layoutSetName, layoutRecordKey)
	require.NoError(t, err)

	rec, err := client.Get(aerospike.NewPolicy(), key)
	require.NoError(t, err)
	require.NotNil(t, rec, "layout marker must exist after first open")
	assert.Equal(t, 128, rec.Bins[utxoBatchSizeBin], "marker records the configured batch size")
}

// Reopening with the same batch size is silent and accepted.
func TestLayoutMarker_SameSizeReopenAccepted(t *testing.T) {
	store, _, tSettings, endpoint := newLayoutTestStore(t, 512)
	if store == nil {
		t.Skip("store open failed")
	}

	ctx := context.Background()
	// Second store on the same namespace and the same size: must open cleanly.
	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s/test?set=utxo&externalStore=file://./data/external-lm2", endpoint))
	require.NoError(t, err)

	second, err := New(ctx, ulogger.NewErrorTestLogger(t), tSettings, aeroURL)
	require.NoError(t, err)
	defer func() { _ = second.Close(ctx) }()

	assert.Equal(t, 512, second.utxoBatchSize)
}

// Reopening the same namespace with a different batch size - the docker (50)
// vs docker.m (512) context swap from the notes - must refuse the store and
// name both numbers.
func TestLayoutMarker_DifferentSizeReopenRefused(t *testing.T) {
	store, _, tSettings, endpoint := newLayoutTestStore(t, 512)
	if store == nil {
		t.Skip("store open failed")
	}

	ctx := context.Background()
	tSettings.UtxoStore.UtxoBatchSize = 50

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s/test?set=utxo&externalStore=file://./data/external-lm3", endpoint))
	require.NoError(t, err)

	_, err = New(ctx, ulogger.NewErrorTestLogger(t), tSettings, aeroURL)
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "utxostore_utxoBatchSize=50")
	assert.Contains(t, msg, "written with utxostore_utxoBatchSize=512")
}

// A record already present with the same value exercises the CREATE_ONLY
// loser path without a genuine race: the pre-write wins, the open re-reads,
// agrees, and proceeds.
func TestLayoutMarker_PreexistingSameValueAccepted(t *testing.T) {
	store, client, _, _ := newLayoutTestStore(t, 64)
	if store == nil {
		t.Skip("store open failed")
	}

	// The marker is already there from the first open; delete and pre-write
	// the same value to simulate another node having recorded it first.
	key, err := aerospike.NewKey(store.namespace, layoutSetName, layoutRecordKey)
	require.NoError(t, err)
	require.NoError(t, client.PutBins(aerospike.NewWritePolicy(0, 0), key, aerospike.NewBin(utxoBatchSizeBin, 64)))

	ctx := context.Background()
	require.NoError(t, store.enforceLayoutMarker(ctx))
}

// The CREATE_ONLY conflict path inside writeLayoutBatchSize: remove the
// marker behind the store's back, race is simulated by pre-writing a
// different value directly, then calling writeLayoutBatchSize (which re-reads
// and must report the disagreement naming the winner's number).
func TestLayoutMarker_WriteRaceReportsWinner(t *testing.T) {
	store, client, _, _ := newLayoutTestStore(t, 128)
	if store == nil {
		t.Skip("store open failed")
	}

	key, err := aerospike.NewKey(store.namespace, layoutSetName, layoutRecordKey)
	require.NoError(t, err)

	ctx := context.Background()

	// Simulate another node having won the first-open race with 512.
	require.NoError(t, client.PutBins(aerospike.NewWritePolicy(0, 0), key, aerospike.NewBin(utxoBatchSizeBin, 512)))

	// Store believes 128: the CREATE_ONLY write conflicts, the re-read sees
	// 512, and the error must name the winner's value.
	raceErr := store.writeLayoutBatchSize(ctx, 128)
	require.Error(t, raceErr)
	assert.Contains(t, raceErr.Error(), "already initialised with utxostore_utxoBatchSize=512")

	// Same race, agreeing value: no error.
	require.NoError(t, store.writeLayoutBatchSize(ctx, 512))
}
