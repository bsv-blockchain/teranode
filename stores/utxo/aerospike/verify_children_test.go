package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupStoreForVerifyTest(t *testing.T) (*Store, *uaerospike.Client) {
	t.Helper()
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	container, err := aeroTest.RunContainer(ctx,
		aeroTest.WithTTLSupport("test"),
	)
	if err != nil {
		t.Skipf("Skipping: Aerospike container not available (%v)", err)
	}

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	t.Cleanup(func() {
		client.Close()
	})

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=test&externalStore=file://./data/external&block_retention=100", host, port))
	require.NoError(t, err)

	store, err := New(ctx, logger, tSettings, aeroURL)
	require.NoError(t, err)

	return store, client
}

func TestHandleExtraRecords_DAHSetWithDrift(t *testing.T) {
	store, client := setupStoreForVerifyTest(t)
	ctx := context.Background()

	batchSize := store.utxoBatchSize // typically 20000, but we need a small one
	// Override to create multi-record tx with few outputs
	store.utxoBatchSize = 2

	// Build a tx with 6 outputs → master(2) + child1(2) + child2(2) = totalExtraRecs=2
	largeTx := bt.NewTx()
	err := largeTx.From(
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		7000,
	)
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		err = largeTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)
	}

	txID := largeTx.TxIDChainHash()

	_, err = store.Create(ctx, largeTx, 1)
	require.NoError(t, err)

	masterKey, aErr := aerospike.NewKey(store.namespace, store.setName, txID.CloneBytes())
	require.NoError(t, aErr)

	// Mine the tx and set up master record for DAH conditions
	aErr = client.Put(nil, masterKey, aerospike.BinMap{
		fields.SpentUtxos.String():     2,
		fields.BlockIDs.String():       []int{1},
		fields.UnminedSince.String():   nil,
		fields.SpentExtraRecs.String(): 1, // drift: only 1 of 2 extra recs "spent"
	})
	require.NoError(t, aErr)

	// Child records are NOT spent — simulating drift
	// (spentExtraRecs says 1 child is done, but actually none are)

	// Call handleExtraRecords(+1) → this will increment spentExtraRecs to 2 == totalExtraRecs
	// Lua returns DAHSET → Go verifies children → finds not-all-spent → clears DAH
	err = store.handleExtraRecords(ctx, txID, 1)
	require.NoError(t, err)

	// Verify DAH was cleared (drift detected)
	rec, aErr := client.Get(nil, masterKey)
	require.NoError(t, aErr)
	assert.Nil(t, rec.Bins[fields.DeleteAtHeight.String()],
		"DAH should be cleared because children are not actually all-spent")

	// Restore batchSize
	store.utxoBatchSize = batchSize
}

func TestHandleExtraRecords_DAHSetAllSpent(t *testing.T) {
	store, client := setupStoreForVerifyTest(t)
	ctx := context.Background()

	store.utxoBatchSize = 2

	// Build a tx with 4 outputs → master(2) + child1(2) = totalExtraRecs=1
	largeTx := bt.NewTx()
	err := largeTx.From(
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		5000,
	)
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		err = largeTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)
	}

	txID := largeTx.TxIDChainHash()

	_, err = store.Create(ctx, largeTx, 1)
	require.NoError(t, err)

	masterKey, aErr := aerospike.NewKey(store.namespace, store.setName, txID.CloneBytes())
	require.NoError(t, aErr)

	// Mine the tx
	aErr = client.Put(nil, masterKey, aerospike.BinMap{
		fields.SpentUtxos.String():     2,
		fields.BlockIDs.String():       []int{1},
		fields.UnminedSince.String():   nil,
		fields.SpentExtraRecs.String(): 0,
	})
	require.NoError(t, aErr)

	// Mark child1 as fully spent
	childKeySource := uaerospike.CalculateKeySourceInternal(txID, 1)
	childKey, aErr := aerospike.NewKey(store.namespace, store.setName, childKeySource)
	require.NoError(t, aErr)
	aErr = client.Put(nil, childKey, aerospike.BinMap{
		fields.SpentUtxos.String():  2,
		fields.RecordUtxos.String(): 2,
	})
	require.NoError(t, aErr)

	// Call handleExtraRecords(+1) → spentExtraRecs goes from 0 to 1 == totalExtraRecs(1)
	// Lua returns DAHSET → Go verifies child1 → all spent → DAH should be set
	err = store.handleExtraRecords(ctx, txID, 1)
	require.NoError(t, err)

	// Verify DAH was set (all children genuinely spent)
	rec, aErr := client.Get(nil, masterKey)
	require.NoError(t, aErr)
	assert.NotNil(t, rec.Bins[fields.DeleteAtHeight.String()],
		"DAH should be set because all children are genuinely spent")
}

func TestVerifyAllChildrenSpent(t *testing.T) {
	store, client := setupStoreForVerifyTest(t)

	txID := chainhash.HashH([]byte("test-verify-children"))

	t.Run("childCount zero returns true", func(t *testing.T) {
		allSpent, err := store.verifyAllChildrenSpent(context.Background(), &txID, 0)
		require.NoError(t, err)
		assert.True(t, allSpent)
	})

	t.Run("context cancelled returns error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		allSpent, err := store.verifyAllChildrenSpent(ctx, &txID, 2)
		require.Error(t, err)
		assert.False(t, allSpent)
	})

	t.Run("all children spent returns true", func(t *testing.T) {
		// Create child records with spentUtxos == recordUtxos
		for i := uint32(1); i <= 2; i++ {
			keySource := uaerospike.CalculateKeySourceInternal(&txID, i)
			key, aErr := aerospike.NewKey(store.namespace, store.setName, keySource)
			require.NoError(t, aErr)

			aErr = client.Put(nil, key, aerospike.BinMap{
				fields.SpentUtxos.String():  5,
				fields.RecordUtxos.String(): 5,
			})
			require.NoError(t, aErr)
		}

		allSpent, err := store.verifyAllChildrenSpent(context.Background(), &txID, 2)
		require.NoError(t, err)
		assert.True(t, allSpent)
	})

	t.Run("not all children spent returns false", func(t *testing.T) {
		// Child 1: fully spent. Child 2: not fully spent.
		for i := uint32(1); i <= 2; i++ {
			keySource := uaerospike.CalculateKeySourceInternal(&txID, i)
			key, aErr := aerospike.NewKey(store.namespace, store.setName, keySource)
			require.NoError(t, aErr)

			spent := 5
			if i == 2 {
				spent = 3 // not fully spent
			}
			aErr = client.Put(nil, key, aerospike.BinMap{
				fields.SpentUtxos.String():  spent,
				fields.RecordUtxos.String(): 5,
			})
			require.NoError(t, aErr)
		}

		allSpent, err := store.verifyAllChildrenSpent(context.Background(), &txID, 2)
		require.NoError(t, err)
		assert.False(t, allSpent)
	})

	t.Run("invalid spentUtxos type returns error", func(t *testing.T) {
		keySource := uaerospike.CalculateKeySourceInternal(&txID, 1)
		key, aErr := aerospike.NewKey(store.namespace, store.setName, keySource)
		require.NoError(t, aErr)

		// Write a string instead of int for spentUtxos
		aErr = client.Put(nil, key, aerospike.BinMap{
			fields.SpentUtxos.String():  "not-an-int",
			fields.RecordUtxos.String(): 5,
		})
		require.NoError(t, aErr)

		allSpent, verifyErr := store.verifyAllChildrenSpent(context.Background(), &txID, 1)
		require.Error(t, verifyErr)
		assert.False(t, allSpent)
		assert.Contains(t, verifyErr.Error(), "invalid type for spentUtxos")
	})

	t.Run("invalid recordUtxos type returns error", func(t *testing.T) {
		keySource := uaerospike.CalculateKeySourceInternal(&txID, 1)
		key, aErr := aerospike.NewKey(store.namespace, store.setName, keySource)
		require.NoError(t, aErr)

		// Write valid spentUtxos but string for recordUtxos
		aErr = client.Put(nil, key, aerospike.BinMap{
			fields.SpentUtxos.String():  5,
			fields.RecordUtxos.String(): "not-an-int",
		})
		require.NoError(t, aErr)

		allSpent, verifyErr := store.verifyAllChildrenSpent(context.Background(), &txID, 1)
		require.Error(t, verifyErr)
		assert.False(t, allSpent)
		assert.Contains(t, verifyErr.Error(), "invalid type for recordUtxos")
	})

	t.Run("missing record returns error", func(t *testing.T) {
		missingTxID := chainhash.HashH([]byte("does-not-exist"))

		allSpent, err := store.verifyAllChildrenSpent(context.Background(), &missingTxID, 1)
		require.Error(t, err)
		assert.False(t, allSpent)
		assert.Contains(t, err.Error(), "child 1 read failed")
	})
}
