package utxo

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestMockGetDerivedInpointsDoNotLeakIntoFixture pins the gate documented on
// withDerivedTxInpoints across calls, not just on the first one. Deriving into
// the caller's fixture pointer made a later Get that did not ask for
// fields.TxInpoints see the value an earlier Get had derived, so a caller that
// forgot to ask would pass once any other caller had asked first.
func TestMockGetDerivedInpointsDoNotLeakIntoFixture(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("fixture-tx")
	parentTxHash := createTestHash("fixture-parent")
	fixture := &meta.Data{Tx: createTestTransactionWithInputs(parentTxHash, 0)}

	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).Return(fixture, nil)

	withInpoints, err := mockStore.Get(ctx, &txHash, fields.TxInpoints)
	require.NoError(t, err)
	require.Len(t, withInpoints.TxInpoints.ParentTxHashes, 1)
	require.Equal(t, parentTxHash, withInpoints.TxInpoints.ParentTxHashes[0])

	require.Empty(t, fixture.TxInpoints.ParentTxHashes,
		"deriving inpoints must not write through the caller's fixture pointer")

	withoutInpoints, err := mockStore.Get(ctx, &txHash, fields.Tx)
	require.NoError(t, err)
	require.Empty(t, withoutInpoints.TxInpoints.ParentTxHashes,
		"a Get that did not ask for fields.TxInpoints must still see an empty value after one that did")
}

// TestMockGetDerivedInpointsConcurrentSameFixture runs the fan-out shape the
// conflict walks use: many goroutines calling Get on one hash with one shared
// fixture. Under -race this failed while the derivation wrote to the fixture.
func TestMockGetDerivedInpointsConcurrentSameFixture(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("shared-fixture-tx")
	parentTxHash := createTestHash("shared-fixture-parent")
	fixture := &meta.Data{Tx: createTestTransactionWithInputs(parentTxHash, 0)}

	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).Return(fixture, nil)

	const workers = 16

	var wg sync.WaitGroup

	results := make([]*meta.Data, workers)
	errs := make([]error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			results[i], errs[i] = mockStore.Get(ctx, &txHash, fields.TxInpoints)
		}(i)
	}

	wg.Wait()

	for i := 0; i < workers; i++ {
		require.NoError(t, errs[i])
		require.Len(t, results[i].TxInpoints.ParentTxHashes, 1)
	}

	require.Empty(t, fixture.TxInpoints.ParentTxHashes)
}
