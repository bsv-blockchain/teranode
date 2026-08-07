package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func histogramSample(t *testing.T, h interface{ Write(*dto.Metric) error }) (count uint64, sum float64) {
	t.Helper()

	var m dto.Metric
	require.NoError(t, h.Write(&m))

	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// A completed walk records exactly one node-count and one depth observation. The
// stores/utxo free functions have no logger, so these histograms are the only
// observability surface on a cone that grows with every block the node stays
// wedged — a silent 26-minute walk is what #1391 presented as.
func TestGetConflictingChildren_ObservesWalkSizeAndDepth(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	rootHash := chainhash.HashH([]byte("metrics-root"))
	midHash := chainhash.HashH([]byte("metrics-mid"))
	tipHash := chainhash.HashH([]byte("metrics-tip"))

	// One .On per hash with a static return — the style the existing walk tests in
	// process_conflicting_test.go use. MockUtxostore.Get passes the variadic
	// fields to testify as a single slice argument, so one trailing matcher
	// covers it.
	mockStore.On("Get", mock.Anything, &rootHash, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spendpkg.SpendingData{{TxID: &midHash}}}, nil)
	mockStore.On("Get", mock.Anything, &midHash, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spendpkg.SpendingData{{TxID: &tipHash}}}, nil)
	mockStore.On("Get", mock.Anything, &tipHash, mock.Anything).
		Return(&meta.Data{}, nil)

	nodesBefore, nodesSumBefore := histogramSample(t, prometheusUtxoConflictingWalkNodes)
	depthBefore, depthSumBefore := histogramSample(t, prometheusUtxoConflictingWalkDepth)

	children, err := GetConflictingChildren(ctx, mockStore, rootHash, 0)
	require.NoError(t, err)
	require.Len(t, children, 2)

	nodesAfter, nodesSumAfter := histogramSample(t, prometheusUtxoConflictingWalkNodes)
	depthAfter, depthSumAfter := histogramSample(t, prometheusUtxoConflictingWalkDepth)

	require.Equal(t, nodesBefore+1, nodesAfter)
	require.Equal(t, depthBefore+1, depthAfter)

	// three nodes visited: root, mid, tip
	require.InDelta(t, nodesSumBefore+3, nodesSumAfter, 0.001)
	// three levels walked: [root], [mid], [tip]
	require.InDelta(t, depthSumBefore+3, depthSumAfter, 0.001)

	mockStore.AssertExpectations(t)
}
