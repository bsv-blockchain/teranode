package subtreeprocessor

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func recoveryPendingGauge(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "teranode_subtreeprocessor_recovery_pending_seconds" {
			require.Len(t, family.Metric, 1)
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatal("recovery pending duration gauge is not registered")
	return 0
}

func TestUnminedRecoveryCancellationNeverSetsPending(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	ctx, cancel := context.WithCancel(t.Context())
	row := recoveryRow("cancelled-selection")
	err := stp.recoverUnmined(ctx, prevBlockHeader, []chainhash.Hash{row.Hash}, func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		cancel()
		return []*utxostore.UnminedTransaction{row}, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, stp.RecoveryPending())
	require.Zero(t, stp.queue.length())
	require.Zero(t, recoveryPendingGauge(t))
}
