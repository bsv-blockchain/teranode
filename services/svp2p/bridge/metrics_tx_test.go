package bridge

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// histogramCount reads a histogram's sample count; testutil.ToFloat64 covers
// only counters and gauges.
func histogramCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()

	m := &dto.Metric{}
	require.NoError(t, h.Write(m))

	return m.GetHistogram().GetSampleCount()
}

// The five netsync metrics Phase 2 dropped for want of a consumer. Phase 3
// built the consumers (IngestTx and the orphan pool), so the ingest path must
// observe them again under this package's own subsystem.
func TestIngestTx_ObservesHandleTxMsgMetrics(t *testing.T) {
	initPrometheusMetrics()

	before := histogramCount(t, prometheusSvp2pBridgeHandleTxMsg)
	beforeValidate := histogramCount(t, prometheusSvp2pBridgeHandleTxMsgValidate)

	rv := newRecordingValidator(func(chainhash.Hash) (*meta.Data, error) {
		return &meta.Data{Fee: 1, SizeInBytes: 100}, nil
	})

	sm := newIngestTxTestBridge(rv.MockValidator)

	_, err := sm.IngestTx(t.Context(), makeIngestTestTx(t, "metrics-accepted").Bytes(), "peer1:8333")
	require.NoError(t, err)

	require.Equal(t, before+1, histogramCount(t, prometheusSvp2pBridgeHandleTxMsg), "handle_tx_msg observes every ingested tx")
	require.Equal(t, beforeValidate+1, histogramCount(t, prometheusSvp2pBridgeHandleTxMsgValidate), "handle_tx_msg_validate observes the validator call")
}

func TestOrphanPool_ObservesOrphanMetrics(t *testing.T) {
	initPrometheusMetrics()

	tSettings := newOrphanTestSettings(time.Hour, 100)

	parent := makeIngestTestTx(t, "orphan-metrics-parent")
	child := makeIngestTestTx(t, "orphan-metrics-child")
	require.NoError(t, child.Inputs[0].PreviousTxIDAdd(parent.TxIDChainHash()))

	accepted := map[chainhash.Hash]bool{}

	rv := newRecordingValidator(func(h chainhash.Hash) (*meta.Data, error) {
		if accepted[h] {
			return &meta.Data{Fee: 1, SizeInBytes: 100}, nil
		}

		return nil, errors.ErrTxMissingParent
	})

	sm := newOrphanTestBridge(t, rv.MockValidator, tSettings)

	beforeProcess := histogramCount(t, prometheusSvp2pBridgeProcessOrphanTransactions)
	beforeTime := histogramCount(t, prometheusSvp2pBridgeOrphanTime)
	beforeGauge := testutil.ToFloat64(prometheusSvp2pBridgeOrphans)

	res, err := sm.IngestTx(t.Context(), child.Bytes(), "peer1:8333")
	require.NoError(t, err)
	require.True(t, res.Orphan)
	require.Equal(t, beforeGauge+1, testutil.ToFloat64(prometheusSvp2pBridgeOrphans), "orphans gauge counts the pooled tx")

	accepted[*parent.TxIDChainHash()] = true
	accepted[*child.TxIDChainHash()] = true

	res, err = sm.IngestTx(t.Context(), parent.Bytes(), "peer1:8333")
	require.NoError(t, err)
	require.True(t, res.Accepted)
	require.Len(t, res.ReleasedOrphans, 1)

	require.Equal(t, beforeGauge, testutil.ToFloat64(prometheusSvp2pBridgeOrphans), "the released orphan leaves the gauge")
	require.Equal(t, beforeProcess+1, histogramCount(t, prometheusSvp2pBridgeProcessOrphanTransactions), "process_orphan_transactions observes the release walk")
	require.Equal(t, beforeTime+1, histogramCount(t, prometheusSvp2pBridgeOrphanTime), "orphan_time observes how long the orphan waited")
}
