package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/util/testutil/blobfault"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// subtree_stored times storeSubtreeData up to the start of the writes, not the writes. Issue 2269
// needs the write time itself, split into the meta build and the two file writes, to set the
// deadline of a store a reorg waits for.

type storeStepSample struct {
	count uint64
	sum   float64
}

func storeStep(t *testing.T, step string) storeStepSample {
	t.Helper()

	initPrometheusMetrics()

	m := &dto.Metric{}
	require.NoError(t, prometheusBlockAssemblySubtreeStoreStep.WithLabelValues(step).(prometheus.Histogram).Write(m))

	return storeStepSample{m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()}
}

func TestStoreSubtreeData_TimesEachStoreStep(t *testing.T) {
	server, store, subtree, txMap := setup(t)

	fault := blobfault.New(store)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{Block: release, Entered: entered})
	server.subtreeStore = fault

	steps := []string{"meta_build", "meta_write", "subtree_write"}
	before := make(map[string]storeStepSample, len(steps))

	for _, step := range steps {
		before[step] = storeStep(t, step)
	}

	subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{
		Subtree:     subtree,
		ParentTxMap: txMap,
	}, make(chan *subtreeRetrySend, 10))
	require.NoError(t, err)

	<-entered
	time.Sleep(200 * time.Millisecond)
	close(release)

	require.True(t, <-subtreeDone)
	<-allDone

	for _, step := range steps {
		require.Equal(t, before[step].count+1, storeStep(t, step).count, "%s is observed once per store", step)
	}

	require.GreaterOrEqual(t, storeStep(t, "subtree_write").sum-before["subtree_write"].sum, 0.2,
		"the subtree write time includes the time the write was blocked")
}
