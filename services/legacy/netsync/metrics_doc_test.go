package netsync

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// removedNetsyncMetrics are the histograms the inline netsync pipeline exported
// before it was deleted. Their reference rows outlived them; none may come back.
var removedNetsyncMetrics = []string{
	"prepare_subtrees",
	"validate_transactions_legacy_mode",
	"pre_validate_transactions",
	"validate_transactions",
	"extend_transactions",
	"create_utxos",
	"block_tx_size",
	"block_tx_nr_inputs",
	"block_tx_nr_outputs",
	"block_tx_extend",
	"block_tx_validate",
}

// TestNetsyncMetricsAreDocumented cross-checks docs/references/prometheusMetrics.md
// against what this package registers: every teranode_legacy_netsync_ family the
// default registry gathers has a row, and no removed metric keeps one. Nothing
// checked this before, which is how the reference came to list eleven histograms
// the branch deleted and none of the metrics it added.
//
// A vector only gathers once a label value exists, so this also checks that
// park_dispositions_total carries a series for every disposition row from
// registration, before any block has been parked.
func TestNetsyncMetricsAreDocumented(t *testing.T) {
	initPrometheusMetrics()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to determine test file location")

	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")

	docBytes, err := os.ReadFile(filepath.Join(repoRoot, "docs", "references", "prometheusMetrics.md"))
	require.NoError(t, err)

	doc := string(docBytes)

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	const prefix = "teranode_legacy_netsync_"

	seen := 0

	for _, family := range families {
		name := family.GetName()
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		seen++

		require.True(t, strings.Contains(doc, "`"+name+"`"), "%s is registered but has no row in prometheusMetrics.md", name)
	}

	require.Positive(t, seen, "no netsync metric was gathered; initPrometheusMetrics registered nothing")

	dispositions, dispositionsGathered := gatheredFamily(families, prefix+"park_dispositions_total")
	require.True(t, dispositionsGathered, "park_dispositions_total must gather before any block is parked, so the doc check above covers it and a dashboard has a zero baseline")

	labelled := make(map[string]bool)

	for _, m := range dispositions.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "disposition" {
				labelled[l.GetValue()] = true
			}
		}
	}

	for _, d := range parkDispositionRows {
		require.True(t, labelled[d.name], "park_dispositions_total has no series for the row %q; every row is pre-initialised at registration", d.name)
	}

	for _, removed := range removedNetsyncMetrics {
		require.False(t, strings.Contains(doc, "`"+prefix+removed+"`"), "%s was removed with the inline pipeline; its reference row must not come back", prefix+removed)
	}
}

func gatheredFamily(families []*dto.MetricFamily, name string) (*dto.MetricFamily, bool) {
	for _, f := range families {
		if f.GetName() == name {
			return f, true
		}
	}

	return nil, false
}
