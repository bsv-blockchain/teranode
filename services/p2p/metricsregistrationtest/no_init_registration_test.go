// Package metricsregistrationtest verifies that merely importing
// services/p2p (as every pod does transitively via daemon wiring) does not
// register the p2p Prometheus metric set. Regression check for the removed
// package-level init() in services/p2p/metrics.go: that init() registered
// teranode_p2p_* series in every process, including pods that never
// construct a p2p Server (e.g. -p2p=0). It must live in its own package/test
// binary, separate from services/p2p's own tests, because those construct
// *Server directly and would register the metrics regardless of the fix
// under test.
package metricsregistrationtest

import (
	"strings"
	"testing"

	// Import only, exactly as every pod does transitively via daemon
	// wiring. No p2p.NewServer call anywhere in this test binary.
	_ "github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestImportingP2PPackageDoesNotRegisterMetrics(t *testing.T) {
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		require.False(t, strings.HasPrefix(family.GetName(), "teranode_p2p_"),
			"importing services/p2p must not register %s: p2p metrics must only be "+
				"initialized by NewServer, not a package init()", family.GetName())
	}
}
