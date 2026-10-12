package util

import (
	"runtime"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestInitStats_DisabledSkipsPollingGoroutine covers the aerospike_enable_client_metrics
// gate. The client passed in is a zero-value uaerospike.Client: non-nil, so it
// gets past initStats' nil check, but with a nil embedded *aerospike.Client. Any
// code path that actually touched the client would nil-deref, so surviving this
// call is itself proof that the gate returns before the stats-polling setup —
// and the goroutine count confirms no poller was started.
//
// The enabled path is deliberately not exercised here: it starts a goroutine that
// polls client.Stats() against a real cluster, which belongs in an
// aerospike-tagged integration test rather than a unit test.
func TestInitStats_DisabledSkipsPollingGoroutine(t *testing.T) {
	tSettings := &settings.Settings{}
	tSettings.Aerospike.EnableClientMetrics = false
	tSettings.Aerospike.StatsRefreshDuration = 0

	client := &uaerospike.Client{}

	before := runtime.NumGoroutine()

	require.NotPanics(t, func() {
		initStats(ulogger.TestLogger{}, client, tSettings)
	}, "the gate must return before anything dereferences the client")

	require.LessOrEqual(t, runtime.NumGoroutine(), before,
		"no stats-polling goroutine may be started when aerospike_enable_client_metrics=false")
}

// TestInitStats_NilClientIsNoOp locks the pre-existing nil-client guard so the
// new gate above cannot be mistaken for the only early return.
func TestInitStats_NilClientIsNoOp(t *testing.T) {
	tSettings := &settings.Settings{}
	tSettings.Aerospike.EnableClientMetrics = true

	require.NotPanics(t, func() {
		initStats(ulogger.TestLogger{}, nil, tSettings)
	})
}
