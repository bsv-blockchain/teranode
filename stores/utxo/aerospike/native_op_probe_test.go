package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestNativeTeranodeOpProbe_Container exercises the startup capability probe
// (opcode dispatch + spend semantics) against a real aerospike-server.
//
// Against the stock image (the default in CI) the probe must reject and leave
// the store on the UDF path — covering the graceful-fallback branches. The
// fork-image CI job points AEROSPIKE_CONTAINER_IMAGE at the BSV fork and sets
// AEROSPIKE_EXPECT_NATIVE_OPS=true, flipping every assertion here to the
// native path: the probe must succeed (including the double-spend rejection
// stage) and executeTeranodeOp must run natively.
func TestNativeTeranodeOpProbe_Container(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Aerospike.UseNativeTeranodeOps = true

	container, err := runAerospikeTestContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo&externalStore=file://./data/external", host, port))
	require.NoError(t, err)

	store, err := New(ctx, logger, tSettings, aeroURL)
	require.NoError(t, err)

	expectNative := os.Getenv("AEROSPIKE_EXPECT_NATIVE_OPS") == "true"

	probePolicy := func() *aerospike.WritePolicy {
		policy := aerospike.NewWritePolicy(0, 0)
		policy.TotalTimeout = 2 * time.Second
		policy.Expiration = 60
		return policy
	}

	t.Run("probe_decision_matches_server", func(t *testing.T) {
		require.Equal(t, expectNative, store.useNativeTeranodeOps.Load(),
			"probe decision must match the server image the suite runs against")
	})

	t.Run("spend_semantics_probe_direct", func(t *testing.T) {
		require.Equal(t, expectNative, store.probeNativeSpendSemantics(ctx, probePolicy()),
			"spend probe must accept the fork dispatcher and reject a stock server")
	})

	t.Run("probe_honours_cancelled_context", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		require.False(t, store.detectNativeTeranodeOpSupport(cancelled))
	})

	t.Run("executeTeranodeOp_native_path", func(t *testing.T) {
		// Force the native single-record path regardless of the probe outcome:
		// the stock server exercises the error + runtime-demotion branch, the
		// fork server the success branch with a verified mutation.
		prev := store.useNativeTeranodeOps.Load()
		store.useNativeTeranodeOps.Store(true)
		defer store.useNativeTeranodeOps.Store(prev)

		key, err := aerospike.NewKey(store.namespace, store.setName, "_native-op-exec-test")
		require.NoError(t, err)

		policy := probePolicy()

		require.NoError(t, store.client.PutBins(policy, key, aerospike.NewBin("_probe", true)))
		defer func() {
			_, _ = store.client.Delete(policy, key)
		}()

		res, opErr := store.executeTeranodeOp(policy, key, subOpSetLocked, "setLocked", true)
		if expectNative {
			require.NoError(t, opErr)

			parsed, perr := store.ParseLuaMapResponse(res)
			require.NoError(t, perr)
			require.Equal(t, LuaStatusOK, parsed.Status)

			rec, gerr := store.client.Get(nil, key, fields.Locked.String())
			require.NoError(t, gerr)
			require.Equal(t, true, rec.Bins[fields.Locked.String()])
		} else {
			require.Error(t, opErr)
			require.False(t, store.useNativeTeranodeOps.Load(),
				"PARAMETER_ERROR from a stock server must demote the store to the UDF path")
		}
	})
}
