package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	aeropruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
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
	test.SkipIfContainerUnavailable(t, err)

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

	// The start-up probe records its replay-protection verdict, which routes
	// spendMulti: native only on a dispatcher that proved the marker and the
	// idempotent report.
	t.Run("replay_protection_verdict_matches_server", func(t *testing.T) {
		require.Equal(t, expectNative, store.nativeReplayProtection.Load())
		require.Equal(t, expectNative, store.useNativeForSubOp(subOpSpendMulti))
	})

	// Every probe writes a short-lived record and must delete it again, on any
	// server image.
	t.Run("probe_record_cleanup_deletes_it", func(t *testing.T) {
		key, _, cleanup, built := store.buildProbeRecord(probePolicy(), "cleanup test", 1000, 0)
		require.True(t, built)

		exists, err := store.client.Exists(nil, key)
		require.NoError(t, err)
		require.True(t, exists, "fixture: the probe record was written")

		cleanup()

		exists, err = store.client.Exists(nil, key)
		require.NoError(t, err)
		require.False(t, exists, "the probe's cleanup deletes its record")
	})

	t.Run("spend_semantics_probe_direct", func(t *testing.T) {
		require.Equal(t, expectNative, store.probeNativeSpendSemantics(ctx, probePolicy()),
			"spend probe must accept the fork dispatcher and reject a stock server")
	})

	// The replay marker and the idempotent report are enforced in teranode.lua
	// and are invisible to the client, so the gate asks the server rather than
	// assuming. Against a stock server this must answer false, which is what
	// keeps spendMulti on the UDF path where both are enforced in-repo.
	t.Run("replay_protection_probe_direct", func(t *testing.T) {
		require.Equal(t, expectNative, store.probeNativeReplayProtection(ctx, probePolicy()),
			"replay-protection probe must accept a dispatcher that enforces deletedChildren and reports idempotent matches, and reject one that does not")
	})

	// A stock server refuses the opcode, so the probe could not finish. That says nothing about
	// the build's semantics and must not be reported as the mismatch an older fork build gives.
	t.Run("replay_protection_probe_verdict_on_a_server_that_refuses_the_opcode", func(t *testing.T) {
		if expectNative {
			t.Skip("the fork image serves the opcode")
		}

		require.Equal(t, replayProbeUnavailable, store.probeNativeReplayProtectionVerdict(ctx, probePolicy()))
	})

	t.Run("replay_protection_probe_honours_cancelled_context", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		require.False(t, store.probeNativeReplayProtection(cancelled, probePolicy()),
			"a cancelled probe must demote rather than pass by default")
	})

	// Sub-op 15 has its own probe, because a fork build older than it serves
	// every other native op. AEROSPIKE_EXPECT_NATIVE_VERIFIED_MARKER=false runs
	// these against such a build (aerospike-tn:mark) with native ops expected.
	expectVerifiedMarker := expectNative
	if v := os.Getenv("AEROSPIKE_EXPECT_NATIVE_VERIFIED_MARKER"); v != "" {
		expectVerifiedMarker = v == "true"
	}

	t.Run("verified_marker_probe_decision_matches_server", func(t *testing.T) {
		require.Equal(t, expectVerifiedMarker, store.useNativeVerifiedMarkerWrite(),
			"the pruner's marker write must use sub-op 15 exactly when the server serves it")
	})

	t.Run("verified_marker_probe_direct", func(t *testing.T) {
		require.Equal(t, expectVerifiedMarker, store.detectNativeVerifiedMarkerSupport(ctx),
			"the sub-op 15 probe must accept a server that writes the verified marker and refuses the two non-spenders, and reject any other")
	})

	t.Run("verified_marker_probe_honours_cancelled_context", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		require.False(t, store.detectNativeVerifiedMarkerSupport(cancelled))
	})

	// A server without sub-op 15 answers a live marker write with
	// PARAMETER_ERROR. The observer the pruner calls must turn the native
	// marker write off and leave the other native ops alone.
	t.Run("verified_marker_parameter_error_turns_sub_op_15_off", func(t *testing.T) {
		if expectVerifiedMarker {
			t.Skip("the server serves sub-op 15")
		}

		prevOps, prevMarker := store.useNativeTeranodeOps.Load(), store.useNativeVerifiedMarker.Load()
		store.useNativeTeranodeOps.Store(true)
		store.useNativeVerifiedMarker.Store(true)

		defer func() {
			store.useNativeTeranodeOps.Store(prevOps)
			store.useNativeVerifiedMarker.Store(prevMarker)
		}()

		key, err := aerospike.NewKey(store.namespace, store.setName, "_native-verified-marker-demote-test")
		require.NoError(t, err)

		child := chainhash.HashH([]byte("child"))
		spent := make([]byte, 68)
		copy(spent[32:64], child.CloneBytes())

		policy := probePolicy()
		require.NoError(t, store.client.PutBins(policy, key, aerospike.NewBin(fields.Utxos.String(), []any{spent})))

		defer func() {
			_, _ = store.client.Delete(policy, key)
		}()

		record, native := store.prunerVerifiedMarkerRecord(key, []aeropruner.MarkerChild{{Hash: &child, Offsets: []uint32{0}}})
		require.True(t, native)

		_ = store.client.BatchOperate(nil, []aerospike.BatchRecordIfc{record})
		require.True(t, hasResultCode(record.BatchRec().Err, types.PARAMETER_ERROR), "got %v", record.BatchRec().Err)

		store.observePrunerNativeOpError(record.BatchRec().Err)
		require.False(t, store.useNativeVerifiedMarkerWrite())
		require.True(t, store.useNativeTeranodeOps.Load(), "the other native ops stay on")
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
