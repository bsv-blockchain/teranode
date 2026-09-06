package blockassembly

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestLivenessDoesNotRestartAnIdleNode pins the property that makes this probe
// safe to enable: liveness measures whether the main loop can still be
// SERVICED, not whether work arrived. A node with no blocks is healthy — on
// mainnet the gap between blocks is routinely tens of minutes — so an idle
// node must never be restarted (issue 1447).
func TestLivenessDoesNotRestartAnIdleNode(t *testing.T) {
	server, _ := setupServer(t)

	// Shrink the tick so this runs in milliseconds instead of tens of seconds.
	// Set before Start, on this assembler only: a package-level variable would
	// be read by other tests' running loops and race with them.
	const tick = 50 * time.Millisecond
	server.blockAssembler.heartbeatInterval = tick

	// The timeout must sit BETWEEN one tick and the wait below. Above one tick
	// (plus scheduler jitter) so a beating loop stays healthy; below the wait so
	// a loop that had stopped beating would be caught. Review caught the first
	// version of this test passing with the tick disabled — it proved nothing,
	// which is the worst kind of test for a safety property.
	//
	// Both margins are deliberately generous. A beating loop is 250ms inside the
	// timeout, so a one-off GC or scheduler stall under -race cannot fail it; a
	// stopped loop is 5x past the timeout, so the test still cannot pass by luck.
	server.settings.BlockAssembly.LivenessStallTimeout = 6 * tick

	require.NoError(t, server.blockAssembler.Start(t.Context()))

	require.Eventually(t, func() bool {
		return server.blockAssembler.heartbeat.Age() > 0
	}, 5*time.Second, 5*time.Millisecond, "loop must take ownership of the heartbeat")

	// No blocks, no transactions — only the idle tick can keep this healthy.
	time.Sleep(30 * tick)

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "an idle but responsive node must stay healthy: %s", msg)
}

// TestLivenessDoesNotRestartDuringStartup pins the window review found to be
// the most dangerous: the service is constructed during Init, but its loop
// only starts well into Start, behind work that is legitimately unbounded
// (waiting on pending block validation, reloading a large unmined set). Ageing
// the heartbeat through that preamble would report a healthy, still-starting
// node as wedged — and because a restart re-enters the same preamble, the node
// could never finish starting.
func TestLivenessDoesNotRestartDuringStartup(t *testing.T) {
	server, _ := setupServer(t) // Init only — the loop has NOT started

	server.settings.BlockAssembly.LivenessStallTimeout = time.Nanosecond

	time.Sleep(10 * time.Millisecond)

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "a service still starting up must never be restarted: %s", msg)
}

// TestLivenessStartupWorkCannotStartTheClock pins the same window one level
// down, at the seam that actually broke it. validateParentChain beats on each
// completed batch so a long-but-progressing reset is not mistaken for a wedge —
// but it is also reached from Start, via loadUnminedTransactions, BEFORE the
// loop owns the heartbeat. A plain Beat there starts the clock mid-startup and
// the rest of the preamble (bulk-loading the unmined set) then ages it, which
// is precisely the crash loop the never-beaten state exists to prevent. Hence
// BeatIfStarted: silent until the loop has claimed the heartbeat.
func TestLivenessStartupWorkCannotStartTheClock(t *testing.T) {
	server, _ := setupServer(t)
	ba := server.blockAssembler

	// Stand in for startup work that beats before the loop is running.
	ba.heartbeat.BeatIfStarted()

	require.Zero(t, ba.heartbeat.Age(), "startup work must not take ownership of the heartbeat")

	server.settings.BlockAssembly.LivenessStallTimeout = time.Nanosecond

	time.Sleep(10 * time.Millisecond)

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "startup work must not arm the probe: %s", msg)

	// Once the loop owns it, the same call does refresh — otherwise a long reset
	// running inside a select case would look identical to a wedge.
	ba.heartbeat.Beat()
	ba.heartbeat.SetLastBeatForTest(time.Now().Add(-time.Hour))
	ba.heartbeat.BeatIfStarted()

	require.Less(t, ba.heartbeat.Age(), time.Minute, "once started, progress must refresh the heartbeat")
}

// TestLivenessReportsAWedgedLoop pins the other half: once the loop stops being
// serviced for longer than the configured timeout, liveness must report
// unhealthy so the orchestrator can restart the pod.
//
// The loop is deliberately NOT started. Health only needs the assembler to
// exist, and a running loop would race this test — it beats on every pass, so a
// beat landing between the backdated heartbeat and the probe would flip the
// result.
func TestLivenessReportsAWedgedLoop(t *testing.T) {
	server, _ := setupServer(t)

	server.settings.BlockAssembly.LivenessStallTimeout = time.Millisecond

	// Stand in for a loop that can no longer service its select.
	server.blockAssembler.heartbeat.SetLastBeatForTest(time.Now().Add(-time.Hour))

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Contains(t, msg, "has not made progress")
}

// TestLivenessDisabledByDefault pins the opt-in: with the default timeout the
// probe cannot restart anything, so merging this change alters no deployment's
// behaviour until an operator chooses a value.
func TestLivenessDisabledByDefault(t *testing.T) {
	server, _ := setupServer(t)

	require.Zero(t, server.settings.BlockAssembly.LivenessStallTimeout)

	server.blockAssembler.heartbeat.SetLastBeatForTest(time.Now().Add(-24 * time.Hour))

	status, _, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "default must not restart anything")
}

// TestValidateParentChainBeatsOnlyOnceTheLoopOwnsTheHeartbeat pins the seam at
// its real call site rather than through a stand-in. validateParentChain runs
// from BOTH Start, via loadUnminedTransactions, long before startChannelListeners
// exists, AND from reset inside the select case. Beating from the startup call
// arms the probe mid-startup, which is the crash loop the never-beaten state
// exists to prevent. Not beating from the reset call makes a long-but-progressing
// validation indistinguishable from a wedge. Deleting the BeatIfStarted call in
// validateParentChain fails the second half of this test; changing it to a plain
// Beat fails the first.
func TestValidateParentChainBeatsOnlyOnceTheLoopOwnsTheHeartbeat(t *testing.T) {
	ctx := t.Context()

	mockStore := new(utxo.MockUtxostore)

	tSettings := &settings.Settings{}
	// One transaction per batch, so three transactions mean three passes of the
	// batch loop and the beat cannot be an artefact of a single pass.
	tSettings.BlockAssembly.ParentValidationBatchSize = 1

	ba := &BlockAssembler{
		utxoStore: mockStore,
		settings:  tSettings,
		logger:    ulogger.TestLogger{},
	}

	// An all-zero parent hash is how validateParentChain spells "parent is
	// already mined", which keeps this test off the conflicting/cascade paths.
	var minedParent chainhash.Hash

	txs := make([]*utxo.UnminedTransaction, 0, 3)

	for i := 0; i < 3; i++ {
		var hash chainhash.Hash
		hash[0] = byte(i + 1)

		txs = append(txs, &utxo.UnminedTransaction{
			Node:       &subtree.Node{Hash: hash, Fee: 1000, SizeInBytes: 250},
			TxInpoints: singleParentInpointsPtr(minedParent, 0),
			CreatedAt:  i,
		})
	}

	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, unresolved := range args.Get(1).([]*utxo.UnresolvedMetaData) {
				unresolved.Data = &meta.Data{BlockIDs: []uint32{1}}
			}
		}).
		Return(nil)

	bestBlockHeaderIDsMap := map[uint32]bool{1: true}

	// The startup call. The heartbeat must come out of this still unclaimed.
	validTxs, err := ba.validateParentChain(ctx, txs, bestBlockHeaderIDsMap)
	require.NoError(t, err)
	require.Len(t, validTxs, len(txs))

	// Let real time pass, so a heartbeat that had been armed would now have a
	// measurable age rather than reading zero because nothing had elapsed.
	time.Sleep(10 * time.Millisecond)

	age, stalled := ba.heartbeat.Stalled(time.Nanosecond)
	require.Zero(t, age, "startup validation must not take ownership of the heartbeat")
	require.False(t, stalled, "startup validation must not be able to arm the probe")

	// Now the loop owns it. Backdate past any plausible timeout, so only a beat
	// from inside validateParentChain can bring the age back down.
	ba.heartbeat.Beat()
	ba.heartbeat.SetLastBeatForTest(time.Now().Add(-time.Hour))

	_, err = ba.validateParentChain(ctx, txs, bestBlockHeaderIDsMap)
	require.NoError(t, err)

	require.Less(t, ba.heartbeat.Age(), time.Minute,
		"validation progress inside the loop must refresh the heartbeat")
}

// TestHeartbeatIntervalFallback pins the guard added after a zero interval took
// the whole test process down. time.NewTicker panics on a non-positive interval,
// and the ticker is built inside the listener goroutine, so the panic is
// unrecoverable by the test that caused it.
func TestHeartbeatIntervalFallback(t *testing.T) {
	require.Equal(t, defaultHeartbeatInterval, effectiveHeartbeatInterval(0))
	require.Equal(t, defaultHeartbeatInterval, effectiveHeartbeatInterval(-time.Second))
	require.Equal(t, time.Millisecond, effectiveHeartbeatInterval(time.Millisecond))
}

// TestLivenessTimeoutTooTight pins the rule behind the startup warning. A
// timeout at or below twice the idle tick cannot tell an idle loop from a wedged
// one, so enabling it would restart a healthy node.
func TestLivenessTimeoutTooTight(t *testing.T) {
	const tick = time.Second

	require.False(t, livenessTimeoutTooTight(0, tick), "disabled is never too tight")
	require.False(t, livenessTimeoutTooTight(-time.Minute, tick), "disabled is never too tight")
	require.True(t, livenessTimeoutTooTight(tick, tick))
	require.True(t, livenessTimeoutTooTight(2*tick, tick), "the boundary itself is too tight")
	require.False(t, livenessTimeoutTooTight(2*tick+time.Nanosecond, tick))
}

// TestLivenessStartsTheTickerOnANonPositiveInterval runs the guard above through
// the real goroutine. A regression here does not fail cleanly, it panics inside
// the listener goroutine and takes the test binary with it, which is exactly why
// it is worth running rather than only unit-testing the predicate.
func TestLivenessStartsTheTickerOnANonPositiveInterval(t *testing.T) {
	server, _ := setupServer(t)

	server.blockAssembler.heartbeatInterval = 0

	require.NoError(t, server.blockAssembler.Start(t.Context()))

	require.Eventually(t, func() bool {
		return server.blockAssembler.heartbeat.Age() > 0
	}, 5*time.Second, 5*time.Millisecond, "the loop must start rather than panic on a zero interval")

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "a running loop must be healthy: %s", msg)
}

// TestLivenessReportsALoopThatHasStopped exercises the unhealthy path through
// the real loop instead of a backdated timestamp. Every other unhealthy test
// fakes staleness with SetLastBeatForTest on an assembler whose loop never ran,
// so none of them proves that the running loop is what keeps the probe healthy.
// Here the loop starts, beats, and is then stopped, and the probe has to notice.
func TestLivenessReportsALoopThatHasStopped(t *testing.T) {
	server, _ := setupServer(t)

	const tick = 50 * time.Millisecond

	server.blockAssembler.heartbeatInterval = tick
	server.settings.BlockAssembly.LivenessStallTimeout = 6 * tick

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, server.blockAssembler.Start(ctx))

	require.Eventually(t, func() bool {
		return server.blockAssembler.heartbeat.Age() > 0
	}, 5*time.Second, 5*time.Millisecond, "loop must take ownership of the heartbeat")

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "a running loop must be healthy: %s", msg)

	// Stop the loop. Nothing beats after this, which is what a wedge looks like
	// from the probe's side.
	cancel()

	require.Eventually(t, func() bool {
		status, _, err := server.Health(context.Background(), true)

		return err == nil && status == http.StatusServiceUnavailable
	}, 5*time.Second, 10*time.Millisecond, "a loop that has stopped being serviced must be reported as wedged")
}
