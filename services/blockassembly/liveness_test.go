package blockassembly

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
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
	//
	// Sampled continuously rather than once after a sleep. A single sample has a
	// cliff: one scheduler or GC stall longer than the timeout landing on that
	// one instant fails the test. It also under-asserts, because the property is
	// that an idle node stays healthy for the whole window, not that it happens
	// to be healthy at one moment.
	require.Never(t, func() bool {
		status, _, err := server.Health(context.Background(), true)

		return err != nil || status != http.StatusOK
	}, 30*tick, tick/2, "an idle but responsive node must stay healthy for the whole window")
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

// TestLivenessReportsAWedgedLoopAndRecovers exercises the unhealthy path through
// the real loop instead of a backdated timestamp. Every other unhealthy test
// fakes staleness with SetLastBeatForTest on an assembler whose loop never ran,
// so none of them proves that the running loop is what keeps the probe healthy.
//
// The loop is wedged the way a real handler wedges: inside a select case. A reset
// request carries an unbuffered reply channel that nobody reads, so the reset case
// blocks on its reply send and the loop stops being serviced. The probe has to
// notice, name the state the loop is stuck in, and go back to 200 once the reply
// is read and the loop runs again.
func TestLivenessReportsAWedgedLoopAndRecovers(t *testing.T) {
	server, _ := setupServer(t)

	const tick = 50 * time.Millisecond

	server.blockAssembler.heartbeatInterval = tick
	server.settings.BlockAssembly.LivenessStallTimeout = 6 * tick

	require.NoError(t, server.blockAssembler.Start(t.Context()))

	require.Eventually(t, func() bool {
		return server.blockAssembler.heartbeat.Age() > 0
	}, 5*time.Second, 5*time.Millisecond, "loop must take ownership of the heartbeat")

	status, msg, err := server.Health(t.Context(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "a running loop must be healthy: %s", msg)

	replyCh := make(chan error)

	// Registered after setupServer, so it runs before setupServer's cleanup waits
	// on the loop. An assertion failing below would otherwise leave the loop
	// blocked on this send and hang the cleanup.
	var released atomic.Bool

	t.Cleanup(func() {
		if released.CompareAndSwap(false, true) {
			select {
			case <-replyCh:
			case <-time.After(10 * time.Second):
			}
		}
	})

	server.blockAssembler.resetCh <- resetRequest{ErrCh: replyCh}

	require.Eventually(t, func() bool {
		status, msg, err = server.Health(context.Background(), true)

		return err == nil && status == http.StatusServiceUnavailable
	}, 10*time.Second, 10*time.Millisecond, "a loop that has stopped being serviced must be reported as wedged")
	require.Contains(t, msg, "state resetting", "the 503 must name the select case the loop is stuck in")

	// Read the reply: the reset case finishes and the loop beats again.
	released.Store(true)
	<-replyCh

	require.Eventually(t, func() bool {
		status, _, err := server.Health(context.Background(), true)

		return err == nil && status == http.StatusOK
	}, 5*time.Second, 10*time.Millisecond, "the probe must recover once the loop is serviced again")
}

// TestLivenessStaysHealthyThroughShutdown pins the shutdown half. On the
// OS-signal path the daemon cancels the services' context but keeps its health
// server up through the drain, so a loop that returns on ctx.Done without
// disabling its heartbeat would age past the timeout and be reported as wedged
// while it is stopping on purpose. On Kubernetes that liveness failure kills the
// container at once instead of letting the grace period finish the drain.
func TestLivenessStaysHealthyThroughShutdown(t *testing.T) {
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

	cancel()
	server.blockAssembler.Wait()

	// Well past the timeout: without Disable the heartbeat would be 3x stale by
	// the end of this window.
	require.Never(t, func() bool {
		status, _, err := server.Health(context.Background(), true)

		return err != nil || status != http.StatusOK
	}, 20*tick, tick/2, "a loop stopped by its context is shutting down, not wedged")
}

// TestLivenessCatchUpFetchBeats pins the path review found still unbeaten after
// the startup-safety fix: startChannelListeners queues an initial reconcile
// BEFORE the listener goroutine exists, so on any node that restarts behind the
// chain tip the loop claims the heartbeat and then immediately spends its FIRST
// select pass catching up. That work is unbounded, so without a beat inside it a
// healthy catching-up node reports 503, gets restarted, and re-enters the same
// catch-up: the crash loop this whole feature exists to avoid (issue 1447).
//
// The assertion is on the age observed INSIDE subtreeProcessor.Reorg, not on the
// age after processNewBlockAnnouncement returns. Reorg is the step after the
// per-block fetch, so a small age there is evidence the fetch loop beat while it
// was running, and not evidence of some later beat on the way out. Deleting the
// BeatIfStarted calls in getReorgBlocks fails this test.
func TestLivenessCatchUpFetchBeats(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 5, 900)
	addChain(t, items, chain)

	const staleBy = time.Hour

	var ageInsideReorg time.Duration

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Reorg", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) {
			ageInsideReorg = items.blockAssembler.heartbeat.Age()
		}).
		Return(nil)
	injectMockStp(t, items, mockStp)

	// BA sits at genesis while the chain is at height 5, so this takes the
	// catch-up branch and fetches all five blocks.
	items.blockAssembler.setBestBlockHeader(genesis, 0)

	// Stand in for a loop that claimed the heartbeat and then went straight into
	// this catch-up. It must be armed: BeatIfStarted is a no-op before the loop
	// owns the heartbeat, which is what keeps startup safe.
	items.blockAssembler.heartbeat.SetLastBeatForTest(time.Now().Add(-staleBy))
	require.Greater(t, items.blockAssembler.heartbeat.Age(), staleBy/2,
		"precondition: the heartbeat must start this test stale")

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	mockStp.AssertCalled(t, "Reorg", mock.Anything, mock.Anything)
	require.Less(t, ageInsideReorg, time.Minute,
		"the per-block catch-up fetch must beat, or a node restarting behind the tip reports itself wedged")
}

// TestLivenessSubtreeProcessorBeatsForTheLoop pins the wiring between the
// assembler and its subtree processor. MoveForwardBlock and Reorg block the main
// loop for the whole call, so the processor must beat on the loop's behalf at
// each step, and it can only do that if NewBlockAssembler installed the
// heartbeat as its progress hook. The processor's own tests pin WHERE it
// reports progress; this one pins that the reports reach the heartbeat the
// probe reads (issue 1447).
//
// WaitForPendingBlocks is the driver because it reaches the hook with nothing
// else to stub, on the real processor Init built.
//
// It also pins the startup half. Start calls WaitForPendingBlocks before the
// loop owns the heartbeat, so the hook must be BeatIfStarted: a plain Beat there
// would start the clock mid-startup and let the probe report a still-starting
// node as wedged.
func TestLivenessSubtreeProcessorBeatsForTheLoop(t *testing.T) {
	server, _ := setupServer(t)
	ba := server.blockAssembler

	require.NoError(t, ba.subtreeProcessor.WaitForPendingBlocks(t.Context()))
	require.Zero(t, ba.heartbeat.Age(), "progress before the loop owns the heartbeat must not arm it")

	const staleBy = time.Hour

	ba.heartbeat.SetLastBeatForTest(time.Now().Add(-staleBy))
	require.Greater(t, ba.heartbeat.Age(), staleBy/2, "precondition: the heartbeat must start stale")

	require.NoError(t, ba.subtreeProcessor.WaitForPendingBlocks(t.Context()))
	require.Less(t, ba.heartbeat.Age(), time.Minute, "subtree processor progress must reach the main loop's heartbeat")
}

// TestLivenessWaitForBlockMinedSetBeats pins the one wait on block validation
// that lives in the assembler rather than the subtree processor: reset waits on
// each invalid moved-back block, up to about 78s apiece. Each attempt must beat,
// like every other wait on block validation, because a slow dependency is a
// readiness failure and not a wedge (issue 1447).
func TestLivenessWaitForBlockMinedSetBeats(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	const staleBy = time.Hour

	items.blockAssembler.heartbeat.SetLastBeatForTest(time.Now().Add(-staleBy))
	require.Greater(t, items.blockAssembler.heartbeat.Age(), staleBy/2, "precondition: the heartbeat must start stale")

	require.NoError(t, items.blockAssembler.waitForBlockMinedSet(t.Context(), genesis.Hash()))
	require.Less(t, items.blockAssembler.heartbeat.Age(), time.Minute, "an attempt of the mined_set wait must beat")
}
