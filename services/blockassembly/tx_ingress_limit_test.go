// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
)

// addTestTxs enqueues n distinct transactions into block assembly.
//
// These tests size nothing against blockassembly_maxQueueItems, which is 0 (unbounded) here, so the
// enqueue cannot be refused for queue room and the return value carries no information. That is the
// point of the separation being tested: the queue bound and the resident-set bound are independent,
// and this file exercises the latter with the former disabled.
func addTestTxs(t *testing.T, ba *BlockAssembler, n int) {
	t.Helper()

	nodes := make([]subtreepkg.Node, 0, n)
	inpoints := make([]*subtreepkg.TxInpoints, 0, n)

	for i := 0; i < n; i++ {
		nodes = append(nodes, subtreepkg.Node{
			Hash:        *newTx(uint32(i + 1)).TxIDChainHash(),
			Fee:         1,
			SizeInBytes: 100,
		})
		inpoints = append(inpoints, &subtreepkg.TxInpoints{})
	}

	require.True(t, ba.AddTxBatchIfRoom(nodes, inpoints),
		"the ingest queue is unbounded in these tests, so the batch must always be enqueued")
}

// TestResumeWatermark checks how the low watermark is derived from the configured limit.
func TestResumeWatermark(t *testing.T) {
	tests := []struct {
		name             string
		limit            uint64
		configuredResume uint64
		expected         uint64
	}{
		{
			name:     "no limit gives no watermark",
			limit:    0,
			expected: 0,
		},
		{
			name:     "defaults to 90 percent of the limit",
			limit:    1_000_000_000,
			expected: 900_000_000,
		},
		{
			name:     "stays exact for a small limit",
			limit:    10,
			expected: 9,
		},
		{
			name:             "honours a configured value below the limit",
			limit:            1000,
			configuredResume: 250,
			expected:         250,
		},
		{
			name:             "falls back when the configured value equals the limit",
			limit:            1000,
			configuredResume: 1000,
			expected:         900,
		},
		{
			name:             "falls back when the configured value exceeds the limit",
			limit:            1000,
			configuredResume: 5000,
			expected:         900,
		},
		{
			name:     "a limit of one leaves only zero below it",
			limit:    1,
			expected: 0,
		},
		{
			name:     "does not overflow near the top of the range",
			limit:    1 << 62,
			expected: (1 << 62) - (1 << 62 / 10),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := resumeWatermark(tt.limit, tt.configuredResume)
			require.Equal(t, tt.expected, actual)

			if tt.limit > 0 {
				require.Less(t, actual, tt.limit, "the resume watermark must sit below the limit to give hysteresis")
			}
		})
	}
}

// TestTxIngressHysteresis walks the ingress flag across both watermarks.
//
// The flag must set at the limit, hold between the watermarks, and clear only at the resume
// watermark. Holding between the watermarks is what stops the node flapping once it settles at
// the limit.
func TestTxIngressHysteresis(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler
	ba.txIngressLimit = 100
	ba.txIngressResume = 90

	steps := []struct {
		name            string
		count           uint64
		expectedFull    bool
		expectedChanged bool
	}{
		{name: "well below the limit", count: 0, expectedFull: false, expectedChanged: false},
		{name: "just below the limit", count: 99, expectedFull: false, expectedChanged: false},
		{name: "reaching the limit sets the flag", count: 100, expectedFull: true, expectedChanged: true},
		{name: "staying at the limit does not re-fire", count: 100, expectedFull: true, expectedChanged: false},
		{name: "above the limit stays full", count: 250, expectedFull: true, expectedChanged: false},
		{name: "below the limit but above resume holds full", count: 99, expectedFull: true, expectedChanged: false},
		{name: "just above resume still holds full", count: 91, expectedFull: true, expectedChanged: false},
		{name: "reaching resume clears the flag", count: 90, expectedFull: false, expectedChanged: true},
		{name: "staying at resume does not re-fire", count: 90, expectedFull: false, expectedChanged: false},
		{name: "climbing back to the limit sets it again", count: 100, expectedFull: true, expectedChanged: true},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			full, changed := ba.applyTxIngressCount(step.count)

			require.Equal(t, step.expectedFull, full, "full")
			require.Equal(t, step.expectedChanged, changed, "changed")
			require.Equal(t, step.expectedFull, ba.IsTxIngressFull(), "IsTxIngressFull")
		})
	}
}

// TestTxIngressLimitDisabled checks that a zero limit never refuses ingress, whatever the count.
func TestTxIngressLimitDisabled(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	require.Zero(t, ba.txIngressLimit, "the limit must default to disabled")

	for _, count := range []uint64{0, 1, 1_000, 1_000_000_000_000} {
		full, changed := ba.applyTxIngressCount(count)

		require.False(t, full, "a disabled limit must never report full, count %d", count)
		require.False(t, changed, "a disabled limit must never report a change, count %d", count)
		require.False(t, ba.IsTxIngressFull())
	}
}

// TestTransactionsInMemoryCountsQueuedTransactions checks the measurement the limit is compared
// against.
//
// A transaction is queued before it reaches a subtree, so queued transactions have to be counted.
// Counting only what reached a subtree would under-report what block assembly actually holds and
// let it sail past the limit.
func TestTransactionsInMemoryCountsQueuedTransactions(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	// A fresh subtree processor already holds the coinbase placeholder node, so take the starting
	// count as the baseline rather than assuming it is empty.
	baseline := ba.TransactionsInMemory()

	const txCount = 8

	addTestTxs(t, ba, txCount)

	require.Equal(t, int64(txCount), ba.QueueLength(),
		"the transactions should be sitting in the queue, since the processor is not draining it")

	require.Equal(t, baseline+txCount, ba.TransactionsInMemory(),
		"queued transactions must be counted towards what block assembly holds")
}

// TestPublishTxIngressFullReachesBlockchainClient checks the broadcast path end to end.
//
// Block assembly publishes over the blockchain notification bus, and the client caches the value so
// ingress points can read it per transaction without an RPC.
func TestPublishTxIngressFullReachesBlockchainClient(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler
	ctx := t.Context()

	require.False(t, testItems.blockchainClient.IsBlockAssemblyFull(),
		"a client must default to accepting transactions before it hears anything")

	require.NoError(t, ba.publishTxIngressFull(ctx, true))
	require.True(t, testItems.blockchainClient.IsBlockAssemblyFull(),
		"the client must see block assembly report full")

	require.NoError(t, ba.publishTxIngressFull(ctx, false))
	require.False(t, testItems.blockchainClient.IsBlockAssemblyFull(),
		"the client must see block assembly report it has room again")
}

// TestBlockAssemblyFullNotificationCarriesFlag checks the notification contract itself, so the
// producer and the consumers cannot drift apart on the metadata key or its encoding.
func TestBlockAssemblyFullNotificationCarriesFlag(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ctx := t.Context()

	subCh, err := testItems.blockchainClient.Subscribe(ctx, "tx-ingress-test")
	require.NoError(t, err)

	require.NoError(t, testItems.blockAssembler.publishTxIngressFull(ctx, true))

	// The subscription carries every notification type, so skip past any unrelated traffic
	// (block notifications from the store, for example) rather than assuming ours arrives first.
	deadline := time.After(5 * time.Second)

	for {
		select {
		case notification := <-subCh:
			if notification == nil || notification.Type != model.NotificationType_BlockAssemblyFull {
				continue
			}

			require.Equal(t, "true", notification.GetMetadata().GetMetadata()["full"],
				"the notification must carry the flag under the \"full\" key, encoded as a bool string")

			return
		case <-deadline:
			t.Fatal("timed out waiting for the block assembly full notification")
		}
	}
}

// TestTxIngressLimitMonitorFlipsTheFlag exercises the whole loop: real transactions go into a real
// subtree processor, the monitor notices the limit is reached, and the blockchain client that the
// ingress points read ends up reporting full.
func TestTxIngressLimitMonitorFlipsTheFlag(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	const limit = 5

	ba.txIngressLimit = limit
	ba.txIngressResume = resumeWatermark(limit, 0)
	ba.txIngressEvaluateInterval = 10 * time.Millisecond
	ba.txIngressHeartbeatInterval = 100 * time.Millisecond

	ctx := t.Context()

	ba.startTxIngressLimitMonitor(ctx)

	require.False(t, testItems.blockchainClient.IsBlockAssemblyFull(),
		"ingress must be open before the limit is reached")

	addTestTxs(t, ba, limit)

	require.Eventually(t, func() bool {
		return testItems.blockchainClient.IsBlockAssemblyFull()
	}, 5*time.Second, 10*time.Millisecond,
		"the ingress points should have been told block assembly is full (in memory %d, limit %d)",
		ba.TransactionsInMemory(), limit)

	require.True(t, ba.IsTxIngressFull(), "block assembly must agree with what it published")
}

// TestTxIngressLimitMonitorClearsTheFlag exercises the other direction of the same loop.
//
// The requirement has three parts: cap RAM, refuse ingress, and recover. Recovery is the part that
// matters most in production, because a node that never publishes the clearing transition keeps every
// ingress point refusing after block assembly has drained. TestTxIngressHysteresis pins the rule on
// an injected count and TestPublishTxIngressFullReachesBlockchainClient pins the broadcast, but only
// this drives the monitor's own evaluate tick into publishing full=false from a real measurement.
//
// The assembler starts already flagged full, as it would be after filling, while holding nothing.
// The watermarks are set before the monitor starts, so nothing mutates them underneath it.
func TestTxIngressLimitMonitorClearsTheFlag(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	const limit = 100

	ba.txIngressLimit = limit
	ba.txIngressResume = 90
	ba.txIngressEvaluateInterval = 10 * time.Millisecond

	// A heartbeat far longer than the test, so it cannot be what clears the flag. The heartbeat
	// re-announces whatever the flag currently is, so with a short interval it would reopen ingress
	// even if the evaluate branch never published the transition, and this test would pass against a
	// monitor that had lost the ability to announce recovery promptly.
	ba.txIngressHeartbeatInterval = 10 * time.Minute

	ctx := t.Context()

	// Block assembly filled and told the ingress points to stop.
	full, changed := ba.applyTxIngressCount(limit)
	require.True(t, full)
	require.True(t, changed)

	require.NoError(t, ba.publishTxIngressFull(ctx, true))
	require.True(t, testItems.blockchainClient.IsBlockAssemblyFull(),
		"the ingress points must start this test refusing transactions")

	// It now holds far less than the resume watermark, as it would after a block drained it.
	require.LessOrEqual(t, ba.TransactionsInMemory(), ba.txIngressResume,
		"the assembler must be below the resume watermark for the monitor to clear the flag")

	ba.startTxIngressLimitMonitor(ctx)

	require.Eventually(t, func() bool {
		return !testItems.blockchainClient.IsBlockAssemblyFull()
	}, 5*time.Second, 10*time.Millisecond,
		"the monitor must publish the clearing transition, or ingress stays refused after draining")

	require.False(t, ba.IsTxIngressFull(), "block assembly must agree with what it published")
}

// TestTxIngressLimitMonitorHeartbeatReAnnounces pins the re-announcement the ingress points depend on.
//
// The cached flag in each client expires if block assembly stops repeating itself, so the heartbeat is
// what keeps a genuine refusal in force. Deleting that branch would leave a full node quietly
// reopening ingress once the cached refusal aged out, with no other test noticing.
func TestTxIngressLimitMonitorHeartbeatReAnnounces(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	// A limit that is already exceeded, so the flag stays full with no count change to drive it.
	ba.txIngressLimit = 1
	ba.txIngressResume = 0
	ba.txIngressEvaluateInterval = 10 * time.Millisecond
	ba.txIngressHeartbeatInterval = 20 * time.Millisecond

	ctx := t.Context()

	subCh, err := testItems.blockchainClient.Subscribe(ctx, "tx-ingress-heartbeat-test")
	require.NoError(t, err)

	addTestTxs(t, ba, 5)

	ba.startTxIngressLimitMonitor(ctx)

	// Count only the repeats: the first announcement is the transition, so a second one can only
	// have come from the heartbeat.
	announcements := 0
	deadline := time.After(5 * time.Second)

	for announcements < 2 {
		select {
		case notification := <-subCh:
			if notification == nil || notification.Type != model.NotificationType_BlockAssemblyFull {
				continue
			}

			require.Equal(t, "true", notification.GetMetadata().GetMetadata()["full"],
				"a block assembly over its limit must keep announcing that it is full")

			announcements++
		case <-deadline:
			t.Fatalf("the heartbeat must re-announce the current flag; saw %d announcements", announcements)
		}
	}
}

// TestTxIngressLimitMonitorDoesNothingWhenDisabled checks that the default configuration keeps the
// previous behaviour: no monitoring, no notifications, ingress always open.
func TestTxIngressLimitMonitorDoesNothingWhenDisabled(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler
	require.Zero(t, ba.txIngressLimit)

	// Short intervals so "long enough for a running monitor to have spoken" is milliseconds of wall
	// clock rather than the production heartbeat.
	ba.txIngressEvaluateInterval = 10 * time.Millisecond
	ba.txIngressHeartbeatInterval = 20 * time.Millisecond

	ctx := t.Context()

	subCh, err := testItems.blockchainClient.Subscribe(ctx, "tx-ingress-disabled-test")
	require.NoError(t, err)

	ba.startTxIngressLimitMonitor(ctx)

	addTestTxs(t, ba, 20)

	// Wait past both the evaluate and heartbeat intervals, so a running monitor would have spoken.
	deadline := time.After(ba.txIngressHeartbeatInterval*10 + 200*time.Millisecond)

	for {
		select {
		case notification := <-subCh:
			if notification != nil && notification.Type == model.NotificationType_BlockAssemblyFull {
				t.Fatal("a disabled limit must not publish block assembly full notifications")
			}
		case <-deadline:
			require.False(t, ba.IsTxIngressFull())
			require.False(t, testItems.blockchainClient.IsBlockAssemblyFull())

			return
		}
	}
}

// TestTxIngressEvaluateHoldsRefusalWhileUnminedTransactionsLoad pins the rule that keeps a restart
// from reopening ingress.
//
// A block assembly that restarts while full comes up holding nothing and then spends the whole of
// loadUnminedTransactions refilling. Measured naively it looks empty for that entire period, so the
// evaluate tick would announce room to every ingress point moments before the reload takes that room
// back.
//
// The rule therefore has to ESTABLISH the refusal, not merely hold one that is already in force. A
// restarted process has txIngressFull at its zero value — nothing carries the flag across a process
// boundary — so a guard predicated on the flag already being set would be inert in exactly the case
// it is named for. The first subtest is the one that fails if that predicate comes back.
func TestTxIngressEvaluateHoldsRefusalWhileUnminedTransactionsLoad(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	const limit = 5

	ba.txIngressLimit = limit
	ba.txIngressResume = resumeWatermark(limit, 0)

	t.Run("a reloading process with no flag of its own still refuses", func(t *testing.T) {
		require.False(t, ba.IsTxIngressFull(),
			"arrange: a restarted process starts not-full, whatever it was doing when it died")

		ba.unminedTransactionsLoading.Store(true)
		defer ba.unminedTransactionsLoading.Store(false)

		require.LessOrEqual(t, ba.TransactionsInMemory(), ba.txIngressResume,
			"arrange: a restarted assembler is below the resume watermark until the reload refills it")

		full, changed := ba.evaluateTxIngressFull()
		require.True(t, full, "a near-empty assembler mid-reload must not report room")
		require.True(t, changed, "and must publish the refusal, since nothing else will")
		require.True(t, ba.IsTxIngressFull())
	})

	t.Run("the refusal is not re-published on every tick", func(t *testing.T) {
		require.True(t, ba.IsTxIngressFull(), "arrange: carried over from the previous case")

		ba.unminedTransactionsLoading.Store(true)
		defer ba.unminedTransactionsLoading.Store(false)

		full, changed := ba.evaluateTxIngressFull()
		require.True(t, full)
		require.False(t, changed, "the refusal is already in force, so this is not a transition")
	})

	t.Run("the refusal clears once the reload finishes", func(t *testing.T) {
		require.True(t, ba.IsTxIngressFull(), "arrange: carried over from the previous case")
		require.False(t, ba.unminedTransactionsLoading.Load())

		full, changed := ba.evaluateTxIngressFull()
		require.False(t, full, "with the reload done the measurement is meaningful again")
		require.True(t, changed, "so the clearing transition must be published")
	})

	t.Run("startup before the reload begins also refuses", func(t *testing.T) {
		require.False(t, ba.IsTxIngressFull(), "arrange: carried over from the previous case")
		require.False(t, ba.unminedTransactionsLoading.Load(),
			"arrange: the reload has not started yet, as during WaitForPendingBlocks")

		ba.txIngressStartupPending.Store(true)
		defer ba.txIngressStartupPending.Store(false)

		full, changed := ba.evaluateTxIngressFull()
		require.True(t, full,
			"WaitForPendingBlocks and the conflict intent replay run before the reload, and the "+
				"cached refusal at the ingress points can expire during them")
		require.True(t, changed)
	})

	t.Run("a disabled limit is never held full by startup", func(t *testing.T) {
		ba.txIngressFull.Store(false)

		ba.txIngressLimit = 0
		defer func() { ba.txIngressLimit = limit }()

		ba.txIngressStartupPending.Store(true)
		ba.unminedTransactionsLoading.Store(true)

		defer func() {
			ba.txIngressStartupPending.Store(false)
			ba.unminedTransactionsLoading.Store(false)
		}()

		full, changed := ba.evaluateTxIngressFull()
		require.False(t, full, "the default configuration must never refuse, startup included")
		require.False(t, changed)
	})
}

// TestTxIngressLimitMonitorHeartbeatDoesNotAnnounceRoom checks that the heartbeat re-announces a
// refusal and nothing else.
//
// The ingress points expire a cached full=true on their own, so not-full needs no repeating. A
// heartbeat that repeated it would do real harm during startup: a process that restarted while full
// would broadcast full=false every heartbeat while it reloaded, clearing a refusal the ingress
// points were correctly holding, and it would do so far sooner than the expiry that is supposed to
// govern that decision.
func TestTxIngressLimitMonitorHeartbeatDoesNotAnnounceRoom(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	// A limit this assembler is nowhere near, so the flag stays false with no transition to publish.
	ba.txIngressLimit = 1_000_000
	ba.txIngressResume = resumeWatermark(ba.txIngressLimit, 0)
	ba.txIngressEvaluateInterval = 10 * time.Millisecond
	ba.txIngressHeartbeatInterval = 20 * time.Millisecond

	ctx := t.Context()

	subCh, err := testItems.blockchainClient.Subscribe(ctx, "tx-ingress-quiet-heartbeat-test")
	require.NoError(t, err)

	ba.startTxIngressLimitMonitor(ctx)

	addTestTxs(t, ba, 20)

	deadline := time.After(ba.txIngressHeartbeatInterval*10 + 200*time.Millisecond)

	for {
		select {
		case notification := <-subCh:
			if notification != nil && notification.Type == model.NotificationType_BlockAssemblyFull {
				t.Fatalf("a block assembly with room must stay silent, got full=%q",
					notification.GetMetadata().GetMetadata()["full"])
			}
		case <-deadline:
			require.False(t, ba.IsTxIngressFull())
			require.False(t, testItems.blockchainClient.IsBlockAssemblyFull())

			return
		}
	}
}

// TestTxIngressLimitMonitorKeepsIngressRefusedDuringReload is the restart case end to end.
//
// It stands in for a block assembly that was killed while full and has come back. Everything this
// process inherits is on the other side of a process boundary: the ingress points elsewhere still
// hold a cached refusal, but this assembler holds nothing and its own flag is at its zero value.
// Nothing carries txIngressFull across a restart.
//
// So the monitor has to re-establish the refusal from that blank state, and do it before the cached
// refusal at the ingress points expires. Measuring alone cannot: for most of a large reload the
// count sits below the limit, so a monitor that only measured would publish nothing and let every
// ingress point reopen mid-reload — piling new work on a backlog this process already cannot fit.
//
// The arrange below deliberately does NOT pre-set ba.txIngressFull. Setting it would model a state a
// restarted process cannot be in, and would let a monitor that merely holds an existing refusal pass
// this test while the real restart case stayed broken.
func TestTxIngressLimitMonitorKeepsIngressRefusedDuringReload(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	const limit = 100

	ba.txIngressLimit = limit
	ba.txIngressResume = resumeWatermark(limit, 0)
	ba.txIngressEvaluateInterval = 10 * time.Millisecond
	ba.txIngressHeartbeatInterval = 20 * time.Millisecond

	ctx := t.Context()

	// The state a restarted process is actually in: empty, and knowing nothing.
	require.False(t, ba.IsTxIngressFull(), "arrange: a restarted process starts not-full")
	require.False(t, testItems.blockchainClient.IsBlockAssemblyFull())
	require.Less(t, ba.TransactionsInMemory(), ba.txIngressResume,
		"arrange: the reload has not refilled it, so measuring alone would report room")

	ba.unminedTransactionsLoading.Store(true)

	ba.startTxIngressLimitMonitor(ctx)

	require.Eventually(t, func() bool {
		return testItems.blockchainClient.IsBlockAssemblyFull()
	}, 5*time.Second, 10*time.Millisecond,
		"the monitor must re-establish the refusal from nothing, or the ingress points expire theirs mid-reload")

	// And it must hold for the whole reload. Long enough for many evaluate ticks and many heartbeats.
	time.Sleep(ba.txIngressHeartbeatInterval*10 + 200*time.Millisecond)

	require.True(t, testItems.blockchainClient.IsBlockAssemblyFull(),
		"ingress must stay refused for the whole reload, or the node takes new work on top of it")
	require.True(t, ba.IsTxIngressFull())

	// The reload finishes below the limit, so the monitor must now release the ingress points.
	ba.unminedTransactionsLoading.Store(false)

	require.Eventually(t, func() bool {
		return !testItems.blockchainClient.IsBlockAssemblyFull()
	}, 5*time.Second, 10*time.Millisecond,
		"once the reload is done the assembler has room, so ingress must reopen")
}

// TestTxIngressLimitMonitorRefusesBeforeTheReloadBegins covers the rest of the startup window.
//
// Start runs WaitForPendingBlocks and the conflict intent replay before loadUnminedTransactions, and
// both can take longer than blockAssemblyFullTTL. unminedTransactionsLoading is false throughout, so
// a rule keyed only on the reload would let the ingress points expire their cached refusal before
// this process had loaded anything at all.
func TestTxIngressLimitMonitorRefusesBeforeTheReloadBegins(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	ba.txIngressLimit = 100
	ba.txIngressResume = resumeWatermark(ba.txIngressLimit, 0)
	ba.txIngressEvaluateInterval = 10 * time.Millisecond
	ba.txIngressHeartbeatInterval = 20 * time.Millisecond

	ctx := t.Context()

	require.False(t, ba.IsTxIngressFull(), "arrange: a restarted process starts not-full")

	// Start has begun, but the reload has not.
	ba.txIngressStartupPending.Store(true)
	require.False(t, ba.unminedTransactionsLoading.Load())

	ba.startTxIngressLimitMonitor(ctx)

	require.Eventually(t, func() bool {
		return testItems.blockchainClient.IsBlockAssemblyFull()
	}, 5*time.Second, 10*time.Millisecond,
		"a block assembly that has not finished starting must refuse, not announce the room it has not filled yet")

	// Startup completes with the assembler below the resume watermark, so ingress reopens.
	ba.txIngressStartupPending.Store(false)

	require.Eventually(t, func() bool {
		return !testItems.blockchainClient.IsBlockAssemblyFull()
	}, 5*time.Second, 10*time.Millisecond,
		"a started assembler with room must release the ingress points")
}

// TestTxIngressStartupHoldIsInertWithoutALimit checks that the default configuration is untouched by
// the startup hold: a node with no limit must never refuse, at startup or anywhere else.
func TestTxIngressStartupHoldIsInertWithoutALimit(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler
	require.Zero(t, ba.txIngressLimit, "the limit must default to disabled")

	ba.txIngressEvaluateInterval = 10 * time.Millisecond
	ba.txIngressHeartbeatInterval = 20 * time.Millisecond

	ctx := t.Context()

	ba.txIngressStartupPending.Store(true)
	ba.unminedTransactionsLoading.Store(true)

	subCh, err := testItems.blockchainClient.Subscribe(ctx, "tx-ingress-startup-disabled-test")
	require.NoError(t, err)

	ba.startTxIngressLimitMonitor(ctx)

	deadline := time.After(ba.txIngressHeartbeatInterval*10 + 200*time.Millisecond)

	for {
		select {
		case notification := <-subCh:
			if notification != nil && notification.Type == model.NotificationType_BlockAssemblyFull {
				t.Fatal("a disabled limit must not refuse ingress during startup")
			}
		case <-deadline:
			require.False(t, ba.IsTxIngressFull())
			require.False(t, testItems.blockchainClient.IsBlockAssemblyFull())

			return
		}
	}
}

// blockchainClientIsFull is a compile-time check that the ingress points can read the flag through
// the interface they hold, rather than through a concrete client type.
var _ = func(c blockchain.ClientI) bool { return c.IsBlockAssemblyFull() }

// flakyNotificationClient wraps a blockchain client and fails a set number of SendNotification
// calls before letting them through, so a test can lose a specific announcement.
type flakyNotificationClient struct {
	blockchain.ClientI

	remainingFailures atomic.Int64
}

func (c *flakyNotificationClient) SendNotification(ctx context.Context, notification *blockchain_api.Notification) error {
	if c.remainingFailures.Load() > 0 {
		c.remainingFailures.Add(-1)

		return errors.NewServiceError("[test] notification bus unavailable")
	}

	return c.ClientI.SendNotification(ctx, notification)
}

// TestTxIngressLimitMonitorRetriesALostClearingPublish pins the recovery of a transition whose
// publish failed.
//
// The heartbeat re-announces a refusal, so a lost full=true fixes itself. A lost full=false has
// nothing behind it: not-full is deliberately never repeated, and the monitor reports a transition
// only once, so without a retry the announcement is simply gone. Every ingress point would then
// keep refusing until its cached refusal aged out — up to blockAssemblyFullTTL of a node turning
// away transactions it has room for.
//
// The heartbeat here is far longer than the test, and blockAssemblyFullTTL is a minute, so neither
// can be what reopens ingress. Only the retry can.
func TestTxIngressLimitMonitorRetriesALostClearingPublish(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	const limit = 100

	ba.txIngressLimit = limit
	ba.txIngressResume = 90
	ba.txIngressEvaluateInterval = 10 * time.Millisecond
	ba.txIngressHeartbeatInterval = 10 * time.Minute

	ctx := t.Context()

	// Block assembly filled and told the ingress points to stop. This announcement gets through.
	full, changed := ba.applyTxIngressCount(limit)
	require.True(t, full)
	require.True(t, changed)

	require.NoError(t, ba.publishTxIngressFull(ctx, true))
	require.True(t, testItems.blockchainClient.IsBlockAssemblyFull(),
		"arrange: the ingress points must start this test refusing")

	// From here the bus rejects the next few sends, which will swallow the clearing transition and
	// the first retries of it.
	flaky := &flakyNotificationClient{ClientI: ba.blockchainClient}
	flaky.remainingFailures.Store(3)
	ba.blockchainClient = flaky

	// The assembler now holds far less than the resume watermark, as it would after a block drained
	// it, so the monitor decides to clear.
	require.LessOrEqual(t, ba.TransactionsInMemory(), ba.txIngressResume)

	ba.startTxIngressLimitMonitor(ctx)

	require.Eventually(t, func() bool {
		return !testItems.blockchainClient.IsBlockAssemblyFull()
	}, 5*time.Second, 10*time.Millisecond,
		"the clearing transition was lost, so the monitor must retry it; otherwise ingress stays refused until the cached refusal expires")

	require.Zero(t, flaky.remainingFailures.Load(),
		"the test must actually have exercised the failures it arranged")
	require.False(t, ba.IsTxIngressFull())
}
