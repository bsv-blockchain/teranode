// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"testing"
	"time"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

// addTestTxs enqueues n distinct transactions into block assembly.
func addTestTxs(ba *BlockAssembler, n int) {
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

	ba.AddTxBatch(nodes, inpoints)
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

	addTestTxs(ba, txCount)

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

	ba.publishTxIngressFull(ctx, true)
	require.True(t, testItems.blockchainClient.IsBlockAssemblyFull(),
		"the client must see block assembly report full")

	ba.publishTxIngressFull(ctx, false)
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

	testItems.blockAssembler.publishTxIngressFull(ctx, true)

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

	addTestTxs(ba, limit)

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

	ba.publishTxIngressFull(ctx, true)
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

	addTestTxs(ba, 5)

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

	addTestTxs(ba, 20)

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
// back. Only the clearing direction is suppressed — an assembler that fills past the limit during a
// reload must still refuse.
func TestTxIngressEvaluateHoldsRefusalWhileUnminedTransactionsLoad(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	require.NotNil(t, testItems)

	ba := testItems.blockAssembler

	const limit = 5

	ba.txIngressLimit = limit
	ba.txIngressResume = resumeWatermark(limit, 0)

	t.Run("a standing refusal survives the reload", func(t *testing.T) {
		require.True(t, ba.txIngressFull.CompareAndSwap(false, true), "arrange: start this case full")

		ba.unminedTransactionsLoading.Store(true)
		defer ba.unminedTransactionsLoading.Store(false)

		require.LessOrEqual(t, ba.TransactionsInMemory(), ba.txIngressResume,
			"arrange: a restarted assembler is below the resume watermark until the reload refills it")

		full, changed := ba.evaluateTxIngressFull()
		require.True(t, full, "a near-empty assembler mid-reload must not report room")
		require.False(t, changed, "and must not publish a clearing transition")
		require.True(t, ba.IsTxIngressFull())
	})

	t.Run("the refusal clears once the reload finishes", func(t *testing.T) {
		require.True(t, ba.IsTxIngressFull(), "arrange: carried over from the previous case")
		require.False(t, ba.unminedTransactionsLoading.Load())

		full, changed := ba.evaluateTxIngressFull()
		require.False(t, full, "with the reload done the measurement is meaningful again")
		require.True(t, changed, "so the clearing transition must be published")
	})

	t.Run("filling past the limit during a reload still refuses", func(t *testing.T) {
		require.False(t, ba.IsTxIngressFull(), "arrange: carried over from the previous case")

		ba.unminedTransactionsLoading.Store(true)
		defer ba.unminedTransactionsLoading.Store(false)

		addTestTxs(ba, limit)

		require.Eventually(t, func() bool {
			full, _ := ba.evaluateTxIngressFull()
			return full
		}, 5*time.Second, 10*time.Millisecond,
			"the guard must suppress clearing only, never setting (in memory %d, limit %d)",
			ba.TransactionsInMemory(), limit)
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

	addTestTxs(ba, 20)

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
// It stands in for a block assembly that was killed while full and has come back: the ingress points
// still hold their cached refusal, this process holds nothing yet, and loadUnminedTransactions is
// running. Nothing this process does may release those ingress points before the reload finishes.
//
// This is killed by removing the evaluate guard, which is what lets the flag survive the reload. The
// heartbeat guard has its own test: with the evaluate guard in place the flag stays true here, so an
// unconditional heartbeat would re-announce true and this case would not notice.
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

	// The state a restarted process inherits: the ingress points refuse, this assembler is empty.
	ba.publishTxIngressFull(ctx, true)
	require.True(t, testItems.blockchainClient.IsBlockAssemblyFull(),
		"arrange: the ingress points must start this test refusing")

	require.True(t, ba.txIngressFull.CompareAndSwap(false, true), "arrange: this process knows it was full")

	ba.unminedTransactionsLoading.Store(true)

	ba.startTxIngressLimitMonitor(ctx)

	// Long enough for many evaluate ticks and many heartbeats to have run.
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

// blockchainClientIsFull is a compile-time check that the ingress points can read the flag through
// the interface they hold, rather than through a concrete client type.
var _ = func(c blockchain.ClientI) bool { return c.IsBlockAssemblyFull() }
