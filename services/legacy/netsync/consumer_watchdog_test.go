package netsync

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// captureLogger records Warnf lines so a test can assert on what the watchdog
// said. It implements only what the watchdog uses; every other method would be
// dead weight in a test whose subject is one log line.
type captureLogger struct {
	ulogger.Logger

	warns []string
}

func (c *captureLogger) Warnf(format string, args ...interface{}) {
	c.warns = append(c.warns, fmt.Sprintf(format, args...))
}

func TestReportConsumerStallStaysQuietBeforeTheThreshold(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now)

	// One second short of the threshold. A loop that has just admitted a block is
	// not stuck, and a watchdog that says so on every tick is noise.
	sm.reportConsumerStall(now.Add(consumerStallAfter - time.Second))

	require.Empty(t, log.warns)
}

func TestReportConsumerStallReportsOnceAnInterval(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}
	sm.headersFirstMode.Store(true) // a sync in progress, where silence is always worth reporting

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now)

	stalled := now.Add(consumerStallAfter + time.Second)

	sm.reportConsumerStall(stalled)
	sm.reportConsumerStall(stalled.Add(time.Second))
	sm.reportConsumerStall(stalled.Add(consumerStallReportInterval))

	// Twice, not three times: the middle call is inside the interval. A wedge
	// lasting hours must not fill the disk with the same sentence.
	require.Len(t, log.warns, 2)
}

func TestReportConsumerStallSaysWhenTheLoopNeverReachedItsWait(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	now := time.Now()

	sm.noteConsumerAdmitted(now)

	// No snapshot published: the loop is stuck in a block's own work above the
	// select rather than waiting for capacity, which is a different fault and
	// must not be reported as the same one.
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)
	require.Contains(t, log.warns[0], "has not reached its wait")
}

func TestReportConsumerStallStaysQuietUntilSomethingIsAdmitted(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	// The clock was never started, which is the state of a manager whose loop has
	// not run. There is no silence to measure, so there is nothing to say.
	sm.reportConsumerStall(time.Now().Add(time.Hour))

	require.Empty(t, log.warns)
}

func TestPublishConsumerWaitDescribesTheWindowAndTheBarrier(t *testing.T) {
	settled := make(chan struct{})
	close(settled)

	entry := &frontierEntry{
		hash:    chainhash.HashH([]byte("settled entry")),
		height:  754895,
		settled: settled,
	}
	entry.failed.Store(true)

	running := &frontierEntry{
		hash:    chainhash.HashH([]byte("running entry")),
		height:  754896,
		settled: make(chan struct{}),
	}

	log := &captureLogger{Logger: ulogger.TestLogger{}}

	sm := &SyncManager{
		logger: log,
		dispatcher: &blockDispatcher{
			barrier:  true,
			frontier: []*frontierEntry{entry, running},
		},
	}

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)

	line := log.warns[0]

	// A settled entry still in the window is a completion that was never
	// processed, and a running one with nothing alive to finish it is a
	// completion that was lost. The report has to tell them apart.
	require.Contains(t, line, "settled")
	require.Contains(t, line, "failed")
	require.Contains(t, line, "running")
	require.Contains(t, line, "checkpoint barrier is set")
}

func TestPublishConsumerWaitIsSafeToReadFromAnotherGoroutine(t *testing.T) {
	sm := &SyncManager{
		logger:     &captureLogger{Logger: ulogger.TestLogger{}},
		dispatcher: &blockDispatcher{},
	}

	sm.noteConsumerAdmitted(time.Now())

	var stop atomic.Bool

	done := make(chan struct{})

	// The consumer's side: publish repeatedly, as the loop does once per turn.
	go func() {
		defer close(done)

		for i := 0; !stop.Load(); i++ {
			sm.publishConsumerWait(time.Now())
		}
	}()

	// The watchdog's side. Under -race this is the assertion: the snapshot is
	// handed over through an atomic value, so the goroutine whose progress is in
	// question never waits on the goroutine describing it.
	for i := 0; i < 2000; i++ {
		sm.reportConsumerStall(time.Now().Add(time.Hour))
	}

	stop.Store(true)
	<-done
}

func TestDescribeFrontierEntryHandlesANilSlot(t *testing.T) {
	// complete clears a popped slot to nil before resliding, so a snapshot taken
	// mid-pop can see one. Printing "nil" beats panicking inside a watchdog.
	require.Equal(t, "nil", describeFrontierEntry(nil))
}

func TestShortHashLeavesAShortStringAlone(t *testing.T) {
	require.Equal(t, "abc", shortHash("abc"))
	require.Equal(t, "0123456789"[:8], shortHash("0123456789"))
}

func TestConsumerStallLineIsOneLine(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}

	sm := &SyncManager{
		logger:     log,
		dispatcher: &blockDispatcher{},
	}
	sm.headersFirstMode.Store(true)

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)

	// The project's logging convention: one line per message, so a grep for the
	// tag returns the whole finding rather than its first clause.
	require.NotContains(t, log.warns[0], "\n")
	require.False(t, strings.HasSuffix(log.warns[0], " "))
}

// seedStalledHeaderRound puts a manager into a header-cache state resembling
// Hetzner mainnet's at 08:47 on 2026-09-11: headers-first mode on, best names
// the committed height, and the cache names a run of `above` further heights
// above it. The blocks the node actually needed were fifty thousand heights
// below the back of the old header list, which is why the watchdog has to
// print both the committed height and where the cache's own run ends.
func seedStalledHeaderRound(t *testing.T, sm *SyncManager, best int32, above int) {
	t.Helper()

	anchor := chainhash.Hash{0xa0}
	mockCommittedTip(t, sm, uint32(best), 0) //nolint:gosec // a fixture height, never negative

	var nonce uint32
	msg, _ := linkedHeaders(anchor, above, &nonce)

	sm.headerCache = newHeaderCache()
	sm.headerCache.Fill(anchor, best+1, msg.Headers)

	sm.headersFirstMode.Store(true)
}

// stallReport drives one watchdog tick past the threshold and returns the single
// line it wrote. It goes through reportConsumerStall rather than calling
// headerRoundSummary, because the wiring from the watchdog to the header state is
// the thing under test: the summary existed as a private truth all along, and
// what the seven-hour stall lacked was a path from it to the log.
func stallReport(t *testing.T, sm *SyncManager, log *captureLogger) string {
	t.Helper()

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)

	return log.warns[0]
}

// TestConsumerWatchdog_ReportsTheHeaderCacheState is the line that would have
// settled the 800128 stall on the first tick instead of after seven hours and
// a packet capture: the committed height, how many heights the cache names,
// where its run ends, and how many blocks are still owed.
func TestConsumerWatchdog_ReportsTheHeaderCacheState(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	seedStalledHeaderRound(t, sm, 849900, 100)

	line := stallReport(t, sm, log)

	require.Contains(t, line, "best block processed 849900")
	require.Contains(t, line, "the header cache names 100 heights up to 850000")
	require.Contains(t, line, "0 blocks are owed by peers")
}

// TestConsumerWatchdog_ReportsCacheStateEvenWithHeadersFirstOff pins that the
// header-cache clause survives the switch out of headers-first mode. When the
// committed tip passes the final checkpoint the cache can still name up to two
// thousand heights above it, and the download pass keeps placing them, so a
// report at the crossing must still say how many it names. Outside headers-first
// mode the watchdog reports only when something is waiting, here a read loop
// waiting on the download budget; when it does report, the line carries the
// tip's own summary (blocks owed, split from blocks arriving) and the cache's
// remaining run, not the headers-first claim that a getheaders is outstanding.
func TestConsumerWatchdog_ReportsCacheStateEvenWithHeadersFirstOff(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	seedStalledHeaderRound(t, sm, 800128, 12)
	sm.headersFirstMode.Store(false)
	sm.blockPrefetchWaiters.Store(1)

	line := stallReport(t, sm, log)

	require.Contains(t, line, "no block admitted")
	require.Contains(t, line, "0 blocks are owed by peers")
	require.Contains(t, line, "the header cache still names 12 heights up to 800140")
	require.NotContains(t, line, "waiting on a getheaders")
}

// TestConsumerWatchdog_ANilHeaderCacheStillProducesAReport matches the harness
// twelve test files in this package use: a SyncManager built as a struct literal,
// with no header cache and nothing committed. A watchdog that panicked on one of
// those would take the whole message-handling goroutine down, which is a worse
// failure than the stall it reports.
func TestConsumerWatchdog_ANilHeaderCacheStillProducesAReport(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	sm.headersFirstMode.Store(true)

	var line string

	require.NotPanics(t, func() { line = stallReport(t, sm, log) })

	require.Contains(t, line, "best block processed 0")
	require.Contains(t, line, "the header cache is empty")
}

// TestConsumerWait_Describe_NamesDeclinedDrainTurns covers the field that was
// collected and never rendered. A drain that walks its queue, rules every parent
// out and drops them leaves the queue length at zero, so without this a loop
// that has just thrown a turn away reads as a loop with no work.
func TestConsumerWait_Describe_NamesDeclinedDrainTurns(t *testing.T) {
	now := time.Now()

	w := &consumerWait{at: now, parked: 113, drainDeclines: 41}

	line := w.describe(now)

	require.Contains(t, line, "the drain has declined 41 turns",
		"a report that collects the count and prints nothing is the diagnostic stopping where it gets interesting")

	quiet := (&consumerWait{at: now}).describe(now)
	require.False(t, strings.Contains(quiet, "declined"),
		"a drain that has never declined a turn must not add a clause saying so")
}

// tipWatchdogManager is a manager above the final checkpoint for the watchdog
// to read: a real sqlitememory LocalClient (so committedTip and the summary
// read a chain rather than a stand-in), headers-first off, nothing parked,
// nothing in the window, no download slot held, and a live ledger. Everything
// the watchdog's hasWork gate reads is empty here on purpose: an owed block is
// in none of those fields, and that silence is the shape 4ad6e3818 made the
// watchdog blind to.
func tipWatchdogManager(t *testing.T) (*SyncManager, *captureLogger) {
	t.Helper()

	log := &captureLogger{Logger: ulogger.TestLogger{}}

	sm := newPipelineManager(t, memory.New(), 8)
	sm.logger = log
	sm.blockDownloads = newBlockDownloadTracker(blockRequestAssignmentTTL)
	sm.headersFirstMode.Store(false)

	return sm, log
}

// TestConsumerWatchdog_ReportsABlockOwedAtTheTipWithNothingElseWaiting is Major 5
// of the 2026-10-02 re-review: above the last checkpoint a peer accepts our
// getdata and sends nothing, and the node is silent about it. The ledger is the
// only place that block exists, so the watchdog reads it, counts it as work,
// and says what is owed rather than claiming to wait on a getheaders.
func TestConsumerWatchdog_ReportsABlockOwedAtTheTipWithNothingElseWaiting(t *testing.T) {
	sm, log := tipWatchdogManager(t)

	require.True(t, sm.blockDownloads.Add(newTestPeer(t, "10.0.0.1:8333"), chainhash.Hash{0x95}))

	line := stallReport(t, sm, log)

	require.Contains(t, line, "best block processed 0")
	require.Contains(t, line, "1 blocks are owed by peers")
	require.Contains(t, line, "0 of them are arriving now")
	require.NotContains(t, line, "waiting on a getheaders", "at the tip no getheaders is outstanding and the line must not claim one")
}

// TestConsumerWatchdog_StaysQuietAtTheTipWhenNothingIsOwed is the other half:
// a tip node keeping up, with a ledger whose only record is a forgiven one (a
// delivered block's other owner, let off at delivery), has nothing to report.
// Len excludes forgiven records, so that record is not work.
func TestConsumerWatchdog_StaysQuietAtTheTipWhenNothingIsOwed(t *testing.T) {
	sm, log := tipWatchdogManager(t)

	hash := chainhash.Hash{0x96}

	require.True(t, sm.blockDownloads.Add(newTestPeer(t, "10.0.0.2:8333"), hash))
	require.Len(t, sm.blockDownloads.ForgiveOwners(hash, blockRequestRetryInterval), 1)

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Empty(t, log.warns, "a node at the tip with nothing owed, parked, queued or held is keeping up, not stalled")
}
