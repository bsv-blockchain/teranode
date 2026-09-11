package netsync

import (
	"container/list"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
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
	sm.publishConsumerWait(now, true, nil)

	// One second short of the threshold. A loop that has just admitted a block is
	// not stuck, and a watchdog that says so on every tick is noise.
	sm.reportConsumerStall(now.Add(consumerStallAfter - time.Second))

	require.Empty(t, log.warns)
}

func TestReportConsumerStallNamesTheBlockHoldingTheQueueArmShut(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}

	hash := chainhash.HashH([]byte("pending"))

	sm := &SyncManager{
		logger:     log,
		dispatcher: &blockDispatcher{budget: 4096},
	}

	now := time.Now()

	sm.noteConsumerAdmitted(now)

	// The wedge this exists for: a head-processed block the window will not take,
	// so the queue arm is shut and nothing else is head-processed behind it.
	sm.publishConsumerWait(now, false, &blockDispatch{
		msg:      &blockQueueMsg{blockHash: hash},
		height:   754896,
		windowed: true,
	})

	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)

	line := log.warns[0]

	require.Contains(t, line, "no block admitted")
	require.Contains(t, line, "queue arm shut")
	require.Contains(t, line, hash.String()[:8])
	require.Contains(t, line, "754896")
	require.Contains(t, line, "the window is empty")
}

func TestReportConsumerStallReportsOnceAnInterval(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now, true, nil)

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
			budget:   8192,
			inflight: 2048,
			frontier: []*frontierEntry{entry, running},
		},
	}

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now, false, nil)
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
	require.Contains(t, line, "2048 of 8192 window bytes charged")
}

func TestPublishConsumerWaitIsSafeToReadFromAnotherGoroutine(t *testing.T) {
	sm := &SyncManager{
		logger:     &captureLogger{Logger: ulogger.TestLogger{}},
		dispatcher: &blockDispatcher{budget: 1024},
	}

	sm.noteConsumerAdmitted(time.Now())

	var stop atomic.Bool

	done := make(chan struct{})

	// The consumer's side: publish repeatedly, as the loop does once per turn.
	go func() {
		defer close(done)

		for i := 0; !stop.Load(); i++ {
			sm.publishConsumerWait(time.Now(), i%2 == 0, nil)
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
		dispatcher: &blockDispatcher{budget: 1},
	}

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now, false, nil)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)

	// The project's logging convention: one line per message, so a grep for the
	// tag returns the whole finding rather than its first clause.
	require.NotContains(t, log.warns[0], "\n")
	require.False(t, strings.HasSuffix(log.warns[0], " "))
}

// seedStalledHeaderRound puts a manager into the header state Hetzner mainnet was
// in at 08:47 on 2026-09-11: headers-first mode on, a list whose front is the
// round's anchor, headers stacked above it, and a checkpoint ahead. The blocks
// the node actually needed were fifty thousand heights below the back of this
// list, which is why the watchdog has to print both ends of it.
func seedStalledHeaderRound(sm *SyncManager, anchorHeight int32, above int) {
	sm.headerList = list.New()
	sm.headerIndex = make(map[chainhash.Hash]*list.Element)

	anchor := &headerNode{height: anchorHeight, hash: &chainhash.Hash{0xa0}, isAnchor: true}
	sm.headerIndex[*anchor.hash] = sm.headerList.PushBack(anchor)

	for i := 1; i <= above; i++ {
		node := &headerNode{height: anchorHeight + int32(i), hash: &chainhash.Hash{0xb0, byte(i)}}
		sm.headerIndex[*node.hash] = sm.headerList.PushBack(node)
	}

	checkpointHash := chainhash.Hash{0xcc}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 850000, Hash: &checkpointHash}

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
	sm.publishConsumerWait(now, true, nil)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)

	return log.warns[0]
}

// TestConsumerWatchdog_ReportsTheHeaderRoundWhenTheAnchorIsStillTheFront is the
// line that would have settled the 800128 stall on the first tick instead of
// after seven hours and a packet capture.
func TestConsumerWatchdog_ReportsTheHeaderRoundWhenTheAnchorIsStillTheFront(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	seedStalledHeaderRound(sm, 849900, 99)
	sm.startHeader = sm.headerList.Front()

	line := stallReport(t, sm, log)

	require.Contains(t, line, "the header round holds 100 headers")
	require.Contains(t, line, "front height 849900")
	require.Contains(t, line, "the round's anchor")
	require.Contains(t, line, "back height 849999")
	require.Contains(t, line, "aiming at checkpoint 850000")
}

// TestConsumerWatchdog_ReportsANilDownloadCursor pins the other terminal state of
// the walk. A nil startHeader beside a list full of headers switches the only
// fetcher off, and in every log the node writes today it is indistinguishable
// from a round whose anchor never had anything splice onto it. The two want
// opposite fixes, so the report has to name which one it is.
func TestConsumerWatchdog_ReportsANilDownloadCursor(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	seedStalledHeaderRound(sm, 800128, 12)
	sm.startHeader = nil

	line := stallReport(t, sm, log)

	require.Contains(t, line, "the download cursor is nil")
	require.Contains(t, line, "the header round holds 13 headers")
}

// TestConsumerWatchdog_SaysNothingExtraWithHeadersFirstOff keeps this scoped to
// the state it diagnoses. Outside a headers-first round the list is not the thing
// holding blocks up, and a clause about it would be noise on every other stall.
func TestConsumerWatchdog_SaysNothingExtraWithHeadersFirstOff(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	seedStalledHeaderRound(sm, 800128, 12)
	sm.startHeader = sm.headerList.Front()
	sm.headersFirstMode.Store(false)

	line := stallReport(t, sm, log)

	require.Contains(t, line, "no block admitted")
	require.NotContains(t, line, "the header round")
}

// TestConsumerWatchdog_ANilHeaderListStillProducesAReport matches the harness
// twelve test files in this package use: a SyncManager built as a struct literal,
// with no header list and no checkpoint. A watchdog that panicked on one of those
// would take the whole message-handling goroutine down, which is a worse failure
// than the stall it reports.
func TestConsumerWatchdog_ANilHeaderListStillProducesAReport(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	sm.headersFirstMode.Store(true)

	var line string

	require.NotPanics(t, func() { line = stallReport(t, sm, log) })

	require.Contains(t, line, "the header round holds no headers")
	require.Contains(t, line, "no checkpoint ahead")
}
