package netsync

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// Above the last checkpoint the node leaves headers-first mode, the header cache
// is empty and blocks arrive on the inv path, one every ten minutes or so. Three
// reports written for headers-first sync kept firing there, and each said
// something untrue or meaningless. These tests pin what each says at the tip.

// infoCaptureLogger records Infof and Warnf lines.
type infoCaptureLogger struct {
	ulogger.Logger

	lines []string
}

func (c *infoCaptureLogger) Infof(format string, args ...interface{}) {
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *infoCaptureLogger) Warnf(format string, args ...interface{}) {
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *infoCaptureLogger) count(tag string) int {
	n := 0

	for _, l := range c.lines {
		if strings.HasPrefix(l, tag) {
			n++
		}
	}

	return n
}

// A block whose height the node does not know (no header cached for it, as at
// the tip) is reported without a height or a lead. It used to print "height 0,
// -968690 ahead of the chain".
func TestDownloadReport_AnUnknownHeightIsLeftOut(t *testing.T) {
	now := time.Now()

	s := &blockStream{total: 60_000_000, start: now.Add(-20 * time.Second), path: admitConverted}
	s.read.Store(60_000_000)

	line, ok := s.report(now, 968690)
	require.True(t, ok)
	require.NotContains(t, line, "height 0")
	require.NotContains(t, line, "ahead of the chain")
	require.Contains(t, line, "height not known")
	require.Contains(t, line, "60.0 MB")
}

// The per-peer queue lines and their summary describe the headers-first
// scheduler, which the tip's inv path does not use, so they are printed only in
// headers-first mode. The download-waste line counts every delivery whatever the
// mode, so it is printed in both.
func TestDownloadQueueReport_OnlyInHeadersFirstMode(t *testing.T) {
	for _, headersFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("headers-first %v", headersFirst), func(t *testing.T) {
			log := &infoCaptureLogger{Logger: ulogger.TestLogger{}}

			sm := newRaceManager(t)
			sm.logger = log
			sm.streams = newStreamRegistry()
			sm.blockSizeTracker = newBlockSizeTracker(10)
			sm.headersFirstMode.Store(headersFirst)

			sm.logDownloadQueues()

			if headersFirst {
				require.Positive(t, log.count("[downloadQueue]"), "the scheduler's queue is reported while it is in use")
			} else {
				require.Zero(t, log.count("[downloadQueue]"), "no queue report for a scheduler the tip does not use")
			}

			require.Equal(t, 1, log.count("[downloadWaste]"), "download waste is reported in every mode")
		})
	}
}

// At the tip no block arrives for minutes at a time, and nothing is waiting:
// that is a node keeping up, not a stuck one. The watchdog stays quiet.
func TestReportConsumerStall_QuietAtTheTipWithNothingWaiting(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}
	sm.headersFirstMode.Store(false)

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now)
	sm.reportConsumerStall(now.Add(10 * time.Minute))

	require.Empty(t, log.warns, "no block for ten minutes at the tip is normal")
}

// Outside headers-first mode, work that is waiting and not admitted is still a
// stall, and still reported: here a read loop waiting on the download budget,
// the shape of the 754,895 wedge.
func TestReportConsumerStall_ReportsAtTheTipWhenWorkIsWaiting(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}
	sm.headersFirstMode.Store(false)
	sm.blockPrefetchWaiters.Store(1)

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)
}

// In headers-first mode the node is always waiting for blocks it has asked
// for, so a long silence there is reported whatever the snapshot holds.
func TestReportConsumerStall_ReportsInHeadersFirstMode(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}
	sm.headersFirstMode.Store(true)

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)
}
