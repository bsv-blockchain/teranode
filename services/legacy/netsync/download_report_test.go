package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The chain still waits for blocks after the read-ahead and race changes, and the one long wait
// traced so far (686,127, 146 s) arrived as a raw body rather than converted, which only happens
// when admission is declined. Nothing logged how long a block waited to be admitted, why it was
// declined, or how late it was asked for. Each block's download now reports those when it is
// worth reading.

func TestDownloadReportOnlyForBlocksWorthReading(t *testing.T) {
	now := time.Now()

	quick := &blockStream{height: 100, total: 1_000_000, start: now.Add(-time.Second), path: admitConverted}
	quick.read.Store(1_000_000)
	_, interesting := quick.report(now, 90)
	require.False(t, interesting, "a quick converted block with a prompt slot says nothing new")

	slow := &blockStream{height: 100, total: 300_000_000, start: now.Add(-70 * time.Second), path: admitConverted}
	slow.read.Store(300_000_000)
	_, interesting = slow.report(now, 90)
	require.True(t, interesting, "a download of five seconds or more is reported")

	waited := &blockStream{height: 100, total: 1_000, start: now.Add(-time.Second), path: admitConverted, admitWait: 2 * time.Second}
	waited.read.Store(1_000)
	_, interesting = waited.report(now, 90)
	require.True(t, interesting, "a second or more waiting for an admission slot is reported")

	raw := &blockStream{height: 100, total: 1_000, start: now.Add(-time.Second), path: admitRawTimedOut}
	raw.read.Store(1_000)
	_, interesting = raw.report(now, 90)
	require.True(t, interesting, "a block drained because its admission timed out is always reported")
}

func TestDownloadReportSaysWhatHappened(t *testing.T) {
	now := time.Now()

	s := &blockStream{
		height:      686127,
		total:       62_870_779,
		start:       now.Add(-20 * time.Second),
		requestedAt: now.Add(-50 * time.Second),
		admitWait:   18 * time.Second,
		path:        admitRawTimedOut,
	}
	s.read.Store(62_870_779)

	line, ok := s.report(now, 686126)
	require.True(t, ok)
	require.Contains(t, line, "height 686127")
	require.Contains(t, line, "1 ahead of the chain")
	require.Contains(t, line, "62.9 MB")
	require.Contains(t, line, "in 20s")
	require.Contains(t, line, "bytes began 30s after it was requested")
	require.Contains(t, line, "waited 18s for an admission slot")
	require.Contains(t, line, "drained: the wait for an admission slot timed out")
}

func TestAdmissionIsRecordedOnTheBlocksStream(t *testing.T) {
	r := newStreamRegistry()
	s := r.start(hashN(7), 700, nil, 10, time.Now())

	r.noteAdmission(hashN(7), 3*time.Second, admitRawDuplicate)

	require.Equal(t, 3*time.Second, s.admitWait)
	require.Equal(t, admitRawDuplicate, s.path)

	var nilRegistry *streamRegistry
	nilRegistry.noteAdmission(hashN(7), time.Second, admitConverted)
}

func TestLedgerSaysWhenABlockWasFirstRequested(t *testing.T) {
	tr := newBlockDownloadTracker(blockRequestAssignmentTTL)
	h := hashN(9)

	_, ok := tr.RequestedAt(h)
	require.False(t, ok)

	first := time.Now().Add(-time.Minute)
	tr.now = func() time.Time { return first }
	require.True(t, tr.Add(newTestPeer(t, "10.0.0.1:8333"), h))

	tr.now = time.Now
	require.True(t, tr.Add(newTestPeer(t, "10.0.0.2:8333"), h))

	at, ok := tr.RequestedAt(h)
	require.True(t, ok)
	require.Equal(t, first, at, "the earliest request, not the race's")
}
