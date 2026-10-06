package pruner

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type replayKind int

const (
	evRequest replayKind = iota
	evFinish
)

type replayEvent struct {
	at      int64 // seconds after 2026-10-05T11:52:22Z
	kind    replayKind
	height  uint32
	durMs   int64
	records int64
}

// incidentTimeline is dev-ovh-1 scale-1, 2026-10-05: the pruner ran at ~100%
// duty cycle under load, coalesced multi-block runs from 14:29, and hit the
// 30m query TotalTimeout on block 347 at 17:38. It was noticed ~21h later.
var incidentTimeline = []replayEvent{
	{at: 0, kind: evRequest, height: 311},
	{at: 454, kind: evFinish, height: 311, durMs: 378470, records: 516936980},
	{at: 730, kind: evRequest, height: 312},
	{at: 1367, kind: evFinish, height: 312, durMs: 542170, records: 704618371},
	{at: 1400, kind: evRequest, height: 313},
	{at: 1947, kind: evFinish, height: 313, durMs: 470320, records: 630171759},
	{at: 2131, kind: evRequest, height: 314},
	{at: 2618, kind: evRequest, height: 315},
	{at: 2734, kind: evFinish, height: 314, durMs: 508000, records: 634365913},
	{at: 3272, kind: evFinish, height: 315, durMs: 537550, records: 699371090},
	{at: 3287, kind: evRequest, height: 316},
	{at: 3735, kind: evFinish, height: 316, durMs: 362150, records: 504350156},
	{at: 3775, kind: evRequest, height: 317},
	{at: 4258, kind: evFinish, height: 317, durMs: 415060, records: 567261307},
	{at: 4383, kind: evRequest, height: 318},
	{at: 4874, kind: evFinish, height: 318, durMs: 415360, records: 568307337},
	{at: 5113, kind: evRequest, height: 319},
	{at: 5600, kind: evRequest, height: 320},
	{at: 5789, kind: evFinish, height: 319, durMs: 580920, records: 735021820},
	{at: 6148, kind: evRequest, height: 321},
	{at: 6290, kind: evFinish, height: 320, durMs: 501030, records: 638546609},
	{at: 6635, kind: evRequest, height: 322},
	{at: 6850, kind: evFinish, height: 321, durMs: 559810, records: 715100211},
	{at: 7183, kind: evRequest, height: 323},
	{at: 7186, kind: evFinish, height: 322, durMs: 335550, records: 464491616},
	{at: 7670, kind: evRequest, height: 324},
	{at: 7737, kind: evFinish, height: 323, durMs: 490880, records: 630163239},
	{at: 8063, kind: evFinish, height: 324, durMs: 326640, records: 462401022},
	{at: 8157, kind: evRequest, height: 325},
	{at: 8644, kind: evRequest, height: 326},
	{at: 8709, kind: evFinish, height: 325, durMs: 485430, records: 588229774},
	{at: 9193, kind: evRequest, height: 327},
	{at: 9347, kind: evFinish, height: 326, durMs: 626590, records: 713001468},
	{at: 9354, kind: evRequest, height: 328},
	{at: 9385, kind: evRequest, height: 329},
	{at: 9414, kind: evRequest, height: 330},
	{at: 9419, kind: evRequest, height: 331},
	{at: 9636, kind: evFinish, height: 327, durMs: 288840, records: 449814220},
	{at: 9736, kind: evRequest, height: 332},
	{at: 10470, kind: evRequest, height: 333},
	{at: 11018, kind: evRequest, height: 334},
	{at: 11215, kind: evFinish, height: 331, durMs: 1578610, records: 1921948332},
	{at: 11689, kind: evRequest, height: 335},
	{at: 12421, kind: evRequest, height: 336},
	{at: 12560, kind: evFinish, height: 334, durMs: 1344980, records: 1535471425},
	{at: 12654, kind: evFinish, height: 336, durMs: 93570, records: 144379341},
	{at: 13093, kind: evRequest, height: 337},
	{at: 13220, kind: evFinish, height: 337, durMs: 1, records: 0},
	{at: 13826, kind: evRequest, height: 338},
	{at: 13976, kind: evFinish, height: 338, durMs: 120, records: 86403},
	{at: 14314, kind: evRequest, height: 339},
	{at: 14538, kind: evFinish, height: 339, durMs: 118310, records: 161466725},
	{at: 14803, kind: evRequest, height: 340},
	{at: 15352, kind: evRequest, height: 341},
	{at: 15712, kind: evFinish, height: 340, durMs: 814260, records: 876568624},
	{at: 15840, kind: evRequest, height: 342},
	{at: 16296, kind: evFinish, height: 341, durMs: 583600, records: 638545947},
	{at: 16449, kind: evRequest, height: 343},
	{at: 16938, kind: evRequest, height: 344},
	{at: 17104, kind: evFinish, height: 342, durMs: 807770, records: 836718779},
	{at: 17548, kind: evRequest, height: 345},
	{at: 18219, kind: evRequest, height: 346},
	{at: 18829, kind: evRequest, height: 347},
	{at: 18870, kind: evFinish, height: 344, durMs: 1766990, records: 1833856246},
	{at: 19500, kind: evRequest, height: 348},
	{at: 20049, kind: evRequest, height: 349},
	{at: 20598, kind: evRequest, height: 350},
}

// TestBacklogMonitor_ReplaysDevOvh1Incident pins the default thresholds to the
// failure that motivated them: with defaults, the monitor warns at 13:28 and
// reports BEHIND at 14:29, ~3h before the first query timeout.
func TestBacklogMonitor_ReplaysDevOvh1Incident(t *testing.T) {
	cfg := BacklogConfig{
		Enabled: true, MaxLagBlocks: 3, MaxIncompleteRuns: 2, HeadroomWarnRatio: 0.8,
		HeadroomWindow: 6, FailReadiness: true, LogRepeatInterval: 5 * time.Minute,
	}
	base := time.Date(2026, 10, 5, 11, 52, 22, 0, time.UTC)
	clk := &fakeClock{t: base}
	m := NewBacklogMonitor(cfg, ulogger.TestLogger{}, clk.now)

	firstAtRisk, firstBehind := int64(-1), int64(-1)

	for _, ev := range incidentTimeline {
		clk.t = base.Add(time.Duration(ev.at) * time.Second)

		switch ev.kind {
		case evRequest:
			m.OnPruneRequested(ev.height)
		case evFinish:
			m.OnRunFinished(ev.height, ev.records, time.Duration(ev.durMs)*time.Millisecond, true)
		}

		s := m.Snapshot()
		if s.State == BacklogAtRisk && firstAtRisk < 0 {
			firstAtRisk = ev.at
		}
		if s.State == BacklogBehind && firstBehind < 0 {
			firstBehind = ev.at
		}
	}

	require.Equal(t, int64(5789), firstAtRisk, "AT_RISK should first fire at 13:28:51 (run 319 finishes)")
	require.Equal(t, int64(9414), firstBehind, "BEHIND should first fire at 14:29:16 (request 330, lag 4)")
	require.Less(t, firstAtRisk, firstBehind, "the early warning must precede BEHIND")

	final := m.Snapshot()
	require.Equal(t, BacklogBehind, final.State, "still BEHIND before the 17:38 timeout")
	require.Equal(t, uint32(6), final.LagBlocks)
	require.Equal(t, uint32(344), final.LastCompletedHeight)
}
