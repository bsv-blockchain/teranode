package lanereader

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fillerLane is the lane every test block also takes a position in, so no main-chain block is a
// block without rows, which would make the rule answer no_rows.
const fillerLane = 250

func pruneBlockHash(id uint32) chainhash.Hash {
	var h chainhash.Hash
	h[0], h[1], h[2], h[3] = byte(id), byte(id>>8), byte(id>>16), byte(id>>24)
	h[31] = 2

	return h
}

func laneRows(t *testing.T, laneNum uint8, positions ...uint64) []lane.MapRow {
	t.Helper()

	bm := roaring64.New()
	bm.AddMany(positions)

	rows, err := lane.BuildMapRows(laneNum, bm)
	require.NoError(t, err)

	return rows
}

// pruneIndex builds a main chain of heights 1 to tip, block ID = height, where took[h] are the rows
// of block h beside its filler row, and forks are extra blocks off the main chain.
type pruneIndex struct {
	t      *testing.T
	idx    *lane.Index
	settle uint32
	took   map[uint32][][]lane.MapRow
	forks  []forkBlock
	// noRowsAt is a main-chain height whose block has no rows at all.
	noRowsAt uint32
}

type forkBlock struct {
	id, parent, height uint32
	rows               [][]lane.MapRow
}

func newPruneIndex(t *testing.T, settle uint32) *pruneIndex {
	return &pruneIndex{t: t, settle: settle, took: map[uint32][][]lane.MapRow{}}
}

func (p *pruneIndex) at(height uint32, rows ...[]lane.MapRow) *pruneIndex {
	p.took[height] = append(p.took[height], rows...)

	return p
}

func (p *pruneIndex) fork(id, parent, height uint32, rows ...[]lane.MapRow) *pruneIndex {
	p.forks = append(p.forks, forkBlock{id, parent, height, rows})

	return p
}

// build loads the chain up to tip and returns a judge for retention against it.
func (p *pruneIndex) build(tip uint32) *Reader {
	p.t.Helper()

	p.idx = lane.NewIndex(p.settle)

	for h := uint32(1); h <= tip; h++ {
		var all []lane.MapRow

		if h != p.noRowsAt {
			all = append(all, laneRows(p.t, fillerLane, uint64(h))...)

			for _, r := range p.took[h] {
				all = append(all, r...)
			}
		}

		require.NoError(p.t, p.idx.AddBlock(lane.IndexBlock{ID: h, Height: h, Hash: pruneBlockHash(h), Parent: pruneBlockHash(h - 1)}, all))
	}

	for _, f := range p.forks {
		var all []lane.MapRow
		for _, r := range f.rows {
			all = append(all, r...)
		}

		require.NoError(p.t, p.idx.AddBlock(lane.IndexBlock{ID: f.id, Height: f.height, Hash: pruneBlockHash(f.id), Parent: pruneBlockHash(f.parent)}, all))
	}

	require.NoError(p.t, p.idx.SetTip(pruneBlockHash(tip)))

	r := newReader(ulogger.TestLogger{}, nil, p.settle)
	r.idx.Store(p.idx)

	return r
}

var (
	parentHash  = chainhash.HashH([]byte("parent"))
	spenderHash = chainhash.HashH([]byte("spender"))
	otherHash   = chainhash.HashH([]byte("other spender"))
)

func candidate(parentPos uint64, spenders ...uint64) *PruneCandidate {
	c := &PruneCandidate{Parent: parentHash, ParentPos: parentPos}

	for i, s := range spenders {
		h := spenderHash
		if i > 0 {
			h = otherHash
		}

		c.Spenders = append(c.Spenders, PruneSpender{Hash: h, Pos: s})
	}

	return c
}

func verdictCount(store, today, lanes, reason string) float64 {
	initPruneMetrics()

	return testutil.ToFloat64(prometheusLanePrunerVerdicts.WithLabelValues(store, today, lanes, reason))
}

func TestPruneJudgeAtOffDoesNothing(t *testing.T) {
	var r *Reader

	j := r.PruneJudge(PruneStoreUtxoset, 288)
	require.Nil(t, j)

	require.Equal(t, PruneVerdict{}, j.Judge(TodayDelete, candidate(lane.Pack(7, 500))))
	j.Reads("page", 3)
	j.Unjudged("record_error", 1)
	require.Zero(t, j.Elapsed())
	j.Done()
}

// TestPruneJudgeEachReason walks every reason the lane rule gives: parent P is at lane 7 position
// 500, its spender C at lane 9 position 40, the tip at 300, retention 288 (depth 100) unless the case
// says otherwise.
func TestPruneJudgeEachReason(t *testing.T) {
	parent := lane.Pack(7, 500)
	spender := lane.Pack(9, 40)
	other := lane.Pack(9, 41)

	cases := []struct {
		name      string
		chain     func(p *pruneIndex) *pruneIndex
		retention uint32
		c         *PruneCandidate
		want      PruneVerdict
	}{
		{
			name:  "both below the floor",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)).at(190, laneRows(t, 9, 40)) },
			c:     candidate(parent, spender),
			want:  PruneVerdict{PruneDelete, ReasonSettled},
		},
		{
			name:      "retention 50: rows exactly the depth deep are settled",
			chain:     func(p *pruneIndex) *pruneIndex { return p.at(220, laneRows(t, 7, 500)).at(250, laneRows(t, 9, 40)) },
			retention: 50,
			c:         candidate(parent, spender),
			want:      PruneVerdict{PruneDelete, ReasonSettled},
		},
		{
			name:      "retention 50: a row one short of the depth is shallow",
			chain:     func(p *pruneIndex) *pruneIndex { return p.at(220, laneRows(t, 7, 500)).at(251, laneRows(t, 9, 40)) },
			retention: 50,
			c:         candidate(parent, spender),
			want:      PruneVerdict{PruneKeep, ReasonSpenderShallow},
		},
		{
			name:  "retention 288: a spender 50 deep is shallow, the depth is 100",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)).at(250, laneRows(t, 9, 40)) },
			c:     candidate(parent, spender),
			want:  PruneVerdict{PruneKeep, ReasonSpenderShallow},
		},
		{
			name:  "a shallow parent",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(250, laneRows(t, 7, 500)).at(150, laneRows(t, 9, 40)) },
			c:     candidate(parent, spender),
			want:  PruneVerdict{PruneKeep, ReasonParentShallow},
		},
		{
			name:      "retention 10: a spender 10 deep is settled",
			chain:     func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)).at(290, laneRows(t, 9, 40)) },
			retention: 10,
			c:         candidate(parent, spender),
			want:      PruneVerdict{PruneDelete, ReasonSettled},
		},
		{
			name:      "retention 10: a spender 9 deep is shallow",
			chain:     func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)).at(291, laneRows(t, 9, 40)) },
			retention: 10,
			c:         candidate(parent, spender),
			want:      PruneVerdict{PruneKeep, ReasonSpenderShallow},
		},
		{
			name:  "an unmined spender",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)).at(160, laneRows(t, 9, 39)) },
			c:     candidate(parent, spender),
			want:  PruneVerdict{PruneKeep, ReasonSpenderUnmined},
		},
		{
			name: "a spender only a fork block took",
			chain: func(p *pruneIndex) *pruneIndex {
				return p.at(150, laneRows(t, 7, 500)).at(160, laneRows(t, 9, 39)).fork(5000, 249, 250, laneRows(t, 9, 40))
			},
			c:    candidate(parent, spender),
			want: PruneVerdict{PruneKeep, ReasonSpenderOffChain},
		},
		{
			name:  "an unmined parent",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 499)).at(190, laneRows(t, 9, 40)) },
			c:     candidate(parent, spender),
			want:  PruneVerdict{PruneKeep, ReasonParentUnmined},
		},
		{
			name:  "an unmined conflicting parent is counted conflicting",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 499)).at(190, laneRows(t, 9, 40)) },
			c:     &PruneCandidate{Parent: parentHash, ParentPos: parent, Spenders: []PruneSpender{{spenderHash, spender}}, Conflicting: true},
			want:  PruneVerdict{PruneKeep, ReasonConflicting},
		},
		{
			name: "a conflicting parent only a fork block took is counted conflicting",
			chain: func(p *pruneIndex) *pruneIndex {
				return p.at(150, laneRows(t, 7, 499)).at(190, laneRows(t, 9, 40)).fork(5000, 249, 250, laneRows(t, 7, 500))
			},
			c:    &PruneCandidate{Parent: parentHash, ParentPos: parent, Spenders: []PruneSpender{{spenderHash, spender}}, Conflicting: true},
			want: PruneVerdict{PruneKeep, ReasonConflicting},
		},
		{
			name:  "a zero-output parent, mined deep",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)) },
			c:     candidate(parent),
			want:  PruneVerdict{PruneDelete, ReasonSettled},
		},
		{
			name:  "a zero-output parent, unmined",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 499)) },
			c:     candidate(parent),
			want:  PruneVerdict{PruneKeep, ReasonParentUnmined},
		},
		{
			name:  "a parent with no position",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(190, laneRows(t, 9, 40)) },
			c:     candidate(lane.None, spender),
			want:  PruneVerdict{PruneCannotJudge, ReasonNoPosition},
		},
		{
			name:  "a spender with no position",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)) },
			c:     candidate(parent, lane.None),
			want:  PruneVerdict{PruneCannotJudge, ReasonNoPosition},
		},
		{
			name:  "an unsettled spender outranks one with no position: the keep is certain",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)).at(160, laneRows(t, 9, 39)) },
			c:     candidate(parent, lane.None, other),
			want:  PruneVerdict{PruneKeep, ReasonSpenderUnmined},
		},
		{
			name:  "a position below anything the index loaded",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)).at(190, laneRows(t, 9, 400)) },
			c:     candidate(parent, spender),
			want:  PruneVerdict{PruneCannotJudge, ReasonNotLoaded},
		},
		{
			name:  "slots that could not be read",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)).at(190, laneRows(t, 9, 40)) },
			c:     &PruneCandidate{Parent: parentHash, ParentPos: parent, Spenders: []PruneSpender{{spenderHash, spender}}, Unread: true},
			want:  PruneVerdict{PruneCannotJudge, ReasonSpenderUnread},
		},
		{
			name:  "two spenders, the second unmined",
			chain: func(p *pruneIndex) *pruneIndex { return p.at(150, laneRows(t, 7, 500)).at(190, laneRows(t, 9, 40)) },
			c:     candidate(parent, spender, other),
			want:  PruneVerdict{PruneKeep, ReasonSpenderUnmined},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			retention := tc.retention
			if retention == 0 {
				retention = 288
			}

			r := tc.chain(newPruneIndex(t, 100)).build(300)
			j := r.PruneJudge(PruneStoreUtxoset, retention)
			require.Equal(t, tc.want, j.Judge(TodayDelete, tc.c))
		})
	}
}

// TestPruneJudgeBelowTheFloor: a parent and spender whose blocks folded answer from the floor, one
// comparison, and are settled.
func TestPruneJudgeBelowTheFloor(t *testing.T) {
	r := newPruneIndex(t, 100).at(20, laneRows(t, 7, 500)).at(30, laneRows(t, 9, 40)).build(300)

	require.Equal(t, lane.ViaFloor, r.MainView().Lookup(lane.Pack(7, 500)).Via)
	require.Equal(t, lane.ViaFloor, r.MainView().Lookup(lane.Pack(9, 40)).Via)

	j := r.PruneJudge(PruneStoreAerospike, 288)
	require.Equal(t, PruneVerdict{PruneDelete, ReasonSettled}, j.Judge(TodayDelete, candidate(lane.Pack(7, 500), lane.Pack(9, 40))))
	require.Equal(t, [3]uint64{0, 0, 2}, j.vias)
}

// TestPruneJudgeNoRows: a main chain with a block of no rows may hold a position no row takes, so the
// rule cannot call it unmined.
func TestPruneJudgeNoRows(t *testing.T) {
	p := newPruneIndex(t, 100).at(150, laneRows(t, 7, 500)).at(160, laneRows(t, 9, 39))
	p.noRowsAt = 250

	r := p.build(300)
	j := r.PruneJudge(PruneStoreAerospike, 288)
	require.Equal(t, PruneVerdict{PruneCannotJudge, ReasonNoRows}, j.Judge(TodayDelete, candidate(lane.Pack(7, 500), lane.Pack(9, 40))))
}

func TestPruneJudgeNotReady(t *testing.T) {
	r := newReader(ulogger.TestLogger{}, nil, 100)
	r.idx.Store(lane.NewIndex(100))

	j := r.PruneJudge(PruneStoreUtxoset, 288)
	require.Equal(t, PruneVerdict{PruneCannotJudge, ReasonNotReady}, j.Judge(TodayKeep, candidate(lane.Pack(7, 500))))
}

// TestPruneJudgeCountsBesideToday: each verdict is counted with today's decision, in Done, and the
// rule's time, lookups, reads and unjudged records are counted too.
func TestPruneJudgeCountsBesideToday(t *testing.T) {
	r := newPruneIndex(t, 100).at(150, laneRows(t, 7, 500)).at(190, laneRows(t, 9, 40)).at(160, laneRows(t, 9, 39)).build(300)

	settled := candidate(lane.Pack(7, 500), lane.Pack(9, 40))
	unmined := candidate(lane.Pack(7, 500), lane.Pack(9, 41))

	before := map[string]float64{
		"both delete":  verdictCount(PruneStoreAerospike, PruneDelete, PruneDelete, ReasonSettled),
		"both keep":    verdictCount(PruneStoreAerospike, PruneKeep, PruneKeep, ReasonSpenderUnmined),
		"today only":   verdictCount(PruneStoreAerospike, PruneDelete, PruneKeep, ReasonSpenderUnmined),
		"lanes only":   verdictCount(PruneStoreAerospike, PruneKeep, PruneDelete, ReasonSettled),
		"cannot judge": verdictCount(PruneStoreAerospike, PruneDelete, PruneCannotJudge, ReasonNoPosition),
	}

	j := r.PruneJudge(PruneStoreAerospike, 288)
	j.Judge(TodayDelete, settled)
	j.Judge(TodayKeep, unmined)
	j.Judge(TodayDelete, unmined)
	j.Judge(TodayKeep, settled)
	j.Judge(TodayKeep, settled)
	j.Judge(TodayDelete, candidate(lane.None))
	j.Reads("page", 2)
	j.Unjudged("record_error", 1)
	j.Time(time.Now().Add(-time.Millisecond))

	require.Equal(t, before["lanes only"], verdictCount(PruneStoreAerospike, PruneKeep, PruneDelete, ReasonSettled), "nothing is counted before Done")

	readsBefore := testutil.ToFloat64(prometheusLanePrunerReads.WithLabelValues(PruneStoreAerospike, "page"))
	unjudgedBefore := testutil.ToFloat64(prometheusLanePrunerUnjudged.WithLabelValues(PruneStoreAerospike, "record_error"))
	secondsBefore := testutil.ToFloat64(prometheusLanePrunerSeconds.WithLabelValues(PruneStoreAerospike))
	floorBefore := testutil.ToFloat64(prometheusLanePrunerLookups.WithLabelValues(PruneStoreAerospike, "floor"))
	noneBefore := testutil.ToFloat64(prometheusLanePrunerLookups.WithLabelValues(PruneStoreAerospike, "none"))

	j.Done()

	require.Equal(t, before["both delete"]+1, verdictCount(PruneStoreAerospike, PruneDelete, PruneDelete, ReasonSettled))
	require.Equal(t, before["both keep"]+1, verdictCount(PruneStoreAerospike, PruneKeep, PruneKeep, ReasonSpenderUnmined))
	require.Equal(t, before["today only"]+1, verdictCount(PruneStoreAerospike, PruneDelete, PruneKeep, ReasonSpenderUnmined))
	require.Equal(t, before["lanes only"]+2, verdictCount(PruneStoreAerospike, PruneKeep, PruneDelete, ReasonSettled))
	require.Equal(t, before["cannot judge"]+1, verdictCount(PruneStoreAerospike, PruneDelete, PruneCannotJudge, ReasonNoPosition))
	require.Equal(t, readsBefore+2, testutil.ToFloat64(prometheusLanePrunerReads.WithLabelValues(PruneStoreAerospike, "page")))
	require.Greater(t, testutil.ToFloat64(prometheusLanePrunerSeconds.WithLabelValues(PruneStoreAerospike)), secondsBefore, "the rule's time is added")
	require.Equal(t, unjudgedBefore+1, testutil.ToFloat64(prometheusLanePrunerUnjudged.WithLabelValues(PruneStoreAerospike, "record_error")))
	// Five candidates with a parent position, each looking up the parent and its one spender: both
	// blocks folded, so the floor answers eight and nothing answers the unmined spender twice.
	require.Equal(t, floorBefore+8, testutil.ToFloat64(prometheusLanePrunerLookups.WithLabelValues(PruneStoreAerospike, "floor")))
	require.Equal(t, noneBefore+2, testutil.ToFloat64(prometheusLanePrunerLookups.WithLabelValues(PruneStoreAerospike, "none")))
	require.Positive(t, j.Elapsed())
}

// TestPruneJudgeSkipsARepeatedSpender: one spender of several outputs is looked up once.
func TestPruneJudgeSkipsARepeatedSpender(t *testing.T) {
	r := newPruneIndex(t, 100).at(150, laneRows(t, 7, 500)).at(190, laneRows(t, 9, 40)).build(300)

	j := r.PruneJudge(PruneStoreAerospike, 288)
	require.Equal(t, PruneVerdict{PruneDelete, ReasonSettled}, j.Judge(TodayDelete, candidate(lane.Pack(7, 500), lane.Pack(9, 40), lane.Pack(9, 40), lane.Pack(9, 40))))
	require.Equal(t, uint64(2), j.vias[lane.ViaFloor])
}

func TestInstallIndexIsTheProcessReader(t *testing.T) {
	t.Cleanup(func() { current.Store(nil) })

	idx := lane.NewIndex(100)
	r := InstallIndex(ulogger.TestLogger{}, idx, 100)
	require.Same(t, r, Current())
	require.Nil(t, r.Snapshot(), "no tip yet")
}

// TestPruneJudgeStopsAtTheFirstUnsettledPosition: once one position keeps the parent, the rest are not
// looked up.
func TestPruneJudgeStopsAtTheFirstUnsettledPosition(t *testing.T) {
	r := newPruneIndex(t, 100).at(150, laneRows(t, 7, 500)).at(190, laneRows(t, 9, 40)).build(300)

	j := r.PruneJudge(PruneStoreAerospike, 288)
	require.Equal(t, PruneVerdict{PruneKeep, ReasonSpenderUnmined}, j.Judge(TodayDelete, candidate(lane.Pack(7, 500), lane.Pack(9, 41), lane.Pack(9, 40))))
	require.Equal(t, [3]uint64{1, 0, 1}, j.vias, "the parent and the first spender, not the second")
}

// warnLogger records the format of each warning.
type warnLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	warns []string
}

func (l *warnLogger) Warnf(format string, _ ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.warns = append(l.warns, format)
}

// TestPruneJudgeLogsEachDirectionUnderItsOwnGate: the first disagreements of each direction are
// logged, then no more inside the minute, and one direction's lines do not use up the other's.
func TestPruneJudgeLogsEachDirectionUnderItsOwnGate(t *testing.T) {
	r := newPruneIndex(t, 100).at(150, laneRows(t, 7, 500)).at(190, laneRows(t, 9, 40)).build(300)
	log := &warnLogger{}
	r.logger = log

	settled := candidate(lane.Pack(7, 500), lane.Pack(9, 40))
	unmined := candidate(lane.Pack(7, 500), lane.Pack(9, 41))

	j := r.PruneJudge(PruneStoreUtxoset, 288)

	for range disagreementsLogged + 3 {
		j.Judge(TodayDelete, unmined)
	}

	require.Len(t, log.warns, disagreementsLogged, "the first lines of today's-only direction, then none inside the minute")

	j.Judge(TodayKeep, settled)

	require.Len(t, log.warns, disagreementsLogged+1, "the alarm direction logs under its own gate")
	require.True(t, strings.Contains(log.warns[disagreementsLogged], "%s %s (log line %d)"))

	j.Judge(TodayKeepUndo, settled)
	j.Judge(TodayDelete, settled)
	require.Len(t, log.warns, disagreementsLogged+1, "neither keep_undo nor agreement logs")
}
