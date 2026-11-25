package blockassembly

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
)

// The restart dry run, lanes Phase 1 task P5, at utxostore_laneMigration shadow only.
//
// Once a reload of the pool from the unmined_since index has succeeded, block assembly reads, in
// the background, the window a lanes restart would reload from instead (the lanes design, "Job
// two: block assembly's restart"), and judges each record by its own lane position against the
// reload target's chain. It loads nothing and acts on nothing: it counts what it read, what it
// would keep and why it would drop the rest, and compares what it would keep with what today's
// reload kept, by count, and by hash for a sample.
//
// The window starts at the hand-out time (Aerospike) or height (utxoset) of the first grant handed
// out at or above the cap height, the target's height less utxostore_unminedTxRetention, less a
// margin. A record whose position is below its lane's first such grant came from a grant older
// than the cap, and is dropped as capped; so is every positioned record of a lane with no such
// grant.

const (
	// laneRestartMargin and laneRestartMarginBlocks are how far before the window's start the read
	// begins: five minutes on Aerospike, whose last-update times come from its nodes' clocks and
	// grant times from the blockchain database's; one block on utxoset, whose rows carry the
	// creating process's own tip.
	laneRestartMargin       = 5 * time.Minute
	laneRestartMarginBlocks = 1

	// laneRestartSampleTarget is about how many of today's kept transactions the hash comparison
	// samples, and laneRestartLaneSampleMax how many transactions only the window would keep it
	// holds before it stops adding them.
	laneRestartSampleTarget  = 50_000
	laneRestartLaneSampleMax = 4 * laneRestartSampleTarget

	// laneRestartLogged is how many disagreements of each kind one run logs.
	laneRestartLogged = 5
)

// laneRestartViewWait is how long a run waits for the lane index to reach the target before it
// judges nothing. A variable so a test can shorten it.
var laneRestartViewWait = 2 * time.Minute

// Run outcomes.
const (
	laneRunDone        = "done"
	laneRunUnsupported = "unsupported"
	laneRunNoGrant     = "no_qualifying_grant"
	laneRunError       = "error"
	laneRunCancelled   = "cancelled"
)

// Record outcomes: how the dry run judged one record it read.
const (
	// laneRecKeep: unmined on the target's chain and from a grant inside the cap.
	laneRecKeep = "keep"
	// laneRecMinedRow and laneRecMinedFloor: a block on the target's chain took the position, by a
	// row the index holds or below the lane's floor.
	laneRecMinedRow   = "mined_row"
	laneRecMinedFloor = "mined_floor"
	// laneRecCapped: unmined, from a grant handed out before the cap.
	laneRecCapped = "capped"
	// laneRecConflicting: a double-spend's loser.
	laneRecConflicting = "conflicting"
	// laneRecNoPosition: the record has no lane position.
	laneRecNoPosition = "no_position"
	// laneRecNotReady: the lane index had no view of the target's chain to judge with.
	laneRecNotReady = "not_ready"
	// laneRecAfterReload: created after today's reload began, so not comparable with it.
	laneRecAfterReload = "after_reload"
	// laneRecMissing: utxoset's tx_body row with no identity row (LaneWindowRecord.Missing).
	laneRecMissing = "missing"
	// laneRecNotMaster: an Aerospike pagination record.
	laneRecNotMaster = "not_master"
)

var laneRecOutcomes = []string{laneRecKeep, laneRecMinedRow, laneRecMinedFloor, laneRecCapped, laneRecConflicting,
	laneRecNoPosition, laneRecNotReady, laneRecAfterReload, laneRecMissing, laneRecNotMaster}

// Sample kinds: how a sampled transaction compares between today's pool and the window's.
const (
	laneSampleBoth = "both"
	// laneSampleLanesOnly: the window would keep it, today's reload did not. A disagreement.
	laneSampleLanesOnly = "lanes_only"
	// laneSampleTodayNotRead: in today's pool, from a grant inside the cap, and not in the window.
	// A disagreement: the window missed it.
	laneSampleTodayNotRead = "today_only_not_read"
	// laneSampleTodayMined: in today's pool, and the window read it as mined on the target's
	// chain. A disagreement.
	laneSampleTodayMined = "today_only_mined"
	// laneSampleTodayCapped: in today's pool and from a grant before the cap, which the window
	// drops by design.
	laneSampleTodayCapped = "today_only_capped"
	// laneSampleTodayNoPosition: in today's pool with no lane position.
	laneSampleTodayNoPosition = "today_only_no_position"
	// laneSampleTodayOther: in today's pool and read by the window as one of the other outcomes
	// (conflicting, not_ready, after_reload, missing).
	laneSampleTodayOther = "today_only_other"
)

var laneSampleKinds = []string{laneSampleBoth, laneSampleLanesOnly, laneSampleTodayNotRead, laneSampleTodayMined,
	laneSampleTodayCapped, laneSampleTodayNoPosition, laneSampleTodayOther}

func isLaneSampleDisagreement(kind string) bool {
	return kind == laneSampleLanesOnly || kind == laneSampleTodayNotRead || kind == laneSampleTodayMined
}

// laneRestartToday is what today's reload read and kept, gathered while it runs, and the target it
// judged against. Only the reload's goroutine writes it, before the dry run starts.
type laneRestartToday struct {
	target       chainhash.Hash
	targetHeight uint32
	// ids is the target's chain as block IDs, back to genesis: the reload's own.
	ids map[uint32]bool
	// started is when the reload began: a record created after it may be missing from it.
	started time.Time

	read uint64
	kept uint64

	rate   uint64
	sample map[chainhash.Hash]*laneTodayEntry
}

// laneTodayEntry is one sampled transaction of today's pool, and what the window made of it.
type laneTodayEntry struct {
	pos       uint64
	createdAt int64
	verdict   string
}

// newLaneRestartToday starts gathering for a reload against block assembly's current block, or
// returns nil at utxostore_laneMigration off, where the reload does no added work.
func (b *BlockAssembler) newLaneRestartToday(ids map[uint32]bool, started time.Time) *laneRestartToday {
	if b.settings == nil || !b.settings.UtxoStore.LaneMigration.Stamps() {
		return nil
	}

	b.cancelLaneRestartDryRun()

	header, height := b.CurrentBlock()
	if header == nil {
		return nil
	}

	return &laneRestartToday{target: *header.Hash(), targetHeight: height, ids: ids, started: started}
}

// noteRead records how many records today's reload read.
func (t *laneRestartToday) noteRead(n uint64) {
	if t != nil {
		t.read = n
	}
}

// expect sets the sample rate for about n kept transactions. It must run before keep.
func (t *laneRestartToday) expect(n int) {
	if t == nil {
		return
	}

	t.rate = max(1, (uint64(n)+laneRestartSampleTarget-1)/laneRestartSampleTarget) //nolint:gosec // a count
	t.sample = make(map[chainhash.Hash]*laneTodayEntry, min(n, 2*laneRestartSampleTarget))
}

// keep records transactions today's reload put into the pool.
func (t *laneRestartToday) keep(txs []*utxo.UnminedTransaction) {
	if t == nil {
		return
	}

	if t.rate == 0 {
		t.expect(len(txs))
	}

	for _, tx := range txs {
		if tx == nil || tx.Node == nil {
			continue
		}

		t.kept++

		if laneSampled(tx.Node.Hash, t.rate) {
			t.sample[tx.Node.Hash] = &laneTodayEntry{pos: tx.LanePosition, createdAt: int64(tx.CreatedAt)}
		}
	}
}

// laneSampled is the sample rule, the same for both sides: one transaction in rate, by hash.
func laneSampled(h chainhash.Hash, rate uint64) bool {
	return rate > 0 && binary.LittleEndian.Uint64(h[:8])%rate == 0
}

// laneRestartDeps is what a run reads.
type laneRestartDeps struct {
	logger ulogger.Logger
	window utxo.LaneWindowReader
	// lastUpdate, when the store has it, gives the log lines each record's last-update time.
	lastUpdate utxo.LaneLastUpdateReader
	grants     func(ctx context.Context, height uint32) ([]lane.CapGrant, error)
	// view returns the view of the target's chain, nil when the index cannot give one in time.
	view      func(ctx context.Context, targetHeight uint32, ids map[uint32]bool) *lane.ChainView
	retention uint32
}

// laneRestartResult is one run's counts.
type laneRestartResult struct {
	outcome string
	window  utxo.LaneWindow
	stats   utxo.LaneWindowStats
	// records counts the records read by outcome, and sample the sampled transactions by kind.
	records map[string]uint64
	sample  map[string]uint64

	todayRead, todayKept uint64
	sampleRate           uint64
	// laneSampleOverflow counts sampled transactions only the window would keep that the run did
	// not hold, past laneRestartLaneSampleMax.
	laneSampleOverflow uint64
	// capBlocks is the target chain's blocks from the cap height to the target, and
	// capBlocksWithoutRows those the index holds no usable rows of: positions they took read
	// unmined.
	capBlocks, capBlocksWithoutRows uint64
	duration                        time.Duration
}

// laneRestartRunner keeps at most one run going: a new reload cancels the run of the one before.
type laneRestartRunner struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	// last is the result of the last run that ended, for tests.
	last *laneRestartResult
}

// startLaneRestartDryRun starts the dry run for a reload that succeeded, in the background,
// cancelling any run still going. A nil t (utxostore_laneMigration off) starts nothing.
func (b *BlockAssembler) startLaneRestartDryRun(ctx context.Context, t *laneRestartToday) {
	if t == nil {
		return
	}

	deps := laneRestartDeps{
		logger:    b.logger,
		retention: b.settings.UtxoStore.UnminedTxRetention,
		grants:    b.blockchainClient.LaneCapGrants,
		view:      laneRestartReaderView,
	}

	deps.window, _ = utxo.As[utxo.LaneWindowReader](b.utxoStore)
	deps.lastUpdate, _ = utxo.As[utxo.LaneLastUpdateReader](b.utxoStore)

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})

	b.laneRestart.mu.Lock()
	if b.laneRestart.cancel != nil {
		b.laneRestart.cancel()
	}

	b.laneRestart.cancel, b.laneRestart.done = cancel, done
	b.laneRestart.mu.Unlock()

	go func() {
		defer close(done)
		defer cancel()

		res := runLaneRestartDryRun(runCtx, deps, t)
		publishLaneRestart(res)

		b.laneRestart.mu.Lock()
		b.laneRestart.last = &res
		b.laneRestart.mu.Unlock()
	}()
}

// cancelLaneRestartDryRun cancels a run still going without waiting: a reload is starting, and
// both would read the store at once.
func (b *BlockAssembler) cancelLaneRestartDryRun() {
	b.laneRestart.mu.Lock()
	defer b.laneRestart.mu.Unlock()

	if b.laneRestart.cancel != nil {
		b.laneRestart.cancel()
	}
}

// stopLaneRestartDryRun cancels a run still going and waits for it to end.
func (b *BlockAssembler) stopLaneRestartDryRun() {
	b.laneRestart.mu.Lock()
	cancel, done := b.laneRestart.cancel, b.laneRestart.done
	b.laneRestart.mu.Unlock()

	if cancel == nil {
		return
	}

	cancel()
	<-done
}

// laneRestartReaderView waits for the process's lane index to reach the target's height and
// returns its view of the target's chain, or nil at the wait's end.
func laneRestartReaderView(ctx context.Context, targetHeight uint32, ids map[uint32]bool) *lane.ChainView {
	deadline := time.Now().Add(laneRestartViewWait)

	for {
		r := lanereader.Current()
		if s := r.Snapshot(); s != nil && s.TipHeight() >= targetHeight {
			return r.ViewOfIDs(ids)
		}

		if time.Now().After(deadline) {
			return nil
		}

		t := time.NewTimer(time.Second)

		select {
		case <-ctx.Done():
			t.Stop()

			return nil
		case <-t.C:
		}
	}
}

// runLaneRestartDryRun reads the window and judges it against today's reload.
func runLaneRestartDryRun(ctx context.Context, deps laneRestartDeps, t *laneRestartToday) laneRestartResult {
	start := time.Now()

	res := laneRestartResult{
		records:    map[string]uint64{},
		sample:     map[string]uint64{},
		todayRead:  t.read,
		todayKept:  t.kept,
		sampleRate: t.rate,
	}

	defer func() { res.duration = time.Since(start) }()

	if deps.window == nil {
		res.outcome = laneRunUnsupported
		deps.logger.Warnf("[laneRestart] the UTXO store cannot read the lanes restart window; the dry run reads nothing")

		return res
	}

	capHeight := uint32(0)
	if t.targetHeight > deps.retention {
		capHeight = t.targetHeight - deps.retention
	}

	grants, err := deps.grants(ctx, capHeight)
	if err != nil {
		res.outcome = runOutcome(ctx, laneRunError)
		deps.logger.Warnf("[laneRestart] reading the lane grants at or above height %d: %v", capHeight, err)

		return res
	}

	caps := map[uint8]lane.CapGrant{}

	for _, g := range grants {
		caps[g.Lane] = g

		if len(caps) == 1 || g.HandedAt.Before(res.window.From) {
			res.window.From = g.HandedAt
		}

		if len(caps) == 1 || g.Height < res.window.FromHeight {
			res.window.FromHeight = g.Height
		}
	}

	res.window.Margin, res.window.MarginBlocks = laneRestartMargin, laneRestartMarginBlocks

	j := &laneJudge{t: t, caps: caps, laneSample: map[chainhash.Hash]laneTodayEntry{}}

	if len(caps) == 0 {
		// No position handed out inside the cap: every positioned record is capped, and there is
		// no window to read.
		res.outcome = laneRunNoGrant
		j.finish(&res)
		deps.logger.Infof("[laneRestart] no lane grant handed out at or above height %d (target %s at %d): the window is empty; today's reload read %d and kept %d",
			capHeight, t.target.String(), t.targetHeight, t.read, t.kept)

		return res
	}

	j.view = deps.view(ctx, t.targetHeight, t.ids)
	if j.view != nil {
		res.capBlocks, res.capBlocksWithoutRows = laneCapCoverage(j.view, t.ids, capHeight, t.targetHeight)
	}

	res.stats, err = deps.window.ReadLaneWindow(ctx, res.window, func(batch []utxo.LaneWindowRecord) error {
		for i := range batch {
			res.records[j.judge(&batch[i])]++
		}

		return nil
	})
	if err != nil {
		res.outcome = runOutcome(ctx, laneRunError)
		deps.logger.Warnf("[laneRestart] reading the lanes restart window from %s (height %d): %v", res.window.From.Format(time.RFC3339Nano), res.window.FromHeight, err)

		return res
	}

	res.outcome = laneRunDone
	j.finish(&res)
	logLaneRestartDisagreements(ctx, deps, j, t)

	deps.logger.Infof("[laneRestart] target %s at %d, cap height %d, window from %s (height %d) less %s (%d block): read %d records, %d bytes, in %s (%d in the margin, margin known %t, %d external); today's reload read %d and kept %d; would keep %d; by outcome %v; sample one in %d: %v; %d of %d cap blocks without rows",
		t.target.String(), t.targetHeight, capHeight, res.window.From.Format(time.RFC3339Nano), res.window.FromHeight, res.window.Margin, res.window.MarginBlocks,
		res.stats.Records, res.stats.Bytes, res.stats.Duration, res.stats.MarginRecords, res.stats.MarginKnown, res.stats.External,
		t.read, t.kept, res.records[laneRecKeep], res.records, res.sampleRate, res.sample, res.capBlocksWithoutRows, res.capBlocks)

	return res
}

func runOutcome(ctx context.Context, otherwise string) string {
	if ctx.Err() != nil {
		return laneRunCancelled
	}

	return otherwise
}

// laneCapCoverage counts the target chain's blocks from capHeight to targetHeight, and those the
// view has no usable rows for: not loaded, without rows, or folded on a chain the floors do not
// hold.
func laneCapCoverage(v *lane.ChainView, ids map[uint32]bool, capHeight, targetHeight uint32) (blocks, withoutRows uint64) {
	blocks = uint64(targetHeight-capHeight) + 1
	covered := uint64(0)
	s := v.Snapshot()

	for id := range ids {
		h, ok := v.Height(id)
		if !ok || h < capHeight || h > targetHeight {
			continue
		}

		state, _ := s.Block(id)
		if state == lane.BlockHasRows && (!s.Folded(id) || v.FloorValid()) {
			covered++
		}
	}

	return blocks, blocks - min(blocks, covered)
}

// laneJudge judges the window's records and holds the sample.
type laneJudge struct {
	t    *laneRestartToday
	view *lane.ChainView
	caps map[uint8]lane.CapGrant
	// laneSample holds sampled transactions the window would keep that today's sample does not
	// have, up to laneRestartLaneSampleMax.
	laneSample map[chainhash.Hash]laneTodayEntry
	overflow   uint64
}

// capped reports whether pos came from a grant handed out before the cap.
func (j *laneJudge) capped(pos uint64) bool {
	g, ok := j.caps[lane.Lane(pos)]

	return !ok || g.Below(pos)
}

// judge judges one record and notes it in the sample.
func (j *laneJudge) judge(rec *utxo.LaneWindowRecord) string {
	outcome := j.outcome(rec)

	if rec.NotMaster || rec.Missing || !laneSampled(rec.Hash, j.t.rate) {
		return outcome
	}

	if e, ok := j.t.sample[rec.Hash]; ok {
		e.verdict = outcome

		return outcome
	}

	if outcome == laneRecKeep {
		if len(j.laneSample) >= laneRestartLaneSampleMax {
			j.overflow++
		} else {
			j.laneSample[rec.Hash] = laneTodayEntry{pos: rec.LanePosition, createdAt: rec.CreatedAt, verdict: outcome}
		}
	}

	return outcome
}

func (j *laneJudge) outcome(rec *utxo.LaneWindowRecord) string {
	switch {
	case rec.NotMaster:
		return laneRecNotMaster
	case rec.Missing:
		return laneRecMissing
	case rec.CreatedAt > 0 && rec.CreatedAt >= j.t.started.UnixMilli():
		return laneRecAfterReload
	case !lane.Valid(rec.LanePosition):
		return laneRecNoPosition
	case rec.Conflicting:
		return laneRecConflicting
	case j.view == nil:
		return laneRecNotReady
	}

	a := j.view.Lookup(rec.LanePosition)

	switch {
	case a.Mined && a.Via == lane.ViaFloor:
		return laneRecMinedFloor
	case a.Mined:
		return laneRecMinedRow
	case j.capped(rec.LanePosition):
		return laneRecCapped
	}

	return laneRecKeep
}

// todayKind is how a sampled transaction of today's pool compares with the window.
func (j *laneJudge) todayKind(e *laneTodayEntry) string {
	switch e.verdict {
	case laneRecKeep:
		return laneSampleBoth
	case laneRecMinedRow, laneRecMinedFloor:
		return laneSampleTodayMined
	case laneRecCapped:
		return laneSampleTodayCapped
	case laneRecNoPosition:
		return laneSampleTodayNoPosition
	case "":
		switch {
		case !lane.Valid(e.pos):
			return laneSampleTodayNoPosition
		case j.capped(e.pos):
			return laneSampleTodayCapped
		}

		return laneSampleTodayNotRead
	}

	return laneSampleTodayOther
}

// finish counts the sample into res.
func (j *laneJudge) finish(res *laneRestartResult) {
	for _, e := range j.t.sample {
		res.sample[j.todayKind(e)]++
	}

	if n := uint64(len(j.laneSample)) + j.overflow; n > 0 {
		res.sample[laneSampleLanesOnly] += n
	}

	res.laneSampleOverflow = j.overflow
}

// logLaneRestartDisagreements logs the first laneRestartLogged sampled disagreements of each kind,
// with each transaction's position and its record's creation and, where the store has it,
// last-update time.
func logLaneRestartDisagreements(ctx context.Context, deps laneRestartDeps, j *laneJudge, t *laneRestartToday) {
	logged := map[string]int{}

	logOne := func(kind string, hash chainhash.Hash, e laneTodayEntry) {
		if logged[kind] >= laneRestartLogged {
			return
		}

		logged[kind]++

		laneNum, p := lane.Unpack(e.pos)
		lastUpdate := "unknown"

		if deps.lastUpdate != nil {
			if lut, err := deps.lastUpdate.LaneLastUpdate(ctx, hash); err == nil {
				lastUpdate = lut.UTC().Format(time.RFC3339Nano)
			}
		}

		deps.logger.Warnf("[laneRestart] %s (log line %d): tx %s lane %d position %d created %s last update %s; window verdict %q; target %s at %d",
			kind, logged[kind], hash.String(), laneNum, p, time.UnixMilli(e.createdAt).UTC().Format(time.RFC3339Nano), lastUpdate, e.verdict, t.target.String(), t.targetHeight)
	}

	for hash, e := range t.sample {
		if kind := j.todayKind(e); isLaneSampleDisagreement(kind) {
			logOne(kind, hash, *e)
		}
	}

	for hash, e := range j.laneSample {
		logOne(laneSampleLanesOnly, hash, e)
	}
}
