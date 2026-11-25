package lanereader

import (
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The pruner's lane rule, judged beside today's rule at utxostore_laneMigration shadow, Phase 1 of
// lanes (task P4). Nothing acts on it: the store deletes or keeps what today's rule says, and the
// lane rule's answer is counted beside it (teranode_lane_pruner_verdicts_total).
//
// The rule: a parent may be deleted when its own lane position and every spender's position are
// settled, meaning a row of a main-chain block at least min(100, retention) deep takes it, or it is
// below its lane's floor, which only holds positions at least 100 deep. Take parent P at lane 7
// position 500, spent by C at lane 9 position 40, with the tip at 1,300 and retention 288, so the
// depth is 100. Block 1,150 took P and block 1,190 took C: both are more than 100 deep, so P may go.
// Had block 1,250 taken C, C would be 50 deep: P stays, counted spender_shallow. Had only a fork block
// taken C, P stays, counted spender_off_chain.
//
// The rule fails safe: a position missing from the parent or a slot, an index not loaded yet, a
// position below everything the index saw of its lane (IndexSnapshot.BeforeStart), or a main chain
// holding blocks with no rows (which may hold a position no row takes) all mean keep, counted as
// cannot_judge with the reason. One case reads as unmined instead: a position a block just below
// the index's first loaded block took, inside the range of a loaded row that left it untaken. It
// still means keep, but spender_unmined near the index's start can be such a load artefact.

// The stores the pruner judges, the store label.
const (
	PruneStoreUtxoset   = "utxoset"
	PruneStoreAerospike = "aerospike"
)

// Today's decision, the today label. PruneKeepUndo is a utxoset journal row today carries forward
// only as the undo copy a reorg inside the journal retention may need, because its spender is mined:
// the parent check is not why it is kept, so it is counted apart from PruneKeep.
const (
	PruneDelete   = "delete"
	PruneKeep     = "keep"
	PruneKeepUndo = "keep_undo"
)

// PruneToday is today's decision on a candidate, the one acted on.
type PruneToday uint8

// Today's decisions, named by todayNames.
const (
	TodayDelete PruneToday = iota
	TodayKeep
	TodayKeepUndo
)

var todayNames = [3]string{PruneDelete, PruneKeep, PruneKeepUndo}

// The lane rule's answer, the lanes label: PruneDelete, PruneKeep, or PruneCannotJudge, which keeps
// too.
const PruneCannotJudge = "cannot_judge"

// Reasons, the reason label. ReasonSettled goes with lanes=delete; the next seven with lanes=keep,
// naming the first position found unsettled; the last five with lanes=cannot_judge.
const (
	ReasonSettled = "settled"

	ReasonParentUnmined   = "parent_unmined"
	ReasonParentOffChain  = "parent_off_chain"
	ReasonParentShallow   = "parent_shallow"
	ReasonSpenderUnmined  = "spender_unmined"
	ReasonSpenderOffChain = "spender_off_chain"
	ReasonSpenderShallow  = "spender_shallow"
	ReasonConflicting     = "conflicting"
	ReasonNoPosition      = "no_position"
	ReasonNotReady        = "not_ready"
	ReasonNotLoaded       = "not_loaded"
	ReasonNoRows          = "no_rows"
	ReasonSpenderUnread   = "spender_unread"
)

// Disagreement directions, for the log.
const (
	directionTodayOnly = "today_deletes_lanes_keeps"
	directionLanesOnly = "lanes_deletes_today_keeps"
)

// PruneSpender is one spent output's spender as its slot names it.
type PruneSpender struct {
	Hash chainhash.Hash
	// Pos is the spender's packed lane position the slot holds, lane.None for none.
	Pos uint64
}

// PruneCandidate is one record today's pruner decided on: the parent and the spender of every spent
// output it holds. A record whose outputs are spread over several (Aerospike's paged records) is one
// candidate with every record's slots.
type PruneCandidate struct {
	Parent chainhash.Hash
	// ParentPos is the parent's own packed lane position, lane.None for none.
	ParentPos uint64
	Spenders  []PruneSpender
	// Conflicting is a conflict loser, which today deletes by its delete-at height and the lane rule
	// keeps, since no main-chain block takes it.
	Conflicting bool
	// Unread is set when some of the parent's slots could not be read, such as an Aerospike page
	// another worker deleted first: the lane rule cannot say every spender is settled.
	Unread bool
}

// PruneVerdict is the lane rule's answer for one candidate.
type PruneVerdict struct {
	Lanes  string
	Reason string
}

// pruneReason indexes reasonNames, so a verdict is counted in an array, not a map of strings.
type pruneReason uint8

const (
	reasonSettled pruneReason = iota
	// The six reasons a position is unsettled, the parent's three then the spender's three, in the
	// order unmined, off chain, shallow: unsettledBase(who) plus the kind.
	reasonParentUnmined
	reasonParentOffChain
	reasonParentShallow
	reasonSpenderUnmined
	reasonSpenderOffChain
	reasonSpenderShallow
	reasonConflicting
	reasonNoPosition
	reasonNotReady
	reasonNotLoaded
	reasonNoRows
	reasonSpenderUnread
	numReasons
	// reasonNone is a settled position.
	reasonNone pruneReason = 255
)

var reasonNames = [numReasons]string{
	ReasonSettled, ReasonParentUnmined, ReasonParentOffChain, ReasonParentShallow, ReasonSpenderUnmined,
	ReasonSpenderOffChain, ReasonSpenderShallow, ReasonConflicting, ReasonNoPosition, ReasonNotReady,
	ReasonNotLoaded, ReasonNoRows, ReasonSpenderUnread,
}

// The unsettled kinds, added to unsettledBase.
const (
	kindUnmined pruneReason = iota
	kindOffChain
	kindShallow
)

func unsettledBase(spender bool) pruneReason {
	if spender {
		return reasonSpenderUnmined
	}

	return reasonParentUnmined
}

func (r pruneReason) cannotJudge() bool { return r >= reasonNoPosition }

// The lanes label's index: delete, keep, cannot_judge.
const (
	lanesDelete = iota
	lanesKeep
	lanesCannot
)

var lanesNames = [3]string{PruneDelete, PruneKeep, PruneCannotJudge}

// pruneDecider is the position that decided a verdict, for the disagreement log.
type pruneDecider struct {
	who    string
	hash   chainhash.Hash
	pos    uint64
	answer lane.Answer
}

// PruneJudge judges one batch of pruning decisions against one snapshot of the index, so a batch is
// judged against one chain. It counts locally and adds to the metrics in Done; it is not safe for use
// by more than one goroutine. A nil PruneJudge, as Reader.PruneJudge returns at off, does nothing.
type PruneJudge struct {
	r     *Reader
	store string
	snap  *lane.IndexSnapshot
	// depth is min(settle depth, retention); a row's block this deep or deeper is settled.
	depth uint32
	// noRows is whether the main chain holds a loaded block with no rows.
	noRows bool

	// verdicts counts by today, lanes and reason.
	verdicts [3][3][numReasons]uint64
	vias     [3]uint64
	reads    map[string]uint64
	unjudged map[string]uint64
	// elapsed is the time the store reported in the lane rule (Time).
	elapsed time.Duration
	// gateKeys are the disagreement log's keys for this store, today-only then lanes-only.
	gateKeys [2]string
}

// PruneJudge returns a judge for one batch of store's pruning decisions, with the depth
// min(settle depth, retention), against the index's newest snapshot. It returns nil on a nil Reader,
// so at off a store that asks gets nil and does nothing more.
func (r *Reader) PruneJudge(store string, retention uint32) *PruneJudge {
	if r == nil {
		return nil
	}

	initPruneMetrics()

	start := time.Now()

	j := &PruneJudge{r: r, store: store, snap: r.Snapshot(), depth: min(r.settleDepth, retention),
		gateKeys: [2]string{"pruner/" + store + "/" + directionTodayOnly, "pruner/" + store + "/" + directionLanesOnly}}

	if j.snap != nil {
		j.noRows = j.snap.MainBlocksWithoutRows() > 0
	}

	j.Time(start)

	return j
}

// Judge answers c by the lane rule, counts the answer beside today's decision, and logs the first
// disagreements of each direction. today is today's decision, the only one acted on. It reads
// no clock except to rate-limit a disagreement's log line: the store times each batch it judges, the
// decoding of the candidates included (Time).
func (j *PruneJudge) Judge(today PruneToday, c *PruneCandidate) PruneVerdict {
	if j == nil {
		return PruneVerdict{}
	}

	lanes, reason, d := j.decide(c)

	j.verdicts[today][lanes][reason]++

	v := PruneVerdict{Lanes: lanesNames[lanes], Reason: reasonNames[reason]}

	switch {
	case today == TodayDelete && lanes == lanesKeep:
		j.logDisagreement(0, c, v, d)
	case today == TodayKeep && lanes == lanesDelete:
		j.logDisagreement(1, c, v, d)
	}

	return v
}

// decide is the lane rule: the lanes index, the reason and the position that decided.
func (j *PruneJudge) decide(c *PruneCandidate) (int, pruneReason, pruneDecider) {
	if j.snap == nil {
		return lanesCannot, reasonNotReady, pruneDecider{}
	}

	var (
		keep, cannot     = reasonNone, reasonNone
		keepBy, cannotBy pruneDecider
	)

	judge := func(spender bool, hash chainhash.Hash, pos uint64) {
		reason, a := j.position(spender, pos)

		switch {
		case reason == reasonNone:
		case reason.cannotJudge():
			if cannot == reasonNone {
				cannot, cannotBy = reason, pruneDecider{whoName(spender), hash, pos, a}
			}
		default:
			if keep == reasonNone {
				keep, keepBy = reason, pruneDecider{whoName(spender), hash, pos, a}
			}
		}
	}

	judge(false, c.Parent, c.ParentPos)

	if c.Conflicting && (keep == reasonParentUnmined || keep == reasonParentOffChain) {
		keep = reasonConflicting
	}

	var last uint64

	for i := range c.Spenders {
		// A keep is certain once one position is unsettled, so the rest are not looked up.
		if keep != reasonNone {
			break
		}

		// One spender often takes several outputs, and its slots sit together.
		if i > 0 && c.Spenders[i].Pos == last && lane.Valid(last) {
			continue
		}

		last = c.Spenders[i].Pos
		judge(true, c.Spenders[i].Hash, c.Spenders[i].Pos)
	}

	switch {
	case keep != reasonNone:
		return lanesKeep, keep, keepBy
	case cannot != reasonNone:
		return lanesCannot, cannot, cannotBy
	case c.Unread:
		return lanesCannot, reasonSpenderUnread, pruneDecider{}
	default:
		return lanesDelete, reasonSettled, pruneDecider{}
	}
}

func whoName(spender bool) string {
	if spender {
		return "spender"
	}

	return "parent"
}

// position judges one position: reasonNone when settled, else why it is not or cannot be judged.
func (j *PruneJudge) position(spender bool, pos uint64) (pruneReason, lane.Answer) {
	if !lane.Valid(pos) {
		return reasonNoPosition, lane.Answer{}
	}

	a := j.snap.Main().Lookup(pos)
	j.vias[a.Via]++

	switch a.Via {
	case lane.ViaFloor:
		// The floor holds only positions of blocks at least the settle depth deep, which is at least
		// the rule's depth.
		return reasonNone, a
	case lane.ViaRow:
		if tip := j.snap.TipHeight(); a.Height <= tip && tip-a.Height >= j.depth {
			return reasonNone, a
		}

		return unsettledBase(spender) + kindShallow, a
	}

	if _, _, ok := j.snap.OffMainTaker(pos); ok {
		return unsettledBase(spender) + kindOffChain, a
	}

	if j.snap.BeforeStart(pos) {
		return reasonNotLoaded, a
	}

	if j.noRows {
		return reasonNoRows, a
	}

	return unsettledBase(spender) + kindUnmined, a
}

// Reads counts n extra reads of kind the lane rule needed, beyond what today's pruner reads.
func (j *PruneJudge) Reads(kind string, n int) {
	if j == nil || n <= 0 {
		return
	}

	if j.reads == nil {
		j.reads = map[string]uint64{}
	}

	j.reads[kind] += uint64(n) //nolint:gosec // n is positive
}

// Unjudged counts n records today decided on that the lane rule did not judge, by reason.
func (j *PruneJudge) Unjudged(reason string, n int) {
	if j == nil || n <= 0 {
		return
	}

	if j.unjudged == nil {
		j.unjudged = map[string]uint64{}
	}

	j.unjudged[reason] += uint64(n) //nolint:gosec // n is positive
}

// Time adds the time since start to the time spent in the lane rule. A store calls it around each
// stretch of work it does only for the rule, its reads excluded. That work does no I/O, so
// teranode_lane_pruner_judge_seconds_total, a wall time, approximates the CPU the rule adds.
func (j *PruneJudge) Time(start time.Time) {
	if j != nil {
		j.elapsed += time.Since(start)
	}
}

// Ready reports whether the judge has a snapshot of the index to judge against. Before the index's
// first snapshot every verdict is not_ready, so a store reads nothing extra for the rule.
func (j *PruneJudge) Ready() bool {
	return j != nil && j.snap != nil
}

// Elapsed is the time the judge has spent in the lane rule so far, not counting reads.
func (j *PruneJudge) Elapsed() time.Duration {
	if j == nil {
		return 0
	}

	return j.elapsed
}

// Done adds the batch's counts to the metrics. A judge is done once.
func (j *PruneJudge) Done() {
	if j == nil {
		return
	}

	for today, byLanes := range j.verdicts {
		for lanes, byReason := range byLanes {
			for reason, n := range byReason {
				if n > 0 {
					prometheusLanePrunerVerdicts.WithLabelValues(j.store, todayNames[today], lanesNames[lanes], reasonNames[reason]).Add(float64(n))
				}
			}
		}
	}

	for via, n := range j.vias {
		if n > 0 {
			prometheusLanePrunerLookups.WithLabelValues(j.store, lane.Via(via).String()).Add(float64(n)) //nolint:gosec // via is 0 to 2
		}
	}

	for kind, n := range j.reads {
		prometheusLanePrunerReads.WithLabelValues(j.store, kind).Add(float64(n))
	}

	for reason, n := range j.unjudged {
		prometheusLanePrunerUnjudged.WithLabelValues(j.store, reason).Add(float64(n))
	}

	prometheusLanePrunerSeconds.WithLabelValues(j.store).Add(j.elapsed.Seconds())
}

// logDisagreement logs one disagreement, the first disagreementsLogged of each store and direction
// and then one a minute.
func (j *PruneJudge) logDisagreement(which int, c *PruneCandidate, v PruneVerdict, d pruneDecider) {
	n, ok := j.r.disagree.admit(j.gateKeys[which])
	if !ok {
		return
	}

	direction := [2]string{directionTodayOnly, directionLanesOnly}[which]

	pl, pp := lane.Unpack(c.ParentPos)
	dl, dp := lane.Unpack(d.pos)

	var spenders strings.Builder

	for i, s := range c.Spenders {
		if i == 4 {
			fmt.Fprintf(&spenders, " and %d more", len(c.Spenders)-i)

			break
		}

		sl, sp := lane.Unpack(s.Pos)
		fmt.Fprintf(&spenders, " %s at lane %d position %d", s.Hash.String(), sl, sp)
	}

	j.r.logger.Warnf("[lanereader] pruner %s %s (log line %d): parent %s at lane %d position %d, conflicting=%t; lanes %s (%s), decided by %s %s at lane %d position %d: mined=%t via %s block %d height %d; spenders:%s; index tip %d anchor %d start %d, depth %d",
		j.store, direction, n, c.Parent.String(), pl, pp, c.Conflicting, v.Lanes, v.Reason,
		d.who, d.hash.String(), dl, dp, d.answer.Mined, d.answer.Via, d.answer.BlockID, d.answer.Height,
		spenders.String(), j.snap.TipHeight(), j.snap.AnchorHeight(), j.snap.StartHeight(), j.depth)
}
