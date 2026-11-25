package utxoset

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
)

// The pruner's lane rule on utxoset, at utxostore_laneMigration shadow (lanes Phase 1, task P4).
//
// utxoset has no delete-at-height sweep. A spend deletes the output's UTXO row and writes its undo
// copy, a spend_journal row naming the parent output and its spender (spending_txid). Once a journal
// partition is older than the journal retention the pruner drops it, after moving forward the rows
// whose spender still waits to be mined or was mined recently (collectCopyForward). So the
// per-record decision the lane rule is judged against is each row of a retiring partition: carried
// forward for a waiting spender is today's keep, dropped with the partition is today's delete. A row
// carried for a recently mined spender is kept as the undo copy a reorg inside the journal retention
// may need, not for the parent check, so it is counted keep_undo. The row is the parent's slot
// for that output: its lane_position is the parent's position (where the lane schema installed the
// column), and the spender's position is not on it, so the rule reads it from the spender's identity
// row, containment rows or preserved copy (teranode_lane_pruner_extra_reads_total kind position).
//
// Every row of a retiring partition is read and judged, in Go, against one snapshot of the lane index
// taken when the carried set is read, before the copy-forward moves the kept rows out and the
// partition drops. The judging must not slow the drops, so it is bounded three ways. A pass judges
// only when it retires a single journal partition, as it does at the tip once every 288 blocks; a
// pass catching up a backlog judges none of them (unjudged backlog). A pass spends at most
// laneJudgePassBudget judging; an error or the budget ends a partition's judging (unjudged
// leaf_error) and the drop goes on. And a partition is judged once per process, so a pass that
// retries a failed drop does not count its rows again.
//
// What this does not judge: tx_body partitions and their carry, tx_mined partitions and
// preserved_parent expiry. None of those deletes a parent's slot; each is a copy or a body whose
// retention is set by height alone.

// laneJudgePassBudget bounds the time one pruner pass spends judging.
var laneJudgePassBudget = 2 * time.Minute

// laneJudgeBatch is how many journal rows one round of position reads covers.
const laneJudgeBatch = 10_000

// lanePositionsSQL reads each transaction's own lane position: from its identity row, else any of its
// containment rows, else its preserved copy. Each arm is a fenced primary-key probe, the shape every
// by-txid read here has. A transaction none of them holds, or holds without a position, answers NULL.
const lanePositionsSQL = `
SELECT k.txid, COALESCE(i.lane_position, m.lane_position, p.lane_position)
  FROM unnest($1::smallint[], $2::bytea[]) AS k(leaf, txid)
  LEFT JOIN LATERAL (
       SELECT x.lane_position FROM tx_ident x
        WHERE x.leaf = k.leaf AND x.txid = k.txid
       OFFSET 0) i ON true
  LEFT JOIN LATERAL (
       SELECT x.lane_position FROM tx_mined x
        WHERE x.txid = k.txid AND x.lane_position IS NOT NULL
        LIMIT 1) m ON true
  LEFT JOIN LATERAL (
       SELECT x.lane_position FROM preserved_parent x
        WHERE x.txid = k.txid
       OFFSET 0) p ON true`

// laneJournalJudge judges the rows of one pruner pass's retiring journal partitions.
type laneJournalJudge struct {
	s     *Store
	judge *lanereader.PruneJudge
	// kept is the carried set, each (output key, parent, spender), true for a waiting spender's.
	kept map[carriedKey]bool
	// backlog is a pass retiring more than one journal partition, which judges none.
	backlog bool
	// deadline ends the pass's judging.
	deadline time.Time
}

// carriedKey is one carried spend: the 16-byte output key, the parent and the spender.
type carriedKey [16 + 32 + 32]byte

func newCarriedKey(ukey, parent, spender []byte) carriedKey {
	var k carriedKey

	copy(k[:16], ukey)
	copy(k[16:48], parent)
	copy(k[48:], spender)

	return k
}

// retiringJournalLeaves counts the spend_journal partitions below cutoff, the ones a pass retires.
func retiringJournalLeaves(leaves []leafState, cutoff uint32) int {
	n := 0

	for _, l := range leaves {
		if l.parent == "spend_journal" && l.leaf < cutoff {
			n++
		}
	}

	return n
}

// newLaneJournalJudge returns the pass's judge, against the index's snapshot now, with carry today's
// keep set, for a pass retiring retiring journal partitions. It returns nil at off, where no Reader
// exists.
func (s *Store) newLaneJournalJudge(carry *copyForwardSet, retiring int) *laneJournalJudge {
	var retention uint32
	if s.settings != nil {
		retention = s.settings.GlobalBlockHeightRetention
	}

	j := lanereader.Current().PruneJudge(lanereader.PruneStoreUtxoset, retention)
	if j == nil {
		return nil
	}

	l := &laneJournalJudge{s: s, judge: j, backlog: retiring > 1, deadline: time.Now().Add(laneJudgePassBudget)}
	if l.backlog {
		return l
	}

	l.kept = make(map[carriedKey]bool, len(carry.ukeys))
	for i := range carry.ukeys {
		l.kept[newCarriedKey(carry.ukeys[i], carry.parents[i], carry.spenders[i])] = i < carry.unmined
	}

	return l
}

// done adds the pass's counts to the metrics.
func (l *laneJournalJudge) done() {
	if l != nil {
		l.judge.Done()
	}
}

// judgeLeaf judges every row of the retiring journal partition for leaf. It never fails the pass.
func (l *laneJournalJudge) judgeLeaf(ctx context.Context, leaf uint32) {
	if l == nil {
		return
	}

	if _, seen := l.s.laneJudgedLeaves.Load(leaf); seen {
		return
	}

	switch {
	case l.backlog:
		l.judge.Unjudged("backlog", 1)

		return
	case !time.Now().Before(l.deadline):
		l.judge.Unjudged("budget", 1)

		return
	}

	l.s.laneJudgedLeaves.Store(leaf, struct{}{})

	ctx, cancel := context.WithDeadline(ctx, l.deadline)
	defer cancel()

	start := time.Now()
	ruleBefore := l.judge.Elapsed()

	rows, err := l.judgeLeafRows(ctx, leaf)
	if err != nil {
		l.judge.Unjudged("leaf_error", 1)
		l.s.logger.Warnf("[utxoset] lane rule: journal partition %d judged %d rows, then stopped; the drop goes on: %v", leaf, rows, err)

		return
	}

	l.s.logger.Infof("[utxoset] lane rule: judged %d rows of journal partition %d in %s, %s of it in the rule", rows, leaf,
		time.Since(start), l.judge.Elapsed()-ruleBefore)
}

// laneJournalRow is one journal row as the lane rule reads it.
type laneJournalRow struct {
	ukey, parent, spender []byte
	parentPos             uint64
}

func (l *laneJournalJudge) judgeLeafRows(ctx context.Context, leaf uint32) (int, error) {
	table := fmt.Sprintf("spend_journal_%d", leaf)

	// A leaf detached before the lane schema was installed kept its old columns, as the copy-forward
	// also checks.
	withLane := false

	if l.s.laneCoinColumn {
		has, err := schemaHas(ctx, l.s.pool, table, "lane_position")
		if err != nil {
			return 0, err
		}

		withLane = has
	}

	stmt := "SELECT ukey, txid, spending_txid FROM " + table
	if withLane {
		stmt = "SELECT ukey, txid, spending_txid, lane_position FROM " + table
	}

	// The scan holds one connection; the position reads take another.
	q, err := l.s.pool.Query(ctx, stmt)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] lane rule: read journal partition %d", leaf, err)
	}

	defer q.Close()

	judged := 0
	batch := make([]laneJournalRow, 0, laneJudgeBatch)

	for q.Next() {
		var (
			r   laneJournalRow
			pos *int64
		)

		dest := []any{&r.ukey, &r.parent, &r.spender}
		if withLane {
			dest = append(dest, &pos)
		}

		if err := q.Scan(dest...); err != nil {
			return judged, errors.NewStorageError("[utxoset] lane rule: scan journal partition %d", leaf, err)
		}

		r.parentPos = lane.None
		if pos != nil {
			r.parentPos = lane.FromSigned(*pos)
		}

		batch = append(batch, r)

		if len(batch) == laneJudgeBatch {
			if err := l.judgeBatch(ctx, batch); err != nil {
				return judged, err
			}

			judged += len(batch)
			batch = batch[:0]
		}
	}

	if err := q.Err(); err != nil {
		return judged, errors.NewStorageError("[utxoset] lane rule: read journal partition %d", leaf, err)
	}

	if len(batch) > 0 {
		if err := l.judgeBatch(ctx, batch); err != nil {
			return judged, err
		}

		judged += len(batch)
	}

	return judged, nil
}

// judgeBatch reads the positions a batch of rows needs and judges each row.
func (l *laneJournalJudge) judgeBatch(ctx context.Context, batch []laneJournalRow) error {
	l.judge.Reads("journal_row", len(batch))

	start := time.Now()
	want := make(map[string]struct{}, len(batch))

	for _, r := range batch {
		want[string(r.spender)] = struct{}{}

		if !lane.Valid(r.parentPos) {
			want[string(r.parent)] = struct{}{}
		}
	}

	// The position reads are I/O, counted as reads, not as the rule's time.
	l.judge.Time(start)

	positions, err := l.positions(ctx, want)
	if err != nil {
		return err
	}

	start = time.Now()

	var c lanereader.PruneCandidate

	for _, r := range batch {
		copy(c.Parent[:], r.parent)

		c.ParentPos = r.parentPos
		if !lane.Valid(c.ParentPos) {
			c.ParentPos = positions[string(r.parent)]
		}

		var spender chainhash.Hash
		copy(spender[:], r.spender)

		c.Spenders = append(c.Spenders[:0], lanereader.PruneSpender{Hash: spender, Pos: positions[string(r.spender)]})

		today := lanereader.TodayDelete
		if waiting, kept := l.kept[newCarriedKey(r.ukey, r.parent, r.spender)]; kept {
			today = lanereader.TodayKeepUndo
			if waiting {
				today = lanereader.TodayKeep
			}
		}

		l.judge.Judge(today, &c)
	}

	l.judge.Time(start)

	return nil
}

// positions reads the lane position of each transaction in want; one without answers lane.None.
func (l *laneJournalJudge) positions(ctx context.Context, want map[string]struct{}) (map[string]uint64, error) {
	out := make(map[string]uint64, len(want))

	if !l.s.laneColumn || len(want) == 0 {
		return out, nil
	}

	leaves := make([]int16, 0, len(want))
	txids := make([][]byte, 0, len(want))

	for k := range want {
		leaves = append(leaves, LeafFor([]byte(k)))
		txids = append(txids, []byte(k))
	}

	l.judge.Reads("position", len(txids))

	rows, err := l.s.pool.Query(ctx, lanePositionsSQL, leaves, txids)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset] lane rule: read %d lane positions", len(txids), err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid []byte
			pos  *int64
		)

		if err := rows.Scan(&txid, &pos); err != nil {
			return nil, errors.NewStorageError("[utxoset] lane rule: scan lane positions", err)
		}

		if pos != nil {
			out[string(txid)] = lane.FromSigned(*pos)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset] lane rule: read lane positions", err)
	}

	return out, nil
}
