package utxoset

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/jackc/pgx/v5"
)

// createIdentPlanSQL stores a whole BATCH of MEMPOOL transactions in ONE statement.
//
// It is the claim for a create that carries no mined-block information: a transaction seen
// before any block contains it. Such a transaction claims on tx_ident, and its coins carry the
// unconfirmed sentinel — mined_height 0, block_id 0 — until something stamps them. The
// block-path counterpart is createMinedPlanSQL.
//
// Every parameter is an array, so one statement serves one transaction or a thousand, exactly
// as spendJournalSQL does on the other side. That is what lets the single-transaction path be
// a plan of one rather than a second copy of these predicates living somewhere else.
//
// The three writes a create makes used to be three separate statements on the pool. That cost
// three commits each and was not atomic. A failure between them left an identity row with no
// serialized bytes, which is indistinguishable from a transaction whose bytes have aged out
// of their window, so it read as normal forever. Worse, the retry was refused, because the
// identity claim reported the transaction as already present, so the bytes were never
// written.
//
// Folding them into one statement fixes both at once. Data-modifying common table expressions
// run in a single snapshot, so either all three land or none do, and both the body and the
// coin inserts are gated on the claim having actually inserted that transaction's row. A
// transaction the store already holds therefore writes nothing at all and is absent from the
// result.
//
// The gate is a JOIN against claim rather than an EXISTS, and that is the whole difference
// between this and the per-transaction statement it replaces. EXISTS asks "did anything in
// this statement claim", which is the right question only when the statement carries one
// transaction. The join asks it per transaction, so a batch mixing fresh transactions with
// ones the store already holds writes bodies and coins for exactly the fresh ones.
//
// claim RETURNING gives back leaf and txid rather than the caller's index k, because
// PostgreSQL only lets INSERT ... RETURNING name columns of the target table. The join back
// onto (leaf, txid) recovers k, and it is exact rather than approximate: txid is the full 32
// bytes, and (leaf, txid) is tx_ident's primary key.
//
// The coin insert has no conflict clause and needs none. The coin key is a non-unique 96-bit
// prefix by design and has nothing to conflict on, so idempotence here comes from the claim
// gate. Without that gate a re-applied block would create every output a second time, which
// is the failure this mechanism exists to prevent.
//
// The claim carries THREE guards, and all three are needed. Its own conflict clause catches a
// transaction the mempool already holds. The tx_mined probe keeps a transaction in exactly one
// of the two tables. The own-output coin probe catches the case neither of the other two can
// see: a transaction mined longer ago than the membership retention has no identity row and no
// membership window, and its coins are still live because window retirement stamped them on the
// way out. Without that third guard the claim takes, and because the coin insert is gated on
// the same claim, every output is written a SECOND row -- the coin key is a non-unique 96-bit
// prefix by design, so nothing downstream catches it. That is money-supply inflation.
//
// The spec argued this could not arise because the mempool path spends before it creates, so an
// old transaction's inputs would fail as already-spent and the create would never run. That is
// true of SpendAndCreate in its default mode and false with WithCreateOnly, which skips the
// spend phase entirely. The reachable caller is the validator's CreateConflicting branch
// (services/validator/Validator.go): when every input fails as already-spent it calls
// CreateInUtxoStore with markAsConflicting, which is SpendAndCreate + WithCreateOnly and no
// mined-block info. CreateConflicting is off on the p2p path and ON for every
// subtree-validation entry point, which is the mainline block path at the tip.
//
// The guard is the identical statement createMinedPlanSQL carries, LIMIT 1 OFFSET 0 fence and
// all, for the identical reason: a bare NOT EXISTS is flattened into an anti-join, and the
// fence keeps it a per-row subplan on the coin's packed-key range.
//
// The tx_mined guard is what keeps a transaction in EXACTLY ONE of the two tables. A transaction the longest-chain stamp
// has settled has no identity row at all, so ON CONFLICT sees nothing to conflict with: without
// the guard, a mempool create of an already-settled transaction takes a fresh identity row --
// two homes, and every read-order argument in this store assumes one -- and, because the coin
// insert is gated on that same claim taking, writes every one of its outputs a SECOND time.
// Duplicate coins are the failure the claim mechanism exists to prevent. With the guard such a
// create claims nothing and settle reports it as ErrTxExists, which is exactly what the block
// path already answers for a mempool stray, in the other direction.
//
// It costs one index descent per live window, and the OFFSET 0 inside it is what buys that.
// tx_mined's primary key LEADS with txid, so the probe is cheap, but a bare NOT EXISTS is
// flattened into an anti-join and the planner then costs a hash of the whole window below 500
// per-key probes: measured at 500 keys against 40,000 membership rows, a Hash Anti Join over a
// Seq Scan of the window, 9.4 ms. OFFSET 0 fences the subquery so it stays a per-row subplan on
// the primary key -- the same fence minedByTxidSQL and appendMinedSQL need, for the same
// reason. At 400,000 rows the planner reaches for the index either way, which is exactly why
// the fence has to be written down rather than left to the estimate.
//
// The lock lockTxids takes before this statement is what makes the guard trustworthy, exactly
// as it does for createMinedPlanSQL's three: the read takes no row lock of its own, so two
// concurrent creates of one transaction could otherwise both find nothing.
//
// fee is written as NULL deliberately. The store does not compute it, and block assembly
// rebuilds a mining candidate from size and inpoints instead.
const createIdentPlanSQL = `
WITH t AS (
    SELECT * FROM unnest($1::int[], $2::smallint[], $3::bytea[], $4::int[], $5::int[],
                         $6::bytea[], $7::int[], $8::bytea[], $9::int[], $10::bigint[],
                         $11::smallint[], $12::bytea[], $13::uuid[], $14::uuid[])
        AS t(k, leaf, txid, created_height, off_chain_since, membership, size_in_bytes,
             tx_inpoints, locktime, created_at, flags, raw_tx, lo, hi)
),
claim AS (
    INSERT INTO tx_ident (leaf, txid, created_height, off_chain_since, membership,
                          fee, size_in_bytes, tx_inpoints, locktime, created_at, flags)
    SELECT t.leaf, t.txid, t.created_height, t.off_chain_since, t.membership,
           NULL::bigint, t.size_in_bytes, t.tx_inpoints, t.locktime, t.created_at, t.flags
      FROM t
     WHERE NOT EXISTS (SELECT 1 FROM tx_mined m WHERE m.txid = t.txid LIMIT 1 OFFSET 0)
       AND NOT EXISTS (SELECT 1 FROM utxo u
                        WHERE u.leaf = t.leaf AND u.ukey >= t.lo AND u.ukey <= t.hi AND u.txid = t.txid
                        ORDER BY u.ukey LIMIT 1 OFFSET 0)
    ON CONFLICT (leaf, txid) DO NOTHING
    RETURNING leaf, txid
),
body AS (
    INSERT INTO tx_body (created_height, txid, raw_tx)
    SELECT t.created_height, t.txid, t.raw_tx
      FROM t
      JOIN claim c ON c.leaf = t.leaf AND c.txid = t.txid
),
coins AS (
    INSERT INTO utxo (satoshis, created_height, spendable_from, mined_height, block_id,
                      leaf, flags, ukey, txid, script)
    SELECT o.satoshis, o.created_height, o.spendable_from, 0, 0,
           o.leaf, o.flags, o.ukey, o.txid, o.script
      FROM unnest($15::bigint[], $16::int[], $17::int[], $18::smallint[], $19::smallint[],
                  $20::uuid[], $21::bytea[], $22::bytea[])
        AS o(satoshis, created_height, spendable_from, leaf, flags, ukey, txid, script)
      JOIN claim c ON c.leaf = o.leaf AND c.txid = o.txid
)
SELECT t.k
  FROM t
  JOIN claim c ON c.leaf = t.leaf AND c.txid = t.txid`

// createMinedPlanSQL is the claim for a create that carries mined-block information: every
// block-application create below the checkpoint, and block assembly's coinbase at the tip.
//
// It claims on tx_mined, not tx_ident, so a mined transaction never has an identity row.
// Three of the four idempotence guards live in this statement; the fourth, the advisory
// lock, is taken in a separate statement first (see lockTxids) so that this statement's
// snapshot postdates any competitor's commit.
//
//   - ON CONFLICT on (txid, mined_height, block_id): the same block re-applied.
//   - NOT EXISTS tx_mined (txid, mined_height): the same height under another block id, which
//     is a retry whose block-id reuse failed or a stale sibling block; the caller's
//     ErrTxExists branch stamps the second id instead of recreating coins.
//   - NOT EXISTS tx_ident (leaf, txid): a mempool stray already holds the transaction.
//   - NOT EXISTS utxo in the transaction's own packed-key range: the transaction still has a
//     live coin, at any age. This is SV Node's duplicate check and what refuses the two
//     historic duplicate coinbases. Written with the LIMIT 1 OFFSET 0 fence so the planner
//     cannot swap the range scan for a scan of the whole leaf partition.
//
// tx_inpoints is written NULL: below the checkpoint nothing can un-mine, and at the tip the
// block path only creates coinbases, which have no inputs. fee is written NULL for the same
// reason createIdentPlanSQL writes it NULL -- the store never computes one, and a coinbase
// has none to compute.
//
// The body CTE's `raw_tx IS NOT NULL` is how utxostore_skipTxBodyBelowCheckpoint is applied,
// and it is a filter on the ROW rather than a second statement on purpose. Block application
// below the checkpoint and the tip's own writes reach the same batcher, so one statement
// routinely carries transactions from both sides of the floor; and a second statement would be
// a second plan, which on this store means a second set of estimates to keep honest. The
// caller decides per transaction by handing NULL instead of the bytes (see appendCreate),
// so this statement needs no knowledge of checkpoints at all.
const createMinedPlanSQL = `
WITH t AS (
    SELECT * FROM unnest($1::int[], $2::smallint[], $3::bytea[], $4::int[], $5::int[],
                         $6::int[], $7::int[], $8::int[], $9::bigint[], $10::smallint[],
                         $11::bytea[], $12::uuid[], $13::uuid[])
        AS t(k, leaf, txid, created_height, mined_height, block_id, subtree_idx,
             size_in_bytes, created_at, flags, raw_tx, lo, hi)
),
claim AS (
    INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                          size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
    SELECT t.txid, t.mined_height, t.block_id, t.subtree_idx, t.created_height,
           t.size_in_bytes, NULL::bigint, NULL::bytea, NULL::int, t.created_at, t.flags
      FROM t
     WHERE NOT EXISTS (SELECT 1 FROM tx_mined m WHERE m.txid = t.txid AND m.mined_height = t.mined_height)
       AND NOT EXISTS (SELECT 1 FROM tx_ident i WHERE i.leaf = t.leaf AND i.txid = t.txid)
       AND NOT EXISTS (SELECT 1 FROM utxo u
                        WHERE u.leaf = t.leaf AND u.ukey >= t.lo AND u.ukey <= t.hi AND u.txid = t.txid
                        ORDER BY u.ukey LIMIT 1 OFFSET 0)
    ON CONFLICT (txid, mined_height, block_id) DO NOTHING
    RETURNING txid
),
body AS (
    INSERT INTO tx_body (created_height, txid, raw_tx)
    SELECT t.created_height, t.txid, t.raw_tx
      FROM t JOIN claim c ON c.txid = t.txid
     WHERE t.raw_tx IS NOT NULL
),
coins AS (
    INSERT INTO utxo (satoshis, created_height, spendable_from, mined_height, block_id,
                      leaf, flags, ukey, txid, script)
    SELECT o.satoshis, o.created_height, o.spendable_from, o.mined_height, o.block_id,
           o.leaf, o.flags, o.ukey, o.txid, o.script
      FROM unnest($14::bigint[], $15::int[], $16::int[], $17::int[], $18::int[],
                  $19::smallint[], $20::smallint[], $21::uuid[], $22::bytea[], $23::bytea[])
        AS o(satoshis, created_height, spendable_from, mined_height, block_id, leaf, flags, ukey, txid, script)
      JOIN claim c ON c.txid = o.txid
)
SELECT t.k FROM t JOIN claim c ON c.txid = t.txid`

// createResult is what one queued Create gets back.
type createResult struct {
	data *meta.Data
	err  error
}

// createItem is a single Create waiting for its batch to flush.
type createItem struct {
	tx          *bt.Tx
	blockHeight uint32
	options     *utxo.CreateOptions
	done        chan createResult
}

// createPlan is the argument set for one call of the create statement, however many
// transactions went into it, plus the mapping needed to give each caller its own answer.
//
// Building it in one place is what stops the batched and unbatched paths carrying separate
// copies of the same statement. The single-transaction path is a plan of one.
//
// Two array widths live here. The identity fields carry one element per transaction. The coin
// fields carry one element per SPENDABLE output, flattened across every transaction in the
// batch, and they are tied back to their transaction by the leaf and txid they already carry
// rather than by a separate mapping.
type createPlan struct {
	// One element per transaction that made it into the statement.
	idx        []int32
	leaves     []int16
	txids      [][]byte
	heights    []int32
	offChain   []*int32
	membership [][]byte
	sizes      []int32
	inpoints   [][]byte
	locktimes  []int32
	createdAt  []int64
	txFlags    []int16
	bodies     [][]byte
	// minedRows is true for a transaction that carries mined-block information, which is
	// what sends it to createMinedPlanSQL instead of createIdentPlanSQL. The three fields
	// below carry that block's facts, and are 0 for a mempool create.
	minedRows   []bool
	minedHeight []int32
	blockID     []int32
	subtreeIdx  []int32
	// The packed-key range of the transaction's own outputs, so the block path can ask
	// whether it still has a live coin without scanning its leaf partition.
	lo, hi [][16]byte

	// One element per spendable output, across the whole batch.
	coinSats      []int64
	coinHeights   []int32
	coinSpendable []int32
	coinLeaves    []int16
	coinFlags     []int16
	coinUkeys     [][16]byte
	coinTxids     [][]byte
	coinScripts   [][]byte
	// The block facts, repeated per coin, so the coin row knows its block without a join.
	// Both are 0 for a mempool create: mined_height 0 is the unconfirmed sentinel.
	coinMined    []int32
	coinBlockIDs []int32

	owner   []int        // plan row -> which item in the batch
	txs     []*bt.Tx     // plan row -> its transaction, for error messages
	perItem []*meta.Data // batch item -> the record its caller gets back
	errs    []error      // batch item -> its own error, if it has one
}

// planCreates flattens a batch of transactions into one set of arrays.
func (s *Store) planCreates(items []*createItem) *createPlan {
	p := &createPlan{
		perItem: make([]*meta.Data, len(items)),
		errs:    make([]error, len(items)),
	}

	// One claim per txid per statement, deduplicated HERE rather than left to the database.
	//
	// This is not tidiness. ON CONFLICT DO NOTHING tolerates a repeated key within a single
	// command, but it returns the winning row ONCE, so a second plan row for the same
	// transaction would join that one claim and insert its body a second time. tx_body's
	// primary key then raises a unique violation and takes the whole batch down with it.
	//
	// Reporting the repeat as already-held is also what the caller would have seen had the
	// two offers landed in separate batches, so batch composition does not change the answer.
	seen := make(map[chainhash.Hash]struct{}, len(items))

	for i, it := range items {
		if it.tx == nil {
			p.errs[i] = errors.NewProcessingError("[utxoset][Create] nil tx")
			continue
		}

		txHash := it.tx.TxIDChainHash()

		if _, dup := seen[*txHash]; dup {
			p.errs[i] = errors.NewTxExistsError("[utxoset][Create] %s", txHash.String())
			continue
		}

		data, err := s.appendCreate(p, i, it.tx, it.blockHeight, it.options)
		if err != nil {
			p.errs[i] = err
			continue
		}

		seen[*txHash] = struct{}{}
		p.perItem[i] = data
	}

	p.sortRows()

	return p
}

// sortRows puts the identity rows in one global order, by leaf and txid, for the same reason
// spendPlan.sortRows does: two batches claiming the same transactions in opposite orders would
// wait on each other's speculative inserts and deadlock, and in one order they cannot. The coin
// rows are left as built; nothing about them is unique, so nothing about them can wait.
func (p *createPlan) sortRows() {
	n := len(p.owner)
	if n < 2 {
		return
	}

	order := make([]int, n)
	for i := range order {
		order[i] = i
	}

	sort.SliceStable(order, func(a, b int) bool {
		x, y := order[a], order[b]
		if p.leaves[x] != p.leaves[y] {
			return p.leaves[x] < p.leaves[y]
		}

		return bytes.Compare(p.txids[x], p.txids[y]) < 0
	})

	p.leaves = permute(p.leaves, order)
	p.txids = permute(p.txids, order)
	p.heights = permute(p.heights, order)
	p.offChain = permute(p.offChain, order)
	p.membership = permute(p.membership, order)
	p.sizes = permute(p.sizes, order)
	p.inpoints = permute(p.inpoints, order)
	p.locktimes = permute(p.locktimes, order)
	p.createdAt = permute(p.createdAt, order)
	p.txFlags = permute(p.txFlags, order)
	p.bodies = permute(p.bodies, order)
	p.minedRows = permute(p.minedRows, order)
	p.minedHeight = permute(p.minedHeight, order)
	p.blockID = permute(p.blockID, order)
	p.subtreeIdx = permute(p.subtreeIdx, order)
	p.lo = permute(p.lo, order)
	p.hi = permute(p.hi, order)
	p.owner = permute(p.owner, order)
	p.txs = permute(p.txs, order)

	for k := range p.idx {
		p.idx[k] = int32(k) //nolint:gosec // bounded by batch size
	}
}

// subset projects the plan onto the given transaction rows, carrying each row's coins with it.
//
// The two claim statements take disjoint halves of one batch, and each needs contiguous arrays
// of its own: k is a position in the arrays the statement is handed, so the rows going to one
// statement have to be renumbered from zero. perItem and errs are SHARED with the parent rather
// than copied, because that is where each caller's answer is written and there is one answer per
// caller however the batch was split.
func (p *createPlan) subset(idx []int) *createPlan {
	if len(idx) == len(p.owner) {
		return p
	}

	q := &createPlan{perItem: p.perItem, errs: p.errs}

	keep := make(map[string]struct{}, len(idx))

	for k, i := range idx {
		q.idx = append(q.idx, int32(k)) //nolint:gosec // bounded by batch size
		q.leaves = append(q.leaves, p.leaves[i])
		q.txids = append(q.txids, p.txids[i])
		q.heights = append(q.heights, p.heights[i])
		q.offChain = append(q.offChain, p.offChain[i])
		q.membership = append(q.membership, p.membership[i])
		q.sizes = append(q.sizes, p.sizes[i])
		q.inpoints = append(q.inpoints, p.inpoints[i])
		q.locktimes = append(q.locktimes, p.locktimes[i])
		q.createdAt = append(q.createdAt, p.createdAt[i])
		q.txFlags = append(q.txFlags, p.txFlags[i])
		q.bodies = append(q.bodies, p.bodies[i])
		q.minedRows = append(q.minedRows, p.minedRows[i])
		q.minedHeight = append(q.minedHeight, p.minedHeight[i])
		q.blockID = append(q.blockID, p.blockID[i])
		q.subtreeIdx = append(q.subtreeIdx, p.subtreeIdx[i])
		q.lo = append(q.lo, p.lo[i])
		q.hi = append(q.hi, p.hi[i])
		q.owner = append(q.owner, p.owner[i])
		q.txs = append(q.txs, p.txs[i])

		keep[string(p.txids[i])] = struct{}{}
	}

	// The coin arrays are flattened across the batch and tied to their transaction by the
	// txid they already carry, so the projection is a filter on that txid. Every coin of a
	// selected transaction comes across, and no coin of any other.
	for c, id := range p.coinTxids {
		if _, ok := keep[string(id)]; !ok {
			continue
		}

		q.coinSats = append(q.coinSats, p.coinSats[c])
		q.coinHeights = append(q.coinHeights, p.coinHeights[c])
		q.coinSpendable = append(q.coinSpendable, p.coinSpendable[c])
		q.coinLeaves = append(q.coinLeaves, p.coinLeaves[c])
		q.coinFlags = append(q.coinFlags, p.coinFlags[c])
		q.coinUkeys = append(q.coinUkeys, p.coinUkeys[c])
		q.coinTxids = append(q.coinTxids, p.coinTxids[c])
		q.coinScripts = append(q.coinScripts, p.coinScripts[c])
		q.coinMined = append(q.coinMined, p.coinMined[c])
		q.coinBlockIDs = append(q.coinBlockIDs, p.coinBlockIDs[c])
	}

	return q
}

// runCreatePlan issues the claim and tells each caller whether its own claim took.
//
// One batch can carry both kinds of create, because block application and mempool arrivals
// reach the same batcher, so the plan is split by minedRows and each half goes to its own
// statement. A batch of one kind is not projected at all and its arrays are handed straight to
// the one statement that wants them.
func (s *Store) runCreatePlan(ctx context.Context, q querier, p *createPlan) error {
	if len(p.owner) == 0 {
		return nil
	}

	var identIdx, minedIdx []int

	for i, m := range p.minedRows {
		if m {
			minedIdx = append(minedIdx, i)
		} else {
			identIdx = append(identIdx, i)
		}
	}

	if len(identIdx) > 0 {
		if err := s.runIdentPlan(ctx, q, p.subset(identIdx)); err != nil {
			return err
		}
	}

	if len(minedIdx) > 0 {
		if err := s.runMinedPlan(ctx, q, p.subset(minedIdx)); err != nil {
			return err
		}
	}

	return nil
}

// runIdentPlan claims the mempool half of a plan on the identity table.
func (s *Store) runIdentPlan(ctx context.Context, q querier, p *createPlan) error {
	rows, err := q.Query(ctx, createIdentPlanSQL,
		p.idx, p.leaves, p.txids, p.heights, p.offChain, p.membership, p.sizes,
		p.inpoints, p.locktimes, p.createdAt, p.txFlags, p.bodies, p.lo, p.hi,
		p.coinSats, p.coinHeights, p.coinSpendable, p.coinLeaves, p.coinFlags,
		p.coinUkeys, p.coinTxids, p.coinScripts)
	if err != nil {
		return errors.NewStorageError("[utxoset][Create] store", err)
	}

	return p.settle(rows)
}

// runMinedPlan claims the block half of a plan on the membership table.
func (s *Store) runMinedPlan(ctx context.Context, q querier, p *createPlan) error {
	rows, err := q.Query(ctx, createMinedPlanSQL,
		p.idx, p.leaves, p.txids, p.heights, p.minedHeight, p.blockID, p.subtreeIdx,
		p.sizes, p.createdAt, p.txFlags, p.bodies, p.lo, p.hi,
		p.coinSats, p.coinHeights, p.coinSpendable, p.coinMined, p.coinBlockIDs,
		p.coinLeaves, p.coinFlags, p.coinUkeys, p.coinTxids, p.coinScripts)
	if err != nil {
		return errors.NewStorageError("[utxoset][Create] store mined", err)
	}

	return p.settle(rows)
}

// settle reads back which claims took and reports the rest as transactions the store holds.
//
// Shared by both statements: they claim on different tables but each returns the k of every
// transaction it inserted, so the answer a caller gets does not depend on which one ran.
func (p *createPlan) settle(rows pgx.Rows) error {
	claimed := make(map[int32]struct{}, len(p.owner))

	for rows.Next() {
		var k int32

		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][Create] scan", err)
		}

		claimed[k] = struct{}{}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Create] rows", err)
	}

	if len(claimed) == len(p.owner) {
		return nil
	}

	// A transaction the claim did not insert is one the store already holds. The statement
	// wrote nothing at all for it, because its body and its coins were gated on that same
	// claim, so there is nothing to undo.
	for k := range p.owner {
		if _, ok := claimed[int32(k)]; ok { //nolint:gosec // bounded by batch size
			continue
		}

		item := p.owner[k]
		p.perItem[item] = nil
		p.errs[item] = errors.NewTxExistsError("[utxoset][Create] %s", p.txs[k].TxIDChainHash().String())
	}

	return nil
}

// lockTxids takes a transaction-scoped advisory lock per transaction id, in sorted order.
//
// It runs as its OWN statement before the claim. Under read-committed isolation each statement
// takes a fresh snapshot, so a claim that merely held the lock inside its own statement would
// still be looking at a snapshot from before a competitor committed. Blocking here first means
// the claim's snapshot is taken after the lock is granted, which is after the competitor is
// done. Sorted order is what stops two batches deadlocking on each other's ids.
//
// The lock is what makes the block path's three NOT EXISTS guards trustworthy. They read
// tx_mined, tx_ident and utxo, and none of those reads takes a row lock, so two creates of the
// same transaction could each find nothing and each write a full set of coins. The membership
// key catches that only when both name the same block.
func (s *Store) lockTxids(ctx context.Context, q pgx.Tx, txids [][]byte) error {
	if len(txids) == 0 {
		return nil
	}

	keys := make([]int64, 0, len(txids))
	for _, id := range txids {
		keys = append(keys, int64(binary.BigEndian.Uint64(id[:8]))) //nolint:gosec // a hash prefix as a lock key
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	if _, err := q.Exec(ctx, `SELECT pg_advisory_xact_lock(k) FROM unnest($1::bigint[]) AS k`, keys); err != nil {
		return errors.NewStorageError("[utxoset][Create] lock", err)
	}

	return nil
}

// newCreateBatcher wires the create path through the shared batcher, exactly as the sql and
// aerospike stores do.
//
// background is FALSE, which differs from the sql store's create batcher, and the reason is
// specific to this store rather than a disagreement about deadlocks. This decision is
// independent of drain mode versus greedy accumulate below, which only change how items already
// queued for one dispatch are gathered, not whether dispatch itself runs concurrently -- so
// nothing here needed to move when that choice stopped being fixed.
//
// The sql store's create callback issues statements and nothing else, so two batches running
// at once cannot interfere. This one may also run DDL: a create at a height whose body window
// does not exist yet has to make the window first, and a batch can span several. Concurrent
// batches doing DDL against the same parent table, coordinated through a one-entry cache, is
// far more machinery than the win justifies.
//
// It costs nothing that matters here. The batching win is turning N round trips into one, and
// that is unaffected. What background dispatch adds on top is overlapping one batch's database
// work with the next batch's, and block application is a single writer, so there is no second
// batch to overlap with.
//
// It also makes Close honest. The batcher guarantees only that queued items have been HANDED
// TO the callback; with background dispatch the work was still landing afterwards, which
// showed up as one test's batch inserting into tables a later test had already replaced.
//
// Drain mode and greedy accumulate take the same StoreBatcher* settings the spend-and-create
// batcher reads, for the reason recorded on newSpendAndCreateBatcher: forcing drain traded a
// tip-time win against burst load for a fixed per-flush cost paid on every flush, including
// the small ones a sync produces, and which of those a deployment wants is now its own choice
// rather than a constant fixed here.
func newCreateBatcher(s *Store, size int, duration time.Duration, drainMode, greedyAccumulate bool) *batcher.Batcher[createItem] {
	b := batcher.NewWithPool(size, duration, s.sendCreateBatch, false,
		batcher.WithGreedyAccumulate(greedyAccumulate))

	if drainMode {
		b.SetDrainMode(true)
	}

	return b
}

// sendCreateBatch flushes a batch of Creates as one statement.
func (s *Store) sendCreateBatch(batch []*createItem) {
	// Marks this batch as in flight so Close waits for the database work, not merely for the
	// hand-off. See createInFlight on Store.
	s.createInFlight.Add(1)
	defer s.createInFlight.Done()

	ctx := context.Background()

	// Planned outside any connection: this is processor work, and holding one while doing it
	// would be the nested-acquire hazard in a different disguise.
	plan := s.planCreates(batch)

	// The body window has to exist before the statement runs, for the same reason the spend
	// journal's does: the DDL needs its own connection, and taking one while holding a
	// transaction from the same pool deadlocks with no timeout once writers reach the pool
	// limit. A batch can span windows, so every distinct one is prepared.
	seen := make(map[int32]struct{}, 4)

	for _, h := range plan.heights {
		win := h / TxBodyPartitionBlocks
		if _, dup := seen[win]; dup {
			continue
		}

		seen[win] = struct{}{}

		if err := s.ensureTxBodyPartition(ctx, uint32(h)); err != nil { //nolint:gosec // height is non-negative
			s.failBatch(batch, err)
			return
		}
	}

	// The membership window of every distinct block height the batch claims at, for the same
	// reason and with the same before-the-transaction rule.
	minedSeen := make(map[int32]struct{}, 4)

	for i, m := range plan.minedRows {
		if !m {
			continue
		}

		win := plan.minedHeight[i] / TxMinedPartitionBlocks
		if _, dup := minedSeen[win]; dup {
			continue
		}

		minedSeen[win] = struct{}{}

		if err := s.ensureTxMinedPartition(ctx, uint32(plan.minedHeight[i])); err != nil { //nolint:gosec // height is non-negative
			s.failBatch(batch, err)
			return
		}
	}

	// A transaction of its own, where the batch used to run on the pool. The advisory lock the
	// claim depends on is transaction-scoped, so on the pool it would be taken and released
	// within its own statement and would guard nothing.
	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		s.failBatch(batch, errors.NewStorageError("[utxoset][Create] begin", err))
		return
	}

	committed := false

	defer func() {
		if !committed {
			_ = dbTx.Rollback(ctx)
		}
	}()

	if err := s.lockTxids(ctx, dbTx, plan.txids); err != nil {
		s.failBatch(batch, err)
		return
	}

	if err := s.runCreatePlan(ctx, dbTx, plan); err != nil {
		s.failBatch(batch, err)
		return
	}

	if err := dbTx.Commit(ctx); err != nil {
		s.failBatch(batch, errors.NewStorageError("[utxoset][Create] commit", err))
		return
	}

	committed = true

	for i, item := range batch {
		item.done <- createResult{data: plan.perItem[i], err: plan.errs[i]}
	}
}

// failBatch reports one error to every waiter, so a batch-level failure never leaves a
// caller blocked on a channel nobody will write to.
func (s *Store) failBatch(items []*createItem, err error) {
	for _, item := range items {
		item.done <- createResult{err: err}
	}
}
