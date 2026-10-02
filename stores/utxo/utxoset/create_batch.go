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

// createIdentPlanSQL stores a whole BATCH of transactions that take the identity claim in ONE
// statement.
//
// It is the claim for a create that carries no mined-block information, a transaction seen
// before any block contains it, and, since the store applies the checkpoint test itself, for a
// create that carries a block ABOVE the highest checkpoint. Both claim on tx_ident, and their
// UTXOs carry the unconfirmed sentinel, mined_height 0 and block_id 0, until the deep stamp of
// build step 5 writes the block onto them. The block-path counterpart, for a create carrying a
// block at or below the checkpoint, is createMinedPlanSQL.
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
// written. Folding them into one statement fixes both at once: data-modifying common table
// expressions run in a single snapshot, so either all land or none do, and both the body and
// the UTXO inserts are gated on the claim having actually inserted that transaction's row.
//
// The gate is a JOIN against claim rather than an EXISTS. EXISTS asks "did anything in this
// statement claim", which is the right question only when the statement carries one
// transaction. The join asks it per transaction, so a batch mixing fresh transactions with
// ones the store already holds writes bodies and UTXOs for exactly the fresh ones.
//
// The UTXO insert has no conflict clause and needs none. The UTXO key is a non-unique 96-bit
// prefix by design and has nothing to conflict on, so idempotence here comes from the claim
// gate. Without that gate a re-applied block would create every output a second time, which
// is the failure this mechanism exists to prevent.
//
// The claim carries THREE guards, and all three are needed. Its own conflict clause catches a
// transaction the identity table already holds; while the identity row exists that is the
// whole answer, because identity and containment coexist on purpose now and the row stays
// through mining until the stamp deletes it. After the stamp two guards are left against
// writing every output a second time: the tx_mined probe and the own-output UTXO probe. The
// schema comment calls a second set of outputs money-supply inflation, because the UTXO key is
// a non-unique prefix and nothing downstream would catch it.
//
// The tx_mined probe is bounded below by $25, claim_floor, a SCALAR bind parameter, and the
// bound is what keeps the probe's cost flat as the stamp falls behind: without it the probe
// takes one index descent in every attached window for every key, and attached windows grow by
// one for every 288 blocks of stamp lag. Under force_custom_plan the planner sees the scalar as
// a constant and prunes every window below it before taking a lock. See claimFloor for the
// value and why it is deep enough, and why the lookup floor is NOT safe here.
//
// The own-output UTXO probe is the identical statement createMinedPlanSQL carries, LIMIT 1
// OFFSET 0 fence and all, for the identical reason: a bare NOT EXISTS is flattened into an
// anti-join, and the fence keeps it a per-row subplan on the UTXO's packed-key range. The
// tx_mined probe has the same OFFSET 0 fence, because tx_mined's primary key LEADS with txid
// and a hash anti-join over a whole window is the plan the planner otherwise reaches for at
// batch widths: measured at 500 keys against 40,000 containment rows, 9.4 ms.
//
// The mined CTE is the containment row for a block-carrying create above the checkpoint. It is
// UNCONDITIONAL and does nothing on conflict, exactly as the block path's is: whether or not
// the claim takes, the block does contain the transaction, and the caller's follow-up
// record-mined call would insert the same row anyway. It copies its payload from an existing
// identity row when there is one, for the reason createMinedPlanSQL gives. Rows with no block
// have mined_height 0 and insert nothing. A data-modifying CTE runs to completion whether or
// not the outer query reads it.
//
// The lock lockTxids takes before this statement is what makes the guards trustworthy, exactly
// as it does for createMinedPlanSQL's: the reads take no row lock of their own, so two
// concurrent creates of one transaction could otherwise both find nothing.
//
// fee is written as NULL deliberately. The store does not compute it, and block assembly
// rebuilds a mining candidate from size and inpoints instead.
const createIdentPlanSQL = `
WITH t AS (
    SELECT * FROM unnest($1::int[], $2::smallint[], $3::bytea[], $4::int[], $5::int[],
                         $6::int[], $7::bytea[], $8::int[], $9::bigint[],
                         $10::smallint[], $11::bytea[], $12::uuid[], $13::uuid[],
                         $14::int[], $15::int[], $16::int[])
        AS t(k, leaf, txid, created_height, off_chain_since, size_in_bytes,
             tx_inpoints, locktime, created_at, flags, raw_tx, lo, hi,
             mined_height, block_id, subtree_idx)
),
claim AS (
    INSERT INTO tx_ident (leaf, txid, created_height, off_chain_since,
                          fee, size_in_bytes, tx_inpoints, locktime, created_at, flags)
    SELECT t.leaf, t.txid, t.created_height, t.off_chain_since,
           NULL::bigint, t.size_in_bytes, t.tx_inpoints, t.locktime, t.created_at, t.flags
      FROM t
     WHERE NOT EXISTS (SELECT 1 FROM tx_mined m
                        WHERE m.txid = t.txid
                          AND m.mined_height >= $25::int
                        LIMIT 1 OFFSET 0)
       AND NOT EXISTS (SELECT 1 FROM utxo u
                        WHERE u.leaf = t.leaf AND u.ukey >= t.lo AND u.ukey <= t.hi AND u.txid = t.txid
                        ORDER BY u.ukey LIMIT 1 OFFSET 0)
    ON CONFLICT (leaf, txid) DO NOTHING
    RETURNING leaf, txid
),
mined AS (
    INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                          size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
    SELECT t.txid, t.mined_height, t.block_id, t.subtree_idx,
           COALESCE(i.created_height, t.created_height), t.size_in_bytes,
           i.fee, COALESCE(i.tx_inpoints, t.tx_inpoints), COALESCE(i.locktime, t.locktime),
           COALESCE(i.created_at, t.created_at), COALESCE(i.flags, t.flags)
      FROM t
      LEFT JOIN LATERAL (
        SELECT i.created_height, i.fee, i.tx_inpoints, i.locktime, i.created_at, i.flags
          FROM tx_ident i
         WHERE i.leaf = t.leaf AND i.txid = t.txid
        OFFSET 0 ) AS i ON TRUE
     WHERE t.mined_height > 0
    ON CONFLICT (txid, mined_height, block_id) DO NOTHING
),
body AS (
    INSERT INTO tx_body (created_height, txid, raw_tx)
    SELECT t.created_height, t.txid, t.raw_tx
      FROM t
      JOIN claim c ON c.leaf = t.leaf AND c.txid = t.txid
),
UTXOs AS (
    INSERT INTO utxo (satoshis, created_height, spendable_from, mined_height, block_id,
                      leaf, flags, ukey, txid, script)
    SELECT o.satoshis, o.created_height, o.spendable_from, 0, 0,
           o.leaf, o.flags, o.ukey, o.txid, o.script
      FROM unnest($17::bigint[], $18::int[], $19::int[], $20::smallint[], $21::smallint[],
                  $22::uuid[], $23::bytea[], $24::bytea[])
        AS o(satoshis, created_height, spendable_from, leaf, flags, ukey, txid, script)
      JOIN claim c ON c.leaf = o.leaf AND c.txid = o.txid
)
SELECT t.k
  FROM t
  JOIN claim c ON c.leaf = t.leaf AND c.txid = t.txid`

// createMinedPlanSQL is the block-path claim: a create that carries a block at or below the
// highest checkpoint, where the chain is header-proven and the pair written onto the UTXOs at
// birth is final. That is every block-application create during sync, and the seeding tools.
//
// It is SPLIT into two effects with different conditions, because identity and containment
// coexist now.
//
// The containment insert, mined, is UNCONDITIONAL and does nothing on conflict. A transaction
// that already has an identity row still gets its containment row; only the body and the UTXOs
// are held back. When there is an identity row the containment row copies its payload, its
// created_height and its flags from it. The reason is the caller's follow-up: a refused claim
// makes the caller record the transaction mined in a second call, and that call's insert names
// the same key, so it does nothing. If the first insert were thin, the thin row would be the one
// that survives the stamp, and the transaction would lose its fee, its inpoints and the
// created_height that finds its body. The copy is one identity probe per transaction, which the
// claim makes anyway, and below the checkpoint tx_ident is empty so the probe finds nothing.
//
// The claim itself is the join of t against what mined actually inserted, further refused by
// three checks, and all four parts are needed.
//
//   - JOIN mined: the same block re-applied inserts nothing, so it claims nothing.
//   - NOT EXISTS tx_ident (leaf, txid): a transaction seen before its block already holds the
//     UTXOs; the caller's ErrTxExists branch records the block instead of recreating them.
//   - NOT EXISTS utxo in the transaction's own packed-key range: the transaction still has a
//     live UTXO, at any age. This is SV Node's duplicate check and what refuses the two
//     historic duplicate coinbases. Written with the LIMIT 1 OFFSET 0 fence so the planner
//     cannot swap the range scan for a scan of the whole leaf partition.
//   - NOT EXISTS tx_mined at or above $24, claim_floor: another block already contains the
//     transaction and every output has since been spent, so there is no identity row and no
//     live UTXO, and a second block's insert succeeds because the block id differs. Without
//     this the claim takes and the spent outputs are created again. It is ANY height at or
//     above the floor, not the same height, and it needs no self-exclusion: every part of one
//     statement reads the same snapshot, and that snapshot does not contain the row mined has
//     just inserted. See claimFloor for why the floor is deep enough here.
//
// It stays ONE statement and stays behind lockTxids. None of the checks takes a row lock, so
// two concurrent creates of one transaction from two blocks would each find nothing and each
// write a full set of UTXOs, and the containment key no longer stops them because both inserts
// succeed. lockTxids takes a per-txid advisory lock as its own statement first, so the claim's
// snapshot postdates any competitor's commit. It must be one statement because a second
// statement would see the containment row the first one wrote and would never create a UTXO.
//
// tx_inpoints is written NULL, deliberately and for every transaction this statement creates,
// not just for coinbases, when there is no identity row to copy from. Below the checkpoint quick
// validation routes EVERY transaction in a block through this path, so "the block path only
// creates coinbases" is false there. The readers that resolve a transaction's parents from the
// identity record get nothing from a record created here and fall through to the body; with
// utxostore_skipTxBodyBelowCheckpoint on there is no body either, and those readers then fail
// loudly rather than treating a transaction that spends something as one that spends nothing.
// Neither reader runs for a checkpointed block. Storing the inpoints would cost the WAL bytes
// this store is being shaped to avoid, so the trade is intentional.
//
// fee is written NULL for the same reason createIdentPlanSQL writes it NULL: the store never
// computes one, and a coinbase has none to compute.
//
// The body CTE's raw_tx IS NOT NULL is how utxostore_skipTxBodyBelowCheckpoint is applied, and
// it is a filter on the ROW rather than a second statement on purpose. The caller decides per
// transaction by handing NULL instead of the bytes (see appendCreate), so this statement needs
// no knowledge of checkpoints at all.
const createMinedPlanSQL = `
WITH t AS (
    SELECT * FROM unnest($1::int[], $2::smallint[], $3::bytea[], $4::int[], $5::int[],
                         $6::int[], $7::int[], $8::int[], $9::bigint[], $10::smallint[],
                         $11::bytea[], $12::uuid[], $13::uuid[])
        AS t(k, leaf, txid, created_height, mined_height, block_id, subtree_idx,
             size_in_bytes, created_at, flags, raw_tx, lo, hi)
),
mined AS (
    INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                          size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
    SELECT t.txid, t.mined_height, t.block_id, t.subtree_idx,
           COALESCE(i.created_height, t.created_height), t.size_in_bytes,
           i.fee, i.tx_inpoints, i.locktime,
           COALESCE(i.created_at, t.created_at), COALESCE(i.flags, t.flags)
      FROM t
      LEFT JOIN LATERAL (
        SELECT i.created_height, i.fee, i.tx_inpoints, i.locktime, i.created_at, i.flags
          FROM tx_ident i
         WHERE i.leaf = t.leaf AND i.txid = t.txid
        OFFSET 0 ) AS i ON TRUE
    ON CONFLICT (txid, mined_height, block_id) DO NOTHING
    RETURNING txid
),
claim AS (
    SELECT t.k, t.txid
      FROM t
      JOIN mined x ON x.txid = t.txid
     WHERE NOT EXISTS (SELECT 1 FROM tx_ident i WHERE i.leaf = t.leaf AND i.txid = t.txid)
       AND NOT EXISTS (SELECT 1 FROM utxo u
                        WHERE u.leaf = t.leaf AND u.ukey >= t.lo AND u.ukey <= t.hi AND u.txid = t.txid
                        ORDER BY u.ukey LIMIT 1 OFFSET 0)
       AND NOT EXISTS (SELECT 1 FROM tx_mined m
                        WHERE m.txid = t.txid
                          AND m.mined_height >= $24::int
                        LIMIT 1 OFFSET 0)
),
body AS (
    INSERT INTO tx_body (created_height, txid, raw_tx)
    SELECT t.created_height, t.txid, t.raw_tx
      FROM t JOIN claim c ON c.txid = t.txid
     WHERE t.raw_tx IS NOT NULL
),
UTXOs AS (
    INSERT INTO utxo (satoshis, created_height, spendable_from, mined_height, block_id,
                      leaf, flags, ukey, txid, script)
    SELECT o.satoshis, o.created_height, o.spendable_from, o.mined_height, o.block_id,
           o.leaf, o.flags, o.ukey, o.txid, o.script
      FROM unnest($14::bigint[], $15::int[], $16::int[], $17::int[], $18::int[],
                  $19::smallint[], $20::smallint[], $21::uuid[], $22::bytea[], $23::bytea[])
        AS o(satoshis, created_height, spendable_from, mined_height, block_id, leaf, flags, ukey, txid, script)
      JOIN claim c ON c.txid = o.txid
)
SELECT k FROM claim`

// claimReach is how far below the store's height the create claims' containment probe reads,
// in blocks: 2,016, which is seven 288-block windows.
//
// The danger the probe guards against is a fully spent transaction being re-offered as
// unmined and created a second time. For that its spend phase has to pass as a replay of its
// own spends, which needs the undo copies of the outputs it spent to still exist; those are
// due to drop 1,728 blocks after the spend, and the spend is at or after the height the
// transaction was mined at. So a transaction mined more than 2,016 blocks ago has had its undo
// copies due for at least 288 blocks. The lookup floor of 576 is NOT safe here: a transaction
// mined between 576 and 1,727 blocks ago, fully spent since, would pass the spend phase as a
// self-replay and be out of the lookup floor's sight, so the claim would take.
const claimReach = 2_016

// claimFloor is the lower bound, a height, of the create claims' containment probe: a scalar
// bind parameter, never a join variable, so the planner prunes every window below it at plan
// time.
//
// It is the lower of two values. The first is the store's height less claimReach, aligned
// down to a window edge; H is the height the node last gave the store, and a stale H only
// lowers the floor, which reads more windows and never fewer. The second is the window edge at
// or below the first height of the oldest attached undo partition. That second bound is what
// makes the guard sound however late the undo drops run: a replay needs its undo copies, the
// undo copies live in the attached undo partitions, so a containment row the probe must see
// can be no lower than the oldest of them. With no undo partition attached there is nothing to
// replay and the fixed reach alone stands. A floor below zero is held at zero.
func (s *Store) claimFloor() int32 {
	var floor uint32

	if h := s.GetBlockHeight(); h > claimReach {
		floor = (h - claimReach) / TxMinedPartitionBlocks * TxMinedPartitionBlocks
	}

	if oldest := s.oldestUndoLeaf.Load(); oldest > 0 {
		edge := (oldest - 1) * SpendJournalPartitionBlocks / TxMinedPartitionBlocks * TxMinedPartitionBlocks
		if edge < floor {
			floor = edge
		}
	}

	return int32(floor) //nolint:gosec // a height fits int32
}

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
// Two array widths live here. The identity fields carry one element per transaction. The UTXO
// fields carry one element per SPENDABLE output, flattened across every transaction in the
// batch, and they are tied back to their transaction by the leaf and txid they already carry
// rather than by a separate mapping.
type createPlan struct {
	// One element per transaction that made it into the statement.
	idx       []int32
	leaves    []int16
	txids     [][]byte
	heights   []int32
	offChain  []*int32
	sizes     []int32
	inpoints  [][]byte
	locktimes []int32
	createdAt []int64
	txFlags   []int16
	bodies    [][]byte
	// minedRows is true for a transaction that carries a block at or below the highest
	// checkpoint, which is what sends it to createMinedPlanSQL instead of createIdentPlanSQL.
	// The three fields below carry the block for every transaction that carries one, whichever
	// claim it takes, and are 0 for a create with no block.
	minedRows   []bool
	minedHeight []int32
	blockID     []int32
	subtreeIdx  []int32
	// The packed-key range of the transaction's own outputs, so the block path can ask
	// whether it still has a live UTXO without scanning its leaf partition.
	lo, hi [][16]byte

	// One element per spendable output, across the whole batch.
	utxoSats      []int64
	utxoHeights   []int32
	utxoSpendable []int32
	utxoLeaves    []int16
	utxoFlags     []int16
	utxoUkeys     [][16]byte
	utxoTxids     [][]byte
	utxoScripts   [][]byte
	// The block facts, repeated per UTXO, so the UTXO row knows its block without a join.
	// Both are 0 for a mempool create: mined_height 0 is the unconfirmed sentinel.
	utxoMined    []int32
	utxoBlockIDs []int32

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
// wait on each other's speculative inserts and deadlock, and in one order they cannot. The UTXO
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

// subset projects the plan onto the given transaction rows, carrying each row's UTXOs with it.
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

	// The UTXO arrays are flattened across the batch and tied to their transaction by the
	// txid they already carry, so the projection is a filter on that txid. Every UTXO of a
	// selected transaction comes across, and no UTXO of any other.
	for c, id := range p.utxoTxids {
		if _, ok := keep[string(id)]; !ok {
			continue
		}

		q.utxoSats = append(q.utxoSats, p.utxoSats[c])
		q.utxoHeights = append(q.utxoHeights, p.utxoHeights[c])
		q.utxoSpendable = append(q.utxoSpendable, p.utxoSpendable[c])
		q.utxoLeaves = append(q.utxoLeaves, p.utxoLeaves[c])
		q.utxoFlags = append(q.utxoFlags, p.utxoFlags[c])
		q.utxoUkeys = append(q.utxoUkeys, p.utxoUkeys[c])
		q.utxoTxids = append(q.utxoTxids, p.utxoTxids[c])
		q.utxoScripts = append(q.utxoScripts, p.utxoScripts[c])
		q.utxoMined = append(q.utxoMined, p.utxoMined[c])
		q.utxoBlockIDs = append(q.utxoBlockIDs, p.utxoBlockIDs[c])
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

// runIdentPlan claims the identity half of a plan on the identity table.
func (s *Store) runIdentPlan(ctx context.Context, q querier, p *createPlan) error {
	rows, err := q.Query(ctx, createIdentPlanSQL,
		p.idx, p.leaves, p.txids, p.heights, p.offChain, p.sizes,
		p.inpoints, p.locktimes, p.createdAt, p.txFlags, p.bodies, p.lo, p.hi,
		p.minedHeight, p.blockID, p.subtreeIdx,
		p.utxoSats, p.utxoHeights, p.utxoSpendable, p.utxoLeaves, p.utxoFlags,
		p.utxoUkeys, p.utxoTxids, p.utxoScripts, s.claimFloor())
	if err != nil {
		return errors.NewStorageError("[utxoset][Create] store", err)
	}

	return p.settle(rows)
}

// runMinedPlan claims the block-path half of a plan on the containment table.
//
// It takes the fence lock shared first. The per-transaction locks of lockTxids are already
// held by then, and that order cannot deadlock: the stamp's exclusive holder takes no
// per-transaction lock, and shared holders do not block each other. Below the fence the batch
// is judged per block by the fenced rule before the claim runs: every row present means a
// re-offered block, which the claim then refuses with ErrTxExists as it does above the fence;
// any row absent is the boundary error, because a create there would add a row where every
// row is a winner. A dropped window is refused as it always was.
func (s *Store) runMinedPlan(ctx context.Context, q querier, p *createPlan) error {
	dbTx, ok := q.(pgx.Tx)
	if !ok {
		return errors.NewProcessingError("[utxoset][Create] the block-path claim needs a transaction for the fence lock")
	}

	fence, err := s.takeFenceShared(ctx, dbTx)
	if err != nil {
		return err
	}

	if err := s.judgeFencedCreates(ctx, dbTx, p, fence); err != nil {
		return err
	}

	s.countCreatesAheadOfTip(p)

	rows, err := q.Query(ctx, createMinedPlanSQL,
		p.idx, p.leaves, p.txids, p.heights, p.minedHeight, p.blockID, p.subtreeIdx,
		p.sizes, p.createdAt, p.txFlags, p.bodies, p.lo, p.hi,
		p.utxoSats, p.utxoHeights, p.utxoSpendable, p.utxoMined, p.utxoBlockIDs,
		p.utxoLeaves, p.utxoFlags, p.utxoUkeys, p.utxoTxids, p.utxoScripts, s.claimFloor())
	if err != nil {
		return errors.NewStorageError("[utxoset][Create] store mined", err)
	}

	return p.settle(rows)
}

// lookaheadBlocks is how far above the store's own height a block may be applied under the drop
// rule's premise. The window drop waits one undo partition, 288 blocks, of margin past the tip
// the stamp completed at, so that a spend journalled above that tip cannot leave a (0,0) undo
// copy the window no longer covers. The margin holds while no block is applied more than 287
// heights ahead of the tip the store was last told. A store instance cannot see a block another
// process is applying, so the premise is not enforced; it is counted, and a non-zero count is
// the signal that the margin needs revisiting.
const lookaheadBlocks = 287

// countCreatesAheadOfTip is decision 1's counter: every block-path create whose block is more
// than lookaheadBlocks above the store's height, counted once per block in the plan and logged.
func (s *Store) countCreatesAheadOfTip(p *createPlan) {
	h := s.GetBlockHeight()
	if h == 0 {
		return
	}

	seen := map[int32]struct{}{}

	for i := range p.minedHeight {
		mh := uint32(p.minedHeight[i]) //nolint:gosec // a height is never negative
		if mh <= h+lookaheadBlocks {
			continue
		}

		if _, dup := seen[p.minedHeight[i]]; dup {
			continue
		}

		seen[p.minedHeight[i]] = struct{}{}
		createAheadOfTip.Inc()
		s.logger.Warnf("[utxoset][Create] block %d at height %d is being applied %d heights ahead of the store's height %d, past the %d the window drop rule assumes", p.blockID[i], mh, mh-h, h, lookaheadBlocks)
	}
}

// judgeFencedCreates applies the fenced rule to every block of the plan whose height is below
// the fence. A batch can carry several blocks, so the count is taken per (height, block id).
func (s *Store) judgeFencedCreates(ctx context.Context, q querier, p *createPlan, fence fenceState) error {
	type blockKey struct{ height, block int32 }

	groups := map[blockKey][][]byte{}

	for i := range p.txids {
		h := uint32(p.minedHeight[i]) //nolint:gosec // a height is never negative
		if h >= fence.fence {
			continue
		}

		if fence.dropped(h) {
			return boundaryError("create", "block %d at height %d is in a dropped window; re-creating it would claim every transaction in it afresh",
				p.blockID[i], p.minedHeight[i])
		}

		k := blockKey{p.minedHeight[i], p.blockID[i]}
		groups[k] = append(groups[k], p.txids[i])
	}

	for k, txids := range groups {
		n, err := fencedRowCount(ctx, q, txids, uint32(k.height), uint32(k.block)) //nolint:gosec // heights and ids fit
		if err != nil {
			return err
		}

		if n != int64(len(txids)) {
			return boundaryError("create", "block %d at height %d is below the stamp fence %d and %d of its %d transactions have no containment row; a create there would add a row where every row is a winner",
				k.block, k.height, fence.fence, int64(len(txids))-n, len(txids))
		}
	}

	return nil
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
	// wrote nothing at all for it, because its body and its UTXOs were gated on that same
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
// same transaction could each find nothing and each write a full set of UTXOs. The containment
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

	// The containment window of every distinct block height the batch records, whichever
	// claim the row takes, for the same reason and with the same before-the-transaction rule.
	minedSeen := make(map[int32]struct{}, 4)

	for i, h := range plan.minedHeight {
		if h == 0 {
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
