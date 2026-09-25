package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/jackc/pgx/v5"
)

// The read order is the identity row AND containment by transaction id together, then the
// UTXO, then the undo copy, then the second containment tier for the transactions one of three
// triggers sends there, then the preserved parent LAST. The order is a correctness rule rather
// than a tuning.
//
// Identity and containment are read together, not one after the other, because they coexist:
// a transaction seen before its block keeps its identity row through mining until the stamp
// deletes it, and the identity row carries no block list of its own. So the identity row
// supplies the payload and the unmined marker, the containment rows supply the block ids,
// heights and subtree indexes in (mined_height, block_id) order, and a transaction that has both
// is answered from both.
//
// The first containment read carries a height floor, the lookup floor: the stamp depth plus one
// window below the store's height, aligned down to a window edge. Every unstamped transaction
// sits inside it when the stamp is on time, with one window of slack, and the floor is what
// keeps the read at the same three or four partitions however far behind the stamp falls. What
// the floor can miss, the second tier reads: a transaction with an identity row that believes
// itself mined and no containment in the first tier, a UTXO at (0,0) with no identity row, or an
// undo copy at (0,0) that nothing else answered for. Each of those means containment exists
// below the floor, or nothing can ever stamp the transaction, and the tier reads every attached
// window in ONE snapshot so that a create committing between two of the earlier statements
// cannot be mistaken for corruption.
//
// A UTXO holds ONE block id, and on the ordinary two-step reorg (a fork block recorded as not
// on the longest chain, then a later block making it the main chain) nothing rewrites the UTXOs
// of transactions shared between the two blocks. A UTXO-first read would then hand block
// validation an id that lost, and the parent check stores a valid block as invalid. The
// containment table holds every id while the window lives, so the UTXO is consulted only for a
// transaction with no identity row and no containment in the first tier, and a non-zero pair on
// it is final: the stamp wrote it 288 blocks deep, or the block path wrote it below the
// checkpoint.
//
// The spend journal comes after the UTXO, and that order is the same rule again. A live UTXO is
// the settled record of a transaction that still exists; the journal row is a copy taken off a
// UTXO that has since been destroyed. Ask the journal first and a transaction with one output
// spent and one still live would be answered from the spent one, which is a copy where a record
// was available. Ask it last and it is reached only for a transaction with no live UTXO at all,
// which is the case it exists for: the FULLY-SPENT parent mined more than the lookup reach ago,
// which model/Block.go's checkParentTransactions asks about on most blocks above the highest
// checkpoint. Past both the containment window and the journal's retention the transaction is
// genuinely gone and the store reports not-found, which is what aerospike's delete-at-height
// does and what the shared suite's pruning test requires.
//
// The preserved parent is read LAST. It answers from a COPY of a containment row, taken while
// the row was still there, and a copy taken under the interim rule of an earlier build could
// name the block that lost a reorg. Read last, it can never outrank a UTXO or an undo copy
// carrying a correct pair, and it answers only for the one population it exists for: a fully
// spent parent whose window is gone and whose child is still unmined.

// identByTxidSQL reads the identity rows for a set of transactions, joining each body only if
// it is still inside its window.
//
// The join is LEFT, and that is the point rather than caution. The body window is dropped
// after 288 blocks while the identity row lives for as long as any of the transaction's
// outputs is unspent, at any age, so a body-less row is the ordinary steady state for an old
// transaction. An inner join would report every such transaction as missing, and a missing
// parent makes the validator reject its children.
//
// The body's height comes from the identity row, which is why created_height is immutable
// there: if it moved, the body could not be found.
//
// THE LEAF IS A SCALAR AND THE TXIDS AN ARRAY, so this runs once per leaf group. That is the
// shape leafGroups (set_mined.go) measures and the reason it exists: the paired
// `unnest(l[],t[]) JOIN tx_ident` form this statement used to carry is the one whose plan
// FLIPS with statistics -- a hash of the whole mempool at mempool sizes, index probes only once
// the table is far larger than a mempool ever is. Measured on this schema, 500 keys over eight
// leaves against 40,000 identity rows, eight runs each: the paired form 7.38-7.69 ms for the
// batch, the leaf-scalar form 0.29-0.36 ms per group, so about 2.7 ms for the same 500 keys.
// This is step 1 of every read, on the validator's parent-resolution path.
//
// The leaf is redundant as a FILTER and load-bearing as an ACCESS PATH, exactly as it is in
// moveToMinedSQL: txid is the full 32 bytes and tx_ident_ck makes leaf a function of it, so no
// row can satisfy the txid qual under another leaf. What the scalar buys is partition pruning
// to one leaf and a usable primary key, since txid is its second column.
const identByTxidSQL = `
SELECT i.txid, i.created_height, i.off_chain_since, i.fee, i.size_in_bytes,
       i.tx_inpoints, i.locktime, i.created_at, i.flags, b.raw_tx
  FROM tx_ident i
  LEFT JOIN tx_body b ON b.created_height = i.created_height AND b.txid = i.txid
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])`

// minedByTxidSQL reads every containment row for a set of transactions at or above a height
// floor, $2, in (mined_height, block_id) order. The floor is a scalar bind parameter, never a
// join variable, so under force_custom_plan the planner leaves out every partition below it
// before it takes a lock: the read costs the same three or four partitions at any stamp lag. The primary key leads with txid, so this is one
// descent per window per transaction, and for one transaction id that order is the key's own,
// so no sort step is needed inside a window.
//
// The order replaces the insertion counter the table used to carry. No correctness rule rests
// on it, because no reader may treat the first row as the winner; it exists so that results
// are repeatable, and the shared conformance suite's two-entry assertions hold under it.
//
// The keys sit on the OUTSIDE of a LATERAL with an OFFSET 0 fence, and that is the difference
// between one descent per key and a read of every live window. Written as the plain
// `JOIN tx_mined m ON m.txid = k.txid` the planner is free to hash-join the keys against the
// whole partitioned table, and it does: measured on this schema at 40,000 transactions across
// six windows, 500 keys took 9.9 ms and the plan carried a Seq Scan on every one of the six
// windows, against 3.4 ms and none for the lateral. At 400,000 the planner happened to choose
// index scans for both, which is exactly why the fence has to be in the statement rather than
// left to the estimate: the hash-join cost grows with the size of the live membership set,
// which at 1440 blocks of mainnet is millions of rows, and this read is on the validator's
// parent-resolution path.
//
// OFFSET 0 is the fence itself, the same one utxoFactsSQL relies on: it stops the planner
// pulling the subquery up into the outer join, which is what re-admits the hash join. The
// inner ORDER BY walks the primary key in order; the outer one is what actually guarantees the
// grouping the reader relies on, because the LEFT JOIN to the body may reorder rows.
const minedByTxidSQL = `
SELECT k.txid, m.mined_height, m.block_id, m.subtree_idx, m.size_in_bytes, m.fee,
       m.tx_inpoints, m.locktime, m.created_at, m.flags, b.raw_tx
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT m.mined_height, m.block_id, m.subtree_idx, m.created_height, m.size_in_bytes,
          m.fee, m.tx_inpoints, m.locktime, m.created_at, m.flags
     FROM tx_mined m
    WHERE m.txid = k.txid
      AND m.mined_height >= $2::int
    ORDER BY m.mined_height, m.block_id
   OFFSET 0
 ) AS m
  LEFT JOIN tx_body b ON b.created_height = m.created_height AND b.txid = k.txid
 ORDER BY k.txid, m.mined_height, m.block_id`

// preservedByTxidSQL reads the preserved copies of a set of transactions' membership rows,
// joining each body only if it is still inside its window.
//
// The column list and its order match minedByTxidSQL's exactly, because both feed minedRow: a
// preserved parent has to answer what its membership row would have answered, and two readers
// scanning the same struct in two orders is a bug that compiles.
//
// The keys sit on the OUTSIDE of a LATERAL with an OFFSET 0 fence, and this one was measured
// BOTH ways because the obvious form looked safe and was not. This table is unpartitioned and
// keyed by transaction id, so `WHERE p.txid = ANY($1)` reads like a primary-key probe -- and
// the planner turns it into a Seq Scan with the array as a filter, because it has no
// statistics for an array and a small table is cheap to read whole. Measured at 500 keys
// against 40,000 preserved rows: 2.4 ms and a Seq Scan, against 0.59 ms and one primary-key
// descent per key for the fenced form, and the scan's cost grows with the table while the
// descent's does not. The table is meant to stay small, but "meant to" is not a plan, and this
// read is on the validator's parent-resolution path.
const preservedByTxidSQL = `
SELECT k.txid, p.mined_height, p.block_id, p.subtree_idx, p.size_in_bytes, p.fee,
       p.tx_inpoints, p.locktime, p.created_at, p.flags, b.raw_tx
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT p.mined_height, p.block_id, p.subtree_idx, p.created_height, p.size_in_bytes,
          p.fee, p.tx_inpoints, p.locktime, p.created_at, p.flags
     FROM preserved_parent p
    WHERE p.txid = k.txid
   OFFSET 0
 ) AS p
  LEFT JOIN tx_body b ON b.created_height = p.created_height AND b.txid = k.txid`

// utxoFactsSQL reads one live UTXO per transaction, for the transactions nothing else knows.
// The LATERAL with ORDER BY and LIMIT 1 OFFSET 0 is the fence the planner needs to walk the
// packed-key index instead of scanning the UTXO table, and each half of it was measured:
// without the packed-key range bound the planner reads every leaf partition, and without the
// ORDER BY it materialises the whole range before the LIMIT can stop it. createMinedPlanSQL's
// duplicate-UTXO guard carries the identical fence, for the identical reason.
const utxoFactsSQL = `
SELECT k.txid, hit.mined_height, hit.block_id, hit.flags, b.raw_tx
  FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[]) AS k(leaf, txid, lo, hi)
 CROSS JOIN LATERAL (
   SELECT u.mined_height, u.block_id, u.created_height, u.flags
     FROM utxo u
    WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND u.ukey <= k.hi AND u.txid = k.txid
    ORDER BY u.ukey LIMIT 1 OFFSET 0
 ) AS hit
  LEFT JOIN tx_body b ON b.created_height = hit.created_height AND b.txid = k.txid`

// spentParentFactsSQL reads ONE journal row per transaction, for a fully-spent parent whose
// membership window has already retired. It is the last thing the store can say about a
// transaction before not-found.
//
// The shape is utxoFactsSQL's, and for the same reasons. The keys sit on the OUTSIDE of a
// LATERAL with the ORDER BY / LIMIT 1 / OFFSET 0 fence: OFFSET 0 stops the planner pulling the
// subquery up into the outer join, which is what would re-admit a hash join against every live
// leaf; the packed-key range bound is what makes it a range scan rather than a read of the
// whole leaf; and the ORDER BY is what lets the LIMIT stop the scan instead of materialising
// the range first. ukey is the journal leaf's only index, so this is one range probe per leaf
// per key.
//
// The spend height is not known to the reader -- that is the whole point of the step, the
// caller is asking about a parent it has lost track of -- so there is no partition bound and
// every live leaf is probed. At the journal's 1440-block retention in 288-block leaves that is
// 6 leaves, and 500 keys is therefore 3,000 index descents. Measured on this schema when the
// leaves were 48 blocks wide, 500 keys against 39,990 journal rows across 30 leaves, eight
// runs: 7.4-9.2 ms, an Index Scan on every leaf's ukey index and no Seq Scan on any of them,
// flat across all eight; at six leaves the same shape does a fifth of the descents. That is the
// price of the step and it is worth knowing before the soak, because above the highest
// checkpoint most out-of-block parents reach it.
//
// The full 32-byte txid recheck is not optional. ukey is a non-unique 96-bit prefix by design,
// so the range locates candidates and only the txid establishes identity.
//
// mined_height > 0 filters out the unconfirmed sentinel. A mempool parent's spend journals no
// block, and reporting block id 0 for it would be a lie block validation cannot tell from
// genesis, whose id really is 0. A mempool parent is answered by its identity row at step 1
// anyway; this is the belt to that braces.
//
// Any output of the transaction will do, so there is no preference between the rows a
// multi-output transaction left behind: every one of them was stamped with the same block
// facts, by the block path at create or by window retirement, before any of them was spent.
const spentParentFactsSQL = `
SELECT k.txid, hit.mined_height, hit.block_id, hit.flags, b.raw_tx
  FROM unnest($1::bytea[], $2::uuid[], $3::uuid[]) AS k(txid, lo, hi)
 CROSS JOIN LATERAL (
   SELECT j.mined_height, j.block_id, j.created_height, j.flags
     FROM spend_journal j
    WHERE j.ukey >= k.lo AND j.ukey <= k.hi AND j.txid = k.txid AND j.mined_height > 0
    ORDER BY j.ukey LIMIT 1 OFFSET 0
 ) AS hit
  LEFT JOIN tx_body b ON b.created_height = hit.created_height AND b.txid = k.txid`

// lookupResult is one read of the store: what it found, and what it could not make sense of.
//
// The two maps are separate because a transaction the store HOLDS but cannot decode is not the
// same answer as one it does not hold, and the difference decides what the caller does. A miss
// makes the validator reject a child for a missing parent, which is recoverable and correct. A
// corrupt row is a storage fault on that one transaction, and it belongs on that transaction's
// own entry rather than on the whole batch: BatchDecorate's contract is that a transaction the
// store cannot serve is reported on ITS OWN entry, so one unreadable tx_inpoints must not
// reject every transaction that happened to travel with it.
type lookupResult struct {
	found  map[chainhash.Hash]*meta.Data
	failed map[chainhash.Hash]error
}

func newLookupResult(n int) lookupResult {
	return lookupResult{found: make(map[chainhash.Hash]*meta.Data, n), failed: nil}
}

// fail records a per-transaction fault. The hash counts as RESOLVED from here on, which is the
// point: a corrupt identity row must not fall through to the membership table or the UTXO, or
// the store would answer from a UTXO for a transaction whose real record it just refused to
// read, silently substituting a thinner answer for a fault.
func (r *lookupResult) fail(h chainhash.Hash, err error) {
	if r.failed == nil {
		r.failed = map[chainhash.Hash]error{}
	}

	r.failed[h] = err
}

// resolved reports whether any step has already answered for this hash, either way.
func (r *lookupResult) resolved(h chainhash.Hash) bool {
	if _, ok := r.found[h]; ok {
		return true
	}

	_, ok := r.failed[h]

	return ok
}

// lookupReach is how far below the store's height the first containment read reaches before
// the floor is aligned down to a window edge: the stamp depth plus one window, so a stamp up to
// one window late changes nothing a reader can see.
func (s *Store) lookupReach() uint32 { return s.stampDepth + TxMinedPartitionBlocks }

// lookupFloor is the lower bound, a height, of the first containment read. H is the store's own
// height, the value the node last gave it; a stale H only lowers the floor, which reads more
// partitions and never fewer. A floor below zero is held at zero.
func (s *Store) lookupFloor() int32 {
	h := s.GetBlockHeight()
	if h <= s.lookupReach() {
		return 0
	}

	return int32((h - s.lookupReach()) / TxMinedPartitionBlocks * TxMinedPartitionBlocks) //nolint:gosec // a height fits int32
}

// lookupMany resolves a set of transactions in the read order. Misses are absent from both
// maps; a transaction whose stored row will not decode lands in failed rather than found.
//
// The identity read and the first containment read are issued for EVERY distinct hash, because
// the two coexist and each supplies half the answer (see the read order above). Every later
// step asks only about the hashes nothing before it could answer.
//
// The returned error is for faults that are NOT per-transaction: a dead connection, a syntax
// error, a partition that vanished mid-read. Those really do fail every entry, because nothing
// was answered.
func (s *Store) lookupMany(ctx context.Context, hashes []chainhash.Hash,
	wantChildren bool) (lookupResult, error) {
	res := newLookupResult(len(hashes))
	if len(hashes) == 0 {
		return res, nil
	}

	// Step 1: the identity table and the first containment tier, for every DISTINCT hash. A
	// batch can name the same parent twice, and asking twice would return the row twice and
	// waste the round trip this call exists to save.
	uniq := make([]chainhash.Hash, 0, len(hashes))
	seen := make(map[chainhash.Hash]struct{}, len(hashes))
	txids := make([][]byte, 0, len(hashes))

	for i := range hashes {
		if _, dup := seen[hashes[i]]; dup {
			continue
		}

		seen[hashes[i]] = struct{}{}

		uniq = append(uniq, hashes[i])
		txids = append(txids, hashes[i][:])
	}

	if err := s.readIdentRows(ctx, txids, &res); err != nil {
		return lookupResult{}, err
	}

	if err := s.readMinedInto(ctx, uniq, &res, s.lookupFloor()); err != nil {
		return lookupResult{}, err
	}

	// Trigger 2 of the second tier: an identity row with the marker clear and no containment
	// in the first tier. The store believes the transaction is mined and cannot see where.
	var tier2 []chainhash.Hash

	for _, h := range uniq {
		d, ok := res.found[h]
		if !ok || len(d.BlockIDs) > 0 || d.UnminedSince != 0 {
			continue
		}

		tier2 = append(tier2, h)
		lookupTier2Keys.WithLabelValues("ident_marker_null").Inc()
	}

	// Step 2: the UTXO, for a transaction with no identity row and no containment in the first
	// tier. A non-zero pair answers. A UTXO at (0,0) is trigger 3.
	rest := stillMissing(uniq, &res)

	if len(rest) > 0 {
		zero, err := s.readUTXOFacts(ctx, rest, &res)
		if err != nil {
			return lookupResult{}, err
		}

		for range zero {
			lookupTier2Keys.WithLabelValues("utxo_zero").Inc()
		}

		tier2 = append(tier2, zero...)
		rest = stillMissing(rest, &res)
		rest = without(rest, zero)
	}

	// Step 3: the undo copies, for a fully spent transaction. A copy with a non-zero pair
	// answers. A copy at (0,0) that nothing else answered for is trigger 4.
	if len(rest) > 0 {
		if err := s.readSpentParents(ctx, rest, &res); err != nil {
			return lookupResult{}, err
		}

		rest = stillMissing(rest, &res)
	}

	if len(rest) > 0 {
		zero, err := s.probeZeroUndoCopies(ctx, rest)
		if err != nil {
			return lookupResult{}, err
		}

		for range zero {
			lookupTier2Keys.WithLabelValues("undo_zero").Inc()
		}

		tier2 = append(tier2, zero...)
		rest = without(rest, zero)
	}

	// Step 4: the second tier, one snapshot per leaf group.
	if len(tier2) > 0 {
		if err := s.readTier2(ctx, tier2, &res); err != nil {
			return lookupResult{}, err
		}
	}

	// Step 5: the preserved parent, last, for what is still unanswered.
	if len(rest) > 0 {
		if err := s.readPreserved(ctx, rest, &res); err != nil {
			return lookupResult{}, err
		}
	}

	// The contest, if the caller asked for it, for every transaction ANY step answered. A
	// mined parent is contested exactly as a mempool one is, so this cannot be folded into
	// the identity read: the parents that matter most are the ones that left it.
	if wantChildren {
		if err := s.attachConflictingChildren(ctx, uniq, &res); err != nil {
			return lookupResult{}, err
		}
	}

	return res, nil
}

// without returns hashes with every member of drop removed, order kept.
func without(hashes, drop []chainhash.Hash) []chainhash.Hash {
	if len(drop) == 0 {
		return hashes
	}

	skip := make(map[chainhash.Hash]struct{}, len(drop))
	for _, h := range drop {
		skip[h] = struct{}{}
	}

	out := hashes[:0:0]

	for _, h := range hashes {
		if _, ok := skip[h]; !ok {
			out = append(out, h)
		}
	}

	return out
}

// conflictChildrenSQL names the transactions recorded as contesting each of these
// transactions' UTXOs.
//
// One statement for the whole batch, keyed on the parent's transaction id alone. That is what
// makes it answer for a parent in the identity table, a parent in the membership table, a
// parent whose membership row survives only as a preservation copy, and a parent this store
// knows only from a live UTXO -- the packed column it replaces could only ever answer for the
// first of the four.
//
// DISTINCT because the uniqueness underneath is PER WINDOW: a unique index on a partitioned
// table must include the partition key, so the same (parent, child) pair noted in two windows
// is two legal rows. See the schema comment on conflict_children.
//
// The keys sit on the OUTSIDE of a LATERAL with an OFFSET 0 fence, which is the same shape and
// the same reason as minedByTxidSQL. Written as the plain `WHERE c.parent_txid = ANY($1)` the
// planner has no statistics for an array and guesses a sixth of each window, so it seq-scans
// every live window: measured at 500 keys against 40,000 rows in six windows, 2.3 ms with a
// Seq Scan on all six. OFFSET 0 is the fence itself -- it stops the subquery being pulled up
// into the outer join, which is what re-admits the scan -- and each window's unique index,
// which parent_txid leads, then gives one descent per key per window: 2.7 ms, flat across
// eight executions.
const conflictChildrenSQL = `
SELECT DISTINCT k.parent, hit.child_txid
  FROM unnest($1::bytea[]) AS k(parent)
 CROSS JOIN LATERAL (
   SELECT c.child_txid
     FROM conflict_children c
    WHERE c.parent_txid = k.parent
   OFFSET 0
 ) AS hit`

// attachConflictingChildren fills in the contest on every transaction the read found.
//
// Asked for rather than always run, and it is the second field on this store that works that
// way: everything else a metadata read returns arrives on the row that answered, so narrowing
// a projection would save nothing, while this costs a statement of its own. The shared
// conflict walks name fields.ConflictingChildren when they need it, and the validator's
// parent resolution never does.
//
// A transaction with no contest gets a nil slice rather than an empty one, which is what a
// caller reading "no conflicting children" already expects from every other store.
func (s *Store) attachConflictingChildren(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult) error {
	if len(res.found) == 0 {
		return nil
	}

	parents := make([][]byte, 0, len(res.found))

	for i := range hashes {
		if _, ok := res.found[hashes[i]]; ok {
			parents = append(parents, hashes[i][:])
		}
	}

	if len(parents) == 0 {
		return nil
	}

	rows, err := s.pool.Query(ctx, conflictChildrenSQL, parents)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] conflicting children", err)
	}

	defer rows.Close()

	for rows.Next() {
		var parent, child []byte

		if err := rows.Scan(&parent, &child); err != nil {
			return errors.NewStorageError("[utxoset][lookup] conflicting children scan", err)
		}

		var p, c chainhash.Hash

		copy(p[:], parent)
		copy(c[:], child)

		data := res.found[p]
		if data == nil {
			continue
		}

		data.ConflictingChildren = append(data.ConflictingChildren, c)
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][lookup] conflicting children", err)
	}

	return nil
}

// stillMissing returns the hashes no step so far has answered.
//
// Named for the shadowing it avoids: MarkTransactionsOnLongestChain has a []error local called
// missing, and two different things called the same name in one package is how a reader ends
// up reasoning about the wrong one.
func stillMissing(hashes []chainhash.Hash, res *lookupResult) []chainhash.Hash {
	var rest []chainhash.Hash

	for _, h := range hashes {
		if !res.resolved(h) {
			rest = append(rest, h)
		}
	}

	return rest
}

// readIdentRows fills in every transaction that still holds an identity row: a mempool
// arrival, or one un-mined by a reorg and waiting again.
//
// One statement per LEAF GROUP, not one for the batch, because identByTxidSQL takes the leaf as
// a scalar. That is up to NumLeaves round trips instead of one, and it is still the cheaper
// shape by a wide margin -- see identByTxidSQL for the measurement. The leaf a transaction
// routes to is derived by leafGroups from the txid itself, so this cannot disagree with the
// check constraint about which partition a row lives in.
func (s *Store) readIdentRows(ctx context.Context, txids [][]byte, res *lookupResult) error {
	for _, g := range leafGroups(txids) {
		if err := s.readIdentGroup(ctx, g, res); err != nil {
			return err
		}
	}

	return nil
}

// readIdentGroup is one leaf's worth of readIdentRows.
func (s *Store) readIdentGroup(ctx context.Context, g leafBatch, res *lookupResult) error {
	rows, err := s.pool.Query(ctx, identByTxidSQL, g.leaf, g.txids)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] identity rows", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid []byte
			r    metaRow
		)

		if err := rows.Scan(&txid, &r.createdHeight, &r.offChainSince,
			&r.fee, &r.sizeInBytes, &r.txInpoints, &r.locktime, &r.createdAt,
			&r.flags, &r.rawTx); err != nil {
			return errors.NewStorageError("[utxoset][lookup] identity scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		// A row that will not decode is this transaction's fault alone. See lookupResult.
		data, derr := r.toMeta(&h)
		if derr != nil {
			res.fail(h, derr)

			continue
		}

		res.found[h] = data
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][lookup] identity rows", err)
	}

	return nil
}

// readMinedInto fills in the block lists of every transaction a live containment window names.
//
// One transaction can hold several rows -- one per block that contains it -- and they arrive
// grouped and in (mined_height, block_id) order, which is the order every reader returns and
// the shared conformance suite's assertions about SubtreeIdxs hold under. A transaction the
// identity read already built keeps that record and gains only its blocks here. For one it did
// not, the scalars that describe the transaction come off the FIRST row; every row of one
// transaction carries the same payload, copied from the identity row or from an earlier row,
// so taking the first is the reading that does not depend on how many blocks contain it.
func (s *Store) readMinedInto(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult, floor int32) error {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	rows, err := s.pool.Query(ctx, minedByTxidSQL, txids, floor)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] membership rows", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid []byte
			r    minedRow
		)

		if err := rows.Scan(&txid, &r.minedHeight, &r.blockID, &r.subtreeIdx,
			&r.sizeInBytes, &r.fee, &r.txInpoints, &r.locktime, &r.createdAt,
			&r.flags, &r.rawTx); err != nil {
			return errors.NewStorageError("[utxoset][lookup] membership scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		// A row this loop already refused is not retried on its later rows: the first fault
		// is the answer for the transaction, and appending a second block to a record that
		// was never built would panic on a nil map entry.
		if _, bad := res.failed[h]; bad {
			continue
		}

		data := res.found[h]
		if data == nil {
			// The first row of a transaction builds the record, block and all.
			built, derr := r.toMeta(&h)
			if derr != nil {
				res.fail(h, derr)

				continue
			}

			res.found[h] = built

			continue
		}

		// A later row of the same transaction adds only its own block.
		if derr := r.mergeInto(data, &h); derr != nil {
			delete(res.found, h)
			res.fail(h, derr)
		}
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][lookup] membership rows", err)
	}

	return nil
}

// minedRow is one membership row, or the preserved copy of one, as the read returns it.
//
// It exists so the containment read and the preservation read share one conversion, because
// they read the same columns and have to answer with the same record: the preserved row is a
// copy of the containment row, so a difference between the two readers would be a difference
// between what a parent said yesterday and what it says today. The identity read keeps its own
// conversion (metaRow) because an identity row carries the unmined marker and no block at all.
//
// Every scalar is a pointer where its column is nullable, because which columns are NULL says
// which path wrote the row.
type minedRow struct {
	minedHeight int32
	blockID     int32
	subtreeIdx  int32
	sizeInBytes *int32
	fee         *int64
	txInpoints  []byte
	locktime    *int32
	createdAt   *int64
	flags       int16
	rawTx       []byte
}

// toMeta builds the record for a transaction from one row: the scalars that describe the
// transaction, then the single block this row names.
func (r *minedRow) toMeta(hash *chainhash.Hash) (*meta.Data, error) {
	data := &meta.Data{
		IsCoinbase:  r.flags&FlagCoinbase != 0,
		Conflicting: r.flags&FlagConflicting != 0,
		Locked:      r.flags&FlagLocked != 0,
	}

	if r.sizeInBytes != nil {
		data.SizeInBytes = uint64(*r.sizeInBytes) //nolint:gosec // a size is never negative
	}

	// NULL for every row the block path wrote, and the fee the identity row carried for one
	// the tip's stamp moved here. See the fee column in schema.go.
	if r.fee != nil {
		data.Fee = uint64(*r.fee) //nolint:gosec // a fee is never negative
	}

	if r.locktime != nil {
		data.LockTime = uint32(*r.locktime) //nolint:gosec // a locktime is never negative
	}

	if r.createdAt != nil {
		data.CreatedAt = *r.createdAt
	}

	if len(r.txInpoints) > 0 {
		ip, ierr := subtree.NewTxInpointsFromBytes(r.txInpoints)
		if ierr != nil {
			return nil, errors.NewStorageError("[utxoset][lookup] inpoints %s", hash.String(), ierr)
		}

		data.TxInpoints = ip
	}

	if err := r.mergeInto(data, hash); err != nil {
		return nil, err
	}

	return data, nil
}

// mergeInto adds this row's block to a record already built from an earlier row of the same
// transaction, and decodes the body if the record does not have it yet.
//
// Appending rather than assigning is what the shared conformance suite asserts about
// SubtreeIdxs: one transaction holds one containment row per block that contains it, and they
// arrive grouped and in (mined_height, block_id) order, which is the order a caller reads them
// back in.
func (r *minedRow) mergeInto(data *meta.Data, hash *chainhash.Hash) error {
	data.BlockIDs = append(data.BlockIDs, uint32(r.blockID))             //nolint:gosec // a block id is never negative
	data.BlockHeights = append(data.BlockHeights, uint32(r.minedHeight)) //nolint:gosec // a height is never negative
	data.SubtreeIdxs = append(data.SubtreeIdxs, int(r.subtreeIdx))

	// A body-less row is expected once its window has aged out, so this is a nil transaction
	// rather than an error, exactly as it is on the identity read.
	if data.Tx == nil && len(r.rawTx) > 0 {
		tx, terr := bt.NewTxFromBytes(r.rawTx)
		if terr != nil {
			return errors.NewStorageError("[utxoset][lookup] decode body %s", hash.String(), terr)
		}

		data.Tx = tx
	}

	return nil
}

// readPreserved fills in every transaction whose membership window is gone but whose facts the
// pruner asked to keep, because an unmined child still needs them. See preserved_parent in
// schema.go for why the table exists and why it is small.
//
// One row per transaction, found by primary key -- through the same lateral fence the
// membership and contest reads use, because without it the planner scans this table rather than
// probing it. See preservedByTxidSQL for both measurements.
//
// The body is joined the same way it is everywhere else, on (created_height, txid), and is
// absent whenever its own 288-block window has aged out -- which for a preserved parent is the
// ordinary case, since the transaction is by definition old. A caller that needs the bytes has
// to check, exactly as it does after the identity and membership reads.
func (s *Store) readPreserved(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult) error {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	rows, err := s.pool.Query(ctx, preservedByTxidSQL, txids)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] preserved parents", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid []byte
			r    minedRow
		)

		if err := rows.Scan(&txid, &r.minedHeight, &r.blockID, &r.subtreeIdx,
			&r.sizeInBytes, &r.fee, &r.txInpoints, &r.locktime, &r.createdAt,
			&r.flags, &r.rawTx); err != nil {
			return errors.NewStorageError("[utxoset][lookup] preserved parent scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		// One row per transaction here, so unlike the membership read there is nothing to
		// merge: the copy was taken from a single row and answers with that row's block.
		data, derr := r.toMeta(&h)
		if derr != nil {
			res.fail(h, derr)

			continue
		}

		res.found[h] = data
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][lookup] preserved parents", err)
	}

	return nil
}

// readUTXOFacts is the last step: a transaction nothing else knows about, answered from one of
// its own live UTXOs.
//
// What comes back is deliberately thin. The UTXO carries its block and nothing about the
// transaction's fee, size, inputs or subtree position, which is exactly what a pruned SV Node
// can say about a parent whose block it no longer holds, and all the validator needs to check
// a child's inputs.
//
// A UTXO at (0,0) does not answer. It is returned instead, because it is a trigger for the
// second tier: under invariant I1 such a UTXO belongs to a transaction with an identity row,
// and this step runs only for transactions the identity read did not find.
func (s *Store) readUTXOFacts(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult) ([]chainhash.Hash, error) {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	leaves, ids, los, his := liveUTXOArgs(txids)

	rows, err := s.pool.Query(ctx, utxoFactsSQL, leaves, ids, los, his)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][lookup] UTXO facts", err)
	}

	var zero []chainhash.Hash

	if err := scanBlockFacts(rows, "UTXO facts", res, &zero); err != nil {
		return nil, err
	}

	return zero, nil
}

// zeroUndoProbeSQL names the transactions with an undo copy at (0,0), for the transactions the
// undo read's mined_height > 0 filter left unanswered. Same fenced shape as
// spentParentFactsSQL.
const zeroUndoProbeSQL = `
SELECT k.txid
  FROM unnest($1::bytea[], $2::uuid[], $3::uuid[]) AS k(txid, lo, hi)
 CROSS JOIN LATERAL (
   SELECT 1 AS hit
     FROM spend_journal j
    WHERE j.ukey >= k.lo AND j.ukey <= k.hi AND j.txid = k.txid AND j.mined_height = 0
    ORDER BY j.ukey LIMIT 1 OFFSET 0
 ) AS hit`

// probeZeroUndoCopies is trigger 4 of the second tier: a fully spent transaction whose undo
// copy was taken while its UTXO was still at (0,0). Its containment window is its only home,
// and the drop rule keeps that window attached for as long as the copy can live.
func (s *Store) probeZeroUndoCopies(ctx context.Context, hashes []chainhash.Hash) ([]chainhash.Hash, error) {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	_, ids, los, his := liveUTXOArgs(txids)

	hits, err := queryTxids(ctx, s.pool, zeroUndoProbeSQL, ids, los, his)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][lookup] undo copies at (0,0)", err)
	}

	return hits, nil
}

// readSpentParents is the step past the last one: a transaction with no identity row, no
// membership window, no preserved copy and no live UTXO, answered from the journal row its
// last spend left behind.
//
// It answers exactly what readUTXOFacts answers, and it has to, because the two are the same
// claim from two sources: this transaction was mined in this block and the store no longer
// holds the record that would say more. What comes back is thin -- a block, and the body if
// its window happens to still hold it -- which is all the validator needs to check a child's
// inputs, and all a pruned SV Node could say either.
//
// The packed-key arguments are built by the same liveUTXOArgs the UTXO step uses, so the two
// statements are explained with identical inputs and neither can drift into pinning a plan
// nothing runs. The journal has no leaf column, so the leaf array it returns is unused here:
// the journal is partitioned by spent height, and the ukey range is what locates the row.
func (s *Store) readSpentParents(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult) error {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	_, ids, los, his := liveUTXOArgs(txids)

	rows, err := s.pool.Query(ctx, spentParentFactsSQL, ids, los, his)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] spent parents", err)
	}

	return scanBlockFacts(rows, "spent parents", res, nil)
}

// scanBlockFacts reads the (txid, mined_height, block_id, flags, raw_tx) shape both thin steps
// return, and it is shared rather than copied because the two are one answer from two sources:
// a divergence between them would be a transaction reporting a different block depending on
// whether its last UTXO had been spent yet.
//
// A row at the (0,0) sentinel is handed to zero when the caller supplies it, instead of
// answering: it is a second-tier trigger, not an answer. The undo step supplies nil because its
// statement filters the sentinel out.
func scanBlockFacts(rows pgx.Rows, what string, res *lookupResult, zero *[]chainhash.Hash) error {
	defer rows.Close()

	for rows.Next() {
		var (
			txid        []byte
			minedHeight int32
			blockID     int32
			flags       int16
			rawTx       []byte
		)

		if err := rows.Scan(&txid, &minedHeight, &blockID, &flags, &rawTx); err != nil {
			return errors.NewStorageError("[utxoset][lookup] %s scan", what, err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		if minedHeight == 0 && zero != nil {
			*zero = append(*zero, h)

			continue
		}

		data := &meta.Data{
			IsCoinbase:  flags&FlagCoinbase != 0,
			Conflicting: flags&FlagConflicting != 0,
			Locked:      flags&FlagLocked != 0,
		}

		// mined_height 0 is the unconfirmed sentinel, and an unconfirmed UTXO means the
		// transaction claims no block at all. Reporting block id 0 for it would be a lie
		// that block validation cannot tell from genesis, whose id really is 0.
		if minedHeight > 0 {
			data.BlockIDs = []uint32{uint32(blockID)}         //nolint:gosec // a block id is never negative
			data.BlockHeights = []uint32{uint32(minedHeight)} //nolint:gosec // a height is never negative
			data.SubtreeIdxs = []int{0}
		}

		if len(rawTx) > 0 {
			tx, terr := bt.NewTxFromBytes(rawTx)
			if terr != nil {
				res.fail(h, errors.NewStorageError("[utxoset][lookup] decode body %s", h.String(), terr))

				continue
			}

			data.Tx = tx
		}

		res.found[h] = data
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][lookup] %s", what, err)
	}

	return nil
}

// liveUTXOArgs expands transaction ids into the four parallel arrays utxoFactsSQL takes: the
// partition key, the identity, and the packed-key range covering every output the transaction
// could have created.
//
// It is a named function rather than four lines inline so the plan tests can build exactly the
// arguments the production path builds. A test that explained a hand-written variant would be
// pinning the plan of a statement nothing runs.
func liveUTXOArgs(txids [][]byte) (leaves []int16, ids [][]byte, los, his [][16]byte) {
	leaves = make([]int16, 0, len(txids))
	ids = make([][]byte, 0, len(txids))
	los = make([][16]byte, 0, len(txids))
	his = make([][16]byte, 0, len(txids))

	for _, id := range txids {
		leaves = append(leaves, LeafFor(id))
		ids = append(ids, id)
		los = append(los, Pack(id, 0))
		his = append(his, Pack(id, ^uint32(0)))
	}

	return leaves, ids, los, his
}

// tier2SQL is the second containment tier: one snapshot per leaf group holding the identity
// row, whether a UTXO or an undo copy sits at (0,0), and every containment row at or above the
// dropped floor, $5. Steps 1 to 3 are separate statements under read committed, each with its
// own snapshot, so two of them can disagree without anything being wrong: a child's lookup
// reads tx_ident and finds no row, the parent's unmined create then commits its identity row and
// its UTXOs at (0,0), and the lookup's UTXO step sees a (0,0) UTXO "with no identity row". Only
// a disagreement INSIDE this one statement counts as corruption. The locked bit of a
// containment row is masked with $6: a row below the lookup floor never supplies it, because
// SetLocked's containment arm stops at that floor.
const tier2SQL = `
SELECT k.txid,
       i.marker_set, i.created_height, i.fee, i.size_in_bytes, i.tx_inpoints, i.locktime,
       i.created_at, i.flags,
       uz.hit IS NOT NULL AS utxo_zero,
       jz.hit IS NOT NULL AS undo_zero,
       m.mined_height, m.block_id, m.subtree_idx, m.created_height, m.size_in_bytes,
       m.fee, m.tx_inpoints, m.locktime, m.created_at,
       m.flags & ~$6::smallint AS flags
  FROM unnest($2::bytea[], $3::uuid[], $4::uuid[]) AS k(txid, lo, hi)
  LEFT JOIN LATERAL (
    SELECT i.off_chain_since IS NOT NULL AS marker_set, i.created_height, i.fee,
           i.size_in_bytes, i.tx_inpoints, i.locktime, i.created_at, i.flags
      FROM tx_ident i
     WHERE i.leaf = $1::smallint AND i.txid = k.txid
    OFFSET 0 ) AS i ON TRUE
  LEFT JOIN LATERAL (
    SELECT 1 AS hit FROM utxo u
     WHERE u.leaf = $1::smallint AND u.ukey >= k.lo AND u.ukey <= k.hi
       AND u.txid = k.txid AND u.mined_height = 0
     ORDER BY u.ukey LIMIT 1 OFFSET 0 ) AS uz ON TRUE
  LEFT JOIN LATERAL (
    SELECT 1 AS hit FROM spend_journal j
     WHERE j.ukey >= k.lo AND j.ukey <= k.hi AND j.txid = k.txid AND j.mined_height = 0
     ORDER BY j.ukey LIMIT 1 OFFSET 0 ) AS jz ON TRUE
  LEFT JOIN LATERAL (
    SELECT m.mined_height, m.block_id, m.subtree_idx, m.created_height, m.size_in_bytes,
           m.fee, m.tx_inpoints, m.locktime, m.created_at, m.flags
      FROM tx_mined m
     WHERE m.txid = k.txid
       AND m.mined_height >= $5::int
     ORDER BY m.mined_height, m.block_id
    OFFSET 0 ) AS m ON TRUE
 ORDER BY k.txid, m.mined_height, m.block_id`

// readTier2 answers the transactions the three triggers sent here, one snapshot per leaf group.
//
// Containment rows answer, with the identity row's payload when there is one and the first
// row's otherwise. No containment and an identity row means unmined or waiting: the identity
// record stands, with empty block lists. No containment, no identity row, and a UTXO or an undo
// copy at (0,0) in the same snapshot is corruption: nothing can ever stamp that UTXO, and the
// drop rule keeps a window attached for longer than any (0,0) undo copy of its UTXOs can live.
// That transaction fails with a storage error and the I1 counter climbs; the other
// transactions of the batch are unaffected. Nothing at all means carry on to the preserved
// parent, which is the Delete race: Delete removes every row of a transaction in one statement,
// and a delete committing between an earlier step and this one leaves nothing to find.
func (s *Store) readTier2(ctx context.Context, hashes []chainhash.Hash, res *lookupResult) error {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	floors, err := s.Floors(ctx)
	if err != nil {
		return err
	}

	for _, g := range leafGroups(txids) {
		if err := s.readTier2Group(ctx, g, int32(floors.DroppedFloor), res); err != nil { //nolint:gosec // a height fits int32
			return err
		}
	}

	return nil
}

// tier2Row is one row of tier2SQL: the per-transaction columns and, when there is one, a
// containment row.
type tier2Row struct {
	markerSet *bool
	ident     metaRow
	utxoZero  bool
	undoZero  bool
	mined     *minedRow
}

func (s *Store) readTier2Group(ctx context.Context, g leafBatch, droppedFloor int32, res *lookupResult) error {
	_, ids, los, his := liveUTXOArgs(g.txids)

	rows, err := s.pool.Query(ctx, tier2SQL, g.leaf, ids, los, his, droppedFloor, FlagLocked)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] second tier", err)
	}

	defer rows.Close()

	byTx := map[chainhash.Hash][]tier2Row{}
	order := make([]chainhash.Hash, 0, len(g.txids))

	for rows.Next() {
		var (
			txid           []byte
			r              tier2Row
			identCreatedAt *int32
			identFlags     *int16
			minedHeight    *int32
			blockID        *int32
			subtreeIdx     *int32
			createdHeight  *int32
			sizeInBytes    *int32
			fee            *int64
			txInpoints     []byte
			locktime       *int32
			createdAt      *int64
			flags          *int16
		)

		// Every identity column is NULL when there is no identity row, so the two NOT NULL
		// columns of that table are scanned through pointers here.
		if err := rows.Scan(&txid, &r.markerSet, &identCreatedAt, &r.ident.fee, &r.ident.sizeInBytes,
			&r.ident.txInpoints, &r.ident.locktime, &r.ident.createdAt, &identFlags,
			&r.utxoZero, &r.undoZero,
			&minedHeight, &blockID, &subtreeIdx, &createdHeight, &sizeInBytes, &fee, &txInpoints,
			&locktime, &createdAt, &flags); err != nil {
			return errors.NewStorageError("[utxoset][lookup] second tier scan", err)
		}

		if identCreatedAt != nil {
			r.ident.createdHeight = *identCreatedAt
		}

		if identFlags != nil {
			r.ident.flags = *identFlags
		}

		if minedHeight != nil && blockID != nil {
			m := &minedRow{minedHeight: *minedHeight, blockID: *blockID, sizeInBytes: sizeInBytes,
				fee: fee, txInpoints: txInpoints, locktime: locktime, createdAt: createdAt}

			if subtreeIdx != nil {
				m.subtreeIdx = *subtreeIdx
			}

			if flags != nil {
				m.flags = *flags
			}

			r.mined = m
		}

		var h chainhash.Hash

		copy(h[:], txid)

		if _, ok := byTx[h]; !ok {
			order = append(order, h)
		}

		byTx[h] = append(byTx[h], r)
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][lookup] second tier", err)
	}

	for _, h := range order {
		if _, bad := res.failed[h]; bad {
			continue
		}

		s.settleTier2(h, byTx[h], res)
	}

	return nil
}

// settleTier2 turns one transaction's tier-2 rows into its answer.
func (s *Store) settleTier2(h chainhash.Hash, rows []tier2Row, res *lookupResult) {
	first := rows[0]
	hasIdent := first.markerSet != nil

	var mined []*minedRow

	for i := range rows {
		if rows[i].mined != nil {
			mined = append(mined, rows[i].mined)
		}
	}

	if len(mined) == 0 {
		lookupTier2Empty.Inc()

		if hasIdent {
			// Unmined, or mined in a window this snapshot cannot see, which cannot happen
			// while the window is attached. The identity record from step 1 stands; if step 1
			// did not build it (the create race), build it now.
			if _, ok := res.found[h]; !ok {
				data, derr := first.ident.toMeta(&h)
				if derr != nil {
					res.fail(h, derr)

					return
				}

				res.found[h] = data
			}

			return
		}

		if first.utxoZero || first.undoZero {
			lookupI1Violations.Inc()
			res.fail(h, errors.NewStorageError("[utxoset][lookup] %s has a UTXO or undo copy at (0,0), no identity row and no containment row in one snapshot; nothing can ever stamp it", h.String()))
		}

		return
	}

	lookupTier2Answered.Inc()

	if !hasIdent && first.utxoZero {
		// Containment exists, so the transaction answers, and the UTXO at (0,0) with no
		// identity row is a real break of invariant I1, counted.
		lookupI1Violations.Inc()
	}

	data := res.found[h]
	if data == nil {
		if hasIdent {
			built, derr := first.ident.toMeta(&h)
			if derr != nil {
				res.fail(h, derr)

				return
			}

			data = built
		} else {
			built, derr := mined[0].toMeta(&h)
			if derr != nil {
				res.fail(h, derr)

				return
			}

			data = built
			mined = mined[1:]
		}

		res.found[h] = data
	}

	for _, m := range mined {
		if derr := m.mergeInto(data, &h); derr != nil {
			delete(res.found, h)
			res.fail(h, derr)

			return
		}
	}
}
