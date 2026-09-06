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

// The read order is identity table, then membership by transaction id, then the preserved
// parent, then the coin, and the order is a correctness rule rather than a tuning. A coin
// holds ONE block id, and on the ordinary two-step reorg (a fork block stamped as not on the
// longest chain, then a later block making it the main chain) nothing rewrites the coins of
// transactions shared between the two blocks. A coin-first read would then hand block
// validation an id that lost, and the parent check stores a valid block as invalid. The
// membership table holds every id while the window lives, so the coin is consulted only once
// the window is gone, by which time its block is at least 1440 deep and its coin-carried id is
// the settled one.
//
// The preserved-parent step sits between membership and coin, and its position is the same
// kind of rule. It answers from a COPY of a membership row, taken while the row was still
// there, so it must never be preferred to the row itself: while the window lives, the row is
// the record that gets rewritten by a reorg and the copy is not. Once the window is gone the
// copy is all there is, and it comes before the coin because it carries the whole payload
// where the coin carries only a block.
//
// The spend journal is the FIFTH step, after the coin rather than before it, and that order is
// the same rule again. A live coin is the settled record of a transaction that still exists;
// the journal row is a copy taken off a coin that has since been destroyed. Ask the journal
// first and a transaction with one output spent and one still live would be answered from the
// spent one, which is a copy where a record was available. Ask it last and it is reached only
// for a transaction with no live coin at all, which is the case it exists for.
//
// It exists because nothing else can answer for a FULLY-SPENT parent mined more than the
// membership retention ago, and model/Block.go's checkParentTransactions asks about exactly
// that on most blocks above the highest checkpoint. No identity row (it was mined), no
// membership window (dropped 1440 blocks after it was mined), no preserved copy (preservation
// names parents of children unmined for 144 blocks, and this child is mined in the next
// block), and no coin (the last one was just spent). getParentTxMetaBlockIDs turns the
// resulting not-found into a BlockIncompleteError, which callers retry rather than persist, so
// the block retries forever. Below the highest checkpoint skipOrderAndBlessedBelowCheckpoint
// skips the whole check, which is why a from-genesis sync runs clean until it passes it.
//
// The spec's own version of this step could not have worked: it read the parent's block facts
// from tx_mined by the spent height's partition, and for this parent tx_mined has no row at
// any height. The journal now carries the facts itself, copied off the coin the spend
// destroyed. Trusting a copied block id here is not the mutability the restore rule forbids: a
// parent reaching this step has had its window retired, so its block is at least 1440 deep and
// cannot change. See the spend_journal comment in schema.go.
//
// The step buys the journal's retention and not a block more. Past both retentions the
// transaction is genuinely gone and the store reports not-found, which is what aerospike's
// delete-at-height does and what the shared suite's pruning test requires.

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
SELECT i.txid, i.created_height, i.off_chain_since, i.membership, i.fee, i.size_in_bytes,
       i.tx_inpoints, i.locktime, i.created_at, i.flags, b.raw_tx
  FROM tx_ident i
  LEFT JOIN tx_body b ON b.created_height = i.created_height AND b.txid = i.txid
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])`

// minedByTxidSQL reads every membership row for a set of transactions, across every live
// window, in insertion order. The primary key leads with txid, so this is one descent per
// window per transaction.
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
// OFFSET 0 is the fence itself, the same one coinFactsSQL relies on: it stops the planner
// pulling the subquery up into the outer join, which is what re-admits the hash join. The
// inner ORDER BY walks the primary key in order; the outer one is what actually guarantees the
// grouping the reader relies on, because the LEFT JOIN to the body may reorder rows.
const minedByTxidSQL = `
SELECT k.txid, m.mined_height, m.block_id, m.subtree_idx, m.size_in_bytes, m.fee,
       m.tx_inpoints, m.locktime, m.created_at, m.flags, b.raw_tx
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT m.mined_height, m.block_id, m.subtree_idx, m.created_height, m.size_in_bytes,
          m.fee, m.tx_inpoints, m.locktime, m.created_at, m.flags, m.seq
     FROM tx_mined m
    WHERE m.txid = k.txid
    ORDER BY m.seq
   OFFSET 0
 ) AS m
  LEFT JOIN tx_body b ON b.created_height = m.created_height AND b.txid = k.txid
 ORDER BY k.txid, m.seq`

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

// coinFactsSQL reads one live coin per transaction, for the transactions nothing else knows.
// The LATERAL with ORDER BY and LIMIT 1 OFFSET 0 is the fence the planner needs to walk the
// packed-key index instead of scanning the coin table, and each half of it was measured:
// without the packed-key range bound the planner reads every leaf partition, and without the
// ORDER BY it materialises the whole range before the LIMIT can stop it. createMinedPlanSQL's
// duplicate-coin guard carries the identical fence, for the identical reason.
const coinFactsSQL = `
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
// The shape is coinFactsSQL's, and for the same reasons. The keys sit on the OUTSIDE of a
// LATERAL with the ORDER BY / LIMIT 1 / OFFSET 0 fence: OFFSET 0 stops the planner pulling the
// subquery up into the outer join, which is what would re-admit a hash join against every live
// leaf; the packed-key range bound is what makes it a range scan rather than a read of the
// whole leaf; and the ORDER BY is what lets the LIMIT stop the scan instead of materialising
// the range first. ukey is the journal leaf's only index, so this is one range probe per leaf
// per key.
//
// The spend height is not known to the reader -- that is the whole point of the step, the
// caller is asking about a parent it has lost track of -- so there is no partition bound and
// every live leaf is probed. At the journal's 1440-block retention in 48-block leaves that is
// 30 leaves, and 500 keys is therefore 15,000 index descents. Measured on this schema at 500
// keys against 39,990 journal rows across 30 leaves, eight runs: 7.4-9.2 ms, an Index Scan on
// every leaf's ukey index and no Seq Scan on any of them, flat across all eight. That is the
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
// point: a corrupt identity row must not fall through to the membership table or the coin, or
// the store would answer from a coin for a transaction whose real record it just refused to
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

// lookupMany resolves a set of transactions in the read order. Misses are absent from both
// maps; a transaction whose stored row will not decode lands in failed rather than found.
//
// Each step asks only about the hashes the steps before it could not answer, so a batch of
// ordinary mined parents costs one membership probe each and never touches the coin table,
// a batch of mempool parents never leaves the identity table, and the journal is read only for
// a transaction the four steps above it all missed.
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

	// Step 1: the identity table (mempool and fork-limbo rows).
	//
	// One entry per DISTINCT hash. A batch can name the same parent twice, and asking twice
	// would return the row twice and waste the round trip this call exists to save.
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

	// The later steps are SKIPPED rather than returned from when nothing is left to ask
	// about, because the contest read below has to run whether or not the identity table
	// answered everything.
	rest := stillMissing(uniq, &res)

	if len(rest) > 0 {
		// Step 2: membership by transaction id.
		if err := s.readMinedInto(ctx, rest, &res); err != nil {
			return lookupResult{}, err
		}

		rest = stillMissing(rest, &res)
	}

	if len(rest) > 0 {
		// Step 3: the preserved parent, for a transaction whose membership window has been
		// dropped while an unmined child still needed its facts.
		if err := s.readPreserved(ctx, rest, &res); err != nil {
			return lookupResult{}, err
		}

		rest = stillMissing(rest, &res)
	}

	if len(rest) > 0 {
		// Step 4: the coin.
		if err := s.readCoinFacts(ctx, rest, &res); err != nil {
			return lookupResult{}, err
		}

		rest = stillMissing(rest, &res)
	}

	if len(rest) > 0 {
		// Step 5: the spend journal, for a fully-spent parent whose membership window has
		// already retired. Last, so a live coin is always preferred to a copy taken off a
		// destroyed one.
		if err := s.readSpentParents(ctx, rest, &res); err != nil {
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

// conflictChildrenSQL names the transactions recorded as contesting each of these
// transactions' coins.
//
// One statement for the whole batch, keyed on the parent's transaction id alone. That is what
// makes it answer for a parent in the identity table, a parent in the membership table, a
// parent whose membership row survives only as a preservation copy, and a parent this store
// knows only from a live coin -- the packed column it replaces could only ever answer for the
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

		if err := rows.Scan(&txid, &r.createdHeight, &r.offChainSince, &r.membership,
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

// readMinedRows fills in every transaction a live membership window still names.
//
// One transaction can hold several rows -- one per block that contains it -- and they arrive
// grouped and in insertion order, which is what the conformance suite asserts about
// SubtreeIdxs. The scalars that describe the transaction rather than a block come off the
// FIRST row; the rows are written by one statement per block application, so they agree, and
// taking the first is the reading that does not depend on how many blocks claim it.
func (s *Store) readMinedInto(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult) error {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	rows, err := s.pool.Query(ctx, minedByTxidSQL, txids)
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
// It exists so the membership read and the preservation read share one conversion, because
// they read the same columns and have to answer with the same record: the preserved row is a
// copy of the membership row, so a difference between the two readers would be a difference
// between what a parent said yesterday and what it says today. The identity read keeps its own
// conversion (metaRow) because an identity row packs its blocks into one column rather than
// arriving as one row per block. Three copies of "what a stored transaction means" is what this
// store already carried; this is what stops the preservation read being a fourth.
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
// SubtreeIdxs: one transaction holds one membership row per block that stamped it, and they
// arrive grouped and in insertion order, which is the order a caller reads them back in.
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

// readCoinFacts is the last step: a transaction nothing else knows about, answered from one of
// its own live coins.
//
// What comes back is deliberately thin. The coin carries its block and nothing about the
// transaction's fee, size, inputs or subtree position, which is exactly what a pruned SV Node
// can say about a parent whose block it no longer holds, and all the validator needs to check
// a child's inputs.
func (s *Store) readCoinFacts(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult) error {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	leaves, ids, los, his := liveCoinArgs(txids)

	rows, err := s.pool.Query(ctx, coinFactsSQL, leaves, ids, los, his)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] coin facts", err)
	}

	return scanBlockFacts(rows, "coin facts", res)
}

// readSpentParents is the step past the last one: a transaction with no identity row, no
// membership window, no preserved copy and no live coin, answered from the journal row its
// last spend left behind.
//
// It answers exactly what readCoinFacts answers, and it has to, because the two are the same
// claim from two sources: this transaction was mined in this block and the store no longer
// holds the record that would say more. What comes back is thin -- a block, and the body if
// its window happens to still hold it -- which is all the validator needs to check a child's
// inputs, and all a pruned SV Node could say either.
//
// The packed-key arguments are built by the same liveCoinArgs the coin step uses, so the two
// statements are explained with identical inputs and neither can drift into pinning a plan
// nothing runs. The journal has no leaf column, so the leaf array it returns is unused here:
// the journal is partitioned by spent height, and the ukey range is what locates the row.
func (s *Store) readSpentParents(ctx context.Context, hashes []chainhash.Hash,
	res *lookupResult) error {
	txids := make([][]byte, 0, len(hashes))
	for i := range hashes {
		txids = append(txids, hashes[i][:])
	}

	_, ids, los, his := liveCoinArgs(txids)

	rows, err := s.pool.Query(ctx, spentParentFactsSQL, ids, los, his)
	if err != nil {
		return errors.NewStorageError("[utxoset][lookup] spent parents", err)
	}

	return scanBlockFacts(rows, "spent parents", res)
}

// scanBlockFacts reads the (txid, mined_height, block_id, flags, raw_tx) shape both thin steps
// return, and it is shared rather than copied because the two are one answer from two sources:
// a divergence between them would be a transaction reporting a different block depending on
// whether its last coin had been spent yet.
func scanBlockFacts(rows pgx.Rows, what string, res *lookupResult) error {
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

		data := &meta.Data{
			IsCoinbase:  flags&FlagCoinbase != 0,
			Conflicting: flags&FlagConflicting != 0,
			Locked:      flags&FlagLocked != 0,
		}

		// mined_height 0 is the unconfirmed sentinel, and an unconfirmed coin means the
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

// liveCoinArgs expands transaction ids into the four parallel arrays coinFactsSQL takes: the
// partition key, the identity, and the packed-key range covering every output the transaction
// could have created.
//
// It is a named function rather than four lines inline so the plan tests can build exactly the
// arguments the production path builds. A test that explained a hand-written variant would be
// pinning the plan of a statement nothing runs.
func liveCoinArgs(txids [][]byte) (leaves []int16, ids [][]byte, los, his [][16]byte) {
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
