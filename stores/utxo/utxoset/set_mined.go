package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// stampSQL records a block against every transaction that does not already claim it, and
// clears the mempool marker when the block is on the longest chain.
//
// Every key parameter is an ARRAY, so one statement serves one transaction or a whole block.
// This used to queue one UPDATE per transaction down a single connection, which already cost
// only one round trip, so the saving is not round trips. It is PostgreSQL parsing, planning
// and executing one statement instead of thousands. The create path made the same move and
// measured 4x on the batch flush, at the same batch widths a block arrives in.
//
// A hash named twice in one call is stamped once, because an UPDATE never applies to the same
// target row twice within one statement. That matches what the per-transaction loop did, where
// the second attempt found the block already recorded and skipped it.
//
// The append is guarded rather than unconditional, so replaying a block does not record it
// twice. The guard reads the row as it stood before this statement, which is what makes a batch
// mixing already-stamped and never-stamped transactions come out right in both directions.
//
// Membership is tested on a 12-BYTE BOUNDARY. The column is a concatenation of 12-byte triples
// and the reader unpacks it that way. This used to be a plain substring search, which can match
// bytes STRADDLING two neighbouring triples, read that as already-recorded, and silently skip a
// real append, leaving a transaction that never claims a block which actually contains it.
// unstampSQL carries the identical test, and it matters more there.
//
// The marker is cleared only when the caller states the block is on the longest chain. That
// is the same rule the create gate applies, and for the same reason: "mined into some block"
// and "on the main chain" are different facts, and a transaction whose only block later
// loses must stay in the mempool set.
//
// THE LEAF IS A SCALAR AND THE TXIDS AN ARRAY, so the statement runs once per leaf group. See
// leafGroups for the measurements: it is the only one of the three key shapes whose cost is a
// function of the batch rather than of the mempool.
const stampSQL = `
UPDATE tx_ident i
   SET membership = CASE
           WHEN EXISTS (
                SELECT 1
                  FROM generate_series(0, coalesce(length(i.membership), 0) / 12 - 1) g
                 WHERE substring(i.membership from g * 12 + 1 for 12) = $3::bytea)
           THEN i.membership
           ELSE coalesce(i.membership, '\x'::bytea) || $3::bytea
       END,
       off_chain_since = CASE WHEN $4::boolean THEN NULL ELSE i.off_chain_since END
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])`

// unstampSQL removes one block from a transaction's membership and puts it back in the
// mempool set with a FRESH clock. Array-parameterised for the same reason stampSQL is.
//
// The entry to remove is located on a 12-BYTE BOUNDARY, and that is the worse half of the
// alignment rule rather than a symmetry. This used to splice 12 bytes out from wherever a plain
// substring search first matched. At an unaligned offset that destroys the tail of one triple
// and the head of the next, and the result is still a multiple of 12, so the length constraint
// does not catch it and the reader cannot tell it has been handed two invented blocks. A value
// that is not present on a boundary is not an entry at all, so the right answer is to change
// nothing, which is what the coalesce does when no aligned match exists.
//
// The clock is the store's current tip, NOT the transaction's creation height, and that is
// the fact that decides whether these two columns are one concept or two. A transaction
// created at height 100 and un-mined while the tip is 5,000 must wait from 5,000, or the
// preservation pass fires on it immediately. Both reference stores do the same.
//
// One leaf group at a time, the same key shape stampSQL takes and for the same reason. See
// leafGroups.
const unstampSQL = `
UPDATE tx_ident i
   SET membership = coalesce((
           SELECT overlay(i.membership placing ''::bytea FROM min(g * 12 + 1) FOR 12)
             FROM generate_series(0, coalesce(length(i.membership), 0) / 12 - 1) g
            WHERE substring(i.membership from g * 12 + 1 for 12) = $3::bytea
       ), i.membership),
       off_chain_since = $4::int
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])`

// provePresentSQL is the SECOND statement, and splitting it out is not tidiness.
//
// The interface says every hash asked about must appear in the answer, and an implementation
// that cannot prove it must return an error. A single UPDATE ... RETURNING cannot: it returns
// nothing for a transaction that is already correctly mined, which is indistinguishable from
// the row not existing, so a replayed block would report every transaction in it as missing.
// Asking separately whether the row is there answers the question the contract actually poses.
//
// THE LEAF IS A SCALAR AND THE TXIDS AN ARRAY, so this runs once per leaf group, the same shape
// as the stamp and the move it sits between. It used to carry the paired
// `unnest(l[],t[]) JOIN tx_ident` form, which is the one leafGroups measures and rejects
// because its plan FLIPS with statistics. Measured on this schema, 500 keys over eight leaves
// against 40,000 identity rows, eight runs each: the paired form 5.19-5.31 ms for the batch,
// the leaf-scalar form 0.29-0.33 ms per group, so about 2.5 ms for the same 500 keys. This runs
// on every stamp and every un-mine, at block width.
const provePresentSQL = `
SELECT i.txid, i.membership
  FROM tx_ident i
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])`

// appendMinedSQL records this block against every listed transaction that already has a
// membership row, copying the payload from its earliest row. A transaction with no row at all
// is not appended, so the postcondition still catches an unknown one.
//
// The keys sit on the OUTSIDE of a LATERAL with an OFFSET 0 fence, for the same reason
// minedByTxidSQL does: written as a plain `JOIN tx_mined m ON m.txid = k.txid` the planner is
// free to hash-join the keys against the whole partitioned table, which the measurements
// behind minedByTxidSQL showed as a Seq Scan on every live window at 500 keys. The LATERAL's
// own ORDER BY + LIMIT 1 also picks the earliest row directly, so no outer DISTINCT ON is
// needed.
const appendMinedSQL = `
INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                      size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
SELECT k.txid, $2, $3, $4, m.created_height, m.size_in_bytes, m.fee, m.tx_inpoints,
       m.locktime, m.created_at, m.flags
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT created_height, size_in_bytes, fee, tx_inpoints, locktime, created_at, flags
     FROM tx_mined m
    WHERE m.txid = k.txid
    ORDER BY m.seq
    LIMIT 1 OFFSET 0
 ) AS m
ON CONFLICT (txid, mined_height, block_id) DO NOTHING`

// moveToMinedSQL moves identity rows that, after this block, name exactly one block into the
// membership table, deleting them from the identity table in the same statement.
//
// A row naming two or more blocks is left where it is with its marker cleared: the id-less
// mark-on-longest-chain call cannot later say which of them is main, so the row waits for an
// un-mine or a further stamp to reduce it to one.
//
// A row carrying conflicting children used to be left behind too, because the bookkeeping was
// a column of tx_ident and tx_mined had nowhere to put it -- so a contested transaction was
// pinned in the mempool table for good. That bookkeeping is a side table now, keyed on the
// txid in either table, so a contested parent moves like any other.
//
// The single-block test is an EQUALITY against the packed triple this stamp just appended, not
// a length test, and that does both halves of the job at once: it is true only when the
// membership column holds twelve bytes AND those twelve bytes are THIS block. A row whose one
// triple named some other block would otherwise move under this block's facts. The length test
// alone cannot go wrong today, because stampSQL has already appended this block by the time
// this statement runs, but the equality does not depend on that ordering holding.
//
// mined_height, block_id and subtree_idx come from the parameters rather than from decoding the
// membership bytes, because the equality above has already proved the two agree.
//
// THE LEAF IS A SCALAR AND THE TXIDS AN ARRAY, so this runs once per leaf group, and that is
// the difference between one index descent per key and a read of the whole mempool. See
// leafGroups for the three key shapes measured and the numbers. The LATERAL fence the read
// statements use is not available here: a DELETE cannot laterally reference its own target.
//
// txid = ANY is EXACT despite dropping the pairing. txid is the full 32 bytes and tx_ident_ck
// makes leaf a function of it, so no row can satisfy the txid qual under a leaf other than its
// own. The leaf is therefore redundant as a filter and load-bearing as an access path: it is
// what prunes to the one partition and what makes the primary key usable, since txid is its
// second column.
//
// It RETURNS the txids it moved, and that is what keeps the postcondition cheap. provePresentSQL
// reads tx_ident only, so without this every row the move just settled would be reported absent
// and fall into the residue set -- an appendMinedSQL and a minedIDsByTxid at full block width on
// every longest-chain stamp, which is the ordinary tip path. The ids returned are the ones that
// LEFT tx_ident, taken from the DELETE's own RETURNING rather than the INSERT's: an insert that
// hits the membership key (the same block replayed) reports nothing, yet the identity row is
// gone all the same, so keying off the insert would leak those rows back into the residue.
const moveToMinedSQL = `
WITH moved AS (
    DELETE FROM tx_ident i
     WHERE i.leaf = $1::smallint
       AND i.txid = ANY($2::bytea[])
       AND i.membership = $6::bytea
    RETURNING i.txid, i.created_height, i.size_in_bytes, i.fee, i.tx_inpoints,
              i.locktime, i.created_at, i.flags
),
settled AS (
    INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                          size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
    SELECT m.txid, $3::int, $4::int, $5::int, m.created_height, m.size_in_bytes, m.fee,
           m.tx_inpoints, m.locktime, m.created_at, m.flags
      FROM moved m
    ON CONFLICT (txid, mined_height, block_id) DO NOTHING
)
SELECT txid FROM moved`

// moveBackSQL is the un-mine: it takes a transaction out of the membership table and puts it
// back in the mempool table, with the unconfirmed marker at the CURRENT tip and the blocks it
// still remembers packed as fork triples. It is the exact reverse of moveToMinedSQL, and the
// reverse of moveToMinedSQL is what makes the two tables one store rather than two.
//
// A transaction lives in EXACTLY ONE of the two tables at any time, so `gone` deletes EVERY
// membership row of the txid and not just the un-mined block's. That is an invariant with
// teeth rather than symmetry: the lazy coin stamp at window retirement reads membership rows,
// so a row left behind here would later stamp this transaction's coins into a block it no
// longer settles under, and the read path's identity-then-membership order assumes one home.
// Deleting only the named block's row and re-packing the others as fork triples would leave
// the transaction in both tables at once, claiming the same block twice in two different
// spellings.
//
// Only a transaction that HAS a membership row for the named block takes part, which `named`
// establishes before anything is deleted. A transaction the caller un-mines from a block it was
// never in has nothing to take back, and taking it back anyway would un-settle it out of the
// block it actually IS in -- turning a tolerated absence into a wrong answer. When no block is
// named ($2 NULL) `named` is empty and irrelevant, and every row of every listed transaction
// goes.
//
// There is NO `ON CONFLICT` on the identity insert, deliberately, and the absence is the safety
// property. `gone` has already deleted the membership rows by the time the insert runs, so a
// conflict swallowed here would lose the transaction's block facts from BOTH tables at once. A
// unique violation instead rolls the whole move back, having lost nothing. The conflict means a
// transaction had a home in both tables, which createIdentPlanSQL's membership guard is what
// prevents; if it ever happens, the loud failure is the bug report.
//
// ONE STATEMENT SERVES BOTH DIRECTIONS, and the difference between them is a single predicate
// on `packed`: which triples survive into membership.
//
//   - SetMinedMulti's un-mine NAMES a block, so that block's triple is DROPPED and the others
//     are kept. NULL membership when the named block was the only one, which is what a
//     transaction no block has ever named already carries, so the two spellings of "no block"
//     stay one.
//   - MarkTransactionsOnLongestChain(false) names none -- it says only that the chain the node
//     now believes in does not contain this transaction -- so it cannot drop any single block
//     and ALL of them are kept for a later stamp or un-mine to resolve. $2 and $3 arrive NULL.
//
// The join to `packed` is LEFT for that first case: an un-mine of the only block leaves no
// triples at all, and an inner join would then drop the identity row the un-mine exists to
// write.
//
// The membership is packed from `gone` itself, never from a second read of tx_mined, and that
// closes a PostgreSQL trap as well as being simpler: a data-modifying CTE's effects are NOT
// visible to its sibling CTEs, which all see the snapshot the statement began with, so a
// sibling that re-read tx_mined would still see the rows `gone` is deleting and hand back the
// very block it was told to forget.
//
// The keys reach the DELETE as `txid = ANY(...)`, not as a join against unnest, and the reason
// is the one moveToMinedSQL gives: tx_mined's primary key LEADS with txid, so the array form is
// one index descent per key per live window, while the join form lets the planner hash the keys
// against a window and read it whole -- measured on this schema at 500 keys against a 60,000-row
// window, a Seq Scan and 7 ms of the statement's 13.
//
// The clock is $4, the store's CURRENT height, not the transaction's created_height. See
// unstampSQL: a transaction created at 100 and un-mined at a tip of 5,000 must wait from
// 5,000, or the preservation pass fires on it immediately.
//
// The fee comes back with the row. tx_mined carries it precisely so that this move can return
// it, because the transaction is handed to block assembly, which prices it.
//
// The COINS are reset by a separate statement in the same transaction, resetCoinsSQL, and not
// by a further CTE here. A CTE cannot take the packed-key range as a plain array value -- it
// would have to compute the bounds from the deleted rows -- and the planner then costs the
// range as a join filter and reads all eight coin partitions instead: measured on this schema
// at 400,000 coins, a Seq Scan on every partition and 98 ms of the statement's 108.
const moveBackSQL = `
WITH named AS (
    SELECT DISTINCT m.txid
      FROM tx_mined m
     WHERE m.txid = ANY($1::bytea[])
       AND m.mined_height = $2::int AND m.block_id = $3::int
),
gone AS (
    DELETE FROM tx_mined m
     WHERE m.txid = ANY($1::bytea[])
       AND ($2::int IS NULL OR m.txid IN (SELECT txid FROM named))
    RETURNING m.txid, m.mined_height, m.block_id, m.subtree_idx, m.seq, m.created_height,
              m.size_in_bytes, m.fee, m.tx_inpoints, m.locktime, m.created_at, m.flags
),
keys AS (
    SELECT DISTINCT txid FROM gone
),
packed AS (
    SELECT txid,
           string_agg(mh_triple(block_id, mined_height, subtree_idx),
                      ''::bytea ORDER BY seq) AS membership
      FROM gone
     WHERE $2::int IS NULL
        OR NOT (mined_height = $2::int AND block_id = $3::int)
     GROUP BY txid
),
back AS (
    INSERT INTO tx_ident (leaf, txid, created_height, off_chain_since, membership,
                          fee, size_in_bytes, tx_inpoints, locktime, created_at, flags)
    SELECT (get_byte(g.txid, 0) & 7)::smallint, g.txid, g.created_height, $4::int, p.membership,
           g.fee, g.size_in_bytes, g.tx_inpoints, g.locktime, g.created_at, g.flags
      FROM (SELECT DISTINCT ON (txid) * FROM gone ORDER BY txid, seq) g
      LEFT JOIN packed p ON p.txid = g.txid
)
SELECT txid FROM keys`

// resetCoinsSQL puts a transaction's live coins back to the unconfirmed sentinel.
//
// The packed-key range comes in as PLAIN ARRAY VALUES built by liveCoinArgs and is used inside a
// LATERAL with an OFFSET 0 fence, and BOTH halves of that are load-bearing. The coin table
// carries one index, on the packed key, and the schema says in its own words that a query
// filtering on txid without a packed-key range bound is a review failure. Bounds computed
// inside the statement from a CTE's rows satisfy the letter of that rule and not its point:
// the planner costs them as a join filter and reads every coin partition whole. Bounds passed
// as arrays but joined directly are no better -- measured at 500 keys, a Hash Join against a
// Seq Scan of all eight partitions -- because an UPDATE cannot laterally reference its own
// target, which is the fence every other by-transaction coin read in this store relies on. So
// the fenced read runs first, in a CTE, and the UPDATE then matches on the exact (leaf, ukey)
// it returns.
//
// mined_height > 0 is what selects the coins that need resetting and what leaves a coin already
// at the sentinel untouched. It is the right test where block_id = 0 would not be: block id 0 is
// a legitimate id, and it is mined_height that carries the "unconfirmed" fact.
//
// EVERY stamped coin of the transaction is reset, not only those naming the un-mined block, and
// that follows from the move being whole. A coin stamped with a SIBLING block's id would
// otherwise go on claiming a block the transaction no longer settles under, because after the
// move the transaction is in the mempool table and settles under nothing at all. $5 narrows the
// reset to one block for a caller that has reason to; no caller has today.
//
// The UPDATE rechecks the FULL TXID and not only the (leaf, ukey) the read found the row by,
// and that is a correctness rule rather than a repeated predicate. ukey is a 96-bit prefix and
// NON-UNIQUE by design -- see Pack -- so two transactions in one leaf can share it, and an
// UPDATE keyed on it alone would reset a stranger's coin to the unconfirmed sentinel: a
// spendable coin reading as immature, or a mined coin reading as mempool. Every other by-key
// write in this store rechecks txid for the same reason (spend.go, unspend.go, freeze.go).
//
// A coin that has been spent has no row and needs none: its restore resolves the block facts
// from membership at restore time.
const resetCoinsSQL = `
WITH hit AS (
    SELECT c.leaf, c.ukey, k.txid
      FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[]) AS k(leaf, txid, lo, hi)
     CROSS JOIN LATERAL (
       SELECT u.leaf, u.ukey
         FROM utxo u
        WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND u.ukey <= k.hi AND u.txid = k.txid
          AND u.mined_height > 0
          AND ($5::int IS NULL OR u.block_id = $5::int)
       OFFSET 0
     ) AS c
)
UPDATE utxo u SET mined_height = 0, block_id = 0
  FROM hit
 WHERE u.leaf = hit.leaf AND u.ukey = hit.ukey AND u.txid = hit.txid`

// SetMinedMulti marks transactions as mined in the block described by info.
//
// Three write paths, and the flags on info decide which runs rather than what the store
// holds. A block ON THE LONGEST CHAIN settles the transactions it names, so its stamp is also
// a move (stampAndMove). An UN-MINE takes the block back off the identity row (unstampOnly).
// Anything else is a fork stamp, which only appends the block (forkStamp). Un-mining wins
// over the longest-chain flag when a caller sets both, as it did before the move existed.
//
// Each path answers the postcondition the interface states -- every hash asked about appears
// in the result, or the call fails -- from three sources: the identity row (provePresentSQL),
// the identity row this call MOVED into the membership table, and the membership rows a
// transaction created by the block path already had.
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if len(hashes) == 0 {
		return map[chainhash.Hash][]uint32{}, nil
	}

	// Built once and used by every statement, so the set a stamp acted on and the set the
	// postcondition is checked against cannot disagree.
	txids := make([][]byte, 0, len(hashes))

	for _, h := range hashes {
		if h == nil {
			continue
		}

		txids = append(txids, h[:])
	}

	if len(txids) == 0 {
		return map[chainhash.Hash][]uint32{}, nil
	}

	entry := packMembership([]utxo.MinedBlockInfo{info})

	var (
		out map[chainhash.Hash][]uint32
		err error
	)

	switch {
	case info.UnsetMined:
		out, err = s.unstampAndMoveBack(ctx, txids, entry, info)
	case info.OnLongestChain:
		out, err = s.stampAndMove(ctx, txids, entry, info)
	default:
		out, err = s.forkStamp(ctx, txids, entry, info)
	}

	if err != nil {
		return nil, err
	}

	// Un-mining is exempt from the postcondition below, because the interface says missing
	// entries are tolerated there: a reorg may un-mine a transaction the store has already
	// discarded. Tolerated means it does not error, NOT that the answer is empty. Transactions
	// that DO still exist must still appear, which the conformance suite checks.
	if info.UnsetMined {
		return out, nil
	}

	for _, h := range hashes {
		if h == nil {
			continue
		}

		if _, ok := out[*h]; !ok {
			return nil, errors.NewTxNotFoundError("[utxoset][SetMinedMulti] %s", h.String())
		}
	}

	return out, nil
}

// stampAndMove appends this block to every listed identity row, clears their mempool markers,
// moves the rows the block has settled into the membership table, and answers for the rest.
//
// The statements are ONE TRANSACTION, and that is a correctness rule rather than a saved
// round trip. The read path stops at an identity hit (see lookup.go), so between a committed
// delete from tx_ident and a committed insert into tx_mined a concurrent reader finds the
// transaction in neither table and reports it missing -- which makes the validator reject
// every child of a parent that is merely being mined. Inside one transaction the two states
// are the only two a reader can see.
//
// ensureTxMinedPartition runs BEFORE the transaction opens, because the DDL needs its own pool
// connection; the same rule the create path follows. It is also what lets the residue append
// live inside the transaction: the window it inserts into is the one this call just created
// for the move, so the append needs no DDL of its own.
func (s *Store) stampAndMove(ctx context.Context, txids [][]byte, entry []byte,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if err := s.ensureTxMinedPartition(ctx, info.BlockHeight); err != nil {
		return nil, err
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] begin", err)
	}

	out, residue, err := s.stampMoveAndAppend(ctx, dbTx, txids, entry, info)
	if err != nil {
		_ = dbTx.Rollback(ctx)

		return nil, err
	}

	if err := dbTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] commit stamp and move", err)
	}

	// The block ids of an appended row are read after the commit, on the pool, and only for
	// the hashes that actually needed appending -- which on the ordinary tip path is none,
	// because moveToMinedSQL already answered for every row it moved. Reading committed state
	// through the ordinary membership step keeps "insertion order" a single definition; the
	// write it reports on is already durable, so nothing needs it inside the transaction.
	if len(residue) > 0 {
		ids, err := s.minedIDsByTxid(ctx, residue)
		if err != nil {
			return nil, err
		}

		for h, v := range ids {
			out[h] = v
		}
	}

	return out, nil
}

// stampMoveAndAppend is the body of the longest-chain transaction, split out so that every
// failure inside it leaves through one rollback. It returns the answers it established and the
// transactions still to be answered from the membership table.
//
// The order is fixed. stampSQL first, so moveToMinedSQL's single-block test sees the row as
// this block leaves it: a row that only ever saw this block moves, one that already carried a
// fork triple now carries two and stays. provePresentSQL after the move, so a row cannot be
// counted both as moved and as present.
func (s *Store) stampMoveAndAppend(ctx context.Context, dbTx pgx.Tx,
	txids [][]byte, entry []byte, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32,
	[][]byte, error) {
	groups := leafGroups(txids)

	for _, g := range groups {
		if _, err := dbTx.Exec(ctx, stampSQL, g.leaf, g.txids, entry, true); err != nil {
			return nil, nil, errors.NewStorageError("[utxoset][SetMinedMulti] stamp", err)
		}
	}

	out := make(map[chainhash.Hash][]uint32, len(txids))

	// Every row the move settled named exactly one block -- this one, which is what
	// moveToMinedSQL's equality against the stamped triple proves -- so its answer is known
	// without reading the membership table back.
	for _, g := range groups {
		moved, err := queryTxids(ctx, dbTx, moveToMinedSQL, g.leaf, g.txids,
			int32(info.BlockHeight), int32(info.BlockID), int32(info.SubtreeIdx), entry) //nolint:gosec // heights and ids fit
		if err != nil {
			return nil, nil, errors.NewStorageError("[utxoset][SetMinedMulti] move to membership", err)
		}

		for _, h := range moved {
			out[h] = []uint32{info.BlockID}
		}
	}

	if err := provePresentInto(ctx, dbTx, txids, out); err != nil {
		return nil, nil, err
	}

	// The residue: every transaction neither moved nor present on the identity table, because
	// it never held an identity row (the block path never writes one) or has since lost it.
	// A stamp for such a row is the retry path (Phase 1.5 on ErrTxExists) or a sibling block
	// at the same height. Append a membership row for this block if the transaction has any
	// membership row at all; a transaction in neither table is genuinely unknown.
	residue := absentTxids(txids, out)
	if len(residue) == 0 {
		return out, nil, nil
	}

	if _, err := dbTx.Exec(ctx, appendMinedSQL, residue, int32(info.BlockHeight),
		int32(info.BlockID), int32(info.SubtreeIdx)); err != nil { //nolint:gosec // heights and ids fit
		return nil, nil, errors.NewStorageError("[utxoset][SetMinedMulti] append membership", err)
	}

	return out, residue, nil
}

// forkStamp records a block that is NOT on the longest chain: every listed identity row gains
// the block and keeps its mempool marker, and nothing moves.
//
// No transaction wraps the statements, and that is a real difference from the longest-chain
// path rather than an oversight. Nothing here deletes. The stamp only appends to tx_ident, the
// append only inserts into tx_mined, and they act on disjoint sets of transactions, so there
// is no instant at which a reader finds a transaction in neither table -- which is the whole
// reason stampAndMove needs one.
func (s *Store) forkStamp(ctx context.Context, txids [][]byte, entry []byte,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	for _, g := range leafGroups(txids) {
		if _, err := s.pool.Exec(ctx, stampSQL, g.leaf, g.txids, entry, false); err != nil {
			return nil, errors.NewStorageError("[utxoset][SetMinedMulti] stamp", err)
		}
	}

	out := make(map[chainhash.Hash][]uint32, len(txids))
	if err := provePresentInto(ctx, s.pool, txids, out); err != nil {
		return nil, err
	}

	residue := absentTxids(txids, out)
	if len(residue) == 0 {
		return out, nil
	}

	if err := s.ensureTxMinedPartition(ctx, info.BlockHeight); err != nil {
		return nil, err
	}

	if _, err := s.pool.Exec(ctx, appendMinedSQL, residue, int32(info.BlockHeight),
		int32(info.BlockID), int32(info.SubtreeIdx)); err != nil { //nolint:gosec // heights and ids fit
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] append membership", err)
	}

	ids, err := s.minedIDsByTxid(ctx, residue)
	if err != nil {
		return nil, err
	}

	for h, v := range ids {
		out[h] = v
	}

	return out, nil
}

// unstampAndMoveBack takes one block back off every listed transaction, wherever its stamp
// lives: off the membership column for a transaction still in the mempool table, and out of
// the membership table -- back into the mempool table -- for one the longest-chain stamp had
// settled.
//
// The two statements are ONE TRANSACTION, and for the same reason stampAndMove's are: the read
// path stops at an identity hit, so between a committed delete from tx_mined and a committed
// insert into tx_ident a concurrent reader would find the transaction in neither table and
// report it missing, which makes the validator reject every child of the parent being
// un-mined. Inside one transaction the two states are the only two a reader can see.
//
// No ensureTxMinedPartition, and that is not an omission. Move-back only DELETES from tx_mined;
// the window it deletes from either exists, or the block was never stamped at that height and
// there is nothing to un-mine. Creating a window here would be actively wrong -- the floor
// exists to stop a retired window being recreated.
//
// Both statements run over the WHOLE key set rather than a residue split, because each is
// already confined to the rows the other cannot reach: unstampSQL touches only identity rows,
// and moveBackSQL only transactions that HOLD a membership row for this exact (height, block
// id), which a transaction with an identity row does not.
func (s *Store) unstampAndMoveBack(ctx context.Context, txids [][]byte,
	entry []byte, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	// A fresh clock from the current tip. See unstampSQL.
	height := int32(s.GetBlockHeight()) //nolint:gosec // a chain height fits int32

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] begin", err)
	}

	for _, g := range leafGroups(txids) {
		if _, err := dbTx.Exec(ctx, unstampSQL, g.leaf, g.txids, entry, height); err != nil {
			_ = dbTx.Rollback(ctx)

			return nil, errors.NewStorageError("[utxoset][SetMinedMulti] unstamp", err)
		}
	}

	moved, err := queryTxids(ctx, dbTx, moveBackSQL, txids, int32(info.BlockHeight),
		int32(info.BlockID), height) //nolint:gosec // heights and ids fit
	if err != nil {
		_ = dbTx.Rollback(ctx)

		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] move back to the mempool", err)
	}

	// The coins that are reset are the ones of the transactions that actually MOVED, not of
	// every hash named. An un-mine of a block a transaction was never in moves nothing, and
	// resetting its coins would un-confirm a coin whose block still contains it.
	//
	// No block id is passed, because a transaction that moved is back in the mempool table and
	// settles under no block at all -- not even a sibling that still names it. See
	// resetCoinsSQL.
	if len(moved) > 0 {
		if err := resetCoins(ctx, dbTx, txidsOf(moved), nil); err != nil {
			_ = dbTx.Rollback(ctx)

			return nil, err
		}
	}

	if err := dbTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] commit un-mine", err)
	}

	// Read after the commit, so a transaction the move-back returned to the mempool table
	// answers from the row it now has. A hash in neither table is answered by nobody, which
	// the un-mine tolerates.
	out := make(map[chainhash.Hash][]uint32, len(txids))
	if err := provePresentInto(ctx, s.pool, txids, out); err != nil {
		return nil, err
	}

	return out, nil
}

// txidsOf is the byte-slice form of a set of hashes, as the array-parameterised statements take.
func txidsOf(hashes []chainhash.Hash) [][]byte {
	out := make([][]byte, 0, len(hashes))

	for i := range hashes {
		out = append(out, hashes[i][:])
	}

	return out
}

// resetCoins puts the listed transactions' stamped coins back to the unconfirmed sentinel.
// blockID confines it to the coins of one block; nil resets every stamped coin they hold.
func resetCoins(ctx context.Context, q querier, txids [][]byte, blockID *int32) error {
	leaves, ids, los, his := liveCoinArgs(txids)

	if _, err := q.Exec(ctx, resetCoinsSQL, leaves, ids, los, his, blockID); err != nil {
		return errors.NewStorageError("[utxoset] reset coins to the unconfirmed sentinel", err)
	}

	return nil
}

// provePresentInto records, for every listed transaction that still holds an identity row,
// the blocks that row names. See provePresentSQL for why this is a statement of its own.
//
// One statement per LEAF GROUP, because provePresentSQL takes the leaf as a scalar. It no
// longer takes a leaves argument: leafGroups derives each leaf from the txid, which is what
// tx_ident_ck enforces anyway, so the two cannot disagree about which partition a row is in.
func provePresentInto(ctx context.Context, q querier, txids [][]byte,
	out map[chainhash.Hash][]uint32) error {
	for _, g := range leafGroups(txids) {
		if err := provePresentGroup(ctx, q, g, out); err != nil {
			return err
		}
	}

	return nil
}

// provePresentGroup is one leaf's worth of provePresentInto.
func provePresentGroup(ctx context.Context, q querier, g leafBatch,
	out map[chainhash.Hash][]uint32) error {
	rows, err := q.Query(ctx, provePresentSQL, g.leaf, g.txids)
	if err != nil {
		return errors.NewStorageError("[utxoset][SetMinedMulti] prove", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid       []byte
			membership []byte
		)

		if err := rows.Scan(&txid, &membership); err != nil {
			return errors.NewStorageError("[utxoset][SetMinedMulti] scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		ids, _, _ := unpackMembership(membership)
		out[h] = ids
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][SetMinedMulti] rows", err)
	}

	return nil
}

// absentTxids is the set of listed transactions the answer does not yet cover.
func absentTxids(txids [][]byte, out map[chainhash.Hash][]uint32) [][]byte {
	var residue [][]byte

	for _, txid := range txids {
		var h chainhash.Hash

		copy(h[:], txid)

		if _, ok := out[h]; !ok {
			residue = append(residue, txid)
		}
	}

	return residue
}

// queryTxids runs a statement whose result is one txid column and collects the hashes.
func queryTxids(ctx context.Context, q querier, stmt string, args ...any) ([]chainhash.Hash, error) {
	rows, err := q.Query(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var out []chainhash.Hash

	for rows.Next() {
		var txid []byte

		if err := rows.Scan(&txid); err != nil {
			return nil, err
		}

		var h chainhash.Hash

		copy(h[:], txid)

		out = append(out, h)
	}

	return out, rows.Err()
}

// leafBatch is one leaf partition and the transactions of a batch that route to it.
type leafBatch struct {
	leaf  int16
	txids [][]byte
}

// leafGroups splits a batch of transaction ids by the partition each routes to, so that every
// statement keyed on (leaf, txid) can run with the LEAF AS A SCALAR and the txids as an array.
//
// That key shape is not a style choice, it is the only one of the three that keeps the cost a
// function of the batch. Measured on this schema, postgres 16, 500 keys spread over all eight
// partitions, EXPLAIN (ANALYZE, BUFFERS), best of eight runs down one connection:
//
//	                            40,000 mempool rows      400,000 mempool rows
//	leaf = ANY(...), txid = ANY  5.0 ms, Seq Scan x8      41 ms, Seq Scan x8
//	join against unnest(l[],t[]) 9.3 ms, Hash Join + Seq   2.9 ms, index probes
//	leaf scalar, txid = ANY      0.33 ms per group         0.4 ms per group
//
// Both array forms read the mempool. leaf = ANY puts an array on the primary key's LEADING
// column, which makes the planner cost eight times five hundred index descents instead of five
// hundred, so it prefers a sequential scan and stays with it as the table grows -- 41 ms at
// 400,000 rows, where the index path it rejected runs in 15. The join form's plan is worse
// still in that it FLIPS: a hash of the whole table at mempool sizes, index probes only once
// the table is far larger than a mempool ever is, so its measured cost depends on statistics
// that move. With the leaf a scalar the partition is fixed, the array sits on the key's second
// column, and the plan is an index scan at both sizes.
//
// The groups come back in ASCENDING LEAF ORDER, which is a lock order rather than tidiness:
// every path that writes a batch of identity rows takes their row locks in the same sequence,
// so two concurrent batches sharing transactions cannot deadlock against each other.
func leafGroups(txids [][]byte) []leafBatch {
	var byLeaf [NumLeaves][][]byte

	for _, txid := range txids {
		leaf := LeafFor(txid)
		byLeaf[leaf] = append(byLeaf[leaf], txid)
	}

	out := make([]leafBatch, 0, NumLeaves)

	for leaf := range byLeaf {
		if len(byLeaf[leaf]) == 0 {
			continue
		}

		out = append(out, leafBatch{leaf: int16(leaf), txids: byLeaf[leaf]}) //nolint:gosec // a leaf index fits
	}

	return out
}

// minedIDsByTxid reads the block ids tx_mined records for a set of transactions, in insertion
// order. It goes through the read path's own membership step rather than a bespoke query, so
// the ids SetMinedMulti hands back and the ids an ordinary Get would report can never disagree
// about what "insertion order" means.
//
// A row that will not decode fails THIS CALL rather than being reported as a transaction
// claiming no blocks, which is the conservative reading here: refusing the stamp is
// recoverable, quietly stamping nothing is not. That is why the per-transaction failures are
// collected and returned instead of being handed back alongside the answers, as they are on
// the BatchDecorate path this result type was written for.
func (s *Store) minedIDsByTxid(ctx context.Context, txids [][]byte) (map[chainhash.Hash][]uint32, error) {
	hashes := make([]chainhash.Hash, 0, len(txids))

	for _, txid := range txids {
		var h chainhash.Hash

		copy(h[:], txid)

		hashes = append(hashes, h)
	}

	res := newLookupResult(len(hashes))
	if err := s.readMinedInto(ctx, hashes, &res); err != nil {
		return nil, err
	}

	for _, err := range res.failed {
		return nil, err
	}

	out := make(map[chainhash.Hash][]uint32, len(res.found))
	for h, d := range res.found {
		out[h] = d.BlockIDs
	}

	return out, nil
}
