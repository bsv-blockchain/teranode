package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// markOnLongestChainSQL moves transactions into or out of the mempool set, and reports which
// rows it actually reached.
//
// One statement of arrays, so repairing a thousand transactions costs the same round trip as
// repairing one. RETURNING is not decoration here: the caller is fixing an inconsistency, and
// a row that was silently skipped leaves that inconsistency in place, so it has to know which
// rows moved.
//
// off_chain_since is the only column this touches. Block membership is NOT rewritten, because
// this call does not claim to know which blocks a transaction is in. It answers one narrower
// question, which is whether the chain the node currently believes in contains it.
//
// THE LEAF IS A SCALAR AND THE TXIDS AN ARRAY, so this runs once per leaf group. See
// leafGroups: it is the only key shape here whose cost is a function of the batch rather than
// of the mempool, and txid = ANY is exact on its own because tx_ident_ck makes leaf a function
// of txid.
const markOnLongestChainSQL = `
UPDATE tx_ident i
   SET off_chain_since = $3::int
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])
RETURNING i.txid`

// singleTripleHeightsSQL reads the block height out of the membership of every listed identity
// row that names exactly one block -- which is exactly the set moveSingleToMinedSQL will move.
//
// It exists because of an ordering constraint rather than for information: the membership
// windows those rows will land in have to be created before the transaction opens, since the
// DDL needs its own pool connection, and their heights are not known until the triples are
// decoded. So the heights are read first, the windows created, and only then does the
// transaction that marks and moves begin.
const singleTripleHeightsSQL = `
SELECT DISTINCT ((get_byte(i.membership, 4)::bigint << 24)
               | (get_byte(i.membership, 5)::bigint << 16)
               | (get_byte(i.membership, 6)::bigint <<  8)
               |  get_byte(i.membership, 7)::bigint)::int AS height
  FROM tx_ident i
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])
   AND octet_length(i.membership) = 12`

// moveSingleToMinedSQL settles the identity rows that name exactly ONE block, taking the
// block's facts from the triple the row itself carries.
//
// It is moveToMinedSQL with the parameters removed, and the removal is the point. The stamp
// path is TOLD which block settled the transaction, so it can test the membership against that
// block's packed triple and write the parameters straight through. This call is told nothing
// but "the chain now contains these transactions", so the only block it can name is the one
// the row already claims -- which is sound only when the row claims exactly one. A row naming
// two stays where it is with its marker cleared, because nothing here can say which of them is
// main; it waits for an un-mine or a further stamp to reduce it to one. A row carrying
// conflicting children no longer stays: that bookkeeping is a side table keyed on the txid
// (see conflict_children in schema.go), so it survives the move.
//
// The height, block id and subtree index are decoded big-endian from the twelve bytes, in the
// order mh_triple and packMembership write them, with the bigint casts mh_max needs: an int4
// shift of 255 by 24 wraps negative, silently.
//
// $3 is the LOWEST HEIGHT still covered by a membership window, and a row naming anything
// below it is left exactly where it is. Its window has been dropped and cannot be recreated:
// the floor exists to stop a retired window claiming its transactions afresh and doubling every
// coin still live in it. A single-block row that old is ordinary fork residue -- roughly 300
// blocks of it is what block assembly's startup reload hands this call -- so refusing to settle
// it must not refuse the marker clear for every other hash in the batch. See markOnAndSettle.
//
// One leaf group at a time, for the reason spelled out on leafGroups, and the same
// RETURNING-from-the-DELETE shape moveToMinedSQL uses so a replayed block cannot make a row
// that HAS left tx_ident look as though it stayed.
const moveSingleToMinedSQL = `
WITH moved AS (
    DELETE FROM tx_ident i
     WHERE i.leaf = $1::smallint
       AND i.txid = ANY($2::bytea[])
       AND octet_length(i.membership) = 12
       AND ((get_byte(i.membership, 4)::bigint << 24)
          | (get_byte(i.membership, 5)::bigint << 16)
          | (get_byte(i.membership, 6)::bigint <<  8)
          |  get_byte(i.membership, 7)::bigint) >= $3::bigint
    RETURNING i.txid, i.membership, i.created_height, i.size_in_bytes, i.fee,
              i.tx_inpoints, i.locktime, i.created_at, i.flags
),
settled AS (
    INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                          size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
    SELECT m.txid,
           ((get_byte(m.membership,  4)::bigint << 24)
          | (get_byte(m.membership,  5)::bigint << 16)
          | (get_byte(m.membership,  6)::bigint <<  8)
          |  get_byte(m.membership,  7)::bigint)::int,
           ((get_byte(m.membership,  0)::bigint << 24)
          | (get_byte(m.membership,  1)::bigint << 16)
          | (get_byte(m.membership,  2)::bigint <<  8)
          |  get_byte(m.membership,  3)::bigint)::int,
           ((get_byte(m.membership,  8)::bigint << 24)
          | (get_byte(m.membership,  9)::bigint << 16)
          | (get_byte(m.membership, 10)::bigint <<  8)
          |  get_byte(m.membership, 11)::bigint)::int,
           m.created_height, m.size_in_bytes, m.fee, m.tx_inpoints, m.locktime,
           m.created_at, m.flags
      FROM moved m
    ON CONFLICT (txid, mined_height, block_id) DO NOTHING
)
SELECT txid FROM moved`

// minedPresentSQL names the listed transactions that hold at least one membership row.
//
// The keys sit outside a LATERAL with an OFFSET 0 fence, the same fence minedByTxidSQL needs,
// so this is one index descent per key per live window rather than a read of the whole
// membership set.
const minedPresentSQL = `
SELECT k.txid
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT 1
     FROM tx_mined m
    WHERE m.txid = k.txid
    LIMIT 1 OFFSET 0
 ) AS p`

// MarkTransactionsOnLongestChain records whether the node's current chain contains these
// transactions.
//
// Block assembly calls this at startup, as a repair. It walks the transactions still marked
// as waiting to be mined, finds any that already carry membership of a block on the main
// chain, and hands them here to have the marker cleared. Until this existed the node could
// not start at all once one such transaction was in the store, which is exactly the state an
// ordinary sync produces.
//
// The call now MOVES rows as well as marking them, in both directions, and it has to: the
// mempool table and the membership table are the two halves of one set, and a transaction is
// in whichever half matches what the node last learned about the chain. On the longest chain
// with exactly one block claimed, the row settles into the membership table. Off the longest
// chain, every membership row it holds comes back to the mempool table, its blocks remembered
// as fork triples. Marking a row without moving it would leave a settled transaction that
// nothing can un-mine, and an un-mined one that no unmined-transaction reload can find.
//
// onLongestChain true clears the marker. False sets it to the CURRENT tip rather than to the
// transaction's creation height, which is the same rule the un-mine path follows: a
// transaction created at height 100 and put back in the mempool while the tip is 5,000 must
// wait from 5,000, or the preservation pass fires on it immediately.
//
// A hash the store does not hold is an error rather than a silent skip, matching the sql
// store. The rows it DID reach are still updated, so a partial input repairs what it can.
func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash,
	onLongestChain bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	txids := make([][]byte, 0, len(txHashes))
	for i := range txHashes {
		txids = append(txids, txHashes[i][:])
	}

	var (
		reached []chainhash.Hash
		err     error
	)

	if onLongestChain {
		reached, err = s.markOnAndSettle(ctx, txids)
	} else {
		reached, err = s.markOffAndMoveBack(ctx, txids)
	}

	if err != nil {
		return err
	}

	seen := make(map[chainhash.Hash]struct{}, len(reached))
	for _, h := range reached {
		seen[h] = struct{}{}
	}

	if len(seen) == len(txHashes) {
		return nil
	}

	// Named but absent. Reported rather than swallowed, because the caller asked for a repair
	// and a row it could not reach is a repair that did not happen.
	missing := make([]error, 0, 4)

	for i := range txHashes {
		if _, ok := seen[txHashes[i]]; ok {
			continue
		}

		// Bounded, so one bad batch cannot produce an error the size of a block.
		if len(missing) < 10 {
			missing = append(missing,
				errors.NewTxNotFoundError("[utxoset][MarkTransactionsOnLongestChain] %s", txHashes[i].String()))
		}
	}

	return errors.Join(missing...)
}

// markOnAndSettle clears the mempool marker and settles every row the clearing leaves naming
// exactly one block.
//
// The mark and the move are ONE TRANSACTION, for the reason stampAndMove's are: a settle
// deletes an identity row and inserts a membership row, and between two committed statements a
// concurrent reader would find the transaction in neither table and report it missing.
//
// ensureTxMinedPartition runs before the transaction opens, because the DDL needs its own pool
// connection. Its heights are only known once the single triples have been DECODED, so the
// order is forced: read the heights (singleTripleHeightsSQL), create the windows, then
// Begin -> mark -> move -> Commit.
//
// A height whose window has already been DROPPED is skipped rather than refused. The floor
// makes ensureTxMinedPartition reject it, and that rejection used to abort the whole call
// before the marker clear had run -- so one stale fork triple, which is precisely what block
// assembly's startup reload turns up, could leave the node unable to start. Those rows keep
// their identity row and get their marker cleared, which is the repair the caller asked for;
// only the settle is skipped, and moveSingleToMinedSQL's own floor guard is what skips it.
func (s *Store) markOnAndSettle(ctx context.Context, txids [][]byte) ([]chainhash.Hash, error) {
	groups := leafGroups(txids)

	heights, err := s.singleTripleHeights(ctx, groups)
	if err != nil {
		return nil, err
	}

	floor, err := s.txMinedFloor(ctx)
	if err != nil {
		return nil, err
	}

	// The lowest height any live window can still cover.
	minHeight := floor * TxMinedPartitionBlocks
	dropped := 0

	for _, h := range heights {
		if h < minHeight {
			dropped++

			continue
		}

		if err := s.ensureTxMinedPartition(ctx, h); err != nil {
			return nil, err
		}
	}

	if dropped > 0 {
		s.logger.Warnf("[utxoset][MarkTransactionsOnLongestChain] %d single-block height(s) below the membership floor %d: marker cleared, rows left in the mempool table", dropped, minHeight)
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] begin", err)
	}

	var reached []chainhash.Hash

	for _, g := range groups {
		// NULL, because the chain contains these transactions.
		marked, err := queryTxids(ctx, dbTx, markOnLongestChainSQL, g.leaf, g.txids, nil)
		if err != nil {
			_ = dbTx.Rollback(ctx)

			return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] update", err)
		}

		reached = append(reached, marked...)
	}

	for _, g := range groups {
		if _, err := dbTx.Exec(ctx, moveSingleToMinedSQL, g.leaf, g.txids, int64(minHeight)); err != nil {
			_ = dbTx.Rollback(ctx)

			return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] settle", err)
		}
	}

	if err := dbTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] commit mark and settle", err)
	}

	if len(reached) == len(txids) {
		return reached, nil
	}

	// A transaction ALREADY in the membership table is already in the state this call asks
	// for, so it counts as reached even though no identity row moved. Without this the call
	// would not be idempotent: settling a row here deletes its identity row, and a repeat of
	// the same repair -- an ordinary startup after an interrupted one -- would then report
	// every transaction it had already fixed as one the store does not hold. The probe runs
	// only when something was missed, so the ordinary path still costs two statements.
	also, err := queryTxids(ctx, s.pool, minedPresentSQL, txids)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] probe membership", err)
	}

	return append(reached, also...), nil
}

// markOffAndMoveBack sets the mempool marker and brings every membership row these
// transactions hold back to the mempool table.
//
// ONE TRANSACTION, for the reason markOnAndSettle is. No ensureTxMinedPartition: the move-back
// only deletes from the membership table, and see unstampAndMoveBack for why creating a window
// here would be wrong.
//
// It runs the SAME statement the un-mine runs, with the block height and id NULL. That is the
// whole difference between the two directions: naming a block drops that block's triple from
// the membership the identity row comes back with, naming none keeps them all. See moveBackSQL.
//
// The move-back writes the marker itself, on the rows it inserts, so it does not matter that
// the mark statement ran before those rows existed.
func (s *Store) markOffAndMoveBack(ctx context.Context, txids [][]byte) ([]chainhash.Hash, error) {
	// The current tip, not created_height. See MarkTransactionsOnLongestChain.
	height := int32(s.GetBlockHeight()) //nolint:gosec // a chain height fits int32

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] begin", err)
	}

	var reached []chainhash.Hash

	for _, g := range leafGroups(txids) {
		marked, err := queryTxids(ctx, dbTx, markOnLongestChainSQL, g.leaf, g.txids, height)
		if err != nil {
			_ = dbTx.Rollback(ctx)

			return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] update", err)
		}

		reached = append(reached, marked...)
	}

	// NULL block height and id: this call names no block, so moveBackSQL keeps every triple.
	moved, err := queryTxids(ctx, dbTx, moveBackSQL, txids, nil, nil, height)
	if err != nil {
		_ = dbTx.Rollback(ctx)

		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] move back to the mempool", err)
	}

	// Only the transactions that actually MOVED, and with no block id: each is back in the
	// mempool table and settles under nothing at all, so every one of its stamped coins goes to
	// the sentinel. A hash that held no membership row moved nothing and keeps its coins.
	if len(moved) > 0 {
		if err := resetCoins(ctx, dbTx, txidsOf(moved), nil); err != nil {
			_ = dbTx.Rollback(ctx)

			return nil, err
		}
	}

	if err := dbTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] commit mark and move back", err)
	}

	return append(reached, moved...), nil
}

// singleTripleHeights is the set of block heights named by the listed identity rows that hold
// exactly one triple. See singleTripleHeightsSQL for why it is read separately.
//
// It reads OUTSIDE the transaction that then moves those rows, so a concurrent stamp can append
// a triple, or an un-mine remove one, between the read and the move. That is acceptable here and
// only here: this is a startup repair, and both outcomes are safe. A row that gained a second
// triple no longer satisfies moveSingleToMinedSQL's single-triple test and simply stays, and a
// row whose height changed is one whose window this call may not have created -- in which case
// the INSERT fails and the whole transaction rolls back, losing nothing. A stamp path could not
// tolerate that gap, which is why stampAndMove takes its height from the caller instead.
func (s *Store) singleTripleHeights(ctx context.Context, groups []leafBatch) ([]uint32, error) {
	var out []uint32

	for _, g := range groups {
		heights, err := s.singleTripleHeightsIn(ctx, g)
		if err != nil {
			return nil, err
		}

		out = append(out, heights...)
	}

	return out, nil
}

// singleTripleHeightsIn is one leaf group's worth, split out so the rows are closed by a
// return rather than at the end of the whole loop.
func (s *Store) singleTripleHeightsIn(ctx context.Context, g leafBatch) ([]uint32, error) {
	rows, err := s.pool.Query(ctx, singleTripleHeightsSQL, g.leaf, g.txids)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] read single triples", err)
	}

	defer rows.Close()

	var out []uint32

	for rows.Next() {
		var height int32

		if err := rows.Scan(&height); err != nil {
			return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] scan single triple", err)
		}

		out = append(out, uint32(height)) //nolint:gosec // a stored height is never negative
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] single triple rows", err)
	}

	return out, nil
}
