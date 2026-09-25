package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// recordMinedSQL records that one block contains each listed transaction: one containment row
// per transaction, inserted and never rewritten.
//
// This is the whole of what "record mined" writes. It does NOT move the identity row, does not
// touch a UTXO, and does not care whether the block is on the longest chain; the caller's flag
// decides only the marker write that follows. Two callers recording the same block in either
// order, or the same caller replaying it, leave identical contents, because the insert does
// nothing where the row exists. Nothing here may rank rows by arrival: the insertion counter
// the table used to carry was read as "the earliest row is the winner" and stamped reorg losers
// onto UTXOs.
//
// The payload -- size, fee, inpoints, locktime, created_at, created_height and flags -- is what
// a lookup needs to answer from the row alone once the identity row is gone, and it is copied
// from the identity row when there is one. Otherwise it is copied from an existing containment
// row of the same transaction at or above the lookup floor ($6), which is the coinbase's case on
// the reorg path and the case of a block re-offered under a fresh id: the row that carries
// tx_inpoints is preferred, then the lowest (mined_height, block_id), so the choice is
// deterministic and a thin block-path row never outranks a full one. A transaction with neither
// has no payload to copy and is not inserted, which is what lets the postcondition still catch a
// transaction the store does not hold. The floor keeps the probe to the same partitions at any
// stamp lag; every existing row this read must find is near the tip (a re-offered block is at
// its first attempt's height, and the two duplicate coinbases are 158 and 30 blocks apart).
//
// THE LEAF IS A SCALAR AND THE TXIDS AN ARRAY, so this runs once per leaf group. See leafGroups
// for the measurements: it is the only one of the three key shapes whose cost is a function of
// the batch rather than of the identity table. The two arms sit inside a LATERAL with an
// OFFSET 0 fence, which is the shape every other by-txid probe of tx_mined in this store takes,
// and for the same reason: written as a join the planner is free to hash the keys against a
// whole window.
const recordMinedSQL = `
INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                      size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
SELECT k.txid, $3::int, $4::int, $5::int, p.created_height, p.size_in_bytes, p.fee,
       p.tx_inpoints, p.locktime, p.created_at, p.flags
  FROM unnest($2::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   (SELECT 1 AS pri, i.created_height, i.size_in_bytes, i.fee, i.tx_inpoints, i.locktime,
           i.created_at, i.flags
      FROM tx_ident i
     WHERE i.leaf = $1::smallint AND i.txid = k.txid)
   UNION ALL
   (SELECT 2, m.created_height, m.size_in_bytes, m.fee, m.tx_inpoints, m.locktime,
           m.created_at, m.flags
      FROM tx_mined m
     WHERE m.txid = k.txid
       AND m.mined_height >= $6::int
     ORDER BY (m.tx_inpoints IS NULL), m.mined_height, m.block_id
     LIMIT 1)
   ORDER BY pri LIMIT 1 OFFSET 0
 ) AS p
ON CONFLICT (txid, mined_height, block_id) DO NOTHING`

// clearMarkerSQL clears the unmined marker of every listed identity row, because the caller
// has said a block on the longest chain contains the transaction. A transaction with no
// identity row has no marker to clear and is untouched. Same key shape as recordMinedSQL.
const clearMarkerSQL = `
UPDATE tx_ident i
   SET off_chain_since = NULL
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])`

// unMineSQL is the un-mine: a POINT DELETE on the full containment key, for the one block the
// caller has stopped believing in. It cannot reach another block's row, so a transaction that
// is also contained in a sibling block keeps that sibling's row, and a transaction the caller
// un-mines from a block it was never in matches nothing.
//
// That reverses the earlier rule that an un-mine deleted EVERY containment row of the
// transaction so that it lived in exactly one table. Containment has one home now and the
// identity row stays put through mining, so there is no move to reverse and no second home
// to keep clear.
//
// It RETURNS the deleted rows' transaction ids and flags, and both feed things the caller
// checks. The ids say whose UTXOs the narrowed reset below may touch. The flags carry the
// coinbase bit, which the guard counter needs: a deleted row whose transaction has no identity
// row has just lost its only payload, and no un-mine should ever reach such a transaction
// unless it is a coinbase (see unMine).
const unMineSQL = `
DELETE FROM tx_mined m
 WHERE m.txid = ANY($1::bytea[])
   AND m.mined_height = $2::int
   AND m.block_id = $3::int
RETURNING m.txid, m.flags`

// setMarkerSQL puts every listed identity row back in the unmined set with a FRESH clock.
//
// The clock is the store's current tip, NOT the transaction's creation height, and that is the
// fact that decides whether these two columns are one concept or two. A transaction created at
// height 100 and un-mined while the tip is 5,000 must wait from 5,000, or the preservation pass
// fires on it immediately. Both reference stores do the same. It RETURNS the ids it reached,
// which is how the caller tells an un-mined transaction that has an identity row from one that
// has not.
const setMarkerSQL = `
UPDATE tx_ident i
   SET off_chain_since = $3::int
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])
RETURNING i.txid`

// SetMinedMulti records the block described by info against the listed transactions, or takes
// it back off them when info.UnsetMined is set.
//
// Recording mined is ONE insert plus, when the caller says the block is on the longest chain,
// one marker write. The insert is identical either way. There is no second code path for a
// fork block, no rule about which call came first, and nothing moves between tables: the
// identity row of a transaction seen before its block stays where it is until the deep stamp
// of build step 5 deletes it. Un-mining wins over the longest-chain flag when a caller sets
// both, as it always has.
//
// The answer is the full block id set per transaction, read back from tx_mined after the
// write commits, in (mined_height, block_id) order. That is how the postcondition the interface
// states is met: every hash asked about appears, or the call fails. A transaction with neither
// an identity row nor an existing containment row has no payload to copy and is not inserted,
// so it is absent from the read-back and reported as not found.
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if len(hashes) == 0 {
		return map[chainhash.Hash][]uint32{}, nil
	}

	// Built once and used by every statement, so the set a write acted on and the set the
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

	// The read-back's floor is the lower of the lookup floor and the start of the window
	// holding this block, so the call always sees the row it has just written, or the row a
	// fenced replay found, even when the stamp is more than a window late.
	floor := s.lookupFloor()
	if w := int32(info.BlockHeight / TxMinedPartitionBlocks * TxMinedPartitionBlocks); w < floor { //nolint:gosec // a height fits int32
		floor = w
	}

	if info.UnsetMined {
		if err := s.unMine(ctx, txids, info); err != nil {
			return nil, err
		}

		// Un-mining is exempt from the postcondition below, because the interface says missing
		// entries are tolerated there: a reorg may un-mine a transaction the store has already
		// discarded. Tolerated means it does not error, NOT that the answer is empty.
		// Transactions that DO still exist must still appear, which the conformance suite
		// checks, and they answer from whatever containment they have left.
		return s.minedIDsByTxid(ctx, txids, floor)
	}

	quiet, err := s.recordMined(ctx, txids, info)
	if err != nil {
		return nil, err
	}

	out, err := s.minedIDsByTxid(ctx, txids, floor)
	if err != nil {
		return nil, err
	}

	// A success below the fence that inserted nothing reports the submitted block id for every
	// hash, or the caller's coverage check (model/update-tx-mined.go) would count a gap and the
	// retry loop the quiet outcome exists to end would run anyway.
	if quiet {
		for _, h := range hashes {
			if h == nil {
				continue
			}

			ids := out[*h]

			found := false

			for _, id := range ids {
				if id == info.BlockID {
					found = true

					break
				}
			}

			if !found {
				out[*h] = append(ids, info.BlockID)
			}
		}

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

// recordMined inserts the containment rows and, on the longest chain, clears the markers. It
// reports quiet = true when it wrote no containment row on purpose, below the fence.
//
// ensureTxMinedPartition runs BEFORE the transaction opens, because the DDL needs its own pool
// connection; the same rule the create path follows. It refuses a dropped window, so a call
// that names one never reaches the fence.
//
// The fence lock is the transaction's first statement, and the fence is read as its second.
// Below the fence the rule is: a block off the longest chain inserts nothing and returns
// success, counted, because a valid fork block deeper than 288 that cannot win is still
// recorded by block validation and must settle; a block on the longest chain whose rows all
// exist is a replay, which skips the insert and still writes the markers; a block on the
// longest chain with any row absent is the boundary error, because the main chain cannot run
// 288 blocks past a block whose containment is unwritten, so that state means a refusal was
// bypassed.
//
// The insert and the marker write are ONE TRANSACTION so a reader never sees a transaction
// whose marker is clear before its containment row exists. The reverse order of exposure --
// containment present, marker still set -- is a state the store already tolerates, because a
// fork block followed by a main-chain block produces it, and block assembly's load fix-up
// repairs it.
func (s *Store) recordMined(ctx context.Context, txids [][]byte, info utxo.MinedBlockInfo) (quiet bool, err error) {
	if err := s.ensureTxMinedPartition(ctx, info.BlockHeight); err != nil {
		return false, err
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, errors.NewStorageError("[utxoset][SetMinedMulti] begin", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	fence, err := s.takeFenceShared(ctx, dbTx)
	if err != nil {
		return false, err
	}

	insert := true

	if fence.dropped(info.BlockHeight) {
		if info.OnLongestChain {
			return false, boundaryError("record_mined", "block %d at height %d is on the longest chain and its window is dropped; its mined status was never written, so a refusal above was bypassed",
				info.BlockID, info.BlockHeight)
		}

		fenceNoops.WithLabelValues("dropped_skip").Inc()

		return true, nil
	}

	if fence.fenced(info.BlockHeight) {
		n, err := fencedRowCount(ctx, dbTx, txids, info.BlockHeight, info.BlockID)
		if err != nil {
			return false, err
		}

		switch {
		case n == int64(len(txids)):
			// A replay of a write that already happened. The insert is skipped; the marker
			// write goes ahead as on any replay.
		case info.OnLongestChain:
			return false, boundaryError("record_mined", "block %d at height %d is on the longest chain, below the stamp fence %d, and %d of its %d transactions have no containment row",
				info.BlockID, info.BlockHeight, fence.fence, int64(len(txids))-n, len(txids))
		default:
			fenceNoops.WithLabelValues("off_chain_insert").Inc()
		}

		insert = false
		quiet = true
	}

	floor := s.lookupFloor()

	for _, g := range leafGroups(txids) {
		if insert {
			if _, err := dbTx.Exec(ctx, recordMinedSQL, g.leaf, g.txids,
				int32(info.BlockHeight), int32(info.BlockID), int32(info.SubtreeIdx), floor); err != nil { //nolint:gosec // heights and ids fit
				return false, errors.NewStorageError("[utxoset][SetMinedMulti] record mined", err)
			}
		}

		if !info.OnLongestChain {
			continue
		}

		if _, err := dbTx.Exec(ctx, clearMarkerSQL, g.leaf, g.txids); err != nil {
			return false, errors.NewStorageError("[utxoset][SetMinedMulti] clear marker", err)
		}
	}

	if err := dbTx.Commit(ctx); err != nil {
		return false, errors.NewStorageError("[utxoset][SetMinedMulti] commit record mined", err)
	}

	return quiet, nil
}

// unMine takes one block back off the listed transactions: a point delete of that block's
// containment rows and a fresh unmined clock on every listed identity row. No UTXO is touched.
//
// A block at or below the highest checkpoint is REFUSED. Below the checkpoint every UTXO is
// born from a block-path create with its pair written at birth, and "final at birth" is only
// true if nothing un-mines a checkpoint-certified block. The point delete would remove the
// containment row and leave those UTXOs carrying a pair for a block that is no longer on the
// chain, with no identity row to write a marker on and nothing to correct the pair from. So
// the store refuses, every row unchanged, rather than handling it with a UTXO reset. Above the
// checkpoint the store applies the checkpoint test to creates itself, so no UTXO there has a
// pair a reorg could leave stale: a block-carrying create above the checkpoint writes (0,0)
// and an identity row, and only the deep stamp, 288 blocks down, writes a pair. That is why the
// UTXO reset the un-mine used to run is gone rather than narrowed.
//
// No ensureTxMinedPartition, and that is not an omission. The un-mine only DELETES from
// tx_mined; the window it deletes from either exists, or the block was never recorded at that
// height and there is nothing to un-mine. Creating a window here would be actively wrong -- the
// floor exists to stop a retired window being recreated.
//
// The marker is set on every listed identity row, whether or not this block's row was there to
// delete, as the un-mine has always done: setting it wrongly costs a mined transaction reloaded
// as unmined, which the consistency scan repairs, while leaving it wrongly NULL would lose the
// transaction from block assembly for good. The known cost is that un-mining a FORK block sets
// the marker of a transaction that is still on the main chain; whether that stays is an open
// decision of the design, and until it is answered this is the recommended form.
//
// A deleted row whose transaction has NO identity row, and whose coinbase bit is clear, is
// counted. Such a transaction is one of four things -- created through the block path at or
// below the checkpoint, a coinbase, seeded, or stamped -- and no un-mine should ever reach any
// of them. The delete has destroyed that transaction's only payload, which is why the counter
// must stay at zero and why it is an abort criterion of the soak.
func (s *Store) unMine(ctx context.Context, txids [][]byte, info utxo.MinedBlockInfo) error {
	if model.BelowCheckpoint(s.checkpoints, info.BlockHeight) {
		return errors.NewProcessingError("[utxoset][SetMinedMulti] refusing to un-mine block %d at height %d: the height is at or below the highest checkpoint %d, where every UTXO carries its block from birth and nothing could correct it; an invalidation there needs a resync, not an un-mine",
			info.BlockID, info.BlockHeight, model.HighestCheckpointHeight(s.checkpoints))
	}

	// A fresh clock from the current tip. See setMarkerSQL.
	height := int32(s.GetBlockHeight()) //nolint:gosec // a chain height fits int32

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][SetMinedMulti] begin", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	// Below the fence a row that exists is a winner, and deleting it would take the chain
	// out from under a stamped UTXO: the boundary error. An un-mine that finds no row there
	// is an invalid deep fork block whose row was a loser and is already gone: allowed and
	// counted, so its mined status can settle. A dropped window is skipped the same way.
	fence, err := s.takeFenceShared(ctx, dbTx)
	if err != nil {
		return err
	}

	del := true

	switch {
	case fence.dropped(info.BlockHeight):
		fenceNoops.WithLabelValues("dropped_skip").Inc()

		return nil
	case fence.fenced(info.BlockHeight):
		n, err := fencedRowCount(ctx, dbTx, txids, info.BlockHeight, info.BlockID)
		if err != nil {
			return err
		}

		if n > 0 {
			return boundaryError("un_mine", "block %d at height %d is below the stamp fence %d and %d of its transactions still have a containment row there; every row below the fence names a winner",
				info.BlockID, info.BlockHeight, fence.fence, n)
		}

		fenceNoops.WithLabelValues("unmine_absent").Inc()

		del = false
	}

	var deleted []txidFlags

	if del {
		deleted, err = queryTxidFlags(ctx, dbTx, unMineSQL, txids,
			int32(info.BlockHeight), int32(info.BlockID)) //nolint:gosec // heights and ids fit
		if err != nil {
			return errors.NewStorageError("[utxoset][SetMinedMulti] un-mine", err)
		}
	}

	reached := make(map[chainhash.Hash]struct{}, len(txids))

	for _, g := range leafGroups(txids) {
		marked, err := queryTxids(ctx, dbTx, setMarkerSQL, g.leaf, g.txids, height)
		if err != nil {
			return errors.NewStorageError("[utxoset][SetMinedMulti] set marker", err)
		}

		for _, h := range marked {
			reached[h] = struct{}{}
		}
	}

	if err := dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset][SetMinedMulti] commit un-mine", err)
	}

	for i := range deleted {
		if _, ok := reached[deleted[i].txid]; ok {
			continue
		}

		if deleted[i].flags&FlagCoinbase != 0 {
			continue
		}

		noIdentityReached.WithLabelValues("un_mine").Inc()
	}

	return nil
}

// txidFlags is one (transaction id, flags) pair as the containment statements return it.
type txidFlags struct {
	txid  chainhash.Hash
	flags int16
}

// queryTxidFlags runs a statement whose result is a txid column and a flags column.
func queryTxidFlags(ctx context.Context, q querier, stmt string, args ...any) ([]txidFlags, error) {
	rows, err := q.Query(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var out []txidFlags

	for rows.Next() {
		var (
			txid  []byte
			flags int16
		)

		if err := rows.Scan(&txid, &flags); err != nil {
			return nil, err
		}

		var r txidFlags

		copy(r.txid[:], txid)
		r.flags = flags

		out = append(out, r)
	}

	return out, rows.Err()
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
//	                            40,000 identity rows     400,000 identity rows
//	leaf = ANY(...), txid = ANY  5.0 ms, Seq Scan x8      41 ms, Seq Scan x8
//	join against unnest(l[],t[]) 9.3 ms, Hash Join + Seq   2.9 ms, index probes
//	leaf scalar, txid = ANY      0.33 ms per group         0.4 ms per group
//
// Both array forms read the identity table. leaf = ANY puts an array on the primary key's
// LEADING column, which makes the planner cost eight times five hundred index descents instead
// of five hundred, so it prefers a sequential scan and stays with it as the table grows -- 41 ms
// at 400,000 rows, where the index path it rejected runs in 15. The join form's plan is worse
// still in that it FLIPS: a hash of the whole table at small sizes, index probes only once the
// table is far larger, so its measured cost depends on statistics that move. With the leaf a
// scalar the partition is fixed, the array sits on the key's second column, and the plan is an
// index scan at both sizes.
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

// minedIDsByTxid reads the block ids tx_mined records for a set of transactions, in
// (mined_height, block_id) order. It goes through the read path's own containment step rather
// than a bespoke query, so the ids SetMinedMulti hands back and the ids an ordinary Get would
// report can never disagree about the order.
//
// A row that will not decode fails THIS CALL rather than being reported as a transaction
// claiming no blocks, which is the conservative reading here: refusing the record is
// recoverable, quietly recording nothing is not. That is why the per-transaction failures are
// collected and returned instead of being handed back alongside the answers, as they are on
// the BatchDecorate path this result type was written for.
func (s *Store) minedIDsByTxid(ctx context.Context, txids [][]byte, floor int32) (map[chainhash.Hash][]uint32, error) {
	hashes := make([]chainhash.Hash, 0, len(txids))

	for _, txid := range txids {
		var h chainhash.Hash

		copy(h[:], txid)

		hashes = append(hashes, h)
	}

	res := newLookupResult(len(hashes))
	if err := s.readMinedInto(ctx, hashes, &res, floor); err != nil {
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
