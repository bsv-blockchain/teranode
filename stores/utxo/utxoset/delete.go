package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// deleteTxSQL removes every trace of a transaction: its coins, the undo records for those
// coins, its serialized bytes and its identity row.
//
// One statement of arrays, so deleting one transaction and deleting a thousand run the same
// code, exactly as spendJournalSQL and createIdentPlanSQL / createMinedPlanSQL do on the write
// paths.
//
// Data-modifying common table expressions share one snapshot, so the five deletes commit
// together. That matters for the same reason it mattered to createIdentPlanSQL and
// createMinedPlanSQL: a half-deleted transaction, its identity or membership row gone and its
// coins left, is a live output that nothing can ever reclaim, because reclaim finds coins
// through their identity or membership row.
//
// Coins are found by a RANGE over the packed key rather than by transaction id. The one index
// on that table is on the key, and the key packs the transaction id prefix first precisely so
// that "every output of parent P" is a range scan. See Pack in schema.go. The full 32-byte id
// is still rechecked, because the 96-bit prefix is non-unique by design and can locate a row
// but never authorise acting on one.
//
// The undo records this destroys are the ones where the deleted transaction is the PARENT,
// meaning the payloads for its own already-spent outputs. They must go. Left behind, a later
// unspend would restore a coin whose identity row no longer exists: spendable, invisible to
// reclaim, and permanent.
//
// Records where the deleted transaction is the SPENDER are deliberately left alone. They
// authorise restoring OTHER transactions' coins, and the offline rewind tool unspends before
// it deletes, so destroying them here would turn an ordering mistake into unrecoverable coin
// loss. They are inert anyway, and retire with their partition.
//
// The body is reached through created_height rather than by transaction id, because
// created_height leads the body's primary key and the id alone cannot use it. This is
// createIdentPlanSQL's and createMinedPlanSQL's claim-gates-the-body join, run backwards, and it
// has to run backwards from BOTH: a mempool transaction's body is claimed by tx_ident, but a
// block-path transaction's is claimed by tx_mined instead (see createMinedPlanSQL), and either
// one may have written a tx_body row. gone -- the UNION of what ident and mined actually deleted
// -- is therefore the only complete set of (created_height, txid) pairs to join tx_body against.
// "May" rather than "always" since utxostore_skipTxBodyBelowCheckpoint: a transaction mined at
// or below the checkpoint has no body to reach, and a DELETE that matches nothing is not an
// error, so the join is unchanged either way.
// Joining through ident alone left a mined-only transaction's body behind: no tx_ident row
// exists to gate it, so nothing found it, and it sat there until its 288-block body window aged
// out on its own, contradicting Delete's promise to remove every trace immediately.
//
// mined removes the transaction's membership rows, tx_mined's replacement for the identity row
// on a mined transaction. It is keyed by txid alone -- tx_mined has no leaf column, and its
// primary key leads with txid, so this is an index descent per live window rather than a scan.
// Left behind, a deleted-but-still-a-member transaction would misreport itself as present to
// any later lookup that consults tx_mined, exactly the resurrection the identity delete above
// already guards against for a mempool transaction. It RETURNs created_height and txid for the
// same reason ident does: to feed the body join.
//
// preserved removes the preservation copy of the membership row, if the pruner ever took one.
// It is a COPY rather than a claim, so it contributes nothing to gone and nothing to the count:
// a transaction known only by a preserved row is one whose real rows are already gone. Left
// behind, it would keep answering lookups for a transaction Delete promised to remove every
// trace of, which is the same resurrection the identity and membership deletes guard against.
//
// A transaction can hold several tx_mined rows -- one per window its coins are still claimed
// through, or a coinbase re-org claiming it at more than one height -- all sharing the same
// created_height, so gone runs them through UNION rather than a plain concatenation to collapse
// them to one (created_height, txid) pair before the join.
const deleteTxSQL = `
WITH k AS (
    SELECT * FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[])
        AS t(leaf, txid, lo, hi)
),
coins AS (
    DELETE FROM utxo u USING k
     WHERE u.leaf  = k.leaf
       AND u.ukey >= k.lo
       AND u.ukey <= k.hi
       AND u.txid  = k.txid
),
undo AS (
    DELETE FROM spend_journal j USING k
     WHERE j.ukey >= k.lo
       AND j.ukey <= k.hi
       AND j.txid  = k.txid
),
ident AS (
    DELETE FROM tx_ident i USING k
     WHERE i.leaf = k.leaf AND i.txid = k.txid
    RETURNING i.created_height, i.txid
),
mined AS (
    DELETE FROM tx_mined m USING k
     WHERE m.txid = k.txid
    RETURNING m.created_height, m.txid
),
preserved AS (
    DELETE FROM preserved_parent p USING k
     WHERE p.txid = k.txid
),
gone AS (
    SELECT created_height, txid FROM ident
    UNION
    SELECT created_height, txid FROM mined
),
body AS (
    DELETE FROM tx_body b USING gone g
     WHERE b.created_height = g.created_height AND b.txid = g.txid
)
SELECT count(DISTINCT txid) FROM gone`

// Delete removes a transaction and everything the store holds about it.
//
// A transaction the store does not hold is SUCCESS rather than an error, and that is the
// contract both reference stores keep. Every caller depends on it. The offline rewind tool
// tolerates not-found explicitly, and block assembly's reorg tolerates ONLY a not-found error,
// so returning anything else here aborts a reorg.
//
// Not batched, deliberately. This fires on a reorg and from operator tools, never at validator
// rate, and a batcher would buy nothing but a window in which a caller believes a transaction
// is gone before it is.
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	if hash == nil {
		return errors.NewProcessingError("[utxoset][Delete] nil hash")
	}

	return s.deleteIn(ctx, s.pool, []chainhash.Hash{*hash})
}

// deleteIn is Delete against an arbitrary querier and over a batch, so a future multi-delete
// and any composition inside a database transaction share these predicates rather than growing
// a second copy of them. A single Delete is a batch of one.
func (s *Store) deleteIn(ctx context.Context, q querier, hashes []chainhash.Hash) error {
	if len(hashes) == 0 {
		return nil
	}

	leaves := make([]int16, 0, len(hashes))
	txids := make([][]byte, 0, len(hashes))
	los := make([][16]byte, 0, len(hashes))
	his := make([][16]byte, 0, len(hashes))

	for i := range hashes {
		leaves = append(leaves, LeafFor(hashes[i][:]))
		txids = append(txids, hashes[i][:])
		los = append(los, Pack(hashes[i][:], 0))
		his = append(his, Pack(hashes[i][:], ^uint32(0)))
	}

	// The count is read rather than discarded so that a future batch form can tell its caller
	// what went, but it is deliberately NOT compared against the number of hashes offered. A
	// transaction the store never held deletes nothing, and that is the contract rather than a
	// miss. It counts distinct transactions found through EITHER path -- a mempool transaction
	// via tx_ident or a block-path transaction via tx_mined -- not identity rows alone, so a
	// mined-only delete is not silently reported as zero.
	var removed int64

	if err := q.QueryRow(ctx, deleteTxSQL, leaves, txids, los, his).Scan(&removed); err != nil {
		return errors.NewStorageError("[utxoset][Delete] remove", err)
	}

	return nil
}
