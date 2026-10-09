package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// markOnLongestChainSQL moves transactions into or out of the unmined set, and reports which
// rows it actually reached.
//
// One statement of arrays, so repairing a thousand transactions costs the same round trip as
// repairing one. RETURNING is not decoration here: the caller is fixing an inconsistency, and
// a row that was silently skipped leaves that inconsistency in place, so it has to know which
// rows moved.
//
// off_chain_since is the only column this touches. Containment is NOT rewritten, no row moves
// between tables and no UTXO is reset, because this call does not claim to know which blocks a
// transaction is in. It answers one narrower question, which is whether the chain the node
// currently believes in contains it.
//
// THE LEAF IS A SCALAR AND THE TXIDS AN ARRAY, so this runs once per leaf group. See
// leafGroups: it is the only key shape here whose cost is a function of the batch rather than
// of the identity table, and txid = ANY is exact on its own because tx_ident_ck makes leaf a
// function of txid.
const markOnLongestChainSQL = `
UPDATE tx_ident i
   SET off_chain_since = $3::int
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])
RETURNING i.txid`

// minedPresentSQL names the listed transactions that hold at least one containment row, with
// the flags of one such row.
//
// The keys sit outside a LATERAL with an OFFSET 0 fence, the same fence minedByTxidSQL needs,
// so this is one index descent per key per live window rather than a read of the whole
// containment set. It runs only when a mark call missed a hash, so it is off the ordinary path.
const minedPresentSQL = `
SELECT k.txid, p.flags
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT m.flags
     FROM tx_mined m
    WHERE m.txid = k.txid
    LIMIT 1 OFFSET 0
 ) AS p`

// MarkTransactionsOnLongestChain records whether the node's current chain contains these
// transactions.
//
// Block assembly calls this at startup, as a repair. It walks the transactions still marked
// as waiting to be mined, finds any that already carry containment of a block on the main
// chain, and hands them here to have the marker cleared. Until this existed the node could
// not start at all once one such transaction was in the store, which is exactly the state an
// ordinary sync produces.
//
// It writes ONE nullable integer on the identity row and nothing else. It used to move rows
// between the identity table and the containment table in both directions and reset UTXOs on
// the way back; none of that exists any more, because containment has one home and the
// identity row stays put through mining until the deep stamp of build step 5 deletes it.
//
// onLongestChain true clears the marker. False sets it to the CURRENT tip rather than to the
// transaction's creation height, which is the same rule the un-mine path follows: a
// transaction created at height 100 and put back in the unmined set while the tip is 5,000
// must wait from 5,000, or the preservation pass fires on it immediately. It is set
// unconditionally rather than only when NULL: a stale marker from long ago would make a valid
// moved-back transaction look long overdue and retention could delete it, so resetting the
// clock is the cheap direction.
//
// A hash with containment and no identity row is one of four things -- created through the
// block path at or below the checkpoint, a coinbase, seeded, or already stamped -- and no
// chain change the node admits should mark any of them. Mark-on counts it as reached, as it
// always has, so a repeated repair stays idempotent. Mark-off reports it as ErrTxNotFound in
// the joined error, as it always has, and counts every non-coinbase one, because reaching it
// means a reorg has touched something that should be beyond a reorg's reach.
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
		reached, err = s.markOn(ctx, txids)
	} else {
		reached, err = s.markOff(ctx, txids)
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

// markOn clears the unmined marker of every listed identity row.
//
// A transaction with containment and no identity row is already in the state this call asks
// for, so it counts as reached even though no marker was written. Without this the call would
// not be idempotent for a transaction created through the block path, and block assembly's
// startup repair would report every such transaction as one the store does not hold. The probe
// runs only when something was missed, so the ordinary path costs one statement per leaf group.
func (s *Store) markOn(ctx context.Context, txids [][]byte) ([]chainhash.Hash, error) {
	var reached []chainhash.Hash

	for _, g := range leafGroups(txids) {
		// NULL, because the chain contains these transactions.
		marked, err := queryTxids(ctx, s.pool, markOnLongestChainSQL, g.leaf, g.txids, nil)
		if err != nil {
			return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] update", err)
		}

		reached = append(reached, marked...)
	}

	if len(reached) == len(txids) {
		return reached, nil
	}

	missed := absentTxids(txids, reached)

	also, err := queryTxidFlags(ctx, s.pool, minedPresentSQL, missed)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] probe containment", err)
	}

	for i := range also {
		reached = append(reached, also[i].txid)
	}

	return reached, nil
}

// markOff sets the unmined marker of every listed identity row to the current tip.
//
// A missed hash that has containment is NOT counted as reached, so the caller reports it, and
// every such non-coinbase transaction increments the guard counter: see
// MarkTransactionsOnLongestChain for why no mark-off should ever reach one.
func (s *Store) markOff(ctx context.Context, txids [][]byte) ([]chainhash.Hash, error) {
	// The current tip, not created_height. See MarkTransactionsOnLongestChain.
	height := int32(s.GetBlockHeight()) //nolint:gosec // a chain height fits int32

	var reached []chainhash.Hash

	for _, g := range leafGroups(txids) {
		marked, err := queryTxids(ctx, s.pool, markOnLongestChainSQL, g.leaf, g.txids, height)
		if err != nil {
			return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] update", err)
		}

		reached = append(reached, marked...)
	}

	if len(reached) == len(txids) {
		return reached, nil
	}

	missed := absentTxids(txids, reached)

	contained, err := queryTxidFlags(ctx, s.pool, minedPresentSQL, missed)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] probe containment", err)
	}

	for i := range contained {
		if contained[i].flags&FlagCoinbase != 0 {
			continue
		}

		noIdentityReached.WithLabelValues("mark_off").Inc()
	}

	return reached, nil
}

// absentTxids is the set of listed transactions the reached set does not cover.
func absentTxids(txids [][]byte, reached []chainhash.Hash) [][]byte {
	seen := make(map[chainhash.Hash]struct{}, len(reached))
	for _, h := range reached {
		seen[h] = struct{}{}
	}

	var out [][]byte

	for _, txid := range txids {
		var h chainhash.Hash

		copy(h[:], txid)

		if _, ok := seen[h]; !ok {
			out = append(out, txid)
		}
	}

	return out
}
