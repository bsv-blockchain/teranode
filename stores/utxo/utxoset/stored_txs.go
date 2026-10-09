package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// storedTxsSQL asks the identity table which of a set of transactions it holds, one array per
// leaf, in one statement.
//
// The question comes from a block path below the checkpoint, about the transactions it left
// out of the create wave because they have no spendable output, on every block, so it has to
// cost next to nothing for a set that is almost always absent. A create with no block
// information, which is how an earlier failed attempt stores such a transaction, takes the
// identity claim, and the transaction has no UTXO rows to find it by, so its identity row is
// the record. Marking it mined leaves that row in place; only the stamp moves it, see
// StoredTxs.
//
// Each branch names its leaf as a constant, so it prunes to one partition and puts the array
// on the primary key's second column: an index probe per hash, index-only once the visibility
// map is set, because txid is the only column read and both key columns are in the primary
// key. leaf = ANY across the whole set is what set_mined.go measured flipping to a sequential
// scan, so it is not used. Where a partition is small against the array the planner reads the
// partition whole instead, which is fewer pages than the descents; measured at 1,000 hashes per
// leaf against 25,000 rows per leaf. A branch whose array is empty finds nothing. $1 to $8 are
// the leaves' arrays, in leaf order.
const storedTxsSQL = `
SELECT txid FROM tx_ident WHERE leaf = 0 AND txid = ANY($1::bytea[])
UNION ALL SELECT txid FROM tx_ident WHERE leaf = 1 AND txid = ANY($2::bytea[])
UNION ALL SELECT txid FROM tx_ident WHERE leaf = 2 AND txid = ANY($3::bytea[])
UNION ALL SELECT txid FROM tx_ident WHERE leaf = 3 AND txid = ANY($4::bytea[])
UNION ALL SELECT txid FROM tx_ident WHERE leaf = 4 AND txid = ANY($5::bytea[])
UNION ALL SELECT txid FROM tx_ident WHERE leaf = 5 AND txid = ANY($6::bytea[])
UNION ALL SELECT txid FROM tx_ident WHERE leaf = 6 AND txid = ANY($7::bytea[])
UNION ALL SELECT txid FROM tx_ident WHERE leaf = 7 AND txid = ANY($8::bytea[])`

// StoredTxs answers which of hashes have an identity row. Its signature is the optional
// utxo.StoredTxProber capability, which it satisfies structurally: the store does not name it.
//
// It is narrower than BatchDecorate on purpose. BatchDecorate walks every tier for a hash it
// cannot find, about six array statements including probes of the UTXO table and the undo
// copies, and the hashes this is asked about are almost always absent. An unmined transaction
// is always on the identity table, so that is the one place asked.
//
// What it does not see: a transaction the stamp has already moved to tx_mined. SetMinedMulti
// leaves the identity row where it is; the stamp moves it once the block that mined it is 288
// deep. For the caller's case, a transaction an earlier failed attempt at a block stored as
// unmined, that means some block containing it is already 288 deep, so it is mined and the
// retry's mark is not what records it.
func (s *Store) StoredTxs(ctx context.Context, hashes []*chainhash.Hash) ([]chainhash.Hash, error) {
	var byLeaf [NumLeaves][][]byte

	n := 0

	for _, h := range hashes {
		if h == nil {
			continue
		}

		leaf := LeafFor(h[:])
		byLeaf[leaf] = append(byLeaf[leaf], h[:])
		n++
	}

	if n == 0 {
		return nil, nil
	}

	args := make([]any, NumLeaves)
	for i := range byLeaf {
		if byLeaf[i] == nil {
			byLeaf[i] = [][]byte{}
		}

		args[i] = byLeaf[i]
	}

	rows, err := s.pool.Query(ctx, storedTxsSQL, args...)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][StoredTxs]", err)
	}

	defer rows.Close()

	var out []chainhash.Hash

	for rows.Next() {
		var txid []byte

		if err := rows.Scan(&txid); err != nil {
			return nil, errors.NewStorageError("[utxoset][StoredTxs] scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		out = append(out, h)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][StoredTxs] rows", err)
	}

	return out, nil
}
