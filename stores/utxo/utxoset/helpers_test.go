package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// hashBytes is the transaction id as the store stores it.
func hashBytes(tx *bt.Tx) []byte {
	h := tx.TxIDChainHash()

	return h[:]
}

// hashes wraps one transaction as the slice SetMinedMulti takes.
func hashes(tx *bt.Tx) []*chainhash.Hash {
	return []*chainhash.Hash{tx.TxIDChainHash()}
}

func identExists(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) bool {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_ident WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	return n > 0
}

// spendOneOutput builds a transaction taking one of parent's outputs and applies the spend at
// height, leaving the spender unmined.
func spendOneOutput(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx, vout uint32,
	height uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))
	child.AddOutput(&bt.Output{
		Satoshis:      parent.Outputs[vout].Satoshis - 1_000,
		LockingScript: parent.Outputs[vout].LockingScript,
	})

	_, err := s.Create(ctx, child, height)
	require.NoError(t, err)

	_, err = spendOnly(ctx, s, child, height)
	require.NoError(t, err)

	return child
}

// createDirect writes one transaction through the single create path, in a transaction of its
// own, whatever batcher the store is configured with.
//
// createIn needs a pgx.Tx because the claim's advisory lock is transaction-scoped, so a test
// that wants the single path for setup has to supply one. Setup that went through s.Create
// instead would wait on a flush window and depend on the code under test.
func createDirect(s *Store, ctx context.Context, tx *bt.Tx, height uint32) error {
	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}

	if _, err := s.createIn(ctx, dbTx, tx, height, height); err != nil {
		_ = dbTx.Rollback(ctx)

		return err
	}

	return dbTx.Commit(ctx)
}

// insertCollidingCoin writes a coin row that SHARES another transaction's packed key: the same
// first twelve bytes of txid, so the same leaf and the same ukey, with a different full
// 32-byte txid.
//
// Pack is a 96-bit prefix and NON-UNIQUE by design (see its comment in schema.go), so this row
// is legal and this collision is the one an attacker can buy with 2^48 of work. Any by-key
// write that does not recheck the full txid will hit it, which is what the tests using this
// helper are for. It returns the other transaction id so the caller can read the row back.
func insertCollidingCoin(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx,
	minedHeight, blockID int32) []byte {
	t.Helper()

	other := append([]byte(nil), hashBytes(tx)...)
	// Byte 0 is untouched, so the leaf is the same; bytes 12 onward are outside the packed key,
	// so the ukey is the same too.
	other[31] ^= 0xff

	ukey := Pack(hashBytes(tx), 0)

	_, err := s.pool.Exec(ctx, `
		INSERT INTO utxo (satoshis, created_height, spendable_from, mined_height, block_id,
		                  leaf, flags, ukey, txid, script)
		VALUES (1000, 100, 0, $1, $2, $3, 0, $4, $5,
		        '\x76a914000000000000000000000000000000000000000088ac'::bytea)`,
		minedHeight, blockID, LeafFor(other), ukey, other)
	require.NoError(t, err)

	return other
}

// coinFactsOf reads the block facts off the one coin row carrying this exact txid.
func coinFactsOf(t *testing.T, s *Store, ctx context.Context, txid []byte) (minedHeight, blockID int32) {
	t.Helper()

	lo, hi := Pack(txid, 0), Pack(txid, ^uint32(0))
	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT mined_height, block_id FROM utxo
		 WHERE leaf = $1 AND ukey >= $2 AND ukey <= $3 AND txid = $4`,
		LeafFor(txid), lo, hi, txid).Scan(&minedHeight, &blockID))

	return minedHeight, blockID
}

// plantConflictNote records one (parent, child) contest directly, at a height whose window it
// creates first. Setup that went through SetConflicting would depend on the code under test.
func plantConflictNote(t *testing.T, s *Store, ctx context.Context, height uint32,
	parent, child []byte) {
	t.Helper()

	require.NoError(t, s.ensureSpendJournalPartition(ctx, height))

	_, err := s.pool.Exec(ctx, `
        INSERT INTO conflict_children (noted_height, parent_txid, child_txid)
        VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, int32(height), parent, child)
	require.NoError(t, err)
}

// conflictChildrenOf reads the children recorded against one parent, across every live window,
// deduplicated the way the read path deduplicates them.
func conflictChildrenOf(t *testing.T, s *Store, ctx context.Context, parent []byte) [][]byte {
	t.Helper()

	rows, err := s.pool.Query(ctx, `
        SELECT DISTINCT child_txid FROM conflict_children
         WHERE parent_txid = $1 ORDER BY child_txid`, parent)
	require.NoError(t, err)

	defer rows.Close()

	var out [][]byte

	for rows.Next() {
		var c []byte
		require.NoError(t, rows.Scan(&c))
		out = append(out, c)
	}

	require.NoError(t, rows.Err())

	return out
}
