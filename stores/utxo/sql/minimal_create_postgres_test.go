package sql

import (
	"database/sql"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// pgQueryOutputUTXOHash reads utxo_hash for (txHash, vout) using Postgres ($1) placeholders.
func pgQueryOutputUTXOHash(t *testing.T, db *sql.DB, txHash *chainhash.Hash, vout int) []byte {
	t.Helper()
	row := db.QueryRow(`
		SELECT o.utxo_hash
		FROM outputs o JOIN transactions tx ON tx.id = o.transaction_id
		WHERE tx.hash = $1 AND o.idx = $2`, txHash[:], vout)
	var h []byte
	require.NoError(t, row.Scan(&h))
	return h
}

// pgQueryInputRow reads (previous_tx_satoshis, previous_tx_script, previous_transaction_hash,
// previous_tx_idx) for one input using Postgres ($1) placeholders.
func pgQueryInputRow(t *testing.T, db *sql.DB, txHash *chainhash.Hash, idx int) (int64, []byte, []byte, int32) {
	t.Helper()
	row := db.QueryRow(`
		SELECT i.previous_tx_satoshis, i.previous_tx_script, i.previous_transaction_hash, i.previous_tx_idx
		FROM inputs i JOIN transactions tx ON tx.id = i.transaction_id
		WHERE tx.hash = $1 AND i.idx = $2`, txHash[:], idx)
	var (
		sats     int64
		script   []byte
		prevHash []byte
		prevIdx  int32
	)
	require.NoError(t, row.Scan(&sats, &script, &prevHash, &prevIdx))
	return sats, script, prevHash, prevIdx
}

// TestMinimalCreate_Postgres_CreateCTE exercises the minimal-create path through the
// PRODUCTION Postgres write path — createCTE / buildInputArrays with skipExtended —
// which the sqlitememory tests cannot reach (they only hit createInputsBatched /
// createInputsPerRow). This is the exact backend the below-checkpoint fast path
// targets in production, so a UNNEST array/type bug in the nil-script branch would
// otherwise ship untested. Runs only against a real Postgres testcontainer (skipped
// in -short).
func TestMinimalCreate_Postgres_CreateCTE(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	// Force the createCTE path (multi-value CTE + UNNEST), the production default.
	store.settings.UtxoStore.BatchSQLOperations = true
	require.Equal(t, "postgres", store.engine, "precondition: store is postgres-backed")

	db := store.db.DB

	// Parent (decorated) created via createCTE with the flag OFF.
	parent := newExtendedTxWithOutputs(t, 2)
	_, err := store.Create(ctx, parent, 1)
	require.NoError(t, err)

	// Child input is DECORATED (real parent satoshis + script) so we prove the flag
	// forces the columns to zero through the CTE path, not merely round-trips zeros.
	child := newExtendedSpendingTx(t, parent, 0)
	addOutputs(t, child, 1)
	require.NotZero(t, child.Inputs[0].PreviousTxSatoshis, "precondition: input is decorated")
	require.NotNil(t, child.Inputs[0].PreviousTxScript, "precondition: input is decorated")

	meta, err := store.Create(ctx, child, 2, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err, "minimal create via createCTE must not call GetFees")
	require.Equal(t, uint64(0), meta.Fee, "fee is zero below checkpoint")

	// Output fully persisted with its own (parent-independent) utxo_hash.
	wantHash, err := util.UTXOHashFromOutput(child.TxIDChainHash(), child.Outputs[0], 0)
	require.NoError(t, err)
	require.Equal(t, wantHash[:], pgQueryOutputUTXOHash(t, db, child.TxIDChainHash(), 0),
		"createCTE must persist the output utxo_hash unchanged")

	// Input parent columns zeroed by skipExtended, but the outpoint is retained.
	sats, script, prevHash, prevIdx := pgQueryInputRow(t, db, child.TxIDChainHash(), 0)
	require.Equal(t, int64(0), sats, "createCTE skipExtended must zero previous_tx_satoshis")
	require.Empty(t, script, "createCTE skipExtended must null previous_tx_script")
	require.Equal(t, child.Inputs[0].PreviousTxIDChainHash()[:], prevHash, "outpoint retained for pruner")
	require.Equal(t, int32(child.Inputs[0].PreviousTxOutIndex), prevIdx, "outpoint idx retained")

	// Flag OFF through the SAME createCTE path must persist decoration unchanged
	// (byte-identity of the input columns when the fast path is disabled).
	other := newExtendedSpendingTx(t, parent, 1)
	addOutputs(t, other, 1)
	_, err = store.Create(ctx, other, 2)
	require.NoError(t, err)
	satsOff, scriptOff, _, _ := pgQueryInputRow(t, db, other.TxIDChainHash(), 0)
	require.Equal(t, int64(other.Inputs[0].PreviousTxSatoshis), satsOff, "flag off persists real satoshis via createCTE")
	require.NotEmpty(t, scriptOff, "flag off persists the parent script via createCTE")
}
