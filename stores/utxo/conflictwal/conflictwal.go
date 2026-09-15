// Package conflictwal implements the store-agnostic conflict-resolution
// write-ahead log (WAL) shared by SQL-backed UTXO stores — crash safety for
// ProcessConflicting / ReverseProcessConflicting (see #861).
//
// One row per in-flight operation lives in the conflict_intents table: inserted
// before the operation's first state mutation and deleted once its terminal step
// commits. Rows that survive a restart drive replay. The logic here is engine-
// branched (postgres / sqlite) and operates on a plain *usql.DB so any backend
// with access to a SQL database can reuse it: the SQL UTXO store uses its own
// connection, and the TeraSlab store (whose server cannot hold arbitrary intent
// records) opens a dedicated SQL connection for the WAL alone.
package conflictwal

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/usql"
)

// CreateTable creates the conflict_intents table if it does not exist, using the
// engine-appropriate column types. Backends whose schema is managed elsewhere
// (e.g. the SQL UTXO store) need not call this; backends that open a dedicated
// WAL database (e.g. TeraSlab) do.
func CreateTable(db *usql.DB, engine string) error {
	var ddl string
	if engine == "postgres" {
		ddl = `CREATE TABLE IF NOT EXISTS conflict_intents (
		     intent_id     BYTEA PRIMARY KEY
		    ,kind          TEXT NOT NULL
		    ,block_height  BIGINT NOT NULL
		    ,block_hash    BYTEA NOT NULL
		    ,tx_hashes     BYTEA NOT NULL
		    ,started_at    BIGINT NOT NULL
		);`
	} else {
		ddl = `CREATE TABLE IF NOT EXISTS conflict_intents (
		     intent_id     BLOB PRIMARY KEY
		    ,kind          TEXT NOT NULL
		    ,block_height  BIGINT NOT NULL
		    ,block_hash    BLOB NOT NULL
		    ,tx_hashes     BLOB NOT NULL
		    ,started_at    BIGINT NOT NULL
		);`
	}

	if _, err := db.Exec(ddl); err != nil {
		return errors.NewStorageError("[conflictwal] failed to create conflict_intents table", err)
	}

	return nil
}

// encodeIntentHashes flattens the intent's tx hashes into a single byte slice
// (32 bytes each) for storage in the tx_hashes column.
func encodeIntentHashes(hashes []chainhash.Hash) []byte {
	buf := make([]byte, 0, len(hashes)*chainhash.HashSize)
	for i := range hashes {
		buf = append(buf, hashes[i][:]...)
	}

	return buf
}

// decodeIntentHashes splits a stored tx_hashes blob back into chainhash values.
func decodeIntentHashes(buf []byte) ([]chainhash.Hash, error) {
	if len(buf)%chainhash.HashSize != 0 {
		return nil, errors.NewStorageError("conflict_intents tx_hashes blob length %d is not a multiple of %d", len(buf), chainhash.HashSize)
	}

	hashes := make([]chainhash.Hash, 0, len(buf)/chainhash.HashSize)
	for off := 0; off < len(buf); off += chainhash.HashSize {
		var h chainhash.Hash
		copy(h[:], buf[off:off+chainhash.HashSize])
		hashes = append(hashes, h)
	}

	return hashes, nil
}

// Begin durably records an intent before the operation's first state mutation.
// Idempotent on the deterministic intent id: re-recording the same intent is a
// no-op rather than a duplicate-key error.
func Begin(ctx context.Context, db *usql.DB, engine string, intent utxo.ConflictIntent) error {
	intentID := intent.IntentID()
	blockHash := intent.BlockHash

	var q string
	if engine == "postgres" {
		q = `INSERT INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at)
		     VALUES ($1, $2, $3, $4, $5, $6)
		     ON CONFLICT (intent_id) DO NOTHING`
	} else {
		q = `INSERT OR IGNORE INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at)
		     VALUES ($1, $2, $3, $4, $5, $6)`
	}

	if _, err := db.ExecContext(ctx, q,
		intentID[:],
		string(intent.Kind),
		int64(intent.BlockHeight),
		blockHash[:],
		encodeIntentHashes(intent.TxHashes),
		intent.StartedAt,
	); err != nil {
		return errors.NewStorageError("[conflictwal] failed to record intent %s", intentID.String(), err)
	}

	return nil
}

// Complete removes the intent record once the terminal step committed. Removing
// an absent intent is idempotent (no error).
func Complete(ctx context.Context, db *usql.DB, intentID chainhash.Hash) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM conflict_intents WHERE intent_id = $1`, intentID[:]); err != nil {
		return errors.NewStorageError("[conflictwal] failed to remove intent %s", intentID.String(), err)
	}

	return nil
}

// Pending returns every begun-but-not-completed intent.
func Pending(ctx context.Context, db *usql.DB) ([]utxo.ConflictIntent, error) {
	rows, err := db.QueryContext(ctx, `SELECT kind, block_height, block_hash, tx_hashes, started_at FROM conflict_intents`)
	if err != nil {
		return nil, errors.NewStorageError("[conflictwal] pending query failed", err)
	}
	defer rows.Close()

	var intents []utxo.ConflictIntent

	for rows.Next() {
		var (
			kind        string
			blockHeight int64
			blockHash   []byte
			txHashes    []byte
			startedAt   int64
		)

		if err := rows.Scan(&kind, &blockHeight, &blockHash, &txHashes, &startedAt); err != nil {
			return nil, errors.NewStorageError("[conflictwal] pending scan failed", err)
		}

		bh, err := chainhash.NewHash(blockHash)
		if err != nil {
			return nil, errors.NewStorageError("[conflictwal] corrupt block_hash (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
		}

		hashes, err := decodeIntentHashes(txHashes)
		if err != nil {
			return nil, errors.NewStorageError("[conflictwal] corrupt intent row (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
		}

		intents = append(intents, utxo.ConflictIntent{
			Kind:        utxo.ConflictIntentKind(kind),
			BlockHeight: uint32(blockHeight),
			BlockHash:   *bh,
			TxHashes:    hashes,
			StartedAt:   startedAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[conflictwal] pending row iteration failed", err)
	}

	return intents, nil
}
