package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/usql"
)

// This file implements the conflict-resolution write-ahead log (WAL) for the
// TeraSlab backend — crash safety for ProcessConflicting / ReverseProcessConflicting
// (#861). The TeraSlab server exposes only UTXO-record-shaped RPCs, so it cannot
// hold the WAL; the store opens a dedicated SQL connection (Postgres in prod,
// SQLite for dev — see New) and keeps the intents in a conflict_intents table on
// s.walDB / s.walEngine. The logic mirrors the SQL UTXO backend's inline WAL
// (stores/utxo/sql/conflict_wal.go) — teraslab shares the UTXO-store contract, not
// the SQL store, so it keeps its own copy against walDB rather than a shared pkg.

// createConflictWALTable creates the conflict_intents table (with the recovery
// column) on the dedicated WAL connection. The table is teraslab-private and the
// WAL store is created fresh (the sqlitememory default is per-run), so the column
// is declared directly rather than added by a migration.
func createConflictWALTable(db *usql.DB, engine string) error {
	var ddl string
	if engine == "postgres" {
		ddl = `CREATE TABLE IF NOT EXISTS conflict_intents (
		     intent_id     BYTEA PRIMARY KEY
		    ,kind          TEXT NOT NULL
		    ,block_height  BIGINT NOT NULL
		    ,block_hash    BYTEA NOT NULL
		    ,tx_hashes     BYTEA NOT NULL
		    ,started_at    BIGINT NOT NULL
		    ,recovery      BYTEA
		);`
	} else {
		ddl = `CREATE TABLE IF NOT EXISTS conflict_intents (
		     intent_id     BLOB PRIMARY KEY
		    ,kind          TEXT NOT NULL
		    ,block_height  BIGINT NOT NULL
		    ,block_hash    BLOB NOT NULL
		    ,tx_hashes     BLOB NOT NULL
		    ,started_at    BIGINT NOT NULL
		    ,recovery      BLOB
		);`
	}

	if _, err := db.Exec(ddl); err != nil {
		return errors.NewStorageError("[teraslab] failed to create conflict_intents table", err)
	}

	// Migrate a conflict_intents table created by a pre-recovery build of this
	// branch (CREATE TABLE IF NOT EXISTS above skips an existing table, so it would
	// keep the old schema and BeginConflictIntent's insert into recovery would
	// fail). Mirrors the SQL backend's recovery-column migration.
	if engine == "postgres" {
		if _, err := db.Exec(`ALTER TABLE conflict_intents ADD COLUMN IF NOT EXISTS recovery BYTEA`); err != nil {
			return errors.NewStorageError("[teraslab] failed to add conflict_intents recovery column", err)
		}

		return nil
	}

	// SQLite has no ADD COLUMN IF NOT EXISTS, so add it only when absent.
	var recoveryColumns int
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('conflict_intents') WHERE name = 'recovery'`).Scan(&recoveryColumns); err != nil {
		return errors.NewStorageError("[teraslab] failed to inspect conflict_intents schema", err)
	}

	if recoveryColumns == 0 {
		if _, err := db.Exec(`ALTER TABLE conflict_intents ADD COLUMN recovery BLOB`); err != nil {
			return errors.NewStorageError("[teraslab] failed to add conflict_intents recovery column", err)
		}
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

// BeginConflictIntent durably records an intent (including its recovery
// before-image, if any) before the operation's first state mutation. Idempotent
// on the deterministic intent id: re-inserting the same intent is a no-op rather
// than a duplicate-key error.
func (s *Store) BeginConflictIntent(ctx context.Context, intent utxo.ConflictIntent) error {
	intentID := intent.IntentID()

	var recoveryData []byte
	if intent.Recovery != nil {
		var err error
		recoveryData, err = utxo.EncodeConflictRecovery(intent.Recovery)
		if err != nil {
			return err
		}
	}

	var q string
	if s.walEngine == "postgres" {
		q = `INSERT INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at, recovery)
		     VALUES ($1, $2, $3, $4, $5, $6, $7)
		     ON CONFLICT (intent_id) DO NOTHING`
	} else {
		q = `INSERT OR IGNORE INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at, recovery)
		     VALUES ($1, $2, $3, $4, $5, $6, $7)`
	}

	if _, err := s.walDB.ExecContext(ctx, q,
		intentID[:],
		string(intent.Kind),
		int64(intent.BlockHeight),
		intent.BlockHash[:],
		encodeIntentHashes(intent.TxHashes),
		intent.StartedAt,
		recoveryData,
	); err != nil {
		return errors.NewStorageError("[BeginConflictIntent] failed to record intent %s", intentID.String(), err)
	}

	return nil
}

// CompleteConflictIntent removes the intent record once the terminal step
// committed. Removing an absent intent is idempotent (no error).
func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	if _, err := s.walDB.ExecContext(ctx, `DELETE FROM conflict_intents WHERE intent_id = $1`, intentID[:]); err != nil {
		return errors.NewStorageError("[CompleteConflictIntent] failed to remove intent %s", intentID.String(), err)
	}

	return nil
}

// PendingConflictIntents returns every begun-but-not-completed intent for startup
// replay. The recovery before-image is left nil (callers load it on demand with
// LoadConflictRecovery), matching the SQL/Aerospike backends.
func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	rows, err := s.walDB.QueryContext(ctx, `SELECT intent_id, kind, block_height, block_hash, tx_hashes, started_at FROM conflict_intents`)
	if err != nil {
		return nil, errors.NewStorageError("[PendingConflictIntents] query failed", err)
	}
	defer rows.Close()

	var intents []utxo.ConflictIntent

	for rows.Next() {
		var (
			intentID    []byte
			kind        string
			blockHeight int64
			blockHash   []byte
			txHashes    []byte
			startedAt   int64
		)

		if err := rows.Scan(&intentID, &kind, &blockHeight, &blockHash, &txHashes, &startedAt); err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] scan failed", err)
		}

		id, err := chainhash.NewHash(intentID)
		if err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] corrupt intent_id (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
		}

		bh, err := chainhash.NewHash(blockHash)
		if err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] corrupt block_hash (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
		}

		hashes, err := decodeIntentHashes(txHashes)
		if err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] corrupt intent row (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
		}

		intents = append(intents, utxo.ConflictIntent{
			Kind:        utxo.ConflictIntentKind(kind),
			BlockHeight: uint32(blockHeight),
			BlockHash:   *bh,
			TxHashes:    hashes,
			StartedAt:   startedAt,
			ID:          *id,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[PendingConflictIntents] row iteration failed", err)
	}

	return intents, nil
}
