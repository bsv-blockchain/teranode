package packedsql

import (
	"context"
	"fmt"
	"strconv"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaLockID = 7_265_726_118

const mainDDL = `
CREATE TABLE IF NOT EXISTS packed_txs (
   hash                     BYTEA NOT NULL,
   flags                    SMALLINT NOT NULL DEFAULT 0,
   coinbase_spending_height BIGINT NOT NULL DEFAULT 0,
   total_count              INT NOT NULL,
   page0_count              INT NOT NULL,
   spent_count              INT NOT NULL DEFAULT 0,
   pages_total              INT NOT NULL DEFAULT 0,
   pages_spent              INT NOT NULL DEFAULT 0,
   spends                   BYTEA NOT NULL,
   block_refs               BYTEA,
   delete_at_height         BIGINT,
   unmined_since            BIGINT,
   preserve_until           BIGINT,
   version                  BIGINT NOT NULL,
   lock_time                BIGINT NOT NULL,
   fee                      BIGINT NOT NULL,
   size_in_bytes            BIGINT NOT NULL,
   created_at               BIGINT NOT NULL,
   utxo_hashes              BYTEA NOT NULL,
   inputs                   BYTEA NOT NULL,
   outputs                  BYTEA NOT NULL,
   PRIMARY KEY (hash)
) PARTITION BY HASH (hash);

CREATE TABLE IF NOT EXISTS packed_tx_pages (
   hash            BYTEA NOT NULL,
   page            INT   NOT NULL,
   spendable_count INT   NOT NULL,
   spent_count     INT   NOT NULL DEFAULT 0,
   spends          BYTEA NOT NULL,
   utxo_hashes     BYTEA NOT NULL,
   PRIMARY KEY (hash, page)
) PARTITION BY HASH (hash);

CREATE TABLE IF NOT EXISTS utxo_overrides (
   hash            BYTEA NOT NULL,
   vout            INT NOT NULL,
   frozen          BOOLEAN NOT NULL DEFAULT FALSE,
   spendable_in    BIGINT,
   reassigned_hash BYTEA,
   PRIMARY KEY (hash, vout)
);

CREATE TABLE IF NOT EXISTS conflicting_children (
   hash       BYTEA NOT NULL,
   child_hash BYTEA NOT NULL,
   created_at BIGINT NOT NULL DEFAULT 0,
   PRIMARY KEY (hash, child_hash)
);

CREATE TABLE IF NOT EXISTS conflict_intents (
   intent_id     BYTEA PRIMARY KEY,
   kind          TEXT NOT NULL,
   block_height  BIGINT NOT NULL,
   block_hash    BYTEA NOT NULL,
   tx_hashes     BYTEA NOT NULL,
   started_at    BIGINT NOT NULL
);

ALTER TABLE packed_txs ALTER COLUMN utxo_hashes SET STORAGE EXTERNAL;
ALTER TABLE packed_txs ALTER COLUMN inputs SET STORAGE EXTERNAL;
ALTER TABLE packed_txs ALTER COLUMN outputs SET STORAGE EXTERNAL;
ALTER TABLE packed_tx_pages ALTER COLUMN utxo_hashes SET STORAGE EXTERNAL;

CREATE INDEX IF NOT EXISTS px_ptxs_dah ON packed_txs (delete_at_height) WHERE delete_at_height IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_ptxs_unmined ON packed_txs (unmined_since) WHERE unmined_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_ptxs_preserve ON packed_txs (preserve_until) WHERE preserve_until IS NOT NULL;
CREATE INDEX IF NOT EXISTS px_ptxs_conflict ON packed_txs ((flags & 4)) WHERE (flags & 4) <> 0;
`

func createSchema(ctx context.Context, pool *pgxpool.Pool, partitions, pageSize int) error {
	if partitions < 1 || pageSize < 1 {
		return errors.NewInvalidArgumentError("packedsql: partitions (%d) and pageSize (%d) must be positive", partitions, pageSize)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return errors.NewStorageError("packedsql: failed to acquire connection for schema creation", err)
	}
	defer conn.Release()

	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, schemaLockID); err != nil {
		return errors.NewStorageError("packedsql: failed to acquire schema advisory lock", err)
	}

	defer func() {
		_, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, schemaLockID)
	}()

	if _, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS packed_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		return errors.NewStorageError("packedsql: failed to create packed_meta", err)
	}

	if _, err = conn.Exec(ctx,
		`INSERT INTO packed_meta (key, value) VALUES
		   ('page_size', $1), ('partitions', $2), ('schema_version', '1')
		 ON CONFLICT (key) DO NOTHING`,
		strconv.Itoa(pageSize), strconv.Itoa(partitions)); err != nil {
		return errors.NewStorageError("packedsql: failed to stamp packed_meta", err)
	}

	if err = validateMeta(ctx, conn, "page_size", pageSize); err != nil {
		return err
	}

	if err = validateMeta(ctx, conn, "partitions", partitions); err != nil {
		return err
	}

	if _, err = conn.Exec(ctx, mainDDL); err != nil {
		return errors.NewStorageError("packedsql: failed to create schema", err)
	}

	for i := 0; i < partitions; i++ {
		stmt := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS packed_txs_p%d PARTITION OF packed_txs FOR VALUES WITH (MODULUS %d, REMAINDER %d) WITH (fillfactor = 70, toast_tuple_target = 4096)`,
			i, partitions, i)
		if _, err = conn.Exec(ctx, stmt); err != nil {
			return errors.NewStorageError("packedsql: failed to create packed_txs partition %d", i, err)
		}

		stmt = fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS packed_tx_pages_p%d PARTITION OF packed_tx_pages FOR VALUES WITH (MODULUS %d, REMAINDER %d) WITH (fillfactor = 70, toast_tuple_target = 4096)`,
			i, partitions, i)
		if _, err = conn.Exec(ctx, stmt); err != nil {
			return errors.NewStorageError("packedsql: failed to create packed_tx_pages partition %d", i, err)
		}
	}

	return nil
}

func validateMeta(ctx context.Context, conn *pgxpool.Conn, key string, want int) error {
	var stored string
	if err := conn.QueryRow(ctx, `SELECT value FROM packed_meta WHERE key = $1`, key).Scan(&stored); err != nil {
		return errors.NewStorageError("packedsql: failed to read packed_meta key %s", key, err)
	}

	if stored != strconv.Itoa(want) {
		return errors.NewConfigurationError("packedsql: %s is immutable: database has %s, settings request %d", key, stored, want)
	}

	return nil
}
