package utxoset

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
)

// coinIndexBloatThreshold is the bytes-per-entry line a packed-key index has to cross before
// the pruner rebuilds it.
//
// A bulk-loaded utxo_pN_ukey index measures 31.5 bytes per entry; the same index left to churn
// under spend-then-create traffic plateaus near 63 bytes per entry, twice the floor, because
// PostgreSQL's btree never reclaims a page below its fill factor once deletes hollow it out.
// 55 sits comfortably above the floor, so a freshly built or lightly used index never
// qualifies, and comfortably below the plateau, so a churned one reliably does.
const coinIndexBloatThreshold = 55

// coinIndexNeedsRebuild decides whether one utxo_pN_ukey index has bloated past the point
// worth paying a REINDEX CONCURRENTLY for. See coinIndexBloatThreshold for where 55 comes
// from. Zero rows means nothing to judge: reltuples can read 0 before the first ANALYZE, and
// an empty partition is never the worst offender worth reindexing.
func coinIndexNeedsRebuild(indexBytes, rows int64) bool {
	return rows > 0 && indexBytes/rows > coinIndexBloatThreshold
}

// coinIndexStatsSQL reads pg_relation_size of every utxo_pN_ukey index alongside
// pg_class.reltuples of its utxo_pN partition, in one catalog round trip covering all
// NumLeaves partitions. reltuples is a planner estimate, refreshed by ANALYZE or a vacuum,
// and reads -1 (never analyzed) or 0 (table not yet touched) on a fresh partition; the
// caller clamps negative estimates to zero rather than mistaking "no ANALYZE yet" for
// "genuinely half a billion rows deleted".
const coinIndexStatsSQL = `
SELECT c.relname,
       pg_relation_size(i.indexrelid),
       c.reltuples
  FROM pg_class c
  JOIN pg_index i ON i.indrelid = c.oid
  JOIN pg_class ic ON ic.oid = i.indexrelid
 WHERE c.relname ~ '^utxo_p[0-9]+$'
   AND ic.relname ~ '^utxo_p[0-9]+_ukey$'`

// invalidCoinIndexSQL finds a leftover _ccnew index from a REINDEX CONCURRENTLY that was
// interrupted (crash, cancel, deploy) before it could swap in and drop the old index. Left in
// place it never serves a scan -- pg_index.indisvalid is false -- and it blocks a later
// REINDEX CONCURRENTLY on the same index name.
const invalidCoinIndexSQL = `
SELECT c.relname
  FROM pg_class c
  JOIN pg_index i ON i.indexrelid = c.oid
 WHERE i.indisvalid = false
   AND c.relname ~ '^utxo_p[0-9]+_ukey_ccnew[0-9]*$'`

// rebuildOneBloatedCoinIndex finds the utxo_pN_ukey index with the worst bytes-per-entry
// ratio, and if decide says it has crossed the line, rebuilds it in place.
//
// At most one rebuild runs per call, and Prune calls this exactly once per session, so at
// most one REINDEX CONCURRENTLY is ever in flight for this store. That is deliberate rather
// than a missed opportunity: on a big partition it can run for minutes, and running it
// inline in the pruner's once-per-block cadence means the NEXT block's pruner call simply
// finds the index already rebuilt -- its bytes-per-row back near the 31.5-byte floor -- and
// moves on to whichever partition is now worst. Nothing needs to track "still running";
// PostgreSQL's own catalog is the only state.
//
// decide is coinIndexNeedsRebuild in production and a stub in tests, so the picking logic
// (worst partition first) and the threshold (55 bytes/entry) can be exercised separately.
func (s *Store) rebuildOneBloatedCoinIndex(ctx context.Context, decide func(indexBytes, rows int64) bool) (int, error) {
	if err := s.dropInvalidCoinIndexes(ctx); err != nil {
		return 0, err
	}

	rows, err := s.pool.Query(ctx, coinIndexStatsSQL)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] list coin index stats", err)
	}

	type candidate struct {
		partition   string
		indexBytes  int64
		rows        int64
		bytesPerRow float64
	}

	var (
		best    candidate
		haveOne bool
	)

	for rows.Next() {
		var c candidate

		if err := rows.Scan(&c.partition, &c.indexBytes, &c.rows); err != nil {
			rows.Close()
			return 0, errors.NewStorageError("[utxoset] scan coin index stats", err)
		}

		if c.rows < 0 {
			c.rows = 0
		}

		// rows is floored at 1 for the ranking only, never for the decide() call below: an
		// unanalyzed or genuinely empty partition must still be comparable to the others
		// (bytesPerRow finite rather than +Inf or a divide-by-zero) so the "pick the worst"
		// step always has a total order, even before any partition has real churn.
		divisor := c.rows
		if divisor < 1 {
			divisor = 1
		}

		c.bytesPerRow = float64(c.indexBytes) / float64(divisor)

		if !haveOne || c.bytesPerRow > best.bytesPerRow {
			best = c
			haveOne = true
		}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset] list coin index stats", err)
	}

	if !haveOne || !decide(best.indexBytes, best.rows) {
		return 0, nil
	}

	indexName := best.partition + "_ukey"

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`REINDEX INDEX CONCURRENTLY %s`, indexName)); err != nil {
		return 0, errors.NewStorageError("[utxoset] reindex %s", indexName, err)
	}

	var afterBytes int64

	if err := s.pool.QueryRow(ctx, `SELECT pg_relation_size($1::regclass)`, indexName).Scan(&afterBytes); err != nil {
		return 0, errors.NewStorageError("[utxoset] measure %s after reindex", indexName, err)
	}

	s.logger.Infof("[utxoset] pruner reindexed %s: %d bytes -> %d bytes (%d rows)",
		indexName, best.indexBytes, afterBytes, best.rows)

	return 1, nil
}

// dropInvalidCoinIndexes clears out any _ccnew leftover from a REINDEX CONCURRENTLY that was
// interrupted before it could swap in, so a later REINDEX CONCURRENTLY on the same base index
// name is not blocked by it. See invalidCoinIndexSQL.
func (s *Store) dropInvalidCoinIndexes(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, invalidCoinIndexSQL)
	if err != nil {
		return errors.NewStorageError("[utxoset] list invalid coin indexes", err)
	}

	var names []string

	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset] scan invalid coin index", err)
		}

		names = append(names, name)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset] list invalid coin indexes", err)
	}

	for _, name := range names {
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP INDEX CONCURRENTLY IF EXISTS %s`, name)); err != nil {
			return errors.NewStorageError("[utxoset] drop invalid coin index %s", name, err)
		}

		s.logger.Infof("[utxoset] pruner dropped leftover invalid index %s from an interrupted reindex", name)
	}

	return nil
}
