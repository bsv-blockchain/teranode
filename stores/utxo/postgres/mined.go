package postgres

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// minedChunkSize is the maximum number of hashes per bulk UPDATE.
// Larger chunks = fewer round trips. Simple array append keeps per-row cost
// constant. NOTE: 4000 was A/B'd (one statement per typical mined batch) and
// REGRESSED the 2-shard balanced rate 95.7K -> 86K — the bigger statement
// holds buffers/locks longer and breaks the create/reclaim interleaving.
const minedChunkSize = 2000

// SetMinedMulti updates the block ID for multiple transactions that have been mined.
// Normal path: single UPDATE on txs appending one 12-byte mined_info record.
// UnsetMined path (reorg): a single atomic UPDATE re-aggregates mined_info minus
// the removed record. Returns a map of each hash to its list of block IDs.
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	startTime := time.Now()
	defer func() {
		if prometheusDirectMinedDuration != nil {
			prometheusDirectMinedDuration.Observe(time.Since(startTime).Seconds())
		}
	}()

	if len(hashes) == 0 {
		return make(map[chainhash.Hash][]uint32), nil
	}
	if _, err := blockHeightToInt32(minedBlockInfo.BlockHeight); err != nil {
		return nil, err
	}

	if minedBlockInfo.UnsetMined {
		return s.unsetMinedMulti(ctx, hashes, minedBlockInfo.BlockID, minedBlockInfo.BlockHeight)
	}

	// Pipeline all UPDATE chunks + fetch queries via SendBatch.
	// This eliminates per-chunk round-trip latency.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[SetMinedMulti] acquire connection", err)
	}
	defer conn.Release()

	// Queue UPDATE chunks.
	// Idempotent block append: a STRIDE-ALIGNED containment guard skips the append
	// when this block id is already recorded. mined_info is a flat bytea of fixed
	// 12-byte records (block_id||height||subtree_idx, each int4 big-endian), so the
	// guard matches int4send($2) only at record-aligned offsets (0,12,24,...) via
	// generate_series — NEVER a bare position()/strpos, which could false-match a
	// block-id byte pattern that straddles two adjacent records. Re-processing the
	// same block for the same tx (crash-recovery replay, retry, duplicate call) is
	// then a no-op rather than appending a duplicate record that Get/BatchDecorate
	// would surface verbatim. Mirrors the aerospike (blockExists) and sql (ON CONFLICT
	// DO NOTHING) stores, and the create.go conflicting-children @> pattern.
	retention := int32(s.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	var updateSQL string
	if minedBlockInfo.OnLongestChain {
		// Record mined_at_height for the deferred DAH sweep, and stamp DAH INLINE for
		// any tx that is now FULLY SPENT at the mine event (Design-C, stamp site S6).
		// This generalizes the old zero-spendable inline stamp and closes the
		// "spent-before-mined" orphan gap: a tx whose outputs are all spent while it is
		// unmined is excluded by the spends-driven sweep's mined-gate, so without S6 it
		// would only be caught by the O(table) backstop. S6 evaluates fully-spent right
		// here, covering both orderings (mined-then-spent is covered by the sweep;
		// spent-then-mined is covered here).
		//
		// Setter-C reconcile (proc v15): fully-spentness is now read from the MAINTAINED
		// spent_bits bitmap (folded by the sweep proc — see dah_sweep_proc.go), NOT
		// from an O(history) re-aggregation over the spends table. The bitmap OR is
		// idempotent, so the v13 counter-drift classes — and the ground-truth spends
		// recount they forced onto this path — are structurally gone: bit_count can
		// never overshoot spendable_count. The mine path is now strictly cheaper: one
		// scalar bit_count over a row-local column, no spends probe. The residual
		// stale-snapshot race (a concurrent fold re-setting a bit Unspend just
		// cleared) is healed by the reconciler via dah_dirty_parents, not here.
		//
		// The CASE predicate:
		//   guard: preserve_until IS NULL AND out_count > 0
		//   zero-spendable: spendable_count = 0 (cheap scalar — immediately prune-eligible
		//     once mined; handled explicitly so it never relies on a vacuous bitmap
		//     equality — out_spendables is NULLable, so the 0-spendable case must not
		//     depend on bitmap state at all)
		//   OR fully-spent: spendable_count > 0 AND bit_count(spent_bits) = spendable_count
		//     (every spendable output's bit has been folded — spent_bits ⊆
		//     out_spendables, so equality means fully spent). Reads two row-local
		//     columns; no join.
		//
		// Completion-height formula: GREATEST(last_spend_height, $5/*minedHeight*/)
		// + 1 + $6/*retention*/ — matches the sweep proc's stamp (which also uses
		// last_spend_height) so both stamp sites agree, and a late mine cannot schedule
		// deletion too early. COALESCE(last_spend_height, 0) guards the zero-spendable
		// case where the counter never folded a spend (last_spend_height IS NULL) →
		// completion falls back to the mined height. Cast to int for the INT4 column.
		//
		// mined_at_height is the height of the block this tx is mined into
		// (minedBlockInfo.BlockHeight), bound as $5 — NOT the store's current
		// chain tip. Binding it as a parameter (a) keeps mined_at_height
		// consistent with the height packed into the mined_info record ($3) even
		// under concurrent SetBlockHeight, and (b) preserves a single prepared-plan
		// cache entry across heights (a literal would re-plan per height).
		//
		// The UPDATE is wrapped in a CTE so that any fully-spent tx that gets an inline
		// DAH stamp is also upserted into pending_deletes in the same statement. The
		// outer SELECT returns hash, mined_info so the drain loop is unchanged.
		//
		// FROM unnest(...) JOIN instead of WHERE hash = ANY(...): with = ANY on the
		// hash-partitioned parent every probe descended ALL 8 leaf pkey btrees
		// (measured: ~4.9 index descents per looked-up row across the hot path);
		// the unnest nested-loop join drives per-row runtime partition pruning, so
		// each hash descends exactly its own leaf.
		//
		// $2/$3/$4 are the scalar block_id/height/subtree_idx; the append builds one
		// 12-byte record int4send($2)||int4send($3)||int4send($4). The dedupe guard
		// scans record-aligned 4-byte block-id slices (offsets 0,12,24,...) so a
		// value straddling two records can never false-match (see the comment above).
		updateSQL = `WITH upd AS (
				UPDATE txs SET
					mined_info = CASE
						WHEN COALESCE(octet_length(mined_info), 0) > 0 AND EXISTS (
							SELECT 1 FROM generate_series(0, octet_length(mined_info) / 12 - 1) AS g(i)
							WHERE substr(mined_info, g.i * 12 + 1, 4) = int4send($2)
						)
						THEN mined_info
						ELSE COALESCE(mined_info, ''::bytea) || int4send($2) || int4send($3) || int4send($4)
					END,
					locked = false, unmined_since = NULL,
					mined_at_height = $5,
					delete_at_height = CASE
						WHEN preserve_until IS NULL AND out_count > 0 AND (
							spendable_count = 0
							OR (spendable_count > 0 AND bit_count(spent_bits) = spendable_count)
						)
						THEN (GREATEST(COALESCE(last_spend_height, 0), $5) + 1 + $6)::int
						ELSE delete_at_height
					END
				FROM unnest($1::bytea[]) AS h(v)
				WHERE txs.hash = h.v
				RETURNING txs.hash, mined_info, delete_at_height
			),
			_pd AS (
				INSERT INTO pending_deletes (hash, delete_at_height)
				SELECT hash, delete_at_height FROM upd WHERE delete_at_height IS NOT NULL
				ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
			)
			SELECT hash, mined_info FROM upd`
	} else {
		updateSQL = `UPDATE txs SET
			mined_info = CASE
				WHEN COALESCE(octet_length(mined_info), 0) > 0 AND EXISTS (
					SELECT 1 FROM generate_series(0, octet_length(mined_info) / 12 - 1) AS g(i)
					WHERE substr(mined_info, g.i * 12 + 1, 4) = int4send($2)
				)
				THEN mined_info
				ELSE COALESCE(mined_info, ''::bytea) || int4send($2) || int4send($3) || int4send($4)
			END,
			locked = false
		FROM unnest($1::bytea[]) AS h(v)
		WHERE txs.hash = h.v
		RETURNING txs.hash, mined_info`
	}

	chunkArgsList := make([][]interface{}, 0, (len(hashes)+minedChunkSize-1)/minedChunkSize)
	for i := 0; i < len(hashes); i += minedChunkSize {
		end := i + minedChunkSize
		if end > len(hashes) {
			end = len(hashes)
		}
		chunk := hashes[i:end]
		hashBytes := make([][]byte, len(chunk))
		for j, h := range chunk {
			hashBytes[j] = h[:]
		}
		args := []interface{}{
			hashBytes,
			int32(minedBlockInfo.BlockID),     // $2 block_id (packed via int4send)
			int32(minedBlockInfo.BlockHeight), // $3 height
			int32(minedBlockInfo.SubtreeIdx),  // $4 subtree_idx
		}
		if minedBlockInfo.OnLongestChain {
			// $5: mined_at_height — the block height this tx is mined into
			// (INT4 column; heights < 2^31). $6: retention, for the inline
			// zero-spendable DAH stamp (delete_at_height = $5 + 1 + $6).
			args = append(args, int32(minedBlockInfo.BlockHeight), retention)
		}
		chunkArgsList = append(chunkArgsList, args)
	}

	// Deadlock-retry note (verified empirically, not assumed — see the throwaway
	// pgx.Batch repro this comment is based on): every chunk is queued into ONE
	// pgx.Batch and sent as ONE SendBatch, which pgx pipelines behind a SINGLE
	// terminal Sync. Per the Postgres extended-query protocol, a run of commands
	// between two Sync messages with no explicit BEGIN is an IMPLICIT transaction:
	// an error on ANY queued statement rolls back EVERY statement sent since the
	// last Sync, INCLUDING ones that had already reported success. (Confirmed with
	// a 3-statement batch: a forced error on statement 2 rolled back statement 1's
	// already-"successful" UPDATE — the row was unchanged after Close().) So on a
	// 40P01 the whole chunk batch is atomically a no-op, not a partial one; the
	// correct retry unit is the WHOLE batch, not a single chunk within it, and there
	// is nothing partial to reconcile. Retrying is safe: each UPDATE is idempotent
	// via the stride-aligned mined_info containment guard (see the comment above
	// updateSQL) — re-appending the same 12-byte (block_id, height, subtree_idx)
	// record is a no-op, so re-running the full set of chunks from scratch reproduces
	// the same end state whether this is attempt 1 or attempt N.
	batchRes, err := retryOnPgDeadlock(ctx,
		func() (minedUpdateBatchResult, error) {
			return s.attemptSetMinedUpdateBatch(ctx, conn, updateSQL, chunkArgsList)
		},
		func(attemptNum int, backoff time.Duration) {
			s.logger.Warnf("[SetMinedMulti] postgres deadlock (40P01) on UPDATE batch (%d chunks), retry %d/%d after %s", len(chunkArgsList), attemptNum, pgDeadlockMaxRetries, backoff)
		},
	)
	if err != nil {
		return nil, errors.NewStorageError("[SetMinedMulti] "+batchRes.stage, batchRes.failedChunk, err)
	}
	resultMap := batchRes.resultMap

	// Hard postcondition (interface contract, matches the aerospike store): every
	// input hash MUST appear in the result map and its block_ids MUST contain the
	// block we just mined into. A hash absent from txs (never created, pruned, or
	// a Create/SetMinedMulti race) silently no-ops the UPDATE; surface it as an
	// error rather than returning a partial map that a caller would read as
	// "all mined".
	for _, h := range hashes {
		bids, ok := resultMap[*h]
		if !ok {
			return nil, errors.NewTxNotFoundError("[SetMinedMulti] transaction not found: %s", h)
		}
		found := false
		for _, bid := range bids {
			if bid == minedBlockInfo.BlockID {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.NewStorageError("[SetMinedMulti] block %d not recorded for %s", minedBlockInfo.BlockID, h)
		}
	}

	return resultMap, nil
}

// minedUpdateBatchResult carries attemptSetMinedUpdateBatch's outcome through
// retryOnPgDeadlock's generic T. On success resultMap is populated and stage is
// empty; on failure resultMap is nil and failedChunk/stage identify which chunk and
// stage failed, so a final (post-retry) error message can still name them even if
// the loop exits via ctx cancellation rather than a fresh attempt.
type minedUpdateBatchResult struct {
	resultMap   map[chainhash.Hash][]uint32
	failedChunk int
	stage       string
}

// attemptSetMinedUpdateBatch runs every UPDATE chunk in chunkArgsList as ONE
// pipelined SendBatch and drains their RETURNING rows into a fresh map. On success
// stage is "" (failedChunk is meaningless). On failure it returns the RAW/unwrapped
// error alongside the chunk index and a message template naming the failing stage,
// so the caller can (a) classify a Postgres deadlock via isPgDeadlock on the raw
// error and (b) reconstruct a precise wrapped error once retries are exhausted or
// the error is not a deadlock. See the deadlock-retry note in SetMinedMulti for why
// retrying this whole batch (not one chunk) is the correct and safe unit.
func (s *Store) attemptSetMinedUpdateBatch(ctx context.Context, conn *pgxpool.Conn, updateSQL string, chunkArgsList [][]interface{}) (minedUpdateBatchResult, error) {
	resultMap := make(map[chainhash.Hash][]uint32, len(chunkArgsList)*minedChunkSize)

	batch := &pgx.Batch{}
	for _, args := range chunkArgsList {
		batch.Queue(updateSQL, args...)
	}

	// Send entire batch in one network flush.
	br := conn.SendBatch(ctx, batch)

	// Drain each UPDATE chunk's RETURNING rows straight into the result map —
	// the post-update mined_info bytea comes back on the UPDATE itself (decoded to
	// the block-id list here), so no separate verification SELECT (a second full
	// = ANY probe pass over all partitions, measured as a top-10 CPU consumer) is
	// needed.
	for i := range chunkArgsList {
		rows, err := br.Query()
		if err != nil {
			br.Close()
			return minedUpdateBatchResult{failedChunk: i, stage: "UPDATE chunk %d"}, err
		}
		for rows.Next() {
			var h []byte
			var minedInfo []byte
			if err := rows.Scan(&h, &minedInfo); err != nil {
				rows.Close()
				br.Close()
				return minedUpdateBatchResult{failedChunk: i, stage: "scan mined_info chunk %d"}, err
			}
			var ch chainhash.Hash
			copy(ch[:], h)
			resultMap[ch] = decodeMinedBlockIDs(minedInfo)
		}
		// A mid-stream failure (network reset, statement timeout) makes rows.Next()
		// stop early with the error parked in rows.Err(); without this check a
		// truncated resultMap would be returned as success.
		if err := rows.Err(); err != nil {
			rows.Close()
			br.Close()
			return minedUpdateBatchResult{failedChunk: i, stage: "iterate mined_info chunk %d"}, err
		}
		rows.Close()
	}
	br.Close()

	return minedUpdateBatchResult{resultMap: resultMap}, nil
}

// unsetMinedMulti handles the reorg path: remove a block_id from the arrays.
// If no block_ids remain after removal, sets unmined_since to current block height,
// clears delete_at_height (the tx is no longer mined on the longest chain, so any
// deferred-prune stamp from when it was fully-spent+mined is now invalid), and
// clears locked — mirroring the aerospike setMined(UnsetMined) re-evaluation of DAH
// eligibility. Without the DAH clear a reorged-out tx would be pruned at the stale
// stamp height, destroying still-live UTXOs.
func (s *Store) unsetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32, blockHeight uint32) (map[chainhash.Hash][]uint32, error) {
	resultMap := make(map[chainhash.Hash][]uint32, len(hashes))

	// unmined_since is INT4 (heights < 2^31). Use blockHeight + 1, matching the
	// aerospike Lua (unmined_since = currentBlockHeight) and the sql store
	// (sql.go: s.GetBlockHeight() + 1) so a reorged-out tx is classified
	// identically across backends — a bare blockHeight.Load() made it one block low
	// and thus prune-eligible one block earlier than the gold standard. Floor at 1:
	// a stored unmined_since of 0 is indistinguishable at the meta layer from
	// "mined" — NULL (mined) and 0 both map to UnminedSince==0 in getInternal — so
	// the value must never be 0 (the +1 already guarantees this for tip >= 0, but
	// keep the explicit floor as a guard).
	currentBlockHeight := int32(s.GetBlockHeight()) + 1
	if currentBlockHeight < 1 {
		currentBlockHeight = 1
	}

	// One transaction for the whole set: a reorg that unsets a block_id from many
	// txs commits all-or-nothing rather than leaving some rows updated on failure.
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[UnsetMined] begin", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	for _, hash := range hashes {
		// Remove the matching 12-byte record from the packed mined_info bytea in a
		// SINGLE atomic UPDATE. The previous SELECT-then-UPDATE left a window where a
		// concurrent SetMinedMulti could append a block entry between the read and the
		// write-back, silently dropping it on reorg.
		//
		// Removal re-aggregates every record whose block_id (the first 4 bytes,
		// int4send-encoded) does NOT equal blockID, keeping the record boundaries by
		// walking stride-aligned offsets (g.i*12+1) — the block_id/height/subtree_idx
		// triple always drops together, so the three fields can never misalign (the
		// class of bug the array form once had). "Fully reorged out" is NOT EXISTS a
		// surviving record. All SET expressions read the row's pre-update mined_info,
		// and a single UPDATE statement re-evaluates them against the locked (latest)
		// row version, so a concurrent append is preserved rather than clobbered.
		// RETURNING yields the post-update mined_info for the result map.
		var newMinedInfo []byte
		err := pgxTx.QueryRow(ctx, `
			UPDATE txs t SET
				mined_info = COALESCE((
					SELECT string_agg(substr(t.mined_info, g.i * 12 + 1, 12), ''::bytea ORDER BY g.i)
					FROM generate_series(0, octet_length(t.mined_info) / 12 - 1) AS g(i)
					WHERE substr(t.mined_info, g.i * 12 + 1, 4) <> int4send($2)
				), ''::bytea),
				-- Set unmined_since only when the tx is fully reorged out (no records
				-- remain). Otherwise PRESERVE the existing value: a tx still mined only
				-- on non-longest-chain forks was legitimately unmined at creation and
				-- must keep that marker so the DAH sweep's "unmined_since IS NOT NULL"
				-- guard continues to protect it. Clobbering to NULL here would mis-classify
				-- it as longest-chain-mined (matches aerospike teranode.lua:637-644 and
				-- sql.go:3268-3308, which only write unmined_since when zero blocks remain).
				unmined_since = CASE
					WHEN NOT EXISTS (
						SELECT 1 FROM generate_series(0, octet_length(t.mined_info) / 12 - 1) AS g(i)
						WHERE substr(t.mined_info, g.i * 12 + 1, 4) <> int4send($2)
					)
					THEN $3::int ELSE t.unmined_since END,
				-- Clear the deferred-prune stamp UNCONDITIONALLY on any unset-mined.
				-- Removing a block changes the tx's chain membership, so any existing
				-- delete_at_height is no longer trustworthy. Clearing it always (not
				-- only when the LAST block is removed) closes the partial-reorg window
				-- where one block is removed but others — themselves later reorged out
				-- in a separate call — leave a stale stamp the pruner would act on. The
				-- DAH sweep re-stamps the tx for free once it is genuinely eligible
				-- again; the cost is at most one extra sweep pass.
				delete_at_height = NULL,
				-- Clear mined_at_height when the tx is fully reorged out (no records
				-- remain) so the DAH sweep's mine-arm stops re-enumerating it as a
				-- phantom candidate on every pass over the old mining height. Preserve
				-- it on a partial reorg (still mined on a remaining block).
				mined_at_height = CASE
					WHEN NOT EXISTS (
						SELECT 1 FROM generate_series(0, octet_length(t.mined_info) / 12 - 1) AS g(i)
						WHERE substr(t.mined_info, g.i * 12 + 1, 4) <> int4send($2)
					)
					THEN NULL ELSE t.mined_at_height END,
				locked = false
			WHERE t.hash = $1
			RETURNING t.mined_info`,
			hash[:], int32(blockID), currentBlockHeight,
		).Scan(&newMinedInfo)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Per Interface.go:295-303, UnsetMined tolerates missing/empty entries:
				// the call may no-op for transactions that no longer exist. A reorg
				// unsets every tx in the reorged block, some of which may already have
				// been pruned — that must not abort the whole reorg. Record an empty
				// result and move on (matches aerospike set_mined.go:502-506 and
				// sql.go:3162-3176).
				resultMap[*hash] = []uint32{}
				continue
			}
			return nil, errors.NewStorageError("[UnsetMined] update mined_info for %s", hash, err)
		}

		// Decode the post-update mined_info to the surviving block-id list (the Go
		// result surface is unchanged) and to derive fully-reorged-out below.
		newBlockIDs := decodeMinedBlockIDs(newMinedInfo)

		// C5: remove the hash from the pending_deletes side-table in the same pgxTx so
		// the clear is atomic with the DAH null above. Harmless no-op if the hash was
		// never stamped (e.g. reorged before the sweep ran).
		if _, err := pgxTx.Exec(ctx,
			`DELETE FROM pending_deletes WHERE hash = $1`, hash[:]); err != nil {
			return nil, errors.NewStorageError("[UnsetMined] failed to delete pending_deletes for %s (C5)", hash, err)
		}

		// U1: when fully reorged out (no records remain), insert into pending_unmined.
		// pending_unmined is ALWAYS-ON (no flag). We derive whether the tx is now fully
		// unmined from the post-update mined_info returned by RETURNING above.
		// unmined_since is currentBlockHeight (= s.GetBlockHeight()+1), matching the
		// CASE expression in the UPDATE above — COPY-FROM-RETURNING semantics without
		// re-reading the row. Harmless ON CONFLICT DO UPDATE if the hash was already
		// in pending_unmined from a prior reorg (idempotent).
		//
		// Guard: only insert when the tx is NOT conflicting (mirrors U3 in conflicting.go:491-493).
		// A conflicting tx reorged out must NOT enter pending_unmined — conflicting txs are
		// outside the invariant set {unmined AND NOT conflicting}. The NOT EXISTS subquery
		// reads the txs row just updated (same pgxTx, row is locked), so the check is
		// atomic with the UPDATE above.
		if len(newBlockIDs) == 0 {
			if _, err := pgxTx.Exec(ctx,
				`INSERT INTO pending_unmined (hash, unmined_since)
				 SELECT $1, $2 WHERE NOT EXISTS (
				 	SELECT 1 FROM txs WHERE hash = $1 AND conflicting
				 )
				 ON CONFLICT (hash) DO UPDATE SET unmined_since = EXCLUDED.unmined_since`,
				hash[:], currentBlockHeight); err != nil {
				return nil, errors.NewStorageError("[UnsetMined] failed to insert pending_unmined for %s (U1)", hash, err)
			}
		}

		// Build result.
		result := make([]uint32, len(newBlockIDs))
		for i, bid := range newBlockIDs {
			result[i] = uint32(bid)
		}
		resultMap[*hash] = result
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[UnsetMined] commit", err)
	}

	// Rewind the deferred-DAH sweep watermark to the reorged block's height so the
	// new chain's spends in (blockHeight, tip] — tagged at heights the cursor may
	// already have passed — get folded into spent_bits rather than skipped.
	// Best-effort (no retry/abort — the reorg itself has already committed), but
	// under v15 a LOST rewind is a silent unbounded disk leak, not mere counter
	// drift: new-chain spends below the watermark never fold, their parents never
	// reach bit_count(spent_bits) = spendable_count, and they sit unstamped
	// (unpruned) until the reconciler's slow rotation happens to reach them.
	// RewindDAHWatermark only ever moves the watermark backward (guarded), so a
	// spurious call cannot skip heights.
	if blockHeight > 0 {
		if err := s.RewindDAHWatermark(ctx, int64(blockHeight)); err != nil {
			prometheusDAHWatermarkRewindFailures.Inc()
			s.logger.Errorf("[UnsetMined] rewind DAH watermark to %d failed — spends below the watermark will never fold, their parents stay unstamped/unpruned until the reconciler rotation reaches them (disk leak): %v", blockHeight, err)
		}
	}

	return resultMap, nil
}

// MarkTransactionsOnLongestChain is implemented in conflicting.go.
