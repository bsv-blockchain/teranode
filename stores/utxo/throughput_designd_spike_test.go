package utxo_test

// ---------------------------------------------------------------------------
// Design-D spike: generational DROP-partition reclaim vs cascade row-DELETE
// ---------------------------------------------------------------------------
//
// THROWAWAY performance spike. No production code, no Store interface, no
// reuse outside this file. It exists to prove/disprove ONE claim before a
// large schema redesign of the postgres UTXO store (PR #684):
//
//	"generational DROP-partition reclaim gives sustained create throughput
//	 >= 1.15x the current cascade-DELETE design, with reclaim effectively free"
//
// Both modes share an IDENTICAL schema and IDENTICAL hot path; the ONLY
// difference is how dead rows are reclaimed:
//
//	MODE delete — a pruner row-DELETEs fully-spent+mined txs (and their spend
//	              rows) from expired buckets in chunks; emptied leaves are
//	              dropped so the leaf count stays bounded. NOTE: dropping the
//	              emptied leaf is itself GENEROUS to delete mode (the v7 hash-
//	              partition design never gets to drop anything).
//	MODE drop   — a pruner relocates the few survivors out of an expired
//	              bucket, then DETACHes and DROPs the whole leaf pair (O(1)
//	              catalog work instead of O(rows) heap+index work, and no
//	              dead-tuple/vacuum debt at all).
//
// Bucket sizing: bucket = blockHeight / 64. At the harness's 250ms height
// ticks that is one new leaf pair every 16s — frequent enough that several
// generations expire inside the measured window, rare enough that the DDL is
// well off the hot path. A background goroutine pre-creates the NEXT bucket's
// leaves one bucket ahead of the height ticker so the hot path never runs DDL.
//
// Spend uniqueness: d_spends rows carry the PARENT tx's bucket. Because every
// spend of output (prev_tx_hash, prev_output_idx) stores the SAME bucket (a
// functional dependency on the parent row), the per-leaf UNIQUE constraint
// (prev_tx_hash, prev_output_idx, bucket) is globally exact: two competing
// spends of one output always land in the same leaf and collide there.
// (Relocation in drop mode preserves this: spend rows move WITH their parent
// to the parent's new bucket.)
//
// Spend state is append-only rows, deliberately: an UPDATE-based spent-mask on
// the txs row is a known-dead pattern in this store's history (MVCC bloat on
// the hottest table), so this spike does not model it.
//
// What this spike does NOT model (all out of scope for the reclaim A/B):
// conflicting/frozen/coinbase handling, reorg + unspend, preserve_until,
// block_ids arrays, the unmined-since backstop, external blob storage, the
// Get field mask, and multi-output txs (every tx here has exactly 1 spendable
// output). Absolute TPS is ALSO not comparable to the real store's numbers:
// the real store batches ACROSS workers for every op (500-row UNNEST batchers
// on get/spend/create/unlock); here only CREATE is cross-worker coalesced by
// default, with get/spend/unlock issued per worker. Both modes are built
// identically, so the comparison metric is SPIKE-DROP vs SPIKE-DELETE.
//
// Run (the orchestrator runs these; the shared postgres is busy otherwise):
//
//	THROUGHPUT_WORKERS=10000 go test ./stores/utxo/ \
//	  -run 'TestThroughput_DesignDSpike/mode_delete' -v -timeout 30m
//	THROUGHPUT_WORKERS=10000 go test ./stores/utxo/ \
//	  -run 'TestThroughput_DesignDSpike/mode_drop' -v -timeout 30m
//
// (or both modes + the ratio line in one run, dropping the subtest filter)
// Optional: DESIGND_COALESCE=0 falls back to a per-worker 4-statement
// pgx.Batch per iteration instead of the cross-worker create coalescer.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	// designDBucketHeights: heights per generation bucket. 64 heights at 250ms
	// height ticks = one leaf pair per 16s. Big enough that leaf-creation DDL
	// is rare, small enough that several DROPs land inside the measured window.
	designDBucketHeights = 64
	// designDRetention mirrors prunedRetention: a bucket is reclaimable once its
	// UPPER height bound is more than this many heights behind the tip. A
	// worker's immediate parent is therefore at least retention ticks old before
	// its bucket can be touched.
	designDRetention = 20
	// designDHeightTickMS mirrors prunedHeightTickMS.
	designDHeightTickMS = 250
	// designDMiners/designDMineBatch: background SetMined emulation. 8 miners
	// (vs 12 in the pruned harness) since the mined UPDATE here is a single
	// 2000-pair UNNEST join, cheaper than the real SetMinedMulti.
	designDMiners    = 8
	designDMineBatch = 2000
	// designDMineChanCap mirrors prunedMineChanCap: workers BLOCK on the mine
	// channel, back-pressuring creation to the sustainable mine+reclaim rate.
	designDMineChanCap = 50000
	// Cross-worker create coalescer: mirrors sendCreateBatchUNNEST's 500-row
	// batches; 2ms window mirrors the store's batcher cadence at saturation.
	designDCoalesceBatch  = 500
	designDCoalesceWindow = 2 * time.Millisecond
	designDCoalescers     = 4
	// Pruner cadence + delete-mode chunk size.
	designDPruneInterval = time.Second
	designDDeleteChunk   = 10000
	// Measurement: warmup 3s then 16 reps x 4s (fixed by the spike spec).
	designDWarmup  = 3 * time.Second
	designDReps    = 16
	designDMeasure = 4 * time.Second

	designDStartHeight = int64(200)
)

// ---------------------------------------------------------------------------
// Schema (d_-prefixed, created with raw pgx; LOGGED; range-partitioned by
// generation bucket; leaf pairs created on demand one bucket ahead)
// ---------------------------------------------------------------------------

const designDTxsDDL = `
CREATE TABLE d_txs (
    hash            BYTEA   NOT NULL,
    bucket          INT     NOT NULL,
    fee             BIGINT,
    raw_tx          BYTEA,
    locked          BOOLEAN NOT NULL DEFAULT FALSE,
    mined_at_height BIGINT,
    utxo_hashes     BYTEA[],
    out_spendables  BOOLEAN[],
    PRIMARY KEY (hash, bucket)
) PARTITION BY RANGE (bucket)`

const designDSpendsDDL = `
CREATE TABLE d_spends (
    prev_tx_hash    BYTEA NOT NULL,
    prev_output_idx INT   NOT NULL,
    bucket          INT   NOT NULL,
    spending_data   BYTEA NOT NULL,
    spent_at_height BIGINT,
    UNIQUE (prev_tx_hash, prev_output_idx, bucket)
) PARTITION BY RANGE (bucket)`

// Hot-path SQL. The worker KNOWS its parent's bucket (it created the parent),
// so GET uses the exact (hash, bucket) PK probe. Production would need a
// coarse bucket hint on the reference (or a <=3-leaf probe over the live
// bucket array computed from height-retention); the exact probe here is the
// best case for both modes equally.
const (
	designDGetSQL = `SELECT fee FROM d_txs WHERE hash = $1 AND bucket = $2`

	// One CTE: validate the parent exists at (hash,bucket) with the expected
	// per-output utxo hash, then append the spend row AT THE PARENT'S BUCKET.
	// ON CONFLICT DO NOTHING + RowsAffected==0 => validation/double-spend
	// failure (fatal in this harness).
	designDSpendSQL = `
WITH parent AS (
    SELECT 1 FROM d_txs
    WHERE hash = $1 AND bucket = $2 AND utxo_hashes[$3::int + 1] = $4
)
INSERT INTO d_spends (prev_tx_hash, prev_output_idx, bucket, spending_data, spent_at_height)
SELECT $1, $3::int, $2, $5, $6 FROM parent
ON CONFLICT (prev_tx_hash, prev_output_idx, bucket) DO NOTHING`

	// Single-row create (per-worker batch mode only). The "utxo hash" stored
	// for the tx's single output is the tx hash itself — same 32-byte bytea
	// equality cost as a real utxo hash, no extra hashing in the harness.
	designDCreateRowSQL = `
INSERT INTO d_txs (hash, bucket, fee, raw_tx, locked, utxo_hashes, out_spendables)
VALUES ($1, $2, $3, $4, TRUE, ARRAY[$1::bytea], ARRAY[TRUE])
ON CONFLICT (hash, bucket) DO NOTHING`

	// Cross-worker bulk create, mirroring the store's sendCreateBatchUNNEST.
	// $5 = locked (TRUE for hot-path creates, FALSE for genesis).
	designDCreateUnnestSQL = `
INSERT INTO d_txs (hash, bucket, fee, raw_tx, locked, utxo_hashes, out_spendables)
SELECT u.hash, u.bucket, u.fee, u.raw_tx, $5, ARRAY[u.hash], ARRAY[TRUE]
FROM unnest($1::bytea[], $2::int[], $3::bigint[], $4::bytea[]) AS u(hash, bucket, fee, raw_tx)
ON CONFLICT (hash, bucket) DO NOTHING`

	// PK (hash,bucket) is unchanged by this UPDATE, so it stays HOT-eligible;
	// fillfactor=70 on d_txs leaves keeps page room for the rewrite.
	designDUnlockSQL = `UPDATE d_txs SET locked = FALSE WHERE hash = $1 AND bucket = $2`

	// Relocation recovery (drop mode): the exact-bucket GET missed because the
	// pruner moved the parent; find its new bucket (scans the few live leaves'
	// PKs — rare path). DESC picks the newest copy if a stale one lingers.
	designDFindSQL = `SELECT bucket FROM d_txs WHERE hash = $1 ORDER BY bucket DESC LIMIT 1`

	// Miner: 2000-pair UNNEST join on the (hash,bucket) PK.
	designDMineSQL = `
UPDATE d_txs t SET mined_at_height = $3
FROM unnest($1::bytea[], $2::int[]) AS u(hash, bucket)
WHERE t.hash = u.hash AND t.bucket = u.bucket`
)

func designDLeaf(table string, b int64) string {
	return fmt.Sprintf("%s_b%04d", table, b)
}

// designDResetSchema drops every d_-prefixed table (including leaves DETACHed
// by an aborted previous run, which a CASCADE on the parent would NOT reach)
// and recreates the two partitioned parents.
func designDResetSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables
           WHERE schemaname = 'public'
             AND (tablename LIKE 'd\_txs%' OR tablename LIKE 'd\_spends%')
  LOOP
    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
  END LOOP;
END $$;`); err != nil {
		return err
	}
	for _, ddl := range []string{designDTxsDDL, designDSpendsDDL} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type designDMined struct {
	hash   []byte
	bucket int64
}

type designDCreateReq struct {
	hash   []byte
	bucket int64
	fee    int64
	raw    []byte
	done   chan error
}

type designDParent struct {
	tx     *bt.Tx
	bucket int64 // bucket the parent row was created in (exact GET probe)
}

type designDHarness struct {
	t        *testing.T
	pool     *pgxpool.Pool
	mode     string // "delete" | "drop"
	coalesce bool

	curH atomic.Int64

	bucketMu sync.Mutex
	buckets  map[int64]bool // currently-attached leaf pairs

	mineCh   chan designDMined
	createCh chan designDCreateReq

	// reclaim + diagnostics counters (cumulative for the mode run)
	relocatedTxs    atomic.Int64
	relocatedSpends atomic.Int64
	droppedBuckets  atomic.Int64
	deletedRows     atomic.Int64
	ddlNs           atomic.Int64 // detach+drop wall time
	recoveredGets   atomic.Int64
	minedTotal      atomic.Int64
	failN           atomic.Int64
}

// fail records a fatal harness error (t.Errorf is goroutine-safe); only the
// first few are logged to avoid a 10K-worker stampede of identical messages.
func (h *designDHarness) fail(format string, args ...interface{}) {
	if h.failN.Add(1) <= 5 {
		h.t.Errorf("[designD/"+h.mode+"] "+format, args...)
	}
}

func (h *designDHarness) bucketOf(height int64) int64 { return height / designDBucketHeights }

// ensureBucket creates the leaf pair for bucket b if missing. fillfactor 70 on
// d_txs (room for the HOT unlock/mined rewrites) and 100 on d_spends (append-
// only, pack tight). No other indexes anywhere: no BRIN, no partials — the PK
// and the spends UNIQUE are the entire index footprint, which is itself part
// of the design being tested.
func (h *designDHarness) ensureBucket(ctx context.Context, b int64) error {
	h.bucketMu.Lock()
	defer h.bucketMu.Unlock()
	if h.buckets[b] {
		return nil
	}
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF d_txs FOR VALUES FROM (%d) TO (%d) WITH (fillfactor = 70)`,
			designDLeaf("d_txs", b), b, b+1),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF d_spends FOR VALUES FROM (%d) TO (%d) WITH (fillfactor = 100)`,
			designDLeaf("d_spends", b), b, b+1),
	}
	for _, s := range stmts {
		if _, err := h.pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	h.buckets[b] = true
	return nil
}

func (h *designDHarness) attached() []int64 {
	h.bucketMu.Lock()
	defer h.bucketMu.Unlock()
	out := make([]int64, 0, len(h.buckets))
	for b := range h.buckets {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (h *designDHarness) removeBucket(b int64) {
	h.bucketMu.Lock()
	defer h.bucketMu.Unlock()
	delete(h.buckets, b)
}

// dropLeafPair DETACHes both leaves in one txn (plain DETACH takes a brief
// ACCESS EXCLUSIVE on the parent — that momentary hot-path stall is exactly
// part of what drop mode must pay for, so it is INSIDE the measured window;
// production could soften it with DETACH CONCURRENTLY outside a txn), then
// DROPs the detached tables. Wall time is recorded as reclaim-DDL cost.
func (h *designDHarness) dropLeafPair(ctx context.Context, b int64) error {
	txLeaf, spLeaf := designDLeaf("d_txs", b), designDLeaf("d_spends", b)
	t0 := time.Now()
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return err
	}
	for _, stmt := range []string{
		`ALTER TABLE d_txs DETACH PARTITION ` + txLeaf,
		`ALTER TABLE d_spends DETACH PARTITION ` + spLeaf,
	} {
		if _, err = tx.Exec(ctx, stmt); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	for _, stmt := range []string{`DROP TABLE ` + txLeaf, `DROP TABLE ` + spLeaf} {
		if _, err = h.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	h.ddlNs.Add(time.Since(t0).Nanoseconds())
	h.removeBucket(b)
	h.droppedBuckets.Add(1)
	return nil
}

// designDDoomed reports whether bucket b is past retention at height H: its
// UPPER height bound (b+1)*64-1 is more than `retention` heights behind tip.
func designDDoomed(b, H int64) bool {
	return (b+1)*designDBucketHeights-1 < H-designDRetention
}

// pruneDropOnce (MODE drop): pick the oldest doomed bucket, relocate the few
// survivors (and their spend rows) into the CURRENT bucket, then detach+drop
// the leaf pair.
//
// Survivor rule (kept deliberately simple): a row survives unless it is BOTH
// mined AND fully spent, where fully-spent = count of its spend rows in the
// doomed spends leaf equals its number of spendable outputs. In this workload
// that means: the unspent chain tips (one per worker parked in this bucket)
// plus any spent-but-not-yet-mined stragglers.
//
// Known, accepted races (commented, not fixed — this is a spike):
//   - A spend that lands in the doomed leaf between the relocation copy and the
//     DETACH is lost with the leaf; the relocated parent copy then looks
//     unspent forever and is re-relocated on every subsequent drop. This LEAKS
//     a few rows per drop and INFLATES drop-mode's relocation cost — i.e. it
//     biases AGAINST the mode under test, which is the conservative direction.
//   - A mined-update whose (hash, oldBucket) pair arrives after relocation
//     misses the moved row; same conservative leak. Mining lag (~100ms flush)
//     is << the >=5s doom age, so this is rare.
//
// Workers recover from relocation via designDFindSQL (see resolveSpend).
func (h *designDHarness) pruneDropOnce(ctx context.Context) {
	H := h.curH.Load()
	target := h.bucketOf(H)
	doomed := int64(-1)
	for _, b := range h.attached() {
		if b != target && designDDoomed(b, H) {
			doomed = b
			break // oldest first; one bucket per tick is plenty (1 new bucket / 16s)
		}
	}
	if doomed < 0 {
		return
	}
	if err := h.ensureBucket(ctx, target); err != nil {
		if ctx.Err() == nil {
			h.t.Logf("[designD/drop] ensure target bucket %d: %v", target, err)
		}
		return
	}

	txLeaf, spLeaf := designDLeaf("d_txs", doomed), designDLeaf("d_spends", doomed)

	relocTxs := fmt.Sprintf(`
INSERT INTO d_txs (hash, bucket, fee, raw_tx, locked, mined_at_height, utxo_hashes, out_spendables)
SELECT t.hash, %d, t.fee, t.raw_tx, t.locked, t.mined_at_height, t.utxo_hashes, t.out_spendables
FROM %s t
WHERE NOT (
    t.mined_at_height IS NOT NULL
    AND (SELECT count(*) FROM %s s WHERE s.prev_tx_hash = t.hash)
        = COALESCE(cardinality(array_positions(t.out_spendables, TRUE)), 0)
)
ON CONFLICT (hash, bucket) DO NOTHING`, target, txLeaf, spLeaf)
	ct, err := h.pool.Exec(ctx, relocTxs)
	if err != nil {
		if ctx.Err() == nil {
			h.t.Logf("[designD/drop] relocate txs from bucket %d: %v", doomed, err)
		}
		return
	}
	h.relocatedTxs.Add(ct.RowsAffected())

	// Move the survivors' spend rows with them so the per-leaf UNIQUE stays
	// globally exact at the parent's NEW bucket. The EXISTS probes the target
	// leaf directly: only just-relocated parents can have spend rows still
	// sitting in the doomed spends leaf.
	relocSpends := fmt.Sprintf(`
INSERT INTO d_spends (prev_tx_hash, prev_output_idx, bucket, spending_data, spent_at_height)
SELECT s.prev_tx_hash, s.prev_output_idx, %d, s.spending_data, s.spent_at_height
FROM %s s
WHERE EXISTS (SELECT 1 FROM %s t WHERE t.hash = s.prev_tx_hash)
ON CONFLICT (prev_tx_hash, prev_output_idx, bucket) DO NOTHING`,
		target, spLeaf, designDLeaf("d_txs", target))
	ct, err = h.pool.Exec(ctx, relocSpends)
	if err != nil {
		if ctx.Err() == nil {
			h.t.Logf("[designD/drop] relocate spends from bucket %d: %v", doomed, err)
		}
		return
	}
	h.relocatedSpends.Add(ct.RowsAffected())

	if err := h.dropLeafPair(ctx, doomed); err != nil && ctx.Err() == nil {
		h.t.Logf("[designD/drop] drop bucket %d: %v", doomed, err)
	}
}

// pruneDeleteOnce (MODE delete): identical doomed criterion, but reclaim is
// chunked row-DELETEs of mined+fully-spent txs (spends first, then txs, one
// statement via chained data-modifying CTEs). A leaf pair that drains empty is
// then dropped so the leaf count stays bounded — NOTE this generously favors
// delete mode: the design it stands in for (v7 hash partitions) never gets an
// empty partition to drop, and pays autovacuum debt on top.
func (h *designDHarness) pruneDeleteOnce(ctx context.Context) {
	H := h.curH.Load()
	for _, b := range h.attached() {
		if !designDDoomed(b, H) {
			continue
		}
		txLeaf, spLeaf := designDLeaf("d_txs", b), designDLeaf("d_spends", b)
		del := fmt.Sprintf(`
WITH doomed AS (
    SELECT t.hash FROM %s t
    WHERE t.mined_at_height IS NOT NULL
      AND (SELECT count(*) FROM %s s WHERE s.prev_tx_hash = t.hash)
          = COALESCE(cardinality(array_positions(t.out_spendables, TRUE)), 0)
    LIMIT %d
),
del_spends AS (
    DELETE FROM %s s USING doomed d WHERE s.prev_tx_hash = d.hash
)
DELETE FROM %s t USING doomed d WHERE t.hash = d.hash`,
			txLeaf, spLeaf, designDDeleteChunk, spLeaf, txLeaf)
		for {
			if ctx.Err() != nil {
				return
			}
			ct, err := h.pool.Exec(ctx, del)
			if err != nil {
				if ctx.Err() == nil {
					h.t.Logf("[designD/delete] delete chunk bucket %d: %v", b, err)
				}
				return
			}
			n := ct.RowsAffected()
			h.deletedRows.Add(n)
			if n < int64(designDDeleteChunk) {
				break
			}
		}
		var empty bool
		if err := h.pool.QueryRow(ctx, fmt.Sprintf(
			`SELECT NOT EXISTS (SELECT 1 FROM %s) AND NOT EXISTS (SELECT 1 FROM %s)`,
			txLeaf, spLeaf)).Scan(&empty); err != nil {
			if ctx.Err() == nil {
				h.t.Logf("[designD/delete] empty check bucket %d: %v", b, err)
			}
			return
		}
		if !empty {
			continue // survivors (e.g. unmined tips) keep the leaf alive; retry next tick
		}
		if err := h.dropLeafPair(ctx, b); err != nil && ctx.Err() == nil {
			h.t.Logf("[designD/delete] drop empty bucket %d: %v", b, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Background drivers (height ticker, bucket pre-creator, miners, pruner,
// create coalescers) — same shape as the pruned harness's driver goroutines.
// ---------------------------------------------------------------------------

func (h *designDHarness) startDrivers(ctx context.Context, wg *sync.WaitGroup) {
	// HEIGHT: advance the chain independently of reclaim speed.
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(designDHeightTickMS * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				h.curH.Add(1)
			}
		}
	}()

	// BUCKET PRE-CREATOR: keeps the current AND next bucket's leaves attached
	// one bucket ahead of the height ticker, so the hot path never runs DDL.
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(designDHeightTickMS * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				cur := h.bucketOf(h.curH.Load())
				for _, b := range []int64{cur, cur + 1} {
					if err := h.ensureBucket(ctx, b); err != nil && ctx.Err() == nil {
						h.t.Logf("[designD/%s] ensureBucket(%d): %v", h.mode, b, err)
					}
				}
			}
		}
	}()

	// MINERS: drain created (hash,bucket) pairs, stamp mined_at_height in
	// 2000-pair UNNEST-join batches. Workers BLOCK on mineCh when the pipeline
	// saturates — creation back-pressures to the sustainable rate (same
	// bounded-channel design as the pruned harness).
	for m := 0; m < designDMiners; m++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hashes := make([][]byte, 0, designDMineBatch)
			buckets := make([]int32, 0, designDMineBatch)
			flush := func() {
				if len(hashes) == 0 {
					return
				}
				if _, err := h.pool.Exec(ctx, designDMineSQL, hashes, buckets, h.curH.Load()); err != nil {
					if ctx.Err() == nil {
						h.t.Logf("[designD/%s] mine(%d): %v", h.mode, len(hashes), err)
					}
				} else {
					h.minedTotal.Add(int64(len(hashes)))
				}
				hashes = hashes[:0]
				buckets = buckets[:0]
			}
			tk := time.NewTicker(100 * time.Millisecond) // flush partial batches
			defer tk.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case r := <-h.mineCh:
					hashes = append(hashes, r.hash)
					buckets = append(buckets, int32(r.bucket))
					if len(hashes) >= designDMineBatch {
						flush()
					}
				case <-tk.C:
					flush()
				}
			}
		}()
	}

	// PRUNER: the A/B under test. Everything else above is identical per mode.
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(designDPruneInterval)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				if h.mode == "drop" {
					h.pruneDropOnce(ctx)
				} else {
					h.pruneDeleteOnce(ctx)
				}
			}
		}
	}()

	// CREATE COALESCERS (default mode): creates from many workers funnel into
	// 500-row UNNEST bulk inserts, mirroring the store's sendCreateBatchUNNEST.
	if h.coalesce {
		for c := 0; c < designDCoalescers; c++ {
			wg.Add(1)
			go h.coalescer(ctx, wg)
		}
	}
}

func (h *designDHarness) coalescer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	buf := make([]designDCreateReq, 0, designDCoalesceBatch)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		hashes := make([][]byte, len(buf))
		buckets := make([]int32, len(buf))
		fees := make([]int64, len(buf))
		raws := make([][]byte, len(buf))
		for i, r := range buf {
			hashes[i] = r.hash
			buckets[i] = int32(r.bucket)
			fees[i] = r.fee
			raws[i] = r.raw
		}
		_, err := h.pool.Exec(ctx, designDCreateUnnestSQL, hashes, buckets, fees, raws, true)
		for i := range buf {
			buf[i].done <- err
		}
		buf = buf[:0]
	}
	tk := time.NewTicker(designDCoalesceWindow)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			flush() // answer any stragglers (they get the cancellation error)
			return
		case r := <-h.createCh:
			buf = append(buf, r)
			if len(buf) >= designDCoalesceBatch {
				flush()
			}
		case <-tk.C:
			flush()
		}
	}
}

// ---------------------------------------------------------------------------
// Hot path per tx (mimics runPrunedValidator's Get+Spend+Create+Unlock shape)
// ---------------------------------------------------------------------------

// resolveSpend classifies the batched GET+SPEND outcome. Fast path: parent
// found at the exact bucket and the spend row inserted. In drop mode the
// exact-bucket GET can MISS because the pruner relocated the parent — the
// worker then re-probes for the parent's new bucket and re-issues the spend
// there (the spend's bucket column must follow the parent). Any other zero-row
// spend is fatal: it means validation failed or the output was double-spent.
func (h *designDHarness) resolveSpend(ctx context.Context, getErr, spErr error, spendRows int64, parentHash, childHash []byte, height int64) bool {
	if ctx.Err() != nil {
		return false
	}
	if spErr != nil {
		h.fail("spend: %v", spErr)
		return false
	}
	if getErr == nil && spendRows == 1 {
		return true // fast path
	}
	if getErr != nil && getErr != pgx.ErrNoRows {
		h.fail("get: %v", getErr)
		return false
	}
	if getErr == nil && spendRows == 0 {
		h.fail("spend inserted 0 rows with parent visible (parent=%x…)", parentHash[:8])
		return false
	}
	// getErr == pgx.ErrNoRows: relocation moved the parent (drop mode). In
	// delete mode rows never move, so this path failing the find below IS the
	// fatal signal.
	var nb int64
	if err := h.pool.QueryRow(ctx, designDFindSQL, parentHash).Scan(&nb); err != nil {
		if ctx.Err() == nil {
			h.fail("relocated parent %x… not found anywhere: %v", parentHash[:8], err)
		}
		return false
	}
	ct, err := h.pool.Exec(ctx, designDSpendSQL, parentHash, nb, 0, parentHash, childHash, height)
	if err != nil || ct.RowsAffected() != 1 {
		if ctx.Err() == nil {
			h.fail("recovery spend at bucket %d: rows=%d err=%v", nb, ct.RowsAffected(), err)
		}
		return false
	}
	h.recoveredGets.Add(1)
	return true
}

// workerIter runs one Get+Spend+Create+Unlock cycle, extending the worker's
// chain by one tx. Returns false when the worker must stop (error/shutdown).
//
// BATCHING NOTE: the real store amortizes round trips by batching ACROSS
// workers for every op. The default here coalesces only CREATE cross-worker
// (500-row UNNEST) and issues GET+SPEND as ONE per-worker pgx.Batch (one
// round trip, one implicit txn — which also makes GET and SPEND see one
// snapshot) plus a single UNLOCK. With DESIGND_COALESCE=0 all four statements
// ride one per-worker pgx.Batch instead. Either way both reclaim modes use the
// identical pipeline, so absolute TPS is lower than the store's number but the
// A/B is clean.
func (h *designDHarness) workerIter(ctx context.Context, p *designDParent) bool {
	H := h.curH.Load()
	child := makeChildTx(p.tx)
	parentHash := p.tx.TxIDChainHash().CloneBytes()
	childHash := child.TxIDChainHash().CloneBytes()
	childBucket := h.bucketOf(H)

	if h.coalesce {
		// 1+2. GET + SPEND in one batch / round trip.
		b := &pgx.Batch{}
		b.Queue(designDGetSQL, parentHash, p.bucket)
		b.Queue(designDSpendSQL, parentHash, p.bucket, 0, parentHash, childHash, H)
		br := h.pool.SendBatch(ctx, b)
		var fee int64
		getErr := br.QueryRow().Scan(&fee)
		ct, spErr := br.Exec()
		spendRows := int64(0)
		if spErr == nil {
			spendRows = ct.RowsAffected()
		}
		if cErr := br.Close(); cErr != nil && spErr == nil {
			spErr = cErr
		}
		if !h.resolveSpend(ctx, getErr, spErr, spendRows, parentHash, childHash, H) {
			return false
		}

		// 3. CREATE child through the cross-worker coalescer (locked=true).
		req := designDCreateReq{hash: childHash, bucket: childBucket, fee: 100, raw: child.Bytes(), done: make(chan error, 1)}
		select {
		case h.createCh <- req:
		case <-ctx.Done():
			return false
		}
		if err := <-req.done; err != nil {
			if ctx.Err() == nil {
				h.fail("create: %v", err)
			}
			return false
		}

		// 4. UNLOCK.
		uct, uErr := h.pool.Exec(ctx, designDUnlockSQL, childHash, childBucket)
		if uErr != nil || uct.RowsAffected() != 1 {
			if ctx.Err() == nil {
				h.fail("unlock: rows=%d err=%v", uct.RowsAffected(), uErr)
			}
			return false
		}
	} else {
		// All 4 statements in ONE pgx.Batch (one round trip, one implicit txn,
		// so the CREATE is visible to the UNLOCK that follows it).
		b := &pgx.Batch{}
		b.Queue(designDGetSQL, parentHash, p.bucket)
		b.Queue(designDSpendSQL, parentHash, p.bucket, 0, parentHash, childHash, H)
		b.Queue(designDCreateRowSQL, childHash, childBucket, int64(100), child.Bytes())
		b.Queue(designDUnlockSQL, childHash, childBucket)
		br := h.pool.SendBatch(ctx, b)
		var fee int64
		getErr := br.QueryRow().Scan(&fee)
		ct, spErr := br.Exec()
		spendRows := int64(0)
		if spErr == nil {
			spendRows = ct.RowsAffected()
		}
		_, cErr := br.Exec()
		uct, uErr := br.Exec()
		closeErr := br.Close()
		if ctx.Err() != nil {
			return false
		}
		if cErr != nil || uErr != nil || closeErr != nil {
			h.fail("create/unlock batch: create=%v unlock=%v close=%v", cErr, uErr, closeErr)
			return false
		}
		if uct.RowsAffected() != 1 {
			h.fail("unlock: rows=%d", uct.RowsAffected())
			return false
		}
		// resolveSpend AFTER the batch: on a relocation-miss the child was
		// still created+unlocked above (ON CONFLICT-safe), only the spend needs
		// re-issuing at the parent's new bucket.
		if !h.resolveSpend(ctx, getErr, spErr, spendRows, parentHash, childHash, H) {
			return false
		}
	}

	// Hand the new tx to the miners; blocks when the pipeline is saturated
	// (back-pressure, exactly like the pruned harness).
	select {
	case h.mineCh <- designDMined{hash: childHash, bucket: childBucket}:
	case <-ctx.Done():
		return false
	}

	p.tx = child
	p.bucket = childBucket
	return true
}

// ---------------------------------------------------------------------------
// Per-mode runner + measurement
// ---------------------------------------------------------------------------

// designDLiveRows sums the planner's live-tuple estimate over the d_txs leaves
// (same probe style as prunedTableStats).
func (h *designDHarness) designDLiveRows(ctx context.Context) int64 {
	var live int64
	_ = h.pool.QueryRow(ctx,
		`SELECT COALESCE(sum(n_live_tup),0)::bigint FROM pg_stat_user_tables WHERE relname LIKE 'd\_txs\_b%'`,
	).Scan(&live)
	return live
}

func (h *designDHarness) createGenesis(ctx context.Context, n int, startBucket int64) ([]designDParent, error) {
	parents := make([]designDParent, n)
	const chunk = 2000
	for base := 0; base < n; base += chunk {
		end := base + chunk
		if end > n {
			end = n
		}
		sz := end - base
		hashes := make([][]byte, 0, sz)
		buckets := make([]int32, 0, sz)
		fees := make([]int64, 0, sz)
		raws := make([][]byte, 0, sz)
		for i := base; i < end; i++ {
			g := makeGenesisTx(i)
			parents[i] = designDParent{tx: g, bucket: startBucket}
			hashes = append(hashes, g.TxIDChainHash().CloneBytes())
			buckets = append(buckets, int32(startBucket))
			fees = append(fees, 100)
			raws = append(raws, g.Bytes())
		}
		if _, err := h.pool.Exec(ctx, designDCreateUnnestSQL, hashes, buckets, fees, raws, false); err != nil {
			return nil, err
		}
	}
	// Mark genesis rows mined so they are reclaimable once spent — otherwise
	// 10K never-mined rows pin the genesis bucket forever in BOTH modes
	// (relocated every drop / blocking the leaf-drop in delete mode).
	if _, err := h.pool.Exec(ctx,
		`UPDATE d_txs SET mined_at_height = $1 WHERE bucket = $2`, designDStartHeight, startBucket); err != nil {
		return nil, err
	}
	return parents, nil
}

func runDesignDMode(t *testing.T, mode string, workers int) stableStats {
	ctx := context.Background()

	poolCfg, err := pgxpool.ParseConfig(throughputDSN)
	require.NoError(t, err)
	poolCfg.MaxConns = 100
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	require.NoError(t, err)
	defer pool.Close()
	require.NoError(t, pool.Ping(ctx))
	require.NoError(t, designDResetSchema(ctx, pool))

	h := &designDHarness{
		t:        t,
		pool:     pool,
		mode:     mode,
		coalesce: os.Getenv("DESIGND_COALESCE") != "0",
		buckets:  map[int64]bool{},
		mineCh:   make(chan designDMined, designDMineChanCap),
		createCh: make(chan designDCreateReq, designDCoalesceBatch*designDCoalescers),
	}
	h.curH.Store(designDStartHeight)
	startBucket := h.bucketOf(designDStartHeight)
	require.NoError(t, h.ensureBucket(ctx, startBucket))
	require.NoError(t, h.ensureBucket(ctx, startBucket+1))

	parents, err := h.createGenesis(ctx, workers, startBucket)
	require.NoError(t, err)

	driverCtx, cancel := context.WithCancel(ctx)
	var dwg sync.WaitGroup
	h.startDrivers(driverCtx, &dwg)

	runPhase := func(dur time.Duration) int64 {
		var ops atomic.Int64
		var wg sync.WaitGroup
		wg.Add(workers)
		deadline := time.Now().Add(dur)
		for w := 0; w < workers; w++ {
			w := w
			go func() {
				defer wg.Done()
				p := &parents[w] // chain state persists across phases
				for time.Now().Before(deadline) {
					if !h.workerIter(driverCtx, p) {
						return
					}
					ops.Add(1)
				}
			}()
		}
		wg.Wait()
		return ops.Load()
	}

	// Warmup (discarded): fills the pool, primes plan caches, and lets the
	// miner/pruner pipeline reach steady state before the clock starts.
	_ = runPhase(designDWarmup)

	samples := make([]float64, 0, designDReps)
	for r := 0; r < designDReps; r++ {
		start := time.Now()
		ops := runPhase(designDMeasure)
		elapsed := time.Since(start)
		if elapsed <= 0 {
			continue
		}
		tps := float64(ops) / elapsed.Seconds()
		samples = append(samples, tps)
		t.Logf("[rep] %s mode=%s rep=%d/%d tps=%.0f d_txs_live=%d relocated=%d/%d dropped=%d deleted=%d ddl=%dms recovered=%d mined=%d height=%d buckets=%d mineCh=%d/%d",
			time.Now().Format("15:04:05"), mode, r+1, designDReps, tps,
			h.designDLiveRows(ctx),
			h.relocatedTxs.Load(), h.relocatedSpends.Load(),
			h.droppedBuckets.Load(), h.deletedRows.Load(),
			h.ddlNs.Load()/int64(time.Millisecond), h.recoveredGets.Load(),
			h.minedTotal.Load(), h.curH.Load(), len(h.attached()),
			len(h.mineCh), cap(h.mineCh))
	}

	cancel()
	dwg.Wait()

	st := summarize(samples)
	t.Logf("[designD/%s] reclaim totals: relocatedTxs=%d relocatedSpends=%d droppedBuckets=%d deletedRows=%d ddl=%dms recoveredGets=%d mined=%d failures=%d",
		mode, h.relocatedTxs.Load(), h.relocatedSpends.Load(), h.droppedBuckets.Load(),
		h.deletedRows.Load(), h.ddlNs.Load()/int64(time.Millisecond),
		h.recoveredGets.Load(), h.minedTotal.Load(), h.failN.Load())
	t.Logf("[designD/%s] workers=%-6d median=%9.0f mean=%9.0f CV=%5.1f%% range=[%.0f, %.0f] n=%d%s",
		mode, workers, st.median, st.mean, st.cv, st.min, st.max, st.n, unstableFlag(st.cv, 10.0))
	return st
}

// TestThroughput_DesignDSpike runs the cascade-DELETE baseline and the
// generational DROP candidate over the identical schema + hot path and reports
// median TPS per mode plus the drop/delete ratio (gate: >=1.15x with reclaim
// effectively free). Skipped automatically when postgres is unreachable.
func TestThroughput_DesignDSpike(t *testing.T) {
	cleanDB(t) // skips if no postgres; also clears the store's own tables
	terminateOtherConnections(t)

	workers := envInt("THROUGHPUT_WORKERS", 10000)
	coalesce := os.Getenv("DESIGND_COALESCE") != "0"
	t.Logf("[designD] bucketHeights=%d retention=%d heightTick=%dms miners=%d coalesce=%t workers=%d warmup=%s reps=%dx%s",
		designDBucketHeights, designDRetention, designDHeightTickMS, designDMiners,
		coalesce, workers, designDWarmup, designDReps, designDMeasure)

	medians := map[string]float64{}
	for _, mode := range []string{"delete", "drop"} {
		mode := mode
		t.Run("mode_"+mode, func(t *testing.T) {
			st := runDesignDMode(t, mode, workers)
			medians[mode] = st.median
		})
	}

	if medians["delete"] > 0 && medians["drop"] > 0 {
		t.Logf("[designD] RESULT: drop=%.0f vs delete=%.0f => drop/delete = %.3fx (gate: >=1.15x, reclaim ~free)",
			medians["drop"], medians["delete"], medians["drop"]/medians["delete"])
	}
}
