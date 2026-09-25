package utxo_test

import (
	"context"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	pgstore "github.com/bsv-blockchain/teranode/stores/utxo/postgres"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// 2-shard local experiment
// ---------------------------------------------------------------------------
//
// QUESTION UNDER TEST: how much of the single-instance sustained-with-prune
// wall (~75-88K on this box) is INTRA-INSTANCE contention (8 fixed WALInsert
// slots, one WAL stream, one checkpointer, buffer-mapping locks — wait-event
// sampling showed LWLock:WALInsert dominating) vs raw CPU? Two postgres
// instances on the SAME 16 cores cannot create CPU, so:
//   - result >> single-instance => contention component is real; production
//     2-host sharding multiplier is near-linear, and the long-term client-side
//     hash-routing direction is validated empirically.
//   - result ~= single-instance => the wall is raw CPU; more cores/hosts is
//     the only route past it.
//
// The router is BENCH-ONLY: it embeds shard 0 so the full utxo.Store interface
// is satisfied, and overrides exactly the methods the pruned harness exercises
// with real 2-way routing by txid first bit. It is NOT a production sharding
// layer (no cross-shard tx splitting, no rebalancing, no 2PC).

const throughputDSNShard2 = "postgres://teranode:teranode@localhost:5433/teranode_test"
const throughputDSNShard3 = "postgres://teranode:teranode@localhost:5434/teranode_test"

// cleanDBAt mirrors cleanDB for an arbitrary DSN.
func cleanDBAt(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("no postgres at %s: %v", dsn, err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("no postgres at %s: %v", dsn, err)
	}
	_, _ = pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid != pg_backend_pid()`)
	_, _ = pool.Exec(ctx, `
		DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions, txs, txs_raw,
			dah_watermark, dah_part_watermark, dah_sweep_control CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(BIGINT, INT) CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(INT, BIGINT, INT) CASCADE;
	`)
}

// newPrunedQueueStoreAt mirrors newPrunedQueueStore for an arbitrary DSN.
func newPrunedQueueStoreAt(t *testing.T, dsn string) (*pgstore.Store, func()) {
	t.Helper()
	cleanDBAt(t, dsn)
	ctx := context.Background()

	storeURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn %q: %v", dsn, err)
	}
	storeURL.Scheme = "postgres"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 5
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5
	tSettings.UtxoStore.SpendBatcherSize = 500
	tSettings.UtxoStore.StoreBatcherSize = 500
	// NOTE: fixed 2ms batcher ticks were A/B'd here (each shard sees ~half the
	// worker concurrency, the tier where ticks won in the single-instance
	// sweep) and REGRESSED the 2-shard balanced rate 95.7K -> 88K — the extra
	// flush cadence steals CPU from the reclaim pipeline. Lazy 5ms stays.
	tSettings.GlobalBlockHeightRetention = prunedRetention
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0

	s, err := pgstore.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	if err != nil {
		t.Fatalf("pruned queue store (%s): %v", dsn, err)
	}
	s.Start(ctx)
	return s, func() { s.Stop() }
}

// shardedStore routes the bench hot path across two independent postgres
// stores by txid first bit (h[0]>>7). Embedding shard 0 satisfies the rest of
// the utxo.Store interface; any non-overridden method silently hits shard 0
// only — acceptable ONLY because the pruned harness never calls them.
type shardedStore struct {
	*pgstore.Store // shard 0: default receiver for non-routed methods
	shards         []*pgstore.Store
}

func newShardedStore(stores ...*pgstore.Store) *shardedStore {
	return &shardedStore{Store: stores[0], shards: stores}
}

// shardIdx routes by txid first byte modulo shard count (uniform for random
// txids; workerID-derived genesis hashes distribute fine too).
func (s *shardedStore) shardIdx(h *chainhash.Hash) int {
	return int(h[0]) % len(s.shards)
}

func (s *shardedStore) shardFor(h *chainhash.Hash) *pgstore.Store {
	return s.shards[s.shardIdx(h)]
}

func (s *shardedStore) SetBlockHeight(h uint32) error {
	for _, sh := range s.shards {
		if err := sh.SetBlockHeight(h); err != nil {
			return err
		}
	}
	return nil
}

func (s *shardedStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	return s.shardFor(tx.TxIDChainHash()).Create(ctx, tx, blockHeight, opts...)
}

func (s *shardedStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	return s.shardFor(hash).Get(ctx, hash, f...)
}

// Spend routes by the PARENT tx (spends rows live with the parent). The bench
// children spend outputs of exactly one parent; a production router would have
// to split a tx's inputs across shards.
func (s *shardedStore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, flags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if len(tx.Inputs) == 0 {
		return s.shards[0].Spend(ctx, tx, blockHeight, flags...) // no inputs: pick a deterministic shard rather than panic
	}
	return s.shardFor(tx.Inputs[0].PreviousTxIDChainHash()).Spend(ctx, tx, blockHeight, flags...)
}

func (s *shardedStore) SetLocked(ctx context.Context, hashes []chainhash.Hash, value bool) error {
	byShard := make([][]chainhash.Hash, len(s.shards))
	for _, h := range hashes {
		hh := h
		idx := s.shardIdx(&hh)
		byShard[idx] = append(byShard[idx], h)
	}
	for i := range byShard {
		if len(byShard[i]) == 0 {
			continue
		}
		if err := s.shards[i].SetLocked(ctx, byShard[i], value); err != nil {
			return err
		}
	}
	return nil
}

func (s *shardedStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	perShard := make([][]*chainhash.Hash, len(s.shards))
	for _, h := range hashes {
		idx := s.shardIdx(h)
		perShard[idx] = append(perShard[idx], h)
	}
	out := make(map[chainhash.Hash][]uint32, len(hashes))
	for i := range perShard {
		if len(perShard[i]) == 0 {
			continue
		}
		m, err := s.shards[i].SetMinedMulti(ctx, perShard[i], info)
		if err != nil {
			return nil, err
		}
		for k, v := range m {
			out[k] = v
		}
	}
	return out, nil
}

// GetPrunerService returns a service that drives ALL shards' pruners.
func (s *shardedStore) GetPrunerService() (pruner.Service, error) {
	services := make([]pruner.Service, len(s.shards))
	for i, sh := range s.shards {
		svc, err := sh.GetPrunerService()
		if err != nil {
			return nil, err
		}
		services[i] = svc
	}
	return &shardedPruner{services: services}, nil
}

type shardedPruner struct {
	services []pruner.Service
}

func (p *shardedPruner) Start(ctx context.Context) {
	for _, svc := range p.services {
		svc.Start(ctx)
	}
}

// Prune runs every shard's prune slice IN PARALLEL — each shard's Prune is
// already internally bounded (one sweep slice + bounded delete batches).
func (p *shardedPruner) Prune(ctx context.Context, blockHeight uint32, blockHash string) (int64, error) {
	var wg sync.WaitGroup
	totals := make([]int64, len(p.services))
	errs := make([]error, len(p.services))
	for i := range p.services {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			totals[i], errs[i] = p.services[i].Prune(ctx, blockHeight, blockHash)
		}()
	}
	wg.Wait()
	var total int64
	var firstErr error
	for i := range p.services {
		total += totals[i]
		if errs[i] != nil && firstErr == nil {
			firstErr = errs[i]
		}
	}
	return total, firstErr
}

func (p *shardedPruner) AddObserver(o pruner.Observer) {
	for _, svc := range p.services {
		svc.AddObserver(o)
	}
}

// TestThroughput_QueueStorePruned2Shard runs the EXACT pruned harness against
// two postgres instances (ports 5432 + 5433) behind the bit-routed shardedStore.
//
//	THROUGHPUT_WORKERS=10000 THROUGHPUT_TIER2=0 THROUGHPUT_REPS=16 THROUGHPUT_VERBOSE=1 \
//	  go test ./stores/utxo/ -run TestThroughput_QueueStorePruned2Shard -count=1 -v -timeout 25m
//
// NOTE: the rep-line table telemetry (txs_rows/stamped) and the table-size gate
// poll only shard 0's statPool, so the gate's effective TOTAL cap is ~2x the
// single-instance run's — read absolute table numbers as per-shard.
// requireThroughputEnabled skips the multi-instance sharded throughput harnesses
// unless THROUGHPUT_ENABLE=1. They are deliberate, very expensive perf probes
// (multiple local postgres instances, long runs) that should not fire on a plain
// `go test ./...` just because a local postgres happens to be reachable.
func requireThroughputEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("THROUGHPUT_ENABLE") != "1" {
		t.Skip("sharded throughput harness disabled; set THROUGHPUT_ENABLE=1 to run")
	}
}

func TestThroughput_QueueStorePruned2Shard(t *testing.T) {
	requireThroughputEnabled(t)
	terminateOtherConnections(t)
	cfg := defaultStableCfg()

	statPool, err := pgxpool.New(context.Background(), throughputDSN)
	if err != nil {
		t.Skipf("no postgres: %v", err)
	}
	defer statPool.Close()

	t.Logf("[Pruned 2-Shard] retention=%d heightTick=%dms miners=%d reps=%d warmup=%s measure=%s workers=%v",
		prunedRetention, prunedHeightTickMS, prunedMiners, cfg.reps, cfg.warmup, cfg.measure, cfg.workers)

	for _, w := range cfg.workers {
		s0, stop0 := newPrunedQueueStoreAt(t, throughputDSN)
		s1, stop1 := newPrunedQueueStoreAt(t, throughputDSNShard2)
		sharded := newShardedStore(s0, s1)

		samples := runPrunedValidator(t, sharded, w, cfg, statPool)
		stop0()
		stop1()

		st := summarize(samples)
		t.Logf("[Pruned 2-Shard] workers=%-6d median=%9.0f mean=%9.0f CV=%5.1f%% range=[%.0f, %.0f] n=%d%s",
			w, st.median, st.mean, st.cv, st.min, st.max, st.n, unstableFlag(st.cv, cfg.unstableCV))
	}
}

// TestThroughput_QueueStorePruned3Shard: same harness across THREE local
// instances (5432/5433/5434). Probes whether a third WAL stream/checkpointer
// still buys contention relief or whether total CPU is already the wall.
func TestThroughput_QueueStorePruned3Shard(t *testing.T) {
	requireThroughputEnabled(t)
	terminateOtherConnections(t)
	cfg := defaultStableCfg()

	statPool, err := pgxpool.New(context.Background(), throughputDSN)
	if err != nil {
		t.Skipf("no postgres: %v", err)
	}
	defer statPool.Close()

	for _, w := range cfg.workers {
		s0, stop0 := newPrunedQueueStoreAt(t, throughputDSN)
		s1, stop1 := newPrunedQueueStoreAt(t, throughputDSNShard2)
		s2, stop2 := newPrunedQueueStoreAt(t, throughputDSNShard3)
		sharded := newShardedStore(s0, s1, s2)

		samples := runPrunedValidator(t, sharded, w, cfg, statPool)
		stop0()
		stop1()
		stop2()

		st := summarize(samples)
		t.Logf("[Pruned 3-Shard] workers=%-6d median=%9.0f mean=%9.0f CV=%5.1f%% range=[%.0f, %.0f] n=%d%s",
			w, st.median, st.mean, st.cv, st.min, st.max, st.n, unstableFlag(st.cv, cfg.unstableCV))
	}
}
