package utxoset

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so the create and spend
// statements run unchanged whether they are issued standalone or composed inside one
// transaction by SpendAndCreate. That composition is the whole reason this abstraction
// exists: it is what lets a failed create undo its spends with a ROLLBACK instead of
// compensating logic.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Store is the delete-on-spend UTXO-set store.
//
// It satisfies the same utxo.Store contract as the postgres, aerospike and sql stores,
// so the store-agnostic conformance suite in stores/utxo/tests applies to it unchanged.
// That suite is the specification: M1 is complete when it passes.
type Store struct {
	logger   ulogger.Logger
	settings *settings.Settings
	pool     *pgxpool.Pool

	// utxo.BlockStateFields supplies the chain-tip pair — block height and median
	// block time — and the six Store methods that read and write it, over a single
	// atomic snapshot. Embedding the shared implementation rather than carrying two
	// independent atomics here is what keeps GetBlockHeight and GetBlockState
	// reading the same memory, so they cannot disagree (issue 1443).
	utxo.BlockStateFields

	// journalLeaf is the spend-journal leaf the last spend landed in, so the catalog is only
	// touched when it changes.
	journalLeaf atomic.Uint32

	// journalDDL serialises spend-journal partition creation within this process.
	// CREATE TABLE IF NOT EXISTS is not concurrency-safe in PostgreSQL.
	journalDDL sync.Mutex

	// journalRetention is how far back spends stay undoable, in blocks.
	journalRetention uint32

	// bodyWindow is the tx_body window the last create landed in, so the catalog is only
	// touched when it changes.
	bodyWindow atomic.Uint32

	// bodyDDL serialises tx_body partition creation within this process, for the same
	// reason journalDDL does: CREATE TABLE IF NOT EXISTS is not concurrency-safe.
	bodyDDL sync.Mutex

	// minedWindow is the tx_mined window the last block-path create landed in, so the
	// catalog is only touched when it changes. Holds window+1 so zero means "nothing yet".
	minedWindow atomic.Uint32

	// minedDDL serialises tx_mined window creation within this process.
	minedDDL sync.Mutex

	// bodyRetention is how long the serialized transaction bytes are kept, in blocks.
	bodyRetention uint32

	// bodyCheckpoints is the checkpoint list a MINED create's height is tested against to
	// decide whether it writes serialized bytes at all: the active network's list, when
	// utxostore_skipTxBodyBelowCheckpoint is on, and nil otherwise.
	//
	// The LIST is kept rather than a height, because the test is model.BelowCheckpoint and not
	// a comparison written here. model/checkpoint.go is the single boundary definition every
	// below-checkpoint gate shares -- the outpoint-only spend gate this rides behind included
	// -- and re-deriving it locally is how two gates end up disagreeing. They would already
	// disagree at genesis: BelowCheckpoint excludes height 0, a bare `height <= highest` does
	// not, so the coinbase of the genesis block keeps its body under the shared rule and lost
	// it under the local one.
	//
	// NIL MEANS OFF, and it means off for two reasons that coincide rather than one standing
	// in for the other: the setting being off leaves it nil, and a network with no checkpoints
	// (regtest, teratestnet) has nothing to be below. BelowCheckpoint answers false for both,
	// because it requires a highest checkpoint above 0, so one predicate covers both without a
	// second flag to keep in step with this one.
	//
	// See the setting's longdesc for why below the checkpoint the bytes are not needed: the
	// subtree data files hold them, the outpoint-only spend route reads no parent body, and
	// this store's decorate reads the coin row.
	bodyCheckpoints []chaincfg.Checkpoint

	// coinIndexDecider decides whether a utxo_pN_ukey index has bloated past the point
	// worth a REINDEX CONCURRENTLY. New sets it to coinIndexNeedsRebuild; it exists as a
	// field, rather than the pruner calling that function directly, so a test can swap in a
	// stub and observe that the pruner consulted it, without needing a real index bloated
	// past the threshold or a real REINDEX to complete.
	coinIndexDecider func(indexBytes, rows int64) bool

	// createBatcher collects Create calls arriving from many goroutines and sends them as
	// one pipelined round trip.
	//
	// Without it every Create was three separate round trips and this store ran at roughly
	// 470 creates a second on a local instance, because the cost was round trips rather than
	// rows. Both other implementations of this interface batch, the sql store through four
	// batchers and aerospike through six, and the measured difference on the one call that
	// already took a slice was 19x: 52 microseconds per transaction batched against 1,008
	// unbatched.
	//
	// nil when the configured size is 1 or less, which is how a caller asks for the
	// unbatched path.
	createBatcher *batcher.Batcher[createItem]

	// createInFlight counts batches whose database work has started but not finished.
	//
	// The batcher's own Close guarantees every queued item has been HANDED TO the callback,
	// not that the callback has returned. With background dispatch the callback hands the
	// work to a pool and returns immediately, so writes were still landing after Close. That
	// is wrong on its own terms, and it showed up as one test's batch inserting into tables
	// the next test had already dropped and recreated.
	createInFlight sync.WaitGroup

	// spendAndCreateBatcher collects SpendAndCreate calls, which is every production write,
	// and applies each batch in one database transaction. See spend_and_create_batch.go.
	// nil when the configured size is 1 or less.
	spendAndCreateBatcher  *batcher.Batcher[spendAndCreateItem]
	spendAndCreateInFlight sync.WaitGroup

	// closed is set at the top of Close, so a SpendAndCreate arriving after it takes the
	// single path and gets the pool's error rather than a send on the batcher's closed channel.
	closed atomic.Bool

	// getBatcher funnels single reads into one BatchDecorate call. Reads take no locks, so
	// batches may run concurrently.
	getBatcher  *batcher.Batcher[getItem]
	getInFlight sync.WaitGroup

	// lockBatcher collects the single-hash lock changes that two-phase commit produces, one
	// per mempool transaction. Serialised, because two batches can name the same transaction
	// and the update touches its coin rows.
	lockBatcher  *batcher.Batcher[lockItem]
	lockInFlight sync.WaitGroup
}

// New opens the store and installs the schema.
//
// The store is reached by its own URL scheme, utxoset://, so that an operator selecting it
// has to mean it rather than flipping a query parameter on a postgres:// URL that points at
// an incompatible schema. pgx knows nothing about that scheme, and given it will not fail
// cleanly: it folds the entire URL into a config parameter and falls back to a unix socket,
// which surfaces as a baffling "unrecognized configuration parameter" against the wrong
// host. So normalise it here, at the one place that owns the connection.
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
	dsn := *storeURL
	if dsn.Scheme == "utxoset" {
		dsn.Scheme = "postgres"
	}

	cfg, err := pgxpool.ParseConfig(dsn.String())
	if err != nil {
		return nil, errors.NewStorageError("[utxoset] parse dsn", err)
	}

	// Postgres must not JIT-compile this store's queries, and the reason is the row estimates
	// rather than anything about JIT itself.
	//
	// Every hot statement here hands its batch over as an array and unpacks it with unnest, so
	// the planner has no statistics for the values it will be given. It guesses, and because
	// every table is partitioned the guess is then multiplied by the partition count. On the
	// mainnet soak box the live-coin probe reads about twenty index pages and is costed at
	// 679,043, and the decorate read is costed at 1,465,539. Postgres compiles above 100,000 and inlines
	// and optimises above 500,000, so both clear every threshold on every execution.
	//
	// The compile is not the flat cost it looks like. The same statement took 1,430,530 ms, then
	// 499 ms on the very next execution in the same session, against 28 ms with JIT off. Nothing
	// caches compiled code between executions, so what varied was whether LLVM's own pages were
	// still resident. Under memory pressure they are not, and faulting them back in costs
	// minutes. At its best JIT makes this query eighteen times slower, at its worst fifty
	// thousand. There is no state in which it wins.
	//
	// This rides on the pool rather than the server because the blockchain store shares the same
	// postgres instance. Its heaviest statement is costed at 37,422, well under the threshold, so
	// it neither gains nor loses from JIT and should not have the choice made on its behalf.
	//
	// RuntimeParams travels in the startup packet, so a reconnect carries it too. jit is a
	// USERSET setting, which is what makes that legal.
	cfg.ConnConfig.RuntimeParams["jit"] = "off"

	// Every statement is planned with its actual parameter values, every time. Never from a
	// generic plan.
	//
	// pgx prepares each statement once per connection and PostgreSQL, after the fifth
	// execution, switches a prepared statement to a generic plan if that plan's estimated cost
	// is no worse than the custom plans were. For this store's statements the estimate is
	// wrong in both places that matter. The batch arrives as arrays and is unpacked with
	// unnest, so a generic plan has no idea whether it is joining one key or five hundred. And
	// the eligibility tests on the coin row are bit masks on flags, which the planner cannot
	// estimate at all and costs as if almost no row survives them. Put together, the generic
	// plan can decide the coin table is a handful of rows worth rescanning per key.
	//
	// Measured on a 40,000-row coin table with the 500-key spend statement: executions one to
	// five took 8 ms each and the sixth took 1,070 ms, as did every one after it. That is the
	// generic plan taking over, and it turns a batch that should be linear in its width into
	// one that is quadratic. The single-key statement never showed it because with one key a
	// rescan is one scan.
	//
	// The custom plan costs about 0.2 ms of planning per execution, which a batch of hundreds
	// does not notice. This rides on the pool for the same reason jit does: the setting is
	// USERSET, travels in the startup packet, and does not touch the blockchain store sharing
	// the instance.
	cfg.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_custom_plan"

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset] open pool", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.NewStorageError("[utxoset] ping", err)
	}

	s := &Store{logger: logger, settings: tSettings, pool: pool,
		journalRetention: DefaultSpendJournalRetentionBlocks,
		bodyRetention:    DefaultTxBodyRetentionBlocks,
		coinIndexDecider: coinIndexNeedsRebuild}

	// The SAME checkpoint list the outpoint-only spend gate tests against
	// (model.OutpointOnlyEligible -> model.BelowCheckpoint), so the heights at which this store
	// stops writing bodies cannot drift from the heights at which spends stop reading them.
	// The height in the log line is for the operator; the gate itself never uses it.
	if tSettings.UtxoStore.SkipTxBodyBelowCheckpoint && tSettings.ChainCfgParams != nil {
		s.bodyCheckpoints = tSettings.ChainCfgParams.Checkpoints

		logger.Infof("[utxoset] skipping the tx_body write for transactions mined below the highest hardcoded checkpoint, at height %d; block persister and the asset service need the subtree data files for those blocks, and no body is ever written retroactively", model.HighestCheckpointHeight(s.bodyCheckpoints))
	}

	if err := CreateSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, errors.NewStorageError("[utxoset] create schema", err)
	}

	// The create batcher, sized from the same settings the sql and aerospike stores use, so
	// one knob tunes every implementation.
	//
	// Dispatch is serialised rather than backgrounded, which differs from the sql store's
	// create batcher. See newCreateBatcher for why: this callback may also run DDL, and
	// Close can only be honest about drained work if the callback has actually returned.
	if size := tSettings.UtxoStore.StoreBatcherSize; size > 1 {
		d := time.Duration(tSettings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
		s.createBatcher = newCreateBatcher(s, size, d,
			tSettings.UtxoStore.StoreBatcherDrainMode, tSettings.UtxoStore.StoreBatcherGreedyAccumulate)

		// The production write path. Same width and window as the create batcher, because
		// they carry the same statement; concurrency from the bound every other store's
		// batchers share.
		s.spendAndCreateBatcher = newSpendAndCreateBatcher(s, size, d, tSettings.BatcherBackground,
			tSettings.UtxoStore.BatcherMaxConcurrent,
			tSettings.UtxoStore.StoreBatcherDrainMode, tSettings.UtxoStore.StoreBatcherGreedyAccumulate)
	}

	if size := tSettings.UtxoStore.GetBatcherSize; size > 1 {
		d := time.Duration(tSettings.UtxoStore.GetBatcherDurationMillis) * time.Millisecond
		s.getBatcher = newGetBatcher(s, size, d,
			tSettings.UtxoStore.GetBatcherDrainMode, tSettings.UtxoStore.GetBatcherGreedyAccumulate)
	}

	if size := tSettings.UtxoStore.LockedBatcherSize; size > 1 {
		d := time.Duration(tSettings.UtxoStore.LockedBatcherDurationMillis) * time.Millisecond
		s.lockBatcher = newLockBatcher(s, size, d,
			tSettings.UtxoStore.LockedBatcherDrainMode, tSettings.UtxoStore.LockedBatcherGreedyAccumulate)
	}

	return s, nil
}

// SupportsOutpointOnlySpend reports that this store can spend by outpoint alone.
//
// This gates the below-checkpoint fast path (model.OutpointOnlyEligible). It is true
// here for a structural reason rather than as an optimisation: the UTXO table IS the
// outpoint set, so a spend needs nothing but the outpoint to be authorised.
func (s *Store) SupportsOutpointOnlySpend() bool { return true }

// SetBlockHeight records the chain height.
//
// It deliberately does NOT decide whether spends are reversible. An earlier version of
// this drove sync mode from model.BelowCheckpoint here, switching the journal off for
// the whole initial sync on the reasoning that a reorg is impossible below the
// checkpoint. That reasoning is sound and still irrelevant, because the journal's
// second job outweighs its first: see the spend_journal comment in schema.go for why it
// has no off-switch.
func (s *Store) SetBlockHeight(height uint32) error {
	return s.BlockStateFields.SetBlockHeight(height)
}

func (s *Store) PoolMaxConns() int { return int(s.pool.Config().MaxConns) }

func (s *Store) Close(_ context.Context) error {
	s.closed.Store(true)

	// Stop the batcher BEFORE the pool, and stop it at all.
	//
	// Without this the batcher's goroutine outlives the store: a batch still queued when
	// Close returns flushes afterwards, into a pool that is gone or, worse, into whatever
	// schema exists by then. That surfaced as "no partition of relation tx_body found for
	// row" in a test whose predecessor's batch landed after the tables had been dropped and
	// recreated. Close drains what is queued and shuts the worker down, so a caller that has
	// closed the store has genuinely finished with it.
	if s.createBatcher != nil {
		s.createBatcher.Close() // every queued item has now been handed to the callback
		s.createInFlight.Wait() // and now every callback has actually finished
	}

	if s.spendAndCreateBatcher != nil {
		s.spendAndCreateBatcher.Close()
		s.spendAndCreateInFlight.Wait()
	}

	// getBatcher and lockBatcher take no schema-changing locks, but the same rule applies:
	// a batch still in flight when Close returns would run against a pool that Close is
	// about to close underneath it.
	if s.getBatcher != nil {
		s.getBatcher.Close()
		s.getInFlight.Wait()
	}

	if s.lockBatcher != nil {
		s.lockBatcher.Close()
		s.lockInFlight.Wait()
	}

	s.pool.Close()

	return nil
}

func (s *Store) Health(ctx context.Context, _ bool) (int, string, error) {
	if err := s.pool.Ping(ctx); err != nil {
		return 503, "utxoset: unreachable", err
	}

	return 200, "utxoset: ok", nil
}
