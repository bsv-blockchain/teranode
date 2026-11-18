package pruner

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	storelogger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// The worker against the real utxoset store on a real postgres, with the chain in the
// sqlitememory blockchain store. The worker asks the store for its stamp, one drain runs all 256
// pages of partition 0, the transaction seen at 99 and mined at 100 gets the block that the
// blockchain store actually holds at height 100 written onto its UTXO, its identity row is
// deleted, and the completion floor advances. This is the wiring the unit tests fake.
func TestStampWorkerDrainsTheRealUTXOSetStore(t *testing.T) {
	drainTheRealUTXOSetStore(t, func(store utxo.Store) utxo.Store { return store })
}

// The same drain with the store held through stores/utxo/logger, as every caller holds it when
// logging=true. The wrapper forwards Stamper like any other method, so the worker reaches the
// real store's stamp and stamps it. Nothing finds the stamp by asserting the type of the store
// it holds, which is the step a wrapper used to defeat.
func TestStampWorkerDrainsTheRealUTXOSetStoreThroughTheLoggingWrapper(t *testing.T) {
	drainTheRealUTXOSetStore(t, func(store utxo.Store) utxo.Store {
		return storelogger.New(context.Background(), ulogger.TestLogger{}, store)
	})
}

// freshUTXOSetDSN returns a DSN for an empty database. With UTXOSET_TEST_DSN set, as the utxoset
// package's own tests honour it, it creates a database of its own on that server and drops it
// afterwards, so a machine without Docker still runs these tests against a real postgres.
// Otherwise it takes one from the shared test container, and skips when Docker is absent.
func freshUTXOSetDSN(t *testing.T) string {
	t.Helper()

	if base := os.Getenv("UTXOSET_TEST_DSN"); base != "" {
		ctx := context.Background()

		admin, err := pgx.Connect(ctx, base)
		require.NoError(t, err)

		t.Cleanup(func() { _ = admin.Close(ctx) })

		name := fmt.Sprintf("stamp_worker_%d", time.Now().UnixNano())

		_, err = admin.Exec(ctx, "CREATE DATABASE "+name)
		require.NoError(t, err)

		t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })

		u, err := url.Parse(base)
		require.NoError(t, err)

		u.Path = "/" + name

		return u.String()
	}

	dsn, cleanup, err := tpostgres.SetupTestPostgresContainer()
	if err != nil {
		test.SkipIfContainerUnavailable(t, err)
		t.Fatalf("postgres container unavailable: %v", err)
	}

	t.Cleanup(func() { _ = cleanup() })

	return dsn
}

func drainTheRealUTXOSetStore(t *testing.T, wrap func(utxo.Store) utxo.Store) {
	t.Helper()

	dsn := freshUTXOSetDSN(t)

	ctx := context.Background()
	chain := newTestChain(t, 575)

	// Regtest: no checkpoints, so a create at 99 takes the identity route and is the stamp's
	// work; coinbase maturity 1, which still rounds up to a stamp depth of 288.
	tSettings := test.CreateBaseTestSettings(t)
	require.Empty(t, tSettings.ChainCfgParams.Checkpoints)

	storeURL, err := url.Parse(dsn)
	require.NoError(t, err)

	store, err := utxoset.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(ctx) })

	require.Equal(t, uint32(testStampDepth), store.StampDepth())

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0,
		"76a914000000000000000000000000000000000000000088ac", 100000))
	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	tx.AddOutput(&bt.Output{Satoshis: 5_000, LockingScript: script})

	_, err = store.Create(ctx, tx, 99)
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: chain.ids[100], BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(575))

	s := newStampTestServer(t, chain.client, nil)
	s.utxoStore = wrap(store)
	s.stamper = s.findStamper()
	require.Same(t, store, s.stamper, "the worker drives the utxoset store's own stamp")

	report := s.runStampDrain(s.ctx, notificationAt(chain, 575), false)

	require.Equal(t, "completed", report.outcome)
	require.Equal(t, 1, report.windows)
	require.Equal(t, 256, report.pages)
	require.Equal(t, uint32(0), report.residualLag)

	floors, err := store.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(288), floors.StampCompleteFloor)
	require.Equal(t, uint32(288), floors.StampFence)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	t.Cleanup(pool.Close)

	var minedHeight, blockID int32

	err = pool.QueryRow(ctx, `SELECT mined_height, block_id FROM utxo WHERE txid = $1`, tx.TxIDChainHash()[:]).Scan(&minedHeight, &blockID)
	require.NoError(t, err)
	require.Equal(t, int32(100), minedHeight)
	require.Equal(t, int32(chain.ids[100]), blockID, "the block the blockchain store holds at 100") //nolint:gosec // small

	var identRows int

	err = pool.QueryRow(ctx, `SELECT count(*) FROM tx_ident WHERE txid = $1`, tx.TxIDChainHash()[:]).Scan(&identRows)
	require.NoError(t, err)
	require.Equal(t, 0, identRows, "the identity row is deleted by the stamp")

	var stampedAt int32

	err = pool.QueryRow(ctx, `SELECT stamped_at FROM tx_mined_stamped WHERE window_start = 0`).Scan(&stampedAt)
	require.NoError(t, err)
	require.Equal(t, int32(575+288), stampedAt, "the live tip plus one undo partition of margin")
}
