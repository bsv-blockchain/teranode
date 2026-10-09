package utxoset

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// seededTx builds a transaction the way the seeder rebuilds one from a UTXO snapshot: no
// inputs, each unspent output at its own index and nil in the place of every spent one. It does
// not hash to its real ID, which is why the seeder passes that ID with WithTXID.
func seededTx(unspent map[uint32]uint64) *bt.Tx {
	var maxIndex uint32
	for i := range unspent {
		maxIndex = max(maxIndex, i)
	}

	tx := &bt.Tx{Outputs: make([]*bt.Output, maxIndex+1)}

	for i, sats := range unspent {
		script := bscript.Script([]byte{0x76, 0xa9, 0x14, byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x88, 0xac})
		tx.Outputs[i] = &bt.Output{Satoshis: sats, LockingScript: &script}
	}

	return tx
}

func coinCount(t *testing.T, s *Store, txid chainhash.Hash, vout uint32) (n int, flags int16, spendableFrom int32) {
	t.Helper()

	err := s.pool.QueryRow(context.Background(),
		`SELECT count(*), coalesce(max(flags), 0), coalesce(max(spendable_from), 0) FROM utxo WHERE leaf = $1 AND ukey = $2 AND txid = $3`,
		LeafFor(txid[:]), Pack(txid[:], vout), txid[:]).Scan(&n, &flags, &spendableFrom)
	require.NoError(t, err)

	return n, flags, spendableFrom
}

// The seeder hands the store transactions rebuilt from a UTXO snapshot. The store must file
// their coins under the ID the seeder passes, write only the outputs that are present, keep no
// body (the rebuilt bytes are not the transaction's), and take "coinbase" from the seeder's
// option, because a rebuilt coinbase has no input to recognise it by. Before this, the batch
// path panicked measuring the transaction's size over the nil outputs, and the ID and the
// coinbase flag were derived from the rebuilt transaction.
func TestSeederStyleCreate(t *testing.T) {
	for _, viaBatch := range []bool{true, false} {
		name := "single path"
		if viaBatch {
			name = "batch path"
		}

		t.Run(name, func(t *testing.T) {
			s, ctx := newTestStore(t)

			const height = 100

			for _, coinbase := range []bool{false, true} {
				tx := seededTx(map[uint32]uint64{1: 1000, 3: 3000})

				var id chainhash.Hash
				id[0] = 0x5e
				id[1] = 1
				if coinbase {
					id[1] = 2
				}

				opts := []utxo.CreateOption{
					utxo.WithCreateOnly(),
					utxo.WithTXID(&id),
					utxo.WithSetCoinbase(coinbase),
					utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 0, BlockHeight: height, SubtreeIdx: 0}),
				}

				var err error
				if viaBatch {
					_, _, err = s.SpendAndCreate(ctx, tx, height, opts...)
				} else {
					_, err = s.Create(ctx, tx, height, opts[1:]...)
				}

				require.NoError(t, err, "coinbase=%v", coinbase)

				for vout, want := range map[uint32]int{0: 0, 1: 1, 2: 0, 3: 1} {
					n, flags, spendableFrom := coinCount(t, s, id, vout)
					require.Equal(t, want, n, "coinbase=%v output %d", coinbase, vout)

					if n == 1 {
						require.Equal(t, coinbase, flags&FlagCoinbase != 0, "coinbase=%v output %d: the coinbase flag comes from the seeder's option", coinbase, vout)

						if coinbase {
							require.Equal(t, int32(height+s.settings.ChainCfgParams.CoinbaseMaturity), spendableFrom, "a seeded coinbase keeps its maturity")
						}
					}
				}

				var bodies int
				require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_body WHERE txid = $1`, id[:]).Scan(&bodies))
				require.Zero(t, bodies, "the rebuilt bytes are not the transaction, so no body is kept")
			}
		})
	}
}

// A panic inside a create batch must reach every caller in the batch as an error. Without the
// recovery the batcher swallowed it and Create waited forever; that is how the seeder hung on
// the single-transaction path when a rebuilt transaction first reached this store.
func TestCreateBatchPanicAnswersEveryCaller(t *testing.T) {
	s, _ := newTestStore(t)

	script := bscript.Script([]byte{0x51})
	bad := &bt.Tx{Inputs: []*bt.Input{nil}, Outputs: []*bt.Output{{Satoshis: 1, LockingScript: &script}}}

	items := []*createItem{
		{tx: bad, blockHeight: 1, options: &utxo.CreateOptions{}, done: make(chan createResult, 1)},
		{tx: bad, blockHeight: 1, options: &utxo.CreateOptions{}, done: make(chan createResult, 1)},
	}

	s.sendCreateBatch(items)

	for i, it := range items {
		select {
		case r := <-it.done:
			require.Error(t, r.err, "caller %d", i)
		default:
			t.Fatalf("caller %d got no answer: it would wait forever", i)
		}
	}
}

// openSeedingStore opens a second store on the test database with seeding=true on its URL,
// sharing the schema newTestStore created.
func openSeedingStore(t *testing.T, base *Store) *Store {
	t.Helper()

	u, err := url.Parse(testDSN(t))
	require.NoError(t, err)

	q := u.Query()
	q.Set("seeding", "true")
	u.RawQuery = q.Encode()

	s, err := New(context.Background(), ulogger.TestLogger{}, base.settings, u)
	require.NoError(t, err, "seeding=true must not reach the Postgres driver")

	t.Cleanup(func() { _ = s.Close(context.Background()) })

	require.True(t, s.seeding)

	return s
}

// In seeding mode a seed's create writes the coins alone, with their block height, below and
// above the checkpoint: no containment row and no identity row, so a mainnet seed never puts
// tens of gigabytes of rows on the disk for its hook to clear. Lookups still answer it as mined
// at its height, a re-run is refused as an existing transaction, and a create that is not shaped
// like the seeder's takes its normal route.
func TestSeedingModeWritesCoinsOnly(t *testing.T) {
	base, ctx := newTestStore(t) // mainnet checkpoints
	s := openSeedingStore(t, base)

	count := func(sql string, args ...any) int {
		var n int
		require.NoError(t, s.pool.QueryRow(ctx, sql, args...).Scan(&n))

		return n
	}

	for i, height := range []uint32{500, 999_000} {
		var id chainhash.Hash
		id[0] = 0x60
		id[1] = byte(i)

		opts := []utxo.CreateOption{
			utxo.WithCreateOnly(), utxo.WithTXID(&id), utxo.WithSetCoinbase(false),
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 0, BlockHeight: height, SubtreeIdx: 0}),
		}

		_, _, err := s.SpendAndCreate(ctx, seededTx(map[uint32]uint64{0: 10, 2: 20}), height, opts...)
		require.NoError(t, err, "height %d", height)

		require.Equal(t, 2, count(`SELECT count(*) FROM utxo WHERE txid = $1 AND mined_height = $2`, id[:], int32(height)),
			"height %d: both coins carry their height from birth", height) //nolint:gosec // test heights fit int32
		require.Zero(t, count(`SELECT count(*) FROM tx_mined WHERE txid = $1`, id[:]), "height %d: no containment row", height)
		require.Zero(t, count(`SELECT count(*) FROM tx_ident WHERE txid = $1`, id[:]), "height %d: no identity row", height)

		answers, err := s.ParentOutputsForValidation(ctx, []utxo.Outpoint{{TxID: id, Vout: 2}})
		require.NoError(t, err)
		require.NoError(t, answers[0].Err)
		require.Equal(t, utxo.ParentOutputMined, answers[0].Status, "height %d", height)
		require.Equal(t, height, answers[0].Height)

		_, _, err = s.SpendAndCreate(ctx, seededTx(map[uint32]uint64{0: 10, 2: 20}), height, opts...)
		require.ErrorIs(t, err, errors.ErrTxExists, "height %d: a re-run of the seed skips the transaction", height)
		require.Equal(t, 2, count(`SELECT count(*) FROM utxo WHERE txid = $1`, id[:]), "height %d: no coin written twice", height)
	}

	require.Zero(t, count(`SELECT count(*) FROM pg_class WHERE relname ~ '^tx_mined_w[0-9]+$'`),
		"a seed creates no containment windows")

	// A create without the seeder's shape takes the normal route even in seeding mode.
	normal := seededTx(map[uint32]uint64{0: 30})
	normal.Outputs = normal.Outputs[:1]
	_, err := s.Create(ctx, normal, 1_000_000)
	require.NoError(t, err)
	require.Equal(t, 1, count(`SELECT count(*) FROM tx_ident WHERE txid = $1`, normal.TxIDChainHash()[:]))
}
