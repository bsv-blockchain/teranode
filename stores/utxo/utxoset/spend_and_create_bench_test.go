package utxoset

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// BenchmarkSpendAndCreateConcurrent is the production shape: many goroutines each calling
// SpendAndCreate on an independent transaction, as the validator and both block application
// paths do. It is run twice per worker count, once with the batcher off so every call is its
// own database transaction, and once with it on, so the two can be read side by side.
//
// The reported unit is transactions per second across all workers, which is the number that
// matters on the mainnet box, where the store was committing once per transaction.
func BenchmarkSpendAndCreateConcurrent(b *testing.B) {
	for _, workers := range []int{16, 64, 512} {
		for _, mode := range []string{"single", "window", "window1", "drain"} {
			name := fmt.Sprintf("workers%d/%s", workers, mode)

			b.Run(name, func(b *testing.B) {
				s, ctx := benchStoreWith(b, func(st *settings.Settings) {
					switch mode {
					case "single":
						st.UtxoStore.StoreBatcherSize = 1
					default:
						st.UtxoStore.StoreBatcherSize = 500
						st.UtxoStore.StoreBatcherDurationMillis = 25
						if mode == "window1" {
							st.UtxoStore.StoreBatcherDurationMillis = 1
						}
						st.UtxoStore.BatcherMaxConcurrent = 16
						st.BatcherBackground = true
						st.UtxoStore.StoreBatcherDrainMode = mode == "drain"
					}
				})

				// Untimed: one parent per iteration, written straight through the single
				// create path so the setup measures nothing.
				require.NoError(b, s.ensureTxBodyPartition(ctx, 700_000))

				children := make([]*bt.Tx, b.N)

				for i := 0; i < b.N; i++ {
					parent := benchTx(1)
					if err := createDirect(s, ctx, parent, 700_000); err != nil {
						b.Fatalf("setup: %v", err)
					}

					child := bt.NewTx()
					if err := child.FromUTXOs(&bt.UTXO{
						TxIDHash: parent.TxIDChainHash(), Vout: 0,
						LockingScript: parent.Outputs[0].LockingScript,
						Satoshis:      parent.Outputs[0].Satoshis,
					}); err != nil {
						b.Fatalf("setup child: %v", err)
					}

					child.AddOutput(&bt.Output{Satoshis: 1_000, LockingScript: parent.Outputs[0].LockingScript})
					children[i] = child
				}

				b.ReportAllocs()
				b.ResetTimer()

				var (
					wg   sync.WaitGroup
					next atomic.Int64
					bad  atomic.Value
				)

				next.Store(-1)

				for w := 0; w < workers; w++ {
					wg.Add(1)

					go func() {
						defer wg.Done()

						for {
							i := int(next.Add(1))
							if i >= b.N {
								return
							}

							if _, _, err := s.SpendAndCreate(ctx, children[i], 700_001); err != nil {
								bad.Store(err)
								return
							}
						}
					}()
				}

				wg.Wait()
				b.StopTimer()

				if v := bad.Load(); v != nil {
					b.Fatalf("spendAndCreate: %v", v)
				}

				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tx/s")
				wipe(b, s, ctx)
			})
		}
	}
}

// benchStoreWith is benchStore with the settings adjusted before the store is opened.
func benchStoreWith(b *testing.B, tune func(*settings.Settings)) (*Store, context.Context) {
	b.Helper()

	ctx := context.Background()
	dsn := testDSN(b)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(b, err)
	require.NoError(b, pool.Ping(ctx))

	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS utxo CASCADE;
	                       DROP TABLE IF EXISTS spend_journal CASCADE;
	                       DROP TABLE IF EXISTS tx_ident CASCADE;
	                       DROP TABLE IF EXISTS tx_body CASCADE;
	                       DROP TABLE IF EXISTS applied_block CASCADE;
	                       DROP TABLE IF EXISTS applied_chunk CASCADE;`)
	pool.Close()

	u, err := url.Parse(dsn)
	require.NoError(b, err)

	tSettings := settings.NewSettings()
	if tune != nil {
		tune(tSettings)
	}

	s, err := New(ctx, ulogger.TestLogger{}, tSettings, u)
	require.NoError(b, err)

	b.Cleanup(func() { _ = s.Close(ctx) })

	return s, ctx
}
