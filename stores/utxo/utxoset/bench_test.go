package utxoset

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// Throughput benchmarks for the delete-on-spend store.
//
// These follow the method of the create-path benchmark on the append-only store (PR 684,
// stores/utxo/postgres/create_bench_test.go) so the two are comparable: a throwaway
// database, a clean slate and fresh unique data built OUTSIDE the timer, only the operation
// under test inside it, and the result reported as nanoseconds per row.
//
// They do not port that benchmark's code, because it measures machinery this store does not
// have. That one times an internal batcher flushing a slice of queued items through one
// bulk statement. This store has no batcher: block application calls SpendAndCreate once per
// transaction, so per-transaction cost including its round trip IS the honest unit, and
// measuring anything narrower would flatter it.
//
// What is measured is chosen for what this design claims. Create is the insert cost across
// three tables. Spend is the single DELETE that the whole design rests on, which arbitrates
// the double spend, returns the satoshis and locking script for validation, and reclaims the
// row, all in one statement. SpendAndCreate is what block application actually calls. The
// mined stamp is the update that has to stay off the index.

// benchSeq makes every benchmark transaction unique. A counter, not a random value: an
// earlier version drew satoshis from a one-million-wide random range, and the birthday
// bound put a duplicate transaction id inside a thousand iterations, which the store
// correctly rejected as an existing transaction and which failed the benchmark in its own
// setup rather than in the code under test.
var benchSeq atomic.Uint64

// benchTx builds a unique transaction with n spendable outputs.
func benchTx(n int) *bt.Tx {
	tx := bt.NewTx()

	// A non-coinbase input, so the coinbase maturity path is not taken. The sequence number
	// carries the uniqueness, so the transaction id is distinct without relying on chance.
	_ = tx.From("0000000000000000000000000000000000000000000000000000000000000001",
		uint32(benchSeq.Add(1)&0x7fff_ffff), //nolint:gosec // bounded above
		"76a914000000000000000000000000000000000000000088ac", 100_000)

	for i := 0; i < n; i++ {
		_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(10_000+i))
	}

	return tx
}

// benchStore gives each benchmark a store on the same instance the tests run against, with
// the schema freshly installed.
func benchStore(b *testing.B) (*Store, context.Context) {
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

	s, err := New(ctx, ulogger.TestLogger{}, settings.NewSettings(), u)
	require.NoError(b, err)

	b.Cleanup(func() { _ = s.Close(ctx) })

	return s, ctx
}

// truncateAllSQL empties every table a create or a spend writes to, without dropping the
// schema, so setup cost stays out of the timed section.
//
// EVERY table, and the list is here once rather than spelled out at each call site because it
// has already cost a measurement. tx_mined, conflict_children and preserved_parent were added
// by the reshape and not added here, so a benchmark's later iterations ran against a membership
// table carrying every earlier iteration's rows -- and the block-path create probes tx_mined
// twice per transaction. Attributing a measured slowdown found that the missing truncate
// accounted for two thirds of it. A benchmark whose setup leaks state is measuring the leak.
const truncateAllSQL = `TRUNCATE utxo, tx_ident, tx_body, tx_mined, spend_journal,
                                 conflict_children, preserved_parent CASCADE`

// wipe returns the store to empty without dropping the schema, so setup cost stays out of
// the timed section.
func wipe(b *testing.B, s *Store, ctx context.Context) {
	b.Helper()

	_, err := s.pool.Exec(ctx, truncateAllSQL)
	require.NoError(b, err)
}

// BenchmarkCreate measures storing a transaction: the identity row, the serialized body, and
// one coin row per spendable output.
//
// "Plain" is a mempool arrival, which is left in the waiting set. "Mined" carries block
// information, which additionally packs the membership and clears the waiting marker. The
// two should be close, because the difference is one more column on the same insert.
func BenchmarkCreate(b *testing.B) {
	s, ctx := benchStore(b)

	for _, outs := range []int{1, 2, 8} {
		for _, mined := range []bool{false, true} {
			name := fmt.Sprintf("outputs%d/plain", outs)
			if mined {
				name = fmt.Sprintf("outputs%d/mined", outs)
			}

			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()

				txs := make([]*bt.Tx, b.N)
				for i := range txs {
					txs[i] = benchTx(outs)
				}

				var opts []utxo.CreateOption
				if mined {
					opts = append(opts, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
						BlockID: 1, BlockHeight: 700_000, OnLongestChain: true,
					}))
				}

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if _, err := s.Create(ctx, txs[i], 700_000, opts...); err != nil {
						b.Fatalf("create: %v", err)
					}
				}

				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(outs), "ns/output")
				wipe(b, s, ctx)
			})
		}
	}
}

// BenchmarkSpend measures the statement the whole design rests on.
//
// One DELETE does four jobs: it arbitrates the double spend, because an outpoint that is
// absent deletes zero rows; it returns the satoshis and locking script so script validation
// never reads a parent transaction; it reclaims the row; and it is the write. The journal
// row that makes the spend reversible is written in the same statement.
func BenchmarkSpend(b *testing.B) {
	s, ctx := benchStore(b)

	b.ReportAllocs()

	parents := make([]*bt.Tx, b.N)
	children := make([]*bt.Tx, b.N)

	for i := 0; i < b.N; i++ {
		parents[i] = benchTx(1)
		if _, err := s.Create(ctx, parents[i], 700_000); err != nil {
			b.Fatalf("setup create: %v", err)
		}

		child := bt.NewTx()
		if err := child.FromUTXOs(&bt.UTXO{
			TxIDHash: parents[i].TxIDChainHash(), Vout: 0,
			LockingScript: parents[i].Outputs[0].LockingScript,
			Satoshis:      parents[i].Outputs[0].Satoshis,
		}); err != nil {
			b.Fatalf("setup child: %v", err)
		}

		children[i] = child
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := spendOnly(ctx, s, children[i], 700_000); err != nil {
			b.Fatalf("spend: %v", err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/spend")
	wipe(b, s, ctx)
}

// BenchmarkSpendAndCreate measures what block application actually calls: both halves in one
// database transaction, so a failed create rolls the spends back rather than needing
// compensating logic.
func BenchmarkSpendAndCreate(b *testing.B) {
	s, ctx := benchStore(b)

	b.ReportAllocs()

	children := make([]*bt.Tx, b.N)

	for i := 0; i < b.N; i++ {
		parent := benchTx(1)
		if _, err := s.Create(ctx, parent, 700_000); err != nil {
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

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, _, err := s.SpendAndCreate(ctx, children[i], 700_000); err != nil {
			b.Fatalf("spendAndCreate: %v", err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/tx")
	wipe(b, s, ctx)
}

// BenchmarkGet measures the read path: one probe on the identity row, left-joined to the
// body if it is still inside its window.
func BenchmarkGet(b *testing.B) {
	s, ctx := benchStore(b)

	b.ReportAllocs()

	txs := make([]*bt.Tx, b.N)
	for i := 0; i < b.N; i++ {
		txs[i] = benchTx(1)
		if _, err := s.Create(ctx, txs[i], 700_000); err != nil {
			b.Fatalf("setup: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := s.Get(ctx, txs[i].TxIDChainHash()); err != nil {
			b.Fatalf("get: %v", err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/read")
	wipe(b, s, ctx)
}

// BenchmarkSetMined measures the stamp that records which block mined a transaction.
//
// This is the update that must stay off the index. It appends twelve bytes to the packed
// membership and clears the waiting marker, and at the configured page packing it should be
// done in place without writing an index entry. If that stops being true the cost here
// rises sharply.
func BenchmarkSetMined(b *testing.B) {
	s, ctx := benchStore(b)

	for _, batch := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("batch%d", batch), func(b *testing.B) {
			b.ReportAllocs()

			hashes := make([][]*chainhash.Hash, b.N)
			for i := 0; i < b.N; i++ {
				hs := make([]*chainhash.Hash, batch)

				for j := 0; j < batch; j++ {
					tx := benchTx(1)
					if _, err := s.Create(ctx, tx, 700_000); err != nil {
						b.Fatalf("setup: %v", err)
					}

					hs[j] = tx.TxIDChainHash()
				}

				hashes[i] = hs
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := s.SetMinedMulti(ctx, hashes[i], utxo.MinedBlockInfo{
					BlockID: 1, BlockHeight: 700_001, OnLongestChain: true,
				}); err != nil {
					b.Fatalf("setMined: %v", err)
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(batch), "ns/tx")
			wipe(b, s, ctx)
		})
	}
}

// BenchmarkConcurrent measures what the batchers actually exist for.
//
// Every benchmark above calls from a single goroutine, so each call arrives alone, fills no
// batch, and waits out the flush timer. Measured that way batching looks like a large loss,
// because the only thing being measured is the wait.
//
// Block application does not call that way. It resolves and writes from many goroutines at
// once, which is the shape a batcher turns into one round trip. These run b.N operations
// across a fixed pool of callers and report the per-operation cost, so they are directly
// comparable to the single-threaded numbers above.
func BenchmarkConcurrent(b *testing.B) {
	for _, workers := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("create/workers%d", workers), func(b *testing.B) {
			s, ctx := benchStore(b)
			runConcurrent(b, workers, func() error {
				_, err := s.Create(ctx, benchTx(1), 700_000)

				return err
			})
		})

		b.Run(fmt.Sprintf("read/workers%d", workers), func(b *testing.B) {
			s, ctx := benchStore(b)

			// Untimed: a population to read back.
			const pool = 512

			hashes := make([]*chainhash.Hash, pool)

			for i := 0; i < pool; i++ {
				tx := benchTx(1)
				if _, err := s.Create(ctx, tx, 700_000); err != nil {
					b.Fatalf("setup: %v", err)
				}

				hashes[i] = tx.TxIDChainHash()
			}

			var n atomic.Uint64

			runConcurrent(b, workers, func() error {
				_, err := s.Get(ctx, hashes[n.Add(1)%pool])

				return err
			})
		})
	}
}

// runConcurrent spreads b.N operations across a fixed number of callers.
func runConcurrent(b *testing.B, workers int, op func() error) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	var (
		wg   sync.WaitGroup
		next atomic.Int64
		bad  atomic.Value
	)

	next.Store(int64(b.N))

	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for next.Add(-1) >= 0 {
				if err := op(); err != nil {
					bad.Store(err)
					return
				}
			}
		}()
	}

	wg.Wait()
	b.StopTimer()

	if v := bad.Load(); v != nil {
		b.Fatalf("op failed: %v", v)
	}
}

// BenchmarkCreatePaths is deliberately the same shape as the append-only store's benchmark
// of the same name, so the two can be read side by side.
//
// Same batch sizes, same plain-and-mined split, same reported unit: nanoseconds per row,
// where a row is one transaction inside a batch of n. Setup is untimed and the slate is clean
// per iteration, as it is there. What differs is only what is being flushed, which is the
// point of the comparison.
//
// It drives the batch callback directly rather than going through the batcher's timer, again
// matching the other benchmark. That measures the flush mechanism, not the wait to fill.
func BenchmarkCreatePaths(b *testing.B) {
	s, ctx := benchStore(b)

	for _, n := range []int{100, 500, 1024, 2048} {
		b.Run(fmt.Sprintf("Plain/%d", n), func(b *testing.B) {
			runCreatePathBench(b, s, ctx, n, false)
		})
		b.Run(fmt.Sprintf("Mined/%d", n), func(b *testing.B) {
			runCreatePathBench(b, s, ctx, n, true)
		})
	}
}

func runCreatePathBench(b *testing.B, s *Store, ctx context.Context, n int, mined bool) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Untimed: clean slate plus a fresh unique batch.
		b.StopTimer()

		if _, err := s.pool.Exec(ctx, truncateAllSQL); err != nil {
			b.Fatalf("truncate: %v", err)
		}

		batch := make([]*createItem, n)

		for j := 0; j < n; j++ {
			opts := &utxo.CreateOptions{}
			if mined {
				opts.MinedBlockInfos = []utxo.MinedBlockInfo{{
					BlockID: 1, BlockHeight: 1, OnLongestChain: true,
				}}
			}

			batch[j] = &createItem{
				tx:          benchTx(2), // two outputs, matching the other benchmark
				blockHeight: 1,
				options:     opts,
				done:        make(chan createResult, 1),
			}
		}

		b.StartTimer()

		// Timed: the flush mechanism under test.
		s.sendCreateBatch(batch)

		for _, it := range batch {
			if res := <-it.done; res.err != nil {
				b.Fatalf("create failed: %v", res.err)
			}
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/row")
}
