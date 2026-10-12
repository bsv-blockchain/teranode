package subtreevalidation

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// countingLatencyStore embeds MockUtxostore and intercepts BatchDecorate to
// (a) count the number of records read and (b) inject a per-record latency that
// models an Aerospike batch read (server-side cost scales with record count).
// It lets the benchmark compare the OLD per-tx read pattern against the NEW
// per-level deduped read in both records-read and wall-clock terms.
type countingLatencyStore struct {
	*utxostore.MockUtxostore

	recordsRead    atomic.Int64
	perRecordDelay time.Duration
	resolve        map[chainhash.Hash]*meta.Data
}

func (s *countingLatencyStore) BatchDecorate(_ context.Context, items []*utxostore.UnresolvedMetaData, _ ...fields.FieldName) error {
	s.recordsRead.Add(int64(len(items)))

	if s.perRecordDelay > 0 {
		time.Sleep(s.perRecordDelay * time.Duration(len(items)))
	}

	for _, it := range items {
		if d, ok := s.resolve[it.Hash]; ok {
			it.Data = d
		}
	}

	return nil
}

// buildLevel constructs a level of single-input transactions arranged as
// `parents` distinct parents, each spent by `childrenPerParent` children, and
// returns the level plus a resolve map seeding every parent in the store.
//
//	FanOut    : parents=1,    childrenPerParent=N  (the tx-blaster shape)
//	Distinct  : parents=N,    childrenPerParent=1  (no sharing — dedup no-op)
//	Mixed     : parents=M,    childrenPerParent=K
func buildLevel(b *testing.B, parents, childrenPerParent int) ([]missingTx, map[chainhash.Hash]*meta.Data) {
	b.Helper()

	const lockingScript = "76a914000000000000000000000000000000000000000088ac"

	levelTxs := make([]missingTx, 0, parents*childrenPerParent)
	resolve := make(map[chainhash.Hash]*meta.Data, parents)

	idx := 0

	for p := 0; p < parents; p++ {
		parentHex := fmt.Sprintf("%064x", p+1)

		for c := 0; c < childrenPerParent; c++ {
			tx := bt.NewTx()
			if err := tx.From(parentHex, uint32(c), lockingScript, 1000); err != nil {
				b.Fatalf("build tx: %v", err)
			}

			parentHash := *tx.Inputs[0].PreviousTxIDChainHash()
			resolve[parentHash] = &meta.Data{BlockHeights: []uint32{uint32(p + 1)}}

			levelTxs = append(levelTxs, missingTx{tx: tx, idx: idx})
			idx++
		}
	}

	return levelTxs, resolve
}

// readOldPattern reproduces the pre-change read pattern: the validator resolves
// each transaction's parents independently (one read per distinct parent per
// tx), with NO dedup across transactions in the level — sendGetBatch coalesces
// round-trips but reads the same shared parent record once per child. The
// validator-package parity tests confirm the new path eliminates these reads in
// favour of the prefetch.
func readOldPattern(ctx context.Context, store *countingLatencyStore, levelTxs []missingTx) {
	for _, mTx := range levelTxs {
		seen := make(map[chainhash.Hash]struct{}, len(mTx.tx.Inputs))
		items := make([]*utxostore.UnresolvedMetaData, 0, len(mTx.tx.Inputs))

		for _, in := range mTx.tx.Inputs {
			ph := *in.PreviousTxIDChainHash()
			if _, dup := seen[ph]; dup {
				continue
			}

			seen[ph] = struct{}{}
			items = append(items, &utxostore.UnresolvedMetaData{Hash: ph})
		}

		_ = store.BatchDecorate(ctx, items)
	}
}

// BenchmarkCatchupParentReads compares the OLD per-tx read pattern with the NEW
// per-level deduped bulk read (the real prefetchLevelParents), reporting both
// records-read/op and ns/op. Records-read is the hard, deterministic metric (it
// is what loads Aerospike during catchup); the ns/op numbers under non-zero
// perRecordDelay are illustrative — they show the wall-clock win scaling with
// store latency, but the absolute latency is a modelling choice.
func BenchmarkCatchupParentReads(b *testing.B) {
	ctx := context.Background()

	shapes := []struct {
		name              string
		parents           int
		childrenPerParent int
	}{
		{"FanOut_1x1000", 1, 1000},
		{"FanOut_10x100", 10, 100},
		{"Mixed_100x10", 100, 10},
		{"Distinct_1000x1", 1000, 1},
	}

	// Per-record latency models an Aerospike batch read's server-side cost.
	// 0 isolates record-count + Go overhead; the rest show the wall-clock win.
	delays := []time.Duration{0, time.Microsecond, 50 * time.Microsecond}

	server := &Server{
		settings: settings.NewSettings(),
		logger:   ulogger.TestLogger{},
	}

	for _, shape := range shapes {
		levelTxs, resolve := buildLevel(b, shape.parents, shape.childrenPerParent)

		for _, delay := range delays {
			store := &countingLatencyStore{
				MockUtxostore:  &utxostore.MockUtxostore{},
				perRecordDelay: delay,
				resolve:        resolve,
			}
			server.utxoStore = store

			b.Run(fmt.Sprintf("%s/delay=%s/old", shape.name, delay), func(b *testing.B) {
				store.recordsRead.Store(0)
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					readOldPattern(ctx, store, levelTxs)
				}

				b.StopTimer()
				b.ReportMetric(float64(store.recordsRead.Load())/float64(b.N), "reads/op")
			})

			b.Run(fmt.Sprintf("%s/delay=%s/new", shape.name, delay), func(b *testing.B) {
				store.recordsRead.Store(0)
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if _, err := server.prefetchLevelParents(ctx, levelTxs); err != nil {
						b.Fatalf("prefetchLevelParents: %v", err)
					}
				}

				b.StopTimer()
				b.ReportMetric(float64(store.recordsRead.Load())/float64(b.N), "reads/op")
			})
		}
	}
}
