package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// BenchmarkValidateBatch measures ValidateBatch throughput at different batch
// sizes using a stub UTXO store so the benchmark captures validator pipeline
// overhead rather than store latency. Run locally; not a CI gate.
func BenchmarkValidateBatch(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"n=1", 1},
		{"n=256", 256},
	}

	for _, s := range sizes {
		s := s
		b.Run(s.name, func(b *testing.B) {
			v, stub := newNativeValidator(b)
			installNoopCPUOverride(b, v)

			parents := make([]chainhash.Hash, s.n)
			parentMap := map[[32]byte]*aerospike.ParentRecord{}
			txs := make([]*bt.Tx, s.n)
			for i := 0; i < s.n; i++ {
				parents[i] = chainhash.Hash{byte(i / 256), byte(i % 256), 0x99}
				var key [32]byte
				copy(key[:], parents[i][:])
				parentMap[key] = &aerospike.ParentRecord{BlockHeight: 1}
				txs[i] = minimalTxWithParent(b, parents[i])
			}
			stub.parents = parentMap

			v.overrideBASubmitForTest(func(ctx context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
				return map[chainhash.Hash]error{}
			})
			v.overrideTxMetaPublishForTest(func(*bt.Tx, *meta.Data) {})

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := v.ValidateBatch(context.Background(), txs, 0)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
