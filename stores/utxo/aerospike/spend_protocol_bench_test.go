package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// BenchmarkSpendCompletionProtocol measures the teranode caller-side cost of the
// group-completion protocol for one Spend batch: allocate the shared group, build
// the per-input items, have the dispatcher complete each, and wait once. This is
// the coordination cost that replaced the old per-input {errCh + time.Timer +
// goroutine} protocol.
//
// The authoritative old-vs-new comparison lives in go-batcher's
// completion/completion_benchmark_test.go (per-item channels vs one group). This
// benchmark confirms the win at the teranode call site with the real batchSpend
// item and its complete() helper. Run with -benchmem to read allocs/op.
func BenchmarkSpendCompletionProtocol(b *testing.B) {
	const inputs = 8 // representative multi-input spend

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		group := completion.NewGroup(inputs)

		items := make([]*batchSpend, inputs)
		for j := range items {
			items[j] = &batchSpend{spend: &utxo.Spend{}, group: group}
		}

		// Stand in for the dispatcher signalling every item exactly once.
		for _, it := range items {
			it.complete(nil)
		}

		if err := group.Wait(context.Background(), time.Second); err != nil {
			b.Fatalf("group did not complete: %v", err)
		}
	}
}
