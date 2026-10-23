package aerospike

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

func TestSpendSplitCompletesAllClaimsOnBackendFailure(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		name := "lua"
		if expressions {
			name = "expressions"
		}
		t.Run(name, func(t *testing.T) {
			for _, panicSecond := range []bool{false, true} {
				scenario := "errors"
				if panicSecond {
					scenario = "panic after first part"
				}
				t.Run(scenario, func(t *testing.T) {
					s := newTestStoreForGet(t)
					s.utxoBatchSize = 1
					s.settings.Aerospike.EnableSpendFilterExpressions = expressions
					group := completion.NewGroup(3)
					parent, hash := chainhash.Hash{1}, chainhash.Hash{2}
					batch := make([]*batchSpend, 3)
					for i := range batch {
						spender := chainhash.Hash{byte(i + 3)}
						batch[i] = &batchSpend{spend: &utxo.Spend{
							TxID: &parent, UTXOHash: &hash,
							SpendingData: spendpkg.NewSpendingData(&spender, 0),
						}, blockHeight: 100, group: group}
					}
					calls := 0
					fail := errorOperate()
					s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
						calls++
						if panicSecond && calls == 2 {
							panic("second part failed")
						}
						return fail(policy, records)
					}
					require.NotPanics(t, func() { s.sendSpendBatchLua(batch) })
					requireGroupCompleted(t, group, time.Second)
					expectedCalls := 3
					if panicSecond {
						expectedCalls = 2
					}
					require.Equal(t, expectedCalls, calls)
					for _, item := range batch {
						require.Error(t, item.spend.Err)
						require.NotErrorIs(t, item.spend.Err, errors.ErrSpent)
					}
				})
			}
		})
	}
}
