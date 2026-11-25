package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestFailedSpenderReadHoldsEveryMatchBack: a call meets a historical match, so
// it reads whether its own transaction has a record, and that read fails (here
// because the context has ended). The store cannot tell a pruned replay from an
// ordinary match, so the historical input must fail with a storage error rather
// than pass, and the call must count it as unanswered, which keeps the young
// match out of the rollback. Only the fresh spend the call wrote is reversed.
func TestFailedSpenderReadHoldsEveryMatchBack(t *testing.T) {
	InitPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = 288

	s := &Store{logger: ulogger.TestLogger{}, settings: tSettings}

	tx := bt.NewTx()
	spender := tx.TxIDChainHash()

	newItem := func(seed byte, idempotent bool, recorded uint32) *batchSpend {
		parent := chainhash.HashH([]byte{seed})
		item := &batchSpend{
			spend:             &utxo.Spend{TxID: &parent, Vout: 0, SpendingData: spendpkg.NewSpendingData(spender, int(seed))},
			blockHeight:       2000,
			idempotent:        idempotent,
			recordedHeight:    recorded,
			hasRecordedHeight: recorded != 0,
		}
		item.completed.Store(true)
		item.published.Store(true)

		return item
	}

	historical := newItem(1, true, 1000) // 1,000 blocks old
	young := newItem(2, true, 1999)      // one block old
	fresh := newItem(3, false, 0)        // written by this call

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res := s.resolveSpendCompletions(ctx, tx, []*batchSpend{historical, young, fresh}, false)

	require.ErrorIs(t, historical.spend.Err, errors.ErrStorageError, "an unreadable spender must not let a historical match pass")
	require.NotErrorIs(t, historical.spend.Err, errors.ErrUtxoSpendingTxPruned, "nor refuse it as a replay it may not be")
	require.Equal(t, 1, res.unresolved, "the historical input is unanswered")
	require.NoError(t, young.spend.Err)

	rollback := utxo.RollbackSet(res.spentSpends, res.idempotentSpends, res.prunedRejection || res.unresolved > 0)
	require.Equal(t, []*utxo.Spend{fresh.spend}, rollback, "only the fresh spend is reversed; the young match is held back")
}
