package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// createWaitStore is a utxo.SequentialStore whose Create is the real Aerospike Create, wired to a
// batcher that accepts the item and never completes it, so the store's own batcher wait runs out
// while the item is still queued. Spend and Unspend are recorded fakes: there is no container.
type createWaitStore struct {
	*Store

	spends       []*utxo.Spend
	unspendCalls int
}

func (c *createWaitStore) Spend(context.Context, *bt.Tx, uint32, ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return c.spends, nil
}

func (c *createWaitStore) Unspend(context.Context, []*utxo.Spend, ...bool) error {
	c.unspendCalls++

	return nil
}

// TestCreate_BatcherWaitTimeoutKeepsTheSpends: when Create stops waiting for its batch because the
// store's own batcher wait ran out, the item is still queued and the dispatcher may yet write the
// record. SequentialSpendAndCreate must keep the spends, as it does for a context error. Rolling
// them back would leave a record whose inputs are unspent, which no repeat heals, because a repeat
// finds the record and reports the transaction as existing.
func TestCreate_BatcherWaitTimeoutKeepsTheSpends(t *testing.T) {
	s := newTestStoreForBatchWait(t)
	s.storeBatcher = okBatcher[BatchStoreItem]{}
	s.batcherWait = 20 * time.Millisecond

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0,
		"76a914000000000000000000000000000000000000000088ac", 1000))
	require.NoError(t, tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{1}))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 900))

	t.Run("Create marks the timeout as an unknown outcome", func(t *testing.T) {
		_, err := s.Create(context.Background(), tx, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "did not complete within")
		require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "the error class is unchanged: %v", err)
		require.True(t, utxo.IsCreateOutcomeUnknown(err), "%v", err)
	})

	t.Run("SequentialSpendAndCreate keeps the spends", func(t *testing.T) {
		c := &createWaitStore{Store: s, spends: []*utxo.Spend{{}}}

		md, spends, err := utxo.SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, c, tx, 100)
		require.Error(t, err)
		require.Nil(t, md)
		require.Equal(t, 0, c.unspendCalls, "the spends must not be rolled back while the create may still commit")
		require.Len(t, spends, 1, "the spends stay in effect")
		require.True(t, errors.IsRetryableError(err), "a caller must be able to repeat the call: %v", err)
	})
}
