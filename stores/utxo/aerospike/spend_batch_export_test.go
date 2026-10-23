package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// SendSpendBatchForTest drives an exact batch through the real backend. It is
// compiled only for tests so external-package integration tests need no sleeps
// to establish batch membership or ordering.
func (s *Store) SendSpendBatchForTest(t *testing.T, spends []*utxo.Spend, height uint32) {
	t.Helper()
	group := completion.NewGroup(int32(len(spends)))
	batch := make([]*batchSpend, len(spends))
	for i, spend := range spends {
		batch[i] = &batchSpend{spend: spend, blockHeight: height, group: group}
	}
	s.sendSpendBatchLua(batch)
	require.NoError(t, group.Wait(context.Background(), time.Second), "all batch claims must complete")
}
