package propagation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// panicPropagationAPIClient is a minimal PropagationAPIClient whose
// ProcessTransactionBatch method always panics, used to exercise the
// whole-batch panic-recovery sweep in Client.ProcessTransactionBatch.
type panicPropagationAPIClient struct {
	propagation_api.PropagationAPIClient
}

func (m *panicPropagationAPIClient) ProcessTransactionBatch(ctx context.Context, req *propagation_api.ProcessTransactionBatchRequest, opts ...grpc.CallOption) (*propagation_api.ProcessTransactionBatchResponse, error) {
	panic("boom")
}

// TestProcessTransactionBatch_DispatcherPanic is a regression test for the
// whole-batch panic-recovery sweep in ProcessTransactionBatch: if the gRPC
// call panics mid-dispatch, every item in the batch must still be completed
// so no submitter is stranded blocked on group.Wait.
func TestProcessTransactionBatch_DispatcherPanic(t *testing.T) {
	logger := ulogger.TestLogger{}

	client := &Client{
		client:   &panicPropagationAPIClient{},
		logger:   logger,
		settings: &settings.Settings{},
	}

	// Two items sharing one completion group, as a real batch would.
	batch := make([]*batchItem, 2)
	group := completion.NewGroup(int32(len(batch)))

	for i := range batch {
		batch[i] = &batchItem{
			ctx:   context.Background(),
			tx:    bt.NewTx(),
			group: group,
		}
	}

	require.NotPanics(t, func() {
		_ = client.ProcessTransactionBatch(context.Background(), batch)
	})

	require.NoError(t, group.Wait(context.Background(), 0))

	for _, item := range batch {
		require.Error(t, item.result)
	}
}
