package propagation

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/ordishs/gocore"
)

type fRPC_Propagation struct {
	ps *PropagationServer
}

func (f *fRPC_Propagation) Health(_ context.Context, _ *propagation_api.PropagationApiEmptyMessage) (*propagation_api.PropagationApiHealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		propagationStat.NewStat("Health_frpc").AddTime(start)
	}()

	return &propagation_api.PropagationApiHealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (f *fRPC_Propagation) ProcessTransaction(ctx context.Context, request *propagation_api.PropagationApiProcessTransactionRequest) (*propagation_api.PropagationApiEmptyMessage, error) {
	start := gocore.CurrentTime()
	defer func() {
		propagationStat.NewStat("ProcessTransaction_frpc").AddTime(start)
	}()

	_, err := f.ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: request.Tx,
	})

	return &propagation_api.PropagationApiEmptyMessage{}, err
}

func (f *fRPC_Propagation) ProcessTransactionDebug(ctx context.Context, request *propagation_api.PropagationApiProcessTransactionRequest) (*propagation_api.PropagationApiEmptyMessage, error) {
	start := gocore.CurrentTime()
	defer func() {
		propagationStat.NewStat("ProcessTransactionDebug_frpc").AddTime(start)
	}()

	_, err := f.ps.ProcessTransactionDebug(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: request.Tx,
	})

	return &propagation_api.PropagationApiEmptyMessage{}, err
}
