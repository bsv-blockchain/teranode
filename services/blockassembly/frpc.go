package blockassembly

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-bt/v2/chainhash"
)

type fRPC_BlockAssembly struct {
	ba *BlockAssembly
}

func (f *fRPC_BlockAssembly) Health(ctx context.Context, message *blockassembly_api.BlockassemblyApiEmptyMessage) (*blockassembly_api.BlockassemblyApiHealthResponse, error) {
	return &blockassembly_api.BlockassemblyApiHealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (f *fRPC_BlockAssembly) NewChaintipAndHeight(ctx context.Context, request *blockassembly_api.BlockassemblyApiNewChaintipAndHeightRequest) (*blockassembly_api.BlockassemblyApiEmptyMessage, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fRPC_BlockAssembly) AddTx(ctx context.Context, request *blockassembly_api.BlockassemblyApiAddTxRequest) (*blockassembly_api.BlockassemblyApiAddTxResponse, error) {
	startTime := time.Now()
	prometheusBlockAssemblyAddTx.Inc()

	// Look up the new utxos for this txHash, add them to the utxostore, and add the tx to the subtree builder...
	txHash, err := chainhash.NewHash(request.Txid)
	if err != nil {
		return nil, err
	}

	if err = f.ba.blockAssembler.AddTx(ctx, txHash); err != nil {
		return nil, err
	}

	prometheusBlockAssemblerTransactions.Set(float64(f.ba.blockAssembler.TxCount()))
	prometheusBlockAssemblyAddTxDuration.Observe(time.Since(startTime).Seconds())

	return &blockassembly_api.BlockassemblyApiAddTxResponse{
		Ok: true,
	}, nil
}

func (f *fRPC_BlockAssembly) GetMiningCandidate(ctx context.Context, message *blockassembly_api.BlockassemblyApiEmptyMessage) (*blockassembly_api.ModelMiningCandidate, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fRPC_BlockAssembly) SubmitMiningSolution(ctx context.Context, request *blockassembly_api.BlockassemblyApiSubmitMiningSolutionRequest) (*blockassembly_api.BlockassemblyApiSubmitMiningSolutionResponse, error) {
	//TODO implement me
	panic("implement me")
}
