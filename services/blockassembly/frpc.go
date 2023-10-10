package blockassembly

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
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

func (f *fRPC_BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.BlockassemblyApiAddTxRequest) (*blockassembly_api.BlockassemblyApiAddTxResponse, error) {
	startTime := time.Now()
	prometheusBlockAssemblyAddTx.Inc()
	defer func() {
		prometheusBlockAssemblerTransactions.Set(float64(f.ba.blockAssembler.TxCount()))
		prometheusBlockAssemblyAddTxDuration.Observe(time.Since(startTime).Seconds())
	}()

	if len(req.Txid) != 32 {
		return nil, fmt.Errorf("invalid txid length: %d for %s", len(req.Txid), utils.ReverseAndHexEncodeSlice(req.Txid))
	}

	// create the subtree node
	node := &util.SubtreeNode{
		Hash:        chainhash.Hash(req.Txid),
		Fee:         req.Fee,
		SizeInBytes: req.Size,
	}

	if err := f.ba.blockAssembler.AddTx(node); err != nil {
		return nil, err
	}

	if err := f.ba.storeUtxos(ctx, req.Utxos, req.Locktime); err != nil {
		return nil, err
	}

	return &blockassembly_api.BlockassemblyApiAddTxResponse{
		Ok: true,
	}, nil
}

func (f *fRPC_BlockAssembly) AddTxBatch(ctx context.Context, batch *blockassembly_api.BlockassemblyApiAddTxBatchRequest) (*blockassembly_api.BlockassemblyApiAddTxBatchResponse, error) {
	var err error
	var txIdErrors [][]byte
	for _, req := range batch.TxRequests {
		_, err = f.AddTx(ctx, req)
		if err != nil {
			txIdErrors = append(txIdErrors, req.Txid)
		}
	}
	return &blockassembly_api.BlockassemblyApiAddTxBatchResponse{
		TxIdErrors: txIdErrors,
	}, err
}

func (f *fRPC_BlockAssembly) GetMiningCandidate(ctx context.Context, message *blockassembly_api.BlockassemblyApiEmptyMessage) (*blockassembly_api.ModelMiningCandidate, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fRPC_BlockAssembly) SubmitMiningSolution(ctx context.Context, request *blockassembly_api.BlockassemblyApiSubmitMiningSolutionRequest) (*blockassembly_api.BlockassemblyApiSubmitMiningSolutionResponse, error) {
	//TODO implement me
	panic("implement me")
}
