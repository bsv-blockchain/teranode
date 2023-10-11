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

func (f *fRPC_BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.BlockassemblyApiAddTxRequest) (resp *blockassembly_api.BlockassemblyApiAddTxResponse, err error) {
	startTime := time.Now()
	prometheusBlockAssemblyAddTx.Inc()
	defer func() {
		prometheusBlockAssemblerTransactions.Set(float64(f.ba.blockAssembler.TxCount()))
		prometheusBlockAssemblyAddTxDuration.Observe(time.Since(startTime).Seconds())
	}()

	if len(req.Txid) != 32 {
		return nil, fmt.Errorf("invalid txid length: %d for %s", len(req.Txid), utils.ReverseAndHexEncodeSlice(req.Txid))
	}

	if err = f.ba.blockAssembler.AddTx(&util.SubtreeNode{
		Hash:        chainhash.Hash(req.Txid),
		Fee:         req.Fee,
		SizeInBytes: req.Size,
	}); err != nil {
		return nil, err
	}

	if err = f.ba.frpcStoreUtxos(ctx, req); err != nil {
		return nil, err
	}

	return &blockassembly_api.BlockassemblyApiAddTxResponse{
		Ok: true,
	}, nil
}

// frpcStoreUtxos is mostly a duplicate from the Server, but prevents extra mallocs by using the req directly
func (ba *BlockAssembly) frpcStoreUtxos(ctx context.Context, req *blockassembly_api.BlockassemblyApiAddTxRequest) (err error) {
	startUtxoTime := time.Now()
	defer func() {
		prometheusBlockAssemblerUtxoStoreDuration.Observe(time.Since(startUtxoTime).Seconds())
	}()

	utxoHashes := make([]*chainhash.Hash, len(req.Utxos))

	var i int
	var hashBytes []byte
	for i, hashBytes = range req.Utxos {
		utxoHashes[i], err = chainhash.NewHash(hashBytes)
		if err != nil {
			return err
		}
	}

	if _, err = ba.utxoStore.BatchStore(ctx, utxoHashes, req.Locktime); err != nil {
		return fmt.Errorf("error storing utxos: %w", err)
	}

	return nil
}

func (f *fRPC_BlockAssembly) AddTxBatch(ctx context.Context, batch *blockassembly_api.BlockassemblyApiAddTxBatchRequest) (resp *blockassembly_api.BlockassemblyApiAddTxBatchResponse, err error) {
	var req *blockassembly_api.BlockassemblyApiAddTxRequest
	var txIdErrors [][]byte
	for _, req = range batch.TxRequests {
		_, err = f.AddTx(ctx, req)
		if err != nil {
			txIdErrors = append(txIdErrors, req.Txid)
		}
	}

	return &blockassembly_api.BlockassemblyApiAddTxBatchResponse{
		Ok:         true,
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
