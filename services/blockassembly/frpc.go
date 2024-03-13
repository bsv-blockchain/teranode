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

func (f *fRPC_BlockAssembly) Health(_ context.Context, _ *blockassembly_api.BlockassemblyApiEmptyMessage) (*blockassembly_api.BlockassemblyApiHealthResponse, error) {
	return &blockassembly_api.BlockassemblyApiHealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (f *fRPC_BlockAssembly) HealthGRPC(_ context.Context, _ *blockassembly_api.BlockassemblyApiEmptyMessage) (*blockassembly_api.BlockassemblyApiHealthResponse, error) {
	return &blockassembly_api.BlockassemblyApiHealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (f *fRPC_BlockAssembly) NewChaintipAndHeight(_ context.Context, _ *blockassembly_api.BlockassemblyApiNewChaintipAndHeightRequest) (*blockassembly_api.BlockassemblyApiEmptyMessage, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fRPC_BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.BlockassemblyApiAddTxRequest) (resp *blockassembly_api.BlockassemblyApiAddTxResponse, err error) {
	startTime := time.Now()
	defer func() {
		prometheusBlockAssemblerTransactions.Set(float64(f.ba.blockAssembler.TxCount()))
		prometheusBlockAssemblerQueuedTransactions.Set(float64(f.ba.blockAssembler.QueueLength()))
		prometheusBlockAssemblerSubtrees.Set(float64(f.ba.blockAssembler.SubtreeCount()))
		prometheusBlockAssemblyAddTxDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	if len(req.Txid) != 32 {
		return nil, fmt.Errorf("invalid txid length: %d for %s", len(req.Txid), utils.ReverseAndHexEncodeSlice(req.Txid))
	}

	if f.ba.blockAssemblyCreatesUTXOs {
		if err = f.storeUtxos(ctx, req); err != nil {
			return nil, fmt.Errorf("failed to store utxos: %s", err)
		}
	}

	f.ba.blockAssembler.AddTx(util.SubtreeNode{
		Hash:        chainhash.Hash(req.Txid),
		Fee:         req.Fee,
		SizeInBytes: req.Size,
	})

	return &blockassembly_api.BlockassemblyApiAddTxResponse{
		Ok: true,
	}, nil
}

func (f *fRPC_BlockAssembly) AddTxBatch(ctx context.Context, batch *blockassembly_api.BlockassemblyApiAddTxBatchRequest) (resp *blockassembly_api.BlockassemblyApiAddTxBatchResponse, err error) {
	startTime := time.Now()
	defer func() {
		prometheusBlockAssemblerTransactions.Set(float64(f.ba.blockAssembler.TxCount()))
		prometheusBlockAssemblerQueuedTransactions.Set(float64(f.ba.blockAssembler.QueueLength()))
		prometheusBlockAssemblerSubtrees.Set(float64(f.ba.blockAssembler.SubtreeCount()))
	}()

	var req *blockassembly_api.BlockassemblyApiAddTxRequest
	var txIdErrors [][]byte
	for _, req = range batch.TxRequests {
		if f.ba.blockAssemblyCreatesUTXOs {
			if err = f.storeUtxos(ctx, req); err != nil {
				txIdErrors = append(txIdErrors, req.Txid)
				continue
			}
		}

		f.ba.blockAssembler.AddTx(util.SubtreeNode{
			Hash:        chainhash.Hash(req.Txid),
			Fee:         req.Fee,
			SizeInBytes: req.Size,
		})

		prometheusBlockAssemblyAddTxDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}

	return &blockassembly_api.BlockassemblyApiAddTxBatchResponse{
		Ok:         true,
		TxIdErrors: txIdErrors,
	}, err
}

func (f *fRPC_BlockAssembly) storeUtxos(ctx context.Context, req *blockassembly_api.BlockassemblyApiAddTxRequest) error {
	utxoHashes := make([]chainhash.Hash, len(req.Utxos))
	for i, hash := range req.Utxos {
		utxoHashes[i] = chainhash.Hash(hash)
	}

	if err := f.ba.utxoStore.StoreFromHashes(ctx, chainhash.Hash(req.Txid), utxoHashes, req.Locktime); err != nil {
		return fmt.Errorf("failed to store utxos: %s", err)
	}

	return nil
}

func (f *fRPC_BlockAssembly) RemoveTx(_ context.Context, request *blockassembly_api.BlockassemblyApiRemoveTxRequest) (*blockassembly_api.BlockassemblyApiEmptyMessage, error) {
	startTime := time.Now()
	prometheusBlockAssemblyRemoveTx.Inc()
	defer func() {
		prometheusBlockAssemblyRemoveTxDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	if len(request.Txid) != 32 {
		return nil, fmt.Errorf("invalid txid length: %d for %s", len(request.Txid), utils.ReverseAndHexEncodeSlice(request.Txid))
	}

	hash := chainhash.Hash(request.Txid)

	if err := f.ba.blockAssembler.RemoveTx(hash); err != nil {
		return nil, err
	}

	return &blockassembly_api.BlockassemblyApiEmptyMessage{}, nil
}

func (f *fRPC_BlockAssembly) GetMiningCandidate(_ context.Context, _ *blockassembly_api.BlockassemblyApiEmptyMessage) (*blockassembly_api.ModelMiningCandidate, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fRPC_BlockAssembly) SubmitMiningSolution(_ context.Context, _ *blockassembly_api.BlockassemblyApiSubmitMiningSolutionRequest) (*blockassembly_api.BlockassemblyApiSubmitMiningSolutionResponse, error) {
	//TODO implement me
	panic("implement me")
}
