package blockvalidation

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

type fRPC_BlockValidation struct {
	blockValidation *BlockValidation
	logger          ulogger.Logger
}

func (f *fRPC_BlockValidation) HealthGRPC(ctx context.Context, message *blockvalidation_api.BlockvalidationApiEmptyMessage) (*blockvalidation_api.BlockvalidationApiHealthResponse, error) {
	return &blockvalidation_api.BlockvalidationApiHealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (f *fRPC_BlockValidation) BlockFound(ctx context.Context, request *blockvalidation_api.BlockvalidationApiBlockFoundRequest) (*blockvalidation_api.BlockvalidationApiEmptyMessage, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fRPC_BlockValidation) SubtreeFound(ctx context.Context, request *blockvalidation_api.BlockvalidationApiSubtreeFoundRequest) (*blockvalidation_api.BlockvalidationApiEmptyMessage, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fRPC_BlockValidation) Get(ctx context.Context, request *blockvalidation_api.BlockvalidationApiGetSubtreeRequest) (*blockvalidation_api.BlockvalidationApiGetSubtreeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fRPC_BlockValidation) SetTxMeta(ctx context.Context, request *blockvalidation_api.BlockvalidationApiSetTxMetaRequest) (*blockvalidation_api.BlockvalidationApiSetTxMetaResponse, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "SetTxMeta", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockValidationSetTXMetaCacheFrpc.Inc()
	go func(data [][]byte) {
		hashes := make(map[chainhash.Hash]*txmeta_store.Data)
		for _, meta := range data {
			if len(meta) < 32 {
				f.logger.Errorf("meta data is too short: %v", meta)
				return
			}

			// first 32 bytes is hash
			hash := chainhash.Hash(meta[:32])

			txMetaData, err := txmeta_store.NewMetaDataFromBytes(meta[32:])
			if err != nil {
				f.logger.Errorf("failed to create tx meta data from bytes: %v", err)
				return
			}

			txMetaData.Tx = nil
			hashes[hash] = txMetaData
		}

		if err := f.blockValidation.SetTxMetaCacheMulti(ctx, hashes); err != nil {
			f.logger.Errorf("failed to set tx meta data: %v", err)
		}
	}(request.Data)

	return &blockvalidation_api.BlockvalidationApiSetTxMetaResponse{
		Ok: true,
	}, nil
}
