package blockvalidation

import (
	"context"
	"fmt"
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

func (f *fRPC_BlockValidation) Exists(ctx context.Context, request *blockvalidation_api.BlockvalidationApiExistsSubtreeRequest) (*blockvalidation_api.BlockvalidationApiExistsSubtreeResponse, error) {
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

			dataBytes := meta[32:]
			txMetaData := &txmeta_store.Data{}
			txmeta_store.NewMetaDataFromBytes(&dataBytes, txMetaData)

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

func (f *fRPC_BlockValidation) DelTxMeta(ctx context.Context, request *blockvalidation_api.BlockvalidationApiDelTxMetaRequest) (*blockvalidation_api.BlockvalidationApiDelTxMetaResponse, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "DelTxMeta", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockValidationSetTXMetaCacheDelFrpc.Inc()

	hash, err := chainhash.NewHash(request.Hash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create hash from bytes: %v", err)
	}

	if err = f.blockValidation.DelTxMetaCacheMulti(ctx, hash); err != nil {
		f.logger.Errorf("failed to delete tx meta data: %v", err)
	}

	return &blockvalidation_api.BlockvalidationApiDelTxMetaResponse{
		Ok: true,
	}, nil
}

func (f *fRPC_BlockValidation) SetMinedMulti(ctx context.Context, request *blockvalidation_api.BlockvalidationApiSetMinedMultiRequest) (*blockvalidation_api.BlockvalidationApiSetMinedMultiResponse, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "SetMinedMulti", stats)
	defer func() {
		stat.AddTime(start)
	}()

	f.logger.Warnf("FRPC SetMinedMulti %d: %d", request.BlockId, len(request.Hashes))

	prometheusBlockValidationSetMinedMultiFrpc.Inc()
	go func(data [][]byte) {
		hashes := make([]*chainhash.Hash, 0, len(data))
		for _, hashBytes := range data {
			if len(hashBytes) != 32 {
				f.logger.Errorf("hash is not 32 bytes: %v", hashBytes)
				return
			}

			hash := chainhash.Hash(hashBytes)
			hashes = append(hashes, &hash)
		}

		if err := f.blockValidation.SetTxMetaCacheMinedMulti(ctx, hashes, request.BlockId); err != nil {
			f.logger.Errorf("failed to set mined multi via frpc: %v", err)
		}
	}(request.Hashes)

	return &blockvalidation_api.BlockvalidationApiSetMinedMultiResponse{
		Ok: true,
	}, nil
}
