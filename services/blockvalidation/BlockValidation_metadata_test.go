package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestValidateBlock_MetadataFetchFailure(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	logger := ulogger.TestLogger{}

	nBits, err := model.NewNBitFromSlice([]byte{0x1b, 0x04, 0x86, 0x4c})
	require.NoError(t, err)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()), // nolint:gosec
			Bits:           *nBits,
			Nonce:          2083236893,
		},
	}

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewServiceError("simulated transient DB outage"))

	bv := &BlockValidation{
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              mockBlockchainClient,
		stats:                         gocore.NewStat("test"),
	}
	defer bv.blockExistsCache.Stop()

	err = bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err, "metadata fetch failure must surface as an error, not be swallowed as valid")
	require.True(t, errors.Is(err, errors.ErrServiceError), "expected ServiceError, got %v", err)
}
