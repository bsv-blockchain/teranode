package propagation

import (
	"context"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	null "github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

func setupBatchDirectTestServer(t *testing.T) (*PropagationServer, []*bt.Tx) {
	t.Helper()
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Block height 200 gives coinbase at height 1 enough maturity (199 confirmations > 100 required)
	_ = utxoStore.SetBlockHeight(200)

	txs := transactions.CreateTestTransactionChainWithCount(t, 4)
	_, err = utxoStore.Create(ctx, txs[0], 1)
	require.NoError(t, err)

	validatorInstance, err := validator.New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil)
	require.NoError(t, err)

	txStore, err := null.New(logger)
	require.NoError(t, err)

	ps := &PropagationServer{
		logger:    logger,
		settings:  tSettings,
		txStore:   txStore,
		validator: validatorInstance,
		// validatorKafkaProducerClient is nil — triggers direct batch path
	}

	return ps, txs
}

func TestProcessTransactionBatchDirect_ValidTxs(t *testing.T) {
	ps, txs := setupBatchDirectTestServer(t)
	ctx := context.Background()

	// Only send txs[1] — txs[2] depends on txs[1] which isn't stored yet during parallel Phase 1
	req := &propagation_api.ProcessTransactionBatchRequest{
		Items: []*propagation_api.BatchTransactionItem{
			{Tx: txs[1].ExtendedBytes()},
		},
	}

	resp, err := ps.ProcessTransactionBatch(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.Errors, 1)
	require.Nil(t, resp.Errors[0], "valid tx should have no error")
}

func TestProcessTransactionBatchDirect_InvalidTxBytes(t *testing.T) {
	ps, txs := setupBatchDirectTestServer(t)
	ctx := context.Background()

	req := &propagation_api.ProcessTransactionBatchRequest{
		Items: []*propagation_api.BatchTransactionItem{
			{Tx: []byte{0xba, 0xad, 0xf0, 0x0d}},
			{Tx: txs[1].ExtendedBytes()},
		},
	}

	resp, err := ps.ProcessTransactionBatch(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.Errors, 2)
	require.NotNil(t, resp.Errors[0], "garbage bytes should produce an error")
	require.Nil(t, resp.Errors[1], "valid tx should succeed")
}

func TestProcessTransactionBatchDirect_EmptyBatch(t *testing.T) {
	ps, _ := setupBatchDirectTestServer(t)
	ctx := context.Background()

	req := &propagation_api.ProcessTransactionBatchRequest{
		Items: []*propagation_api.BatchTransactionItem{},
	}

	resp, err := ps.ProcessTransactionBatch(ctx, req)
	require.NoError(t, err)
	require.Empty(t, resp.Errors)
}

func TestProcessTransactionBatchDirect_CoinbaseTx(t *testing.T) {
	ps, txs := setupBatchDirectTestServer(t)
	ctx := context.Background()

	coinbaseTx, coinbaseErr := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, coinbaseErr)

	req := &propagation_api.ProcessTransactionBatchRequest{
		Items: []*propagation_api.BatchTransactionItem{
			{Tx: coinbaseTx.Bytes()},
			{Tx: txs[1].ExtendedBytes()},
		},
	}

	resp, err := ps.ProcessTransactionBatch(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.Errors, 2)
	require.NotNil(t, resp.Errors[0], "coinbase tx should be rejected")
	require.Nil(t, resp.Errors[1], "valid tx should succeed")
}
