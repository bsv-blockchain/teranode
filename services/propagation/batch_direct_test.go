package propagation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupBatchDirectTestServer returns a propagation server on the direct validation
// path (no validator Kafka producer) over an in-process validator, and a linear
// chain of txCount transactions whose root txs[0] is already in the UTXO store.
func setupBatchDirectTestServer(t *testing.T, batchValidation bool, txCount uint32) (*PropagationServer, []*bt.Tx) {
	t.Helper()
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true
	tSettings.Propagation.BatchValidation = batchValidation

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Height high enough for the maturity of the coinbase root created at height 1.
	require.NoError(t, utxoStore.SetBlockState(200, 1_700_000_000))

	txs := transactions.CreateTestTransactionChainWithCount(t, txCount+1)

	_, err = utxoStore.Create(ctx, txs[0], 1)
	require.NoError(t, err)

	validatorInstance, err := validator.New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	txStore, err := null.New(logger)
	require.NoError(t, err)

	ps := &PropagationServer{
		logger:    logger,
		settings:  tSettings,
		txStore:   txStore,
		validator: validatorInstance,
		// validatorKafkaProducerClient is nil: direct validation path
	}

	return ps, txs
}

func TestProcessTransactionBatchDirect_UsesBatchValidator(t *testing.T) {
	ps, _ := setupBatchDirectTestServer(t, true, 2)

	_, ok := ps.directBatchValidator()
	require.True(t, ok, "in-process validator with propagation_batchValidation must use the batch path")

	ps.settings.Propagation.BatchValidation = false
	_, ok = ps.directBatchValidator()
	require.False(t, ok, "the batch path is opt-in")
}

func TestProcessTransactionBatchDirect_ValidChain(t *testing.T) {
	ps, txs := setupBatchDirectTestServer(t, true, 4)

	// Children before parents: the batch must still validate the parents first.
	req := &propagation_api.ProcessTransactionBatchRequest{
		Items: []*propagation_api.BatchTransactionItem{
			{Tx: txs[3].ExtendedBytes()},
			{Tx: txs[2].ExtendedBytes()},
			{Tx: txs[1].ExtendedBytes()},
		},
	}

	resp, err := ps.ProcessTransactionBatch(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, resp.Errors, 3)

	for i, e := range resp.Errors {
		require.Nil(t, e, "item %d", i)
	}
}

func TestProcessTransactionBatchDirect_EmptyBatch(t *testing.T) {
	ps, _ := setupBatchDirectTestServer(t, true, 2)

	resp, err := ps.ProcessTransactionBatch(context.Background(), &propagation_api.ProcessTransactionBatchRequest{
		Items: []*propagation_api.BatchTransactionItem{},
	})
	require.NoError(t, err)
	require.Empty(t, resp.Errors)
}

// batchDirectMixedRequest returns a batch with one item of every failure class the
// pre-validation and validation steps distinguish, around one valid transaction.
func batchDirectMixedRequest(t *testing.T, txs []*bt.Tx) *propagation_api.ProcessTransactionBatchRequest {
	t.Helper()

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	badScriptTx := txs[1].Clone()
	badScript := bscript.Script([]byte{0xde, 0xad})
	badScriptTx.Inputs[0].UnlockingScript = &badScript

	return &propagation_api.ProcessTransactionBatchRequest{
		Items: []*propagation_api.BatchTransactionItem{
			{Tx: []byte{0xba, 0xad, 0xf0, 0x0d}},
			{Tx: coinbaseTx.Bytes()},
			{Tx: badScriptTx.ExtendedBytes()},
			{Tx: txs[1].ExtendedBytes()},
		},
	}
}

// TestProcessTransactionBatchDirect_ErrorsMatchPerTxPath pins the per-item error
// contract: every item of the batched path carries exactly the error the
// per-transaction path returns for it.
func TestProcessTransactionBatchDirect_ErrorsMatchPerTxPath(t *testing.T) {
	perTx, txsA := setupBatchDirectTestServer(t, false, 2)
	batched, txsB := setupBatchDirectTestServer(t, true, 2)

	// The chain is deterministic, so both servers see the same transactions.
	require.Equal(t, txsA[1].TxID(), txsB[1].TxID())

	respPerTx, err := perTx.ProcessTransactionBatch(context.Background(), batchDirectMixedRequest(t, txsA))
	require.NoError(t, err)

	respBatched, err := batched.ProcessTransactionBatch(context.Background(), batchDirectMixedRequest(t, txsB))
	require.NoError(t, err)

	require.Len(t, respBatched.Errors, len(respPerTx.Errors))

	for i := range respPerTx.Errors {
		if respPerTx.Errors[i] == nil {
			assert.Nil(t, respBatched.Errors[i], "item %d", i)
			continue
		}

		require.NotNil(t, respBatched.Errors[i], "item %d", i)
		assert.Equal(t, respPerTx.Errors[i].Code, respBatched.Errors[i].Code, "item %d", i)
		assert.Equal(t, respPerTx.Errors[i].Error(), respBatched.Errors[i].Error(), "item %d", i)
	}

	assert.NotNil(t, respBatched.Errors[0], "garbage bytes")
	assert.NotNil(t, respBatched.Errors[1], "coinbase")
	assert.NotNil(t, respBatched.Errors[2], "bad script")
	assert.Nil(t, respBatched.Errors[3], "valid tx")
}

func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()

	m := &dto.Metric{}
	require.NoError(t, h.Write(m))

	return m.GetHistogram().GetSampleCount()
}

// TestProcessTransactionBatchDirect_Metrics checks that the batched path records
// the per-transaction metrics processTransaction records: size and processing time
// for each accepted transaction, nothing for a rejected one.
func TestProcessTransactionBatchDirect_Metrics(t *testing.T) {
	ps, txs := setupBatchDirectTestServer(t, true, 3)

	sizeBefore := histogramSampleCount(t, prometheusTransactionSize)
	processedBefore := histogramSampleCount(t, prometheusProcessedTransactions)

	req := &propagation_api.ProcessTransactionBatchRequest{
		Items: []*propagation_api.BatchTransactionItem{
			{Tx: txs[1].ExtendedBytes()},
			{Tx: []byte{0xba, 0xad, 0xf0, 0x0d}},
			{Tx: txs[2].ExtendedBytes()},
		},
	}

	resp, err := ps.ProcessTransactionBatch(context.Background(), req)
	require.NoError(t, err)
	require.Nil(t, resp.Errors[0])
	require.NotNil(t, resp.Errors[1])
	require.Nil(t, resp.Errors[2])

	assert.Equal(t, sizeBefore+2, histogramSampleCount(t, prometheusTransactionSize))
	assert.Equal(t, processedBefore+2, histogramSampleCount(t, prometheusProcessedTransactions))
}
