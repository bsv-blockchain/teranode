package validator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/validator/validator_api"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// ExtendedMockValidator extends TestMockValidator with additional methods
type ExtendedMockValidator struct {
	TestMockValidator
	getBlockHeightFunc     func() uint32
	getMedianBlockTimeFunc func() uint32
}

func (m *ExtendedMockValidator) GetBlockHeight() uint32 {
	if m.getBlockHeightFunc != nil {
		return m.getBlockHeightFunc()
	}
	return 101
}

func (m *ExtendedMockValidator) GetMedianBlockTime() uint32 {
	if m.getMedianBlockTimeFunc != nil {
		return m.getMedianBlockTimeFunc()
	}
	return uint32(time.Now().Unix())
}

// TestServerInit tests the Init method comprehensively
func TestServerInit(t *testing.T) {
	t.Run("successful init with disabled block assembly", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.Disabled = true
		utxoStore := &utxo.MockUtxostore{}
		blockchainClient := &blockchain.Mock{}

		server := NewServer(logger, tSettings, utxoStore, blockchainClient, nil, nil, nil, nil)

		ctx := context.Background()
		err := server.Init(ctx)
		require.NoError(t, err)
		require.NotNil(t, server.validator)
		require.Equal(t, ctx, server.ctx)
	})

	t.Run("error when block assembly enabled but client nil", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.Disabled = false
		utxoStore := &utxo.MockUtxostore{}
		blockchainClient := &blockchain.Mock{}

		server := NewServer(logger, tSettings, utxoStore, blockchainClient, nil, nil, nil, nil)

		ctx := context.Background()
		err := server.Init(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "blockassembly client is nil while enabled")
	})

	t.Run("successful init with block assembly client", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.Disabled = false
		utxoStore := &utxo.MockUtxostore{}
		blockchainClient := &blockchain.Mock{}
		blockAssemblyClient := &blockassembly.Mock{}

		server := NewServer(logger, tSettings, utxoStore, blockchainClient, nil, nil, nil, blockAssemblyClient)

		ctx := context.Background()
		err := server.Init(ctx)
		require.NoError(t, err)
		require.NotNil(t, server.validator)
	})
}

// TestServerStop tests the Stop method
func TestServerStop(t *testing.T) {
	t.Run("stop with kafka signal", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		server.kafkaSignal = make(chan os.Signal, 1)

		signalReceived := make(chan bool)
		go func() {
			<-server.kafkaSignal
			signalReceived <- true
		}()

		err := server.Stop(context.Background())
		require.NoError(t, err)

		select {
		case <-signalReceived:
			// Signal received as expected
		case <-time.After(1 * time.Second):
			t.Fatal("kafka signal not received")
		}
	})

	t.Run("stop without kafka signal or consumer", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		err := server.Stop(context.Background())
		require.NoError(t, err)
	})
}

// TestServerHealth tests the Health method
func TestServerHealth(t *testing.T) {
	t.Run("liveness check", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		status, msg, err := server.Health(context.Background(), true)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "OK", msg)
	})

	t.Run("readiness with no dependencies", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Validator.GRPCListenAddress = ""
		tSettings.Validator.HTTPListenAddress = ""

		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		status, _, err := server.Health(context.Background(), false)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
	})

	t.Run("readiness with gRPC configured", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Validator.GRPCListenAddress = "localhost:8081"
		tSettings.Validator.HTTPListenAddress = ""

		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		status, msg, _ := server.Health(context.Background(), false)
		require.NotEqual(t, http.StatusOK, status)
		require.Contains(t, msg, "gRPC Server")
	})

	t.Run("readiness with HTTP configured", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Validator.GRPCListenAddress = ""
		tSettings.Validator.HTTPListenAddress = ":8090"

		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		status, msg, _ := server.Health(context.Background(), false)
		require.NotEqual(t, http.StatusOK, status)
		require.Contains(t, msg, "HTTP Server")
	})
}

// TestServerHealthGRPC tests the HealthGRPC method
func TestServerHealthGRPC(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Validator.GRPCListenAddress = ""
	tSettings.Validator.HTTPListenAddress = ""

	server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

	response, err := server.HealthGRPC(context.Background(), &validator_api.EmptyMessage{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.True(t, response.Ok)
	// The details field contains JSON-formatted health check results
	require.Contains(t, response.Details, "status")
	require.Contains(t, response.Details, "200")
}

// TestServerValidateTransaction tests the ValidateTransaction gRPC method
func TestServerValidateTransaction(t *testing.T) {
	t.Run("valid transaction", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)

		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		txid, _ := chainhash.NewHashFromStr("63f7f771376f9f9369e650d7a72d1f0328c2e5582eb3381b913a4a36dc78ec6e")
		server.validator = &TestMockValidator{
			validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
				return &meta.Data{
					Fee:         32279815860,
					SizeInBytes: 245,
					TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{*txid}, Idxs: [][]uint32{{0}}},
				}, nil
			},
		}

		req := &validator_api.ValidateTransactionRequest{
			TransactionData: sampleTx,
			BlockHeight:     100,
		}

		response, err := server.ValidateTransaction(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, response)
		require.True(t, response.Valid)
		require.NotNil(t, response.Txid)
		require.NotNil(t, response.Metadata)
	})

	t.Run("invalid transaction bytes", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)

		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		req := &validator_api.ValidateTransactionRequest{
			TransactionData: []byte("invalid"),
		}

		response, err := server.ValidateTransaction(context.Background(), req)
		require.Error(t, err)
		require.NotNil(t, response)
		require.False(t, response.Valid)
	})

	t.Run("with all validation options", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)

		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		txid, _ := chainhash.NewHashFromStr("63f7f771376f9f9369e650d7a72d1f0328c2e5582eb3381b913a4a36dc78ec6e")
		server.validator = &TestMockValidator{
			validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
				return &meta.Data{
					Fee:         32279815860,
					SizeInBytes: 245,
					TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{*txid}, Idxs: [][]uint32{{0}}},
				}, nil
			},
		}

		skipUtxo := true
		addToBlock := true
		skipPolicy := true
		createConflict := true

		req := &validator_api.ValidateTransactionRequest{
			TransactionData:      sampleTx,
			BlockHeight:          100,
			SkipUtxoCreation:     &skipUtxo,
			AddTxToBlockAssembly: &addToBlock,
			SkipPolicyChecks:     &skipPolicy,
			CreateConflicting:    &createConflict,
		}

		response, err := server.ValidateTransaction(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, response)
		require.True(t, response.Valid)
	})

	t.Run("validation error", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)

		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		server.validator = &TestMockValidator{
			validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
				return nil, errors.NewServiceError("validation failed")
			},
		}

		req := &validator_api.ValidateTransactionRequest{
			TransactionData: sampleTx,
		}

		response, err := server.ValidateTransaction(context.Background(), req)
		require.Error(t, err)
		require.NotNil(t, response)
		require.False(t, response.Valid)
	})
}

// TestServerValidateTransactionBatch tests batch validation
func TestServerValidateTransactionBatch(t *testing.T) {
	t.Run("successful batch", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)

		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		txid, _ := chainhash.NewHashFromStr("63f7f771376f9f9369e650d7a72d1f0328c2e5582eb3381b913a4a36dc78ec6e")
		server.validator = &TestMockValidator{
			validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
				return &meta.Data{
					Fee:         32279815860,
					SizeInBytes: 245,
					TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{*txid}, Idxs: [][]uint32{{0}}},
				}, nil
			},
		}

		req := &validator_api.ValidateTransactionBatchRequest{
			Transactions: []*validator_api.ValidateTransactionRequest{
				{TransactionData: sampleTx, BlockHeight: 100},
				{TransactionData: sampleTx, BlockHeight: 101},
			},
		}

		response, err := server.ValidateTransactionBatch(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, response)
		require.True(t, response.Valid)
		require.Len(t, response.Metadata, 2)
		require.Len(t, response.Errors, 2)
	})

	t.Run("partial failures", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)

		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		var callCount int32
		txid, _ := chainhash.NewHashFromStr("63f7f771376f9f9369e650d7a72d1f0328c2e5582eb3381b913a4a36dc78ec6e")
		server.validator = &TestMockValidator{
			validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
				count := atomic.AddInt32(&callCount, 1)
				if count == 1 {
					return &meta.Data{
						Fee:         32279815860,
						SizeInBytes: 245,
						TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{*txid}, Idxs: [][]uint32{{0}}},
					}, nil
				}
				return nil, errors.NewServiceError("validation error")
			},
		}

		req := &validator_api.ValidateTransactionBatchRequest{
			Transactions: []*validator_api.ValidateTransactionRequest{
				{TransactionData: sampleTx},
				{TransactionData: sampleTx},
			},
		}

		response, err := server.ValidateTransactionBatch(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, response)
		require.True(t, response.Valid)
		require.Len(t, response.Errors, 2)

		// Count successes and failures - order is not guaranteed due to parallel processing
		var successCount, failureCount int
		for _, err := range response.Errors {
			if err == nil {
				successCount++
			} else {
				failureCount++
			}
		}
		require.Equal(t, 1, successCount, "Expected exactly one successful validation")
		require.Equal(t, 1, failureCount, "Expected exactly one failed validation")
	})
}

// TestServerGetBlockHeight tests the GetBlockHeight method
func TestServerGetBlockHeight(t *testing.T) {
	t.Run("valid height", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		server.validator = &ExtendedMockValidator{
			getBlockHeightFunc: func() uint32 {
				return 12345
			},
		}

		response, err := server.GetBlockHeight(context.Background(), &validator_api.EmptyMessage{})
		require.NoError(t, err)
		require.NotNil(t, response)
		require.Equal(t, uint32(12345), response.Height)
	})

	t.Run("zero height error", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		server.validator = &ExtendedMockValidator{
			getBlockHeightFunc: func() uint32 {
				return 0
			},
		}

		response, err := server.GetBlockHeight(context.Background(), &validator_api.EmptyMessage{})
		require.Error(t, err)
		require.Nil(t, response)
		require.Contains(t, err.Error(), "cannot get block height")
	})
}

// TestServerGetMedianBlockTime tests the GetMedianBlockTime method
func TestServerGetMedianBlockTime(t *testing.T) {
	t.Run("valid median time", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		server.validator = &ExtendedMockValidator{
			getMedianBlockTimeFunc: func() uint32 {
				return 1609459200
			},
		}

		response, err := server.GetMedianBlockTime(context.Background(), &validator_api.EmptyMessage{})
		require.NoError(t, err)
		require.NotNil(t, response)
		require.Equal(t, uint32(1609459200), response.MedianTime)
	})

	t.Run("zero median time error", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		server.validator = &ExtendedMockValidator{
			getMedianBlockTimeFunc: func() uint32 {
				return 0
			},
		}

		response, err := server.GetMedianBlockTime(context.Background(), &validator_api.EmptyMessage{})
		require.Error(t, err)
		require.Nil(t, response)
		require.Contains(t, err.Error(), "cannot get median block time")
	})
}

// TestExtractValidationParams tests parameter extraction
func TestExtractValidationParams(t *testing.T) {
	e := echo.New()

	t.Run("no parameters", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/tx", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		height, options := extractValidationParams(c)
		require.Equal(t, uint32(0), height)
		require.False(t, options.SkipUtxoCreation)
		require.True(t, options.AddTXToBlockAssembly) // Default is true
		require.False(t, options.SkipPolicyChecks)
		require.False(t, options.CreateConflicting)
	})

	t.Run("all parameters true", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/tx?blockHeight=100&skipUtxoCreation=true&addTxToBlockAssembly=true&skipPolicyChecks=true&createConflicting=true", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		height, options := extractValidationParams(c)
		require.Equal(t, uint32(100), height)
		require.True(t, options.SkipUtxoCreation)
		require.True(t, options.AddTXToBlockAssembly)
		require.True(t, options.SkipPolicyChecks)
		require.True(t, options.CreateConflicting)
	})

	t.Run("parameters with 1", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/tx?skipUtxoCreation=1&addTxToBlockAssembly=1&skipPolicyChecks=1&createConflicting=1", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		_, options := extractValidationParams(c)
		require.True(t, options.SkipUtxoCreation)
		require.True(t, options.AddTXToBlockAssembly)
		require.True(t, options.SkipPolicyChecks)
		require.True(t, options.CreateConflicting)
	})

	t.Run("parameters explicitly false", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/tx?addTxToBlockAssembly=false", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		_, options := extractValidationParams(c)
		require.False(t, options.AddTXToBlockAssembly)
	})

	t.Run("invalid block height", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/tx?blockHeight=invalid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		height, _ := extractValidationParams(c)
		require.Equal(t, uint32(0), height)
	})
}

// TestHandleSingleTx tests single transaction handling edge cases
func TestHandleSingleTx(t *testing.T) {
	t.Run("invalid body", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/tx", &failingReader{})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		handler := server.handleSingleTx(context.Background())
		err := handler(c)
		require.NoError(t, err) // Handler returns response, not error
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "Invalid request body")
	})

	t.Run("validation failure", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		server.validator = &TestMockValidator{
			validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
				return nil, errors.NewServiceError("validation error")
			},
		}

		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/tx", strings.NewReader(string(sampleTx)))
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		handler := server.handleSingleTx(context.Background())
		err := handler(c)
		require.NoError(t, err) // Handler returns response, not error
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		require.Contains(t, rec.Body.String(), "Failed to process transaction")
		require.Contains(t, rec.Body.String(), "validation error")
	})
}

// TestHandleMultipleTx tests multiple transaction handling edge cases
func TestHandleMultipleTx(t *testing.T) {
	t.Run("invalid transaction format", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/txs", strings.NewReader("invalid"))
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		handler := server.handleMultipleTx(context.Background())
		err := handler(c)
		require.NoError(t, err) // Handler returns response, not error
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "Invalid request body")
	})
}

// TestStartHTTPServer tests HTTP server initialization
func TestStartHTTPServer(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Validator.HTTPRateLimit = 1000
	server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := server.startHTTPServer(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, server.httpServer)

	routes := server.httpServer.Routes()
	hasPostTx := false
	hasPostTxs := false
	hasGetHealth := false

	for _, route := range routes {
		if route.Method == "POST" && route.Path == "/tx" {
			hasPostTx = true
		}
		if route.Method == "POST" && route.Path == "/txs" {
			hasPostTxs = true
		}
		if route.Method == "GET" && route.Path == "/health" {
			hasGetHealth = true
		}
	}

	require.True(t, hasPostTx)
	require.True(t, hasPostTxs)
	require.True(t, hasGetHealth)
}

// Helper types
type failingReader struct{}

func (f *failingReader) Read(p []byte) (n int, err error) {
	return 0, errors.NewStorageError("read error")
}
