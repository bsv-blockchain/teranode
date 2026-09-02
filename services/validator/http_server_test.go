package validator_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Sample transaction in hex format for testing
const sampleTxHex = "010000000000000000ef016ec78dc364a3a911b38b32e58e5c228030f172da7d550e699939f6f3771f7f63010000006b483045022100dc6378291c4f8e06a54b0b60701cff7719c985db20537cd16d0350abe2f749660220694d67764dd8501e32190e6209a48103e3c7b202bb8c7682fc4c86e890c58409412103c3e59e22c5a32e54183a43993e8f584f709785f440fbf6f960551cb32041c5a9ffffffff062c0c00000000001976a914c52f8797b57f0b0cfc5856a5dd4a6f491a41822c88ac03d0070000000000001976a9149e10b4a781c5be9f67367c3994fb3419aafd358e88ac2a240c00000000001976a914c52f8797b57f0b0cfc5856a5dd4a6f491a41822c88ac00000000000000000a006a075354554b2e434f00000000"

// TestHTTPEndpoints tests the HTTP endpoints of the validator server
func TestHTTPEndpoints(t *testing.T) {
	// Create a test logger
	logger := ulogger.TestLogger{}

	// Parse the sample transaction
	txBytes, err := hex.DecodeString(sampleTxHex)
	require.NoError(t, err)

	t.Run("Single transaction via HTTP endpoint", func(t *testing.T) {
		// Create test settings with policy checks disabled
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.Disabled = true

		// Create a mock UTXO store with expectations
		utxoMock := &utxo.MockUtxostore{}
		utxoMock.On("GetBlockState").Return(utxo.BlockState{Height: 1000, MedianTime: 1625097600})
		utxoMock.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).Return(nil)

		// Add expectation for the Get method which may be called regardless of skipUtxoCreation.
		// BlockHeights must be non-empty so the validator does not take the fallback path
		// (which stamps the unconfirmedParentHeight sentinel — BDK then rejects with
		// bad-txns-unconfirmed-input-in-block in consensus mode); this mock represents
		// a parent that is already confirmed in a real block.
		metaData := &meta.Data{
			Fee:          32279815860,
			SizeInBytes:  245,
			BlockHeights: []uint32{999},
		}
		utxoMock.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(metaData, nil)

		// Mock the SpendAndCreate method with empty slice of *utxo.Spend for the return value
		utxoMock.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, nil)

		// Create a new server
		server := validator.NewServer(logger, tSettings, utxoMock, nil, nil, nil, nil, nil, nil)

		// Initialize the server without starting the gRPC/HTTP servers
		err := server.Init(context.Background())
		require.NoError(t, err)

		// Test HTTP handler for a single transaction
		e := echo.New()

		// Build URL with parameters to bypass validation issues
		queryParams := "skipUtxoCreation=true&addTxToBlockAssembly=false&skipPolicyChecks=true&createConflicting=false&candidateParentMedianTime=1625097600"
		req := httptest.NewRequest(http.MethodPost, "/tx?"+queryParams, bytes.NewReader(txBytes))
		rec := httptest.NewRecorder()

		req.Header.Set("Content-Type", "application/octet-stream")

		// Create a context
		c := e.NewContext(req, rec)

		// Get the handler function
		ctx := context.Background()
		handler := server.HTTP().HandleSingleTx(ctx)

		// Call the handler
		err = handler(c)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, rec.Code, "Response body: %s", rec.Body.String())
	})

	t.Run("Multiple transactions via HTTP endpoint", func(t *testing.T) {
		// Create test settings with policy checks disabled
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.Disabled = true

		// Create a mock UTXO store
		utxoMock := &utxo.MockUtxostore{}
		utxoMock.On("GetBlockState").Return(utxo.BlockState{Height: 1000, MedianTime: 1625097600})
		utxoMock.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).Return(nil)

		// Add expectation for the Get method. BlockHeights must be non-empty so the
		// validator does not take the fallback path (which stamps the
		// unconfirmedParentHeight sentinel — BDK then rejects with
		// bad-txns-unconfirmed-input-in-block in consensus mode); this mock represents
		// a parent that is already confirmed in a real block.
		metaData := &meta.Data{
			Fee:          32279815860,
			SizeInBytes:  245,
			BlockHeights: []uint32{999},
		}
		utxoMock.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(metaData, nil)

		// Mock the SpendAndCreate method with empty slice of *utxo.Spend for the return value
		utxoMock.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, nil)

		// Create a new server
		server := validator.NewServer(logger, tSettings, utxoMock, nil, nil, nil, nil, nil, nil)

		// Initialize the server
		err := server.Init(context.Background())
		require.NoError(t, err)

		// Test HTTP handler for multiple transactions
		e := echo.New()

		// Create buffer with multiple transactions
		var buf bytes.Buffer

		buf.Write(txBytes)
		buf.Write(txBytes)

		// Build URL with parameters to bypass validation issues
		queryParams := "skipUtxoCreation=true&addTxToBlockAssembly=false&skipPolicyChecks=true&createConflicting=false&candidateParentMedianTime=1625097600"
		req := httptest.NewRequest(http.MethodPost, "/txs?"+queryParams, &buf)
		rec := httptest.NewRecorder()

		req.Header.Set("Content-Type", "application/octet-stream")

		// Create context
		c := e.NewContext(req, rec)

		// Get handler
		ctx := context.Background()
		handler := server.HTTP().HandleMultipleTx(ctx)

		// Call handler
		err = handler(c)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, rec.Code, "Response body: %s", rec.Body.String())
	})
}
