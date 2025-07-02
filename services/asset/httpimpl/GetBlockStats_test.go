package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlockStats(t *testing.T) {
	initPrometheusMetrics()

	hash := chainhash.HashH([]byte("test"))

	testBlockStats := &model.BlockStats{
		BlockCount:         100,
		TxCount:            1000,
		MaxHeight:          100,
		AvgBlockSize:       1.5,
		AvgTxCountPerBlock: 10,
		FirstBlockTime:     1234567890,
		LastBlockTime:      1234567990,
		ChainWork:          hash.CloneBytes(),
	}

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockStats", mock.Anything).Return(testBlockStats, nil)

		// set echo context
		echoContext.SetPath("/blocks/stats")

		// Call GetBlockStats handler
		err := httpServer.GetBlockStats(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, float64(100), response["block_count"])
		assert.Equal(t, float64(1000), response["tx_count"])
		assert.Equal(t, float64(100), response["max_height"])
		assert.Equal(t, float64(1.5), response["avg_block_size"])
		assert.Equal(t, float64(10), response["avg_tx_count_per_block"])
		assert.Equal(t, float64(1234567890), response["first_block_time"])
		assert.Equal(t, float64(1234567990), response["last_block_time"])
		assert.Equal(t, "n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=", response["chain_work"])
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockStats", mock.Anything).Return(nil, errors.NewServiceError("error retrieving block statistics"))

		// set echo context
		echoContext.SetPath("/blocks/stats")

		// Call GetBlockStats handler
		err := httpServer.GetBlockStats(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "SERVICE_ERROR (59): error retrieving block statistics", echoErr.Message)
	})
}
