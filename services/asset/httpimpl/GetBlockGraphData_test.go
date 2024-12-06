package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlockGraphData(t *testing.T) {
	initPrometheusMetrics()

	testDataPoints := &model.BlockDataPoints{
		DataPoints: []*model.DataPoint{
			{
				Timestamp: 12345678,
				TxCount:   12,
			},
			{
				Timestamp: 12345679,
				TxCount:   13,
			},
			{
				Timestamp: 12345680,
				TxCount:   14,
			},
		},
	}

	t.Run("Valid period 24h", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockGraphData", mock.Anything, mock.Anything).Return(testDataPoints, nil)

		// set echo context
		echoContext.SetPath("/blocks/graph/:period")
		echoContext.SetParamNames("period")
		echoContext.SetParamValues("24h")

		// Call GetBlockGraphData handler
		err := httpServer.GetBlockGraphData(echoContext)
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
		dataPoints := response["data_points"].([]interface{})
		require.NotNil(t, dataPoints)
		assert.Equal(t, 3, len(dataPoints))

		dataPoint0 := dataPoints[0].(map[string]interface{})
		assert.Equal(t, float64(12345678), dataPoint0["timestamp"])
		assert.Equal(t, float64(12), dataPoint0["tx_count"])

		dataPoint1 := dataPoints[1].(map[string]interface{})
		assert.Equal(t, float64(12345679), dataPoint1["timestamp"])
		assert.Equal(t, float64(13), dataPoint1["tx_count"])

		dataPoint2 := dataPoints[2].(map[string]interface{})
		assert.Equal(t, float64(12345680), dataPoint2["timestamp"])
		assert.Equal(t, float64(14), dataPoint2["tx_count"])
	})

	t.Run("Valid periods", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockGraphData", mock.Anything, mock.Anything).Return(testDataPoints, nil)

		// set echo context
		echoContext.SetPath("/blocks/graph/:period")
		echoContext.SetParamNames("period")

		periods := []string{"2h", "6h", "12h", "24h", "1w", "1m", "3m"}

		for _, period := range periods {
			echoContext.SetParamValues(period)

			// Call GetBlockGraphData handler
			err := httpServer.GetBlockGraphData(echoContext)
			if err != nil {
				t.Fatal(err)
			}

			// Check response status code
			assert.Equal(t, http.StatusOK, responseRecorder.Code)
		}
	})

	t.Run("Invalid period", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/blocks/graph/:period")
		echoContext.SetParamNames("period")
		echoContext.SetParamValues("invalid")

		// Call GetBlockGraphData handler
		err := httpServer.GetBlockGraphData(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "Error: INVALID_ARGUMENT (error code: 1), Message: a valid period is required", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockGraphData", mock.Anything, mock.Anything).Return(nil, errors.NewProcessingError("error getting block graph data"))

		// set echo context
		echoContext.SetPath("/blocks/graph/:period")
		echoContext.SetParamNames("period")
		echoContext.SetParamValues("24h")

		// Call GetBlockGraphData handler
		err := httpServer.GetBlockGraphData(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "Error: PROCESSING (error code: 4), Message: error getting block graph data", echoErr.Message)
	})
}
