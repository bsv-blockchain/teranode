package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPickBucketSeconds_RangeRules(t *testing.T) {
	tests := []struct {
		name         string
		rangeSeconds int64
		want         int64
	}{
		{"zero range", 0, 0},
		{"at 24h boundary", 86400, 0},
		{"just over 24h", 86401, bucket1h},
		{"at 1w boundary", 604800, bucket1h},
		{"just over 1w", 604801, bucket6h},
		{"at 3m boundary", 7776000, bucket6h},
		{"just over 3m", 7776001, bucket1d},
		{"at 1y boundary", 31536000, bucket1d},
		{"just over 1y", 31536001, bucket1w},
		{"at 5y boundary", 157680000, bucket1w},
		{"just over 5y", 157680001, bucket30d},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, pickBucketSeconds(tt.rangeSeconds))
		})
	}
}

func TestAggregateDataPoints_BucketsAndSums(t *testing.T) {
	// Three points spanning ~1h — directly tests aggregation with explicit bucket1h
	base := uint32(1_700_000_000)
	in := &model.BlockDataPoints{
		DataPoints: []*model.DataPoint{
			{Timestamp: base, TxCount: 10},
			{Timestamp: base + 300, TxCount: 5},  // same 1h bucket as base
			{Timestamp: base + 3600, TxCount: 7}, // next 1h bucket
		},
	}
	out := aggregateDataPoints(in, bucket1h)
	require.Equal(t, 2, len(out.DataPoints))

	b0 := (base / uint32(bucket1h)) * uint32(bucket1h)
	b1 := ((base + 3600) / uint32(bucket1h)) * uint32(bucket1h)

	assert.Equal(t, b0, out.DataPoints[0].Timestamp)
	assert.Equal(t, uint64(15), out.DataPoints[0].TxCount)
	assert.Equal(t, b1, out.DataPoints[1].Timestamp)
	assert.Equal(t, uint64(7), out.DataPoints[1].TxCount)
}

func TestAggregateDataPoints_EmptyAndSingle(t *testing.T) {
	t.Run("empty input passthrough", func(t *testing.T) {
		in := &model.BlockDataPoints{DataPoints: []*model.DataPoint{}}
		out := aggregateDataPoints(in, bucket1h)
		require.Equal(t, in, out)
	})

	t.Run("zero bucket passthrough", func(t *testing.T) {
		in := &model.BlockDataPoints{
			DataPoints: []*model.DataPoint{
				{Timestamp: 1_700_000_000, TxCount: 42},
			},
		}
		out := aggregateDataPoints(in, 0)
		require.Equal(t, in, out)
	})

	t.Run("single point bucketed", func(t *testing.T) {
		ts := uint32(1_700_000_000)
		in := &model.BlockDataPoints{
			DataPoints: []*model.DataPoint{
				{Timestamp: ts, TxCount: 42},
			},
		}
		out := aggregateDataPoints(in, bucket1h)
		require.Equal(t, 1, len(out.DataPoints))
		expected := (ts / uint32(bucket1h)) * uint32(bucket1h)
		assert.Equal(t, expected, out.DataPoints[0].Timestamp)
		assert.Equal(t, uint64(42), out.DataPoints[0].TxCount)
	})
}

func TestAggregateDataPoints_UnsortedInput(t *testing.T) {
	// Feed points in reverse order — output must be sorted ASC and buckets correct.
	base := uint32(1_700_000_000)
	in := &model.BlockDataPoints{
		DataPoints: []*model.DataPoint{
			{Timestamp: base + 3600, TxCount: 7}, // second bucket, given first
			{Timestamp: base + 300, TxCount: 5},  // first bucket
			{Timestamp: base, TxCount: 10},        // first bucket
		},
	}
	out := aggregateDataPoints(in, bucket1h)
	require.Equal(t, 2, len(out.DataPoints))

	b0 := (base / uint32(bucket1h)) * uint32(bucket1h)
	b1 := ((base + 3600) / uint32(bucket1h)) * uint32(bucket1h)

	assert.Equal(t, b0, out.DataPoints[0].Timestamp)
	assert.Equal(t, uint64(15), out.DataPoints[0].TxCount)
	assert.Equal(t, b1, out.DataPoints[1].Timestamp)
	assert.Equal(t, uint64(7), out.DataPoints[1].TxCount)
}

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

		periods := []string{"2h", "6h", "12h", "24h", "1w", "1m", "3m", "all"}

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
		assert.Equal(t, "INVALID_ARGUMENT (1): a valid period is required", echoErr.Message)
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
		assert.Equal(t, "PROCESSING (4): error getting block graph data", echoErr.Message)
	})

	t.Run("All period passes periodMillis=0", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Expect the mock to be called with exactly periodMillis=0 (no time filter).
		mockRepo.On("GetBlockGraphData", uint64(0)).Return(&model.BlockDataPoints{
			DataPoints: []*model.DataPoint{
				{Timestamp: 1230768000, TxCount: 1}, // 2009-01-01
				{Timestamp: 1231372800, TxCount: 2}, // 2009-01-07, ~7d later
			},
		}, nil)

		echoContext.SetPath("/blocks/graph/:period")
		echoContext.SetParamNames("period")
		echoContext.SetParamValues("all")

		err := httpServer.GetBlockGraphData(echoContext)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Bucketing applied when data spans more than 24h", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Data spans ~3 days — pickBucketSeconds will select bucket1h.
		// Four blocks: two in the same 1h bucket, two in later buckets.
		base := uint32(1700000000)
		mockRepo.On("GetBlockGraphData", mock.Anything, mock.Anything).Return(&model.BlockDataPoints{
			DataPoints: []*model.DataPoint{
				{Timestamp: base, TxCount: 10},
				{Timestamp: base + 1800, TxCount: 5},   // same 1h bucket as base
				{Timestamp: base + 7200, TxCount: 20},  // 2h later, different bucket
				{Timestamp: base + 86400*3, TxCount: 3}, // 3 days later, different bucket
			},
		}, nil)

		echoContext.SetPath("/blocks/graph/:period")
		echoContext.SetParamNames("period")
		echoContext.SetParamValues("all")

		err := httpServer.GetBlockGraphData(echoContext)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var response map[string]interface{}
		require.NoError(t, json.Unmarshal(responseRecorder.Body.Bytes(), &response))

		dataPoints := response["data_points"].([]interface{})
		// base and base+1800 share a bucket; base+7200 and base+86400*3 are each their own.
		assert.Equal(t, 3, len(dataPoints))

		// First bucket sums base (10) + base+1800 (5) = 15.
		dp0 := dataPoints[0].(map[string]interface{})
		assert.Equal(t, float64(15), dp0["tx_count"])
	})
}
