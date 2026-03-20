package httpimpl

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/mock"
)

// BenchmarkGetTransactionJSON benchmarks the GetTransaction handler in JSON mode
func BenchmarkGetTransactionJSON(b *testing.B) {
	initPrometheusMetrics()

	// Create a dummy testing.T for setup
	t := &testing.T{}
	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

	// Set up mock to return a transaction
	mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

	// Set echo context
	echoContext.SetPath("/tx/:hash")
	echoContext.SetParamNames("hash")
	echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Reset response recorder for each iteration
		responseRecorder.Body.Reset()
		responseRecorder.Header().Del("Content-Type")

		err := httpServer.GetTransaction(JSON)(echoContext)
		if err != nil {
			b.Fatalf("GetTransaction handler failed: %v", err)
		}

		if responseRecorder.Code != http.StatusOK {
			b.Fatalf("Expected status 200, got %d", responseRecorder.Code)
		}
	}
}

// BenchmarkGetTransactionBinary benchmarks the GetTransaction handler in BINARY_STREAM mode
func BenchmarkGetTransactionBinary(b *testing.B) {
	initPrometheusMetrics()

	// Create a dummy testing.T for setup
	t := &testing.T{}
	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

	// Set up mock to return a transaction
	mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

	// Set echo context
	echoContext.SetPath("/tx/:hash")
	echoContext.SetParamNames("hash")
	echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Reset response recorder for each iteration
		responseRecorder.Body.Reset()
		responseRecorder.Header().Del("Content-Type")

		err := httpServer.GetTransaction(BINARY_STREAM)(echoContext)
		if err != nil {
			b.Fatalf("GetTransaction handler failed: %v", err)
		}

		if responseRecorder.Code != http.StatusOK {
			b.Fatalf("Expected status 200, got %d", responseRecorder.Code)
		}
	}
}

// BenchmarkGetTransactionHex benchmarks the GetTransaction handler in HEX mode
func BenchmarkGetTransactionHex(b *testing.B) {
	initPrometheusMetrics()

	// Create a dummy testing.T for setup
	t := &testing.T{}
	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

	// Set up mock to return a transaction
	mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

	// Set echo context
	echoContext.SetPath("/tx/:hash")
	echoContext.SetParamNames("hash")
	echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Reset response recorder for each iteration
		responseRecorder.Body.Reset()
		responseRecorder.Header().Del("Content-Type")

		err := httpServer.GetTransaction(HEX)(echoContext)
		if err != nil {
			b.Fatalf("GetTransaction handler failed: %v", err)
		}

		if responseRecorder.Code != http.StatusOK {
			b.Fatalf("Expected status 200, got %d", responseRecorder.Code)
		}
	}
}
