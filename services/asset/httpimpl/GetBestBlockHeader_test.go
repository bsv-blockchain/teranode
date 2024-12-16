package httpimpl

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBestBlockHeader(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid JSON mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBestBlockHeader", mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		echoContext.SetPath("/bestblockheader/json")

		err := httpServer.GetBestBlockHeader(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "application/json; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))

		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		require.NotNil(t, response)
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", response["hash"])
		assert.Equal(t, float64(1), response["version"])
		assert.Equal(t, float64(1), response["height"])
		assert.Equal(t, float64(2), response["txCount"])
		assert.Equal(t, float64(3), response["sizeInBytes"])
		assert.Equal(t, float64(432645644), response["time"])
		assert.Equal(t, float64(12435623), response["nonce"])
		assert.Equal(t, "00000000", response["bits"])
		assert.Equal(t, "Miner", response["miner"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", response["previousblockhash"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", response["merkleroot"])

		mockRepo.AssertNumberOfCalls(t, "GetBestBlockHeader", 1)
	})

	t.Run("Valid BINARY_STREAM mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBestBlockHeader", mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		echoContext.SetPath("/bestblockheader")

		err := httpServer.GetBestBlockHeader(BINARY_STREAM)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, echo.MIMEOctetStream, responseRecorder.Header().Get("Content-Type"))
		assert.Equal(t, testBlockHeader.Bytes(), responseRecorder.Body.Bytes())
	})

	t.Run("Valid HEX mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBestBlockHeader", mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		echoContext.SetPath("/bestblockheader/hex")

		err := httpServer.GetBestBlockHeader(HEX)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))
		assert.Equal(t, hex.EncodeToString(testBlockHeader.Bytes()), responseRecorder.Body.String())
	})

	t.Run("Invalid mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBestBlockHeader", mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		echoContext.SetPath("/bestblockheader")

		var (
			invalidReadMode ReadMode = 999
		)

		err := httpServer.GetBestBlockHeader(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "Error: INVALID_ARGUMENT (error code: 1), Message: invalid read mode", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, errors.NewProcessingError("error getting best block header"))

		echoContext.SetPath("/best-block-header")
		echoContext.SetParamNames("mode")
		echoContext.SetParamValues("JSON")

		err := httpServer.GetBestBlockHeader(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "Error: PROCESSING (error code: 4), Message: error getting best block header", echoErr.Message)
	})
}
