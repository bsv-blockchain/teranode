package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlockHeaders(t *testing.T) {
	initPrometheusMetrics()

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headers/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		err := httpServer.GetBlockHeaders(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var response []map[string]interface{}

		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		require.NotNil(t, response)
		assert.Equal(t, 1, len(response))
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", response[0]["hash"])
		assert.Equal(t, float64(1), response[0]["version"])
		assert.Equal(t, float64(1), response[0]["height"])
		assert.Equal(t, float64(2), response[0]["txCount"])
		assert.Equal(t, float64(3), response[0]["sizeInBytes"])
		assert.Equal(t, float64(432645644), response[0]["time"])
		assert.Equal(t, float64(12435623), response[0]["nonce"])
		assert.Equal(t, "00000000", response[0]["bits"])
		assert.Equal(t, "Miner", response[0]["miner"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", response[0]["previousblockhash"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", response[0]["merkleroot"])
	})

	t.Run("Binary success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headers/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		err := httpServer.GetBlockHeaders(BINARY_STREAM)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, 80, len(responseRecorder.Body.Bytes()))

		blockHeaderFromRec, err := model.NewBlockHeaderFromBytes(responseRecorder.Body.Bytes())
		require.NoError(t, err)

		require.NotNil(t, blockHeaderFromRec)
		assert.Equal(t, testBlockHeader.Version, blockHeaderFromRec.Version)
		assert.Equal(t, testBlockHeader.Timestamp, blockHeaderFromRec.Timestamp)
		assert.Equal(t, testBlockHeader.Nonce, blockHeaderFromRec.Nonce)
		assert.Equal(t, testBlockHeader.Bits, blockHeaderFromRec.Bits)
		assert.Equal(t, testBlockHeader.HashPrevBlock, blockHeaderFromRec.HashPrevBlock)
		assert.Equal(t, testBlockHeader.HashMerkleRoot, blockHeaderFromRec.HashMerkleRoot)
	})

	t.Run("Hex success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headers/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		err := httpServer.GetBlockHeaders(HEX)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))
		assert.Equal(t, 160, len(responseRecorder.Body.Bytes()))
	})

	t.Run("Invalid hash", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewProcessingError("error getting block header"))

		echoContext.SetPath("/block/headers/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalid", "100")

		err := httpServer.GetBlockHeaders(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash length", echoErr.Message)
	})

	t.Run("Invalid hash character", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeaders", mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99y")

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): invalid hash string -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewProcessingError("error getting block headers"))

		echoContext.SetPath("/block/headers/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		err := httpServer.GetBlockHeaders(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "PROCESSING (4): error getting block headers", echoErr.Message)
	})

	t.Run("Repository not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewNotFoundError("block headers not found"))

		echoContext.SetPath("/block/headers/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		err := httpServer.GetBlockHeaders(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusNotFound, echoErr.Code)
		assert.Equal(t, "NOT_FOUND (3): block headers not found", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headers/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		var invalidReadMode ReadMode = 999

		err := httpServer.GetBlockHeaders(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})

	t.Run("Invalid number of headers", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headers/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", "abc")
		echoContext.QueryParams().Set("n", "abc")

		err := httpServer.GetBlockHeaders(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid number of headers", echoErr.Message)
	})
}
