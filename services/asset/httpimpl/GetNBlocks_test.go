package httpimpl

import (
	"bytes"
	"encoding/hex"
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

func TestGetNBlocks(t *testing.T) {
	initPrometheusMetrics()

	blocks := []*model.Block{
		testBlock,
	}

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlocks", mock.Anything, mock.Anything, mock.Anything).Return(blocks, nil)

		// set echo context
		echoContext.SetPath("/blocks/n/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "50")

		// Call GetNBlocks handler
		err := httpServer.GetNBlocks(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response []map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, 1, len(response))
		assert.Equal(t, float64(123), response[0]["transaction_count"])
		assert.Equal(t, float64(321), response[0]["size_in_bytes"])
		assert.Equal(t, float64(100), response[0]["height"])
		assert.Equal(t, float64(666), response[0]["id"])

		header := response[0]["header"].(map[string]interface{})
		assert.Equal(t, float64(1), header["version"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", header["hash_prev_block"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", header["hash_merkle_root"])
		assert.Equal(t, float64(432645644), header["timestamp"])
		assert.Equal(t, "00000000", header["bits"])
		assert.Equal(t, float64(12435623), header["nonce"])

		coinbaseTx := response[0]["coinbase_tx"].(map[string]interface{})
		assert.Equal(t, blocks[0].CoinbaseTx.String(), coinbaseTx["hex"])
	})

	t.Run("Binary success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlocks", mock.Anything, mock.Anything, mock.Anything).Return(blocks, nil)

		// set echo context
		echoContext.SetPath("/blocks/n/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "50")

		// Call GetNBlocks handler
		err := httpServer.GetNBlocks(BINARY_STREAM)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		responseBytes := responseRecorder.Body.Bytes()
		assert.Len(t, responseBytes, 229)

		// read the response bytes into a block
		block, err := model.NewBlockFromBytes(responseBytes)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, block)
		assert.Equal(t, blocks[0].Header, block.Header)
		assert.Equal(t, blocks[0].CoinbaseTx.String(), block.CoinbaseTx.String())
		assert.Equal(t, blocks[0].TransactionCount, block.TransactionCount)
		assert.Equal(t, blocks[0].SizeInBytes, block.SizeInBytes)
		assert.Equal(t, blocks[0].Subtrees, block.Subtrees)
		assert.Equal(t, blocks[0].Height, block.Height)
	})

	t.Run("Hex success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlocks", mock.Anything, mock.Anything, mock.Anything).Return(blocks, nil)

		// set echo context
		echoContext.SetPath("/blocks/n/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "50")

		// Call GetNBlocks handler
		err := httpServer.GetNBlocks(HEX)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))

		// Check response body
		responseHex := responseRecorder.Body.String()
		assert.Len(t, responseHex, 458)

		// read the response bytes into a block
		responseBytes, err := hex.DecodeString(responseHex)
		require.NoError(t, err)

		block, err := model.NewBlockFromBytes(responseBytes)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, block)
		assert.Equal(t, blocks[0].Header, block.Header)
		assert.Equal(t, blocks[0].CoinbaseTx.String(), block.CoinbaseTx.String())
		assert.Equal(t, blocks[0].TransactionCount, block.TransactionCount)
		assert.Equal(t, blocks[0].SizeInBytes, block.SizeInBytes)
		assert.Equal(t, blocks[0].Subtrees, block.Subtrees)
		assert.Equal(t, blocks[0].Height, block.Height)
	})

	t.Run("Binary success multiple blocks", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		multiBlocks := []*model.Block{
			testBlock,
			testBlock,
			testBlock,
		}

		// set mock response
		mockRepo.On("GetBlocks", mock.Anything, mock.Anything, mock.Anything).Return(multiBlocks, nil)

		// set echo context
		echoContext.SetPath("/blocks/n/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "50")

		// Call GetNBlocks handler
		err := httpServer.GetNBlocks(BINARY_STREAM)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		responseBytes := responseRecorder.Body.Bytes()
		assert.Len(t, responseBytes, 229*3)

		reader := bytes.NewReader(responseBytes)

		for i := 0; i < 3; i++ {
			// read the blocks sequentially from the reader
			block, err := model.NewBlockFromReader(reader)
			require.NoError(t, err)

			// Check response fields
			require.NotNil(t, block)
			assert.Equal(t, multiBlocks[i].Header, block.Header)
			assert.Equal(t, multiBlocks[i].CoinbaseTx.String(), block.CoinbaseTx.String())
			assert.Equal(t, multiBlocks[i].TransactionCount, block.TransactionCount)
			assert.Equal(t, multiBlocks[i].SizeInBytes, block.SizeInBytes)
			assert.Equal(t, multiBlocks[i].Subtrees, block.Subtrees)
			assert.Equal(t, multiBlocks[i].Height, block.Height)
		}
	})

	t.Run("Invalid hash", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/blocks/n/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalid")

		// Call GetNBlocks handler
		err := httpServer.GetNBlocks(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash length", echoErr.Message)
	})

	t.Run("Invalid number of blocks", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/blocks/n/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "invalid")

		// Call GetNBlocks handler
		err := httpServer.GetNBlocks(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid number of blocks -> UNKNOWN (0): strconv.Atoi: parsing \"invalid\": invalid syntax", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlocks", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("error getting block"))

		// set echo context
		echoContext.SetPath("/blocks/n/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "50")

		// Call GetNBlocks handler
		err := httpServer.GetNBlocks(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): error getting blocks -> STORAGE_ERROR (59): error getting block", echoErr.Message)
	})

	t.Run("Repository not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlocks", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("blocks not found"))

		// set echo context
		echoContext.SetPath("/blocks/n/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "50")

		// Call GetNBlocks handler
		err := httpServer.GetNBlocks(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): blocks not found", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlocks", mock.Anything, mock.Anything, mock.Anything).Return(blocks, nil)

		// set echo context
		echoContext.SetPath("/blocks/n/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "50")

		var (
			invalidReadMode ReadMode = 999
		)

		// Call GetNBlocks handler
		err := httpServer.GetNBlocks(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})
}
