package httpimpl

import (
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestParseNumberOfHeaders exercises the shared 'n' parsing/bounding logic used by both
// common-ancestor header handlers. A negative n must never survive to the uint32 cast
// that both callers perform, since that would turn e.g. -1 into 4294967295.
func TestParseNumberOfHeaders(t *testing.T) {
	initPrometheusMetrics()

	t.Run("negative n is rejected", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)

		n, err := httpServer.parseNumberOfHeaders("-1")
		require.Error(t, err)
		assert.Equal(t, 0, n)
		assert.GreaterOrEqual(t, n, 0, "n must never be negative going into the uint32 cast")
		assert.Equal(t, "INVALID_ARGUMENT (1): number of headers must not be negative", err.Error())
	})

	t.Run("large negative n is rejected", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)

		n, err := httpServer.parseNumberOfHeaders("-2147483648")
		require.Error(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("zero n falls back to default", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)

		n, err := httpServer.parseNumberOfHeaders("0")
		require.NoError(t, err)
		assert.Equal(t, 100, n)
	})

	t.Run("absent n falls back to default", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)

		n, err := httpServer.parseNumberOfHeaders("")
		require.NoError(t, err)
		assert.Equal(t, 100, n)
	})

	t.Run("n at documented max is honoured", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)

		n, err := httpServer.parseNumberOfHeaders("10000")
		require.NoError(t, err)
		assert.Equal(t, 10_000, n)
	})

	t.Run("n above documented max is clamped, not rejected", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)

		n, err := httpServer.parseNumberOfHeaders("15000")
		require.NoError(t, err)
		assert.Equal(t, 10_000, n)
	})

	t.Run("non-numeric n is rejected", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)

		n, err := httpServer.parseNumberOfHeaders("not-a-number")
		require.Error(t, err)
		assert.Equal(t, 0, n)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid number of headers", err.Error())
	})

	t.Run("Asset.MaxBlockHeaders tightens the cap below 10000", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxBlockHeaders = 500

		n, err := httpServer.parseNumberOfHeaders("10000")
		require.NoError(t, err)
		assert.Equal(t, 500, n)
	})

	t.Run("Asset.MaxBlockHeaders default of 0 preserves the 10000 catchup ceiling", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)

		n, err := httpServer.parseNumberOfHeaders("10000")
		require.NoError(t, err)
		assert.Equal(t, 10_000, n, "catchup sends n=10000 and must not be tightened by default")
	})

	t.Run("absent n is still clamped when Asset.MaxBlockHeaders is below the default", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxBlockHeaders = 50

		n, err := httpServer.parseNumberOfHeaders("")
		require.NoError(t, err)
		assert.Equal(t, 50, n, "the default must go through the cap, not bypass it")
	})

	t.Run("n=0 is still clamped when Asset.MaxBlockHeaders is below the default", func(t *testing.T) {
		httpServer, _, _, _ := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxBlockHeaders = 50

		n, err := httpServer.parseNumberOfHeaders("0")
		require.NoError(t, err)
		assert.Equal(t, 50, n, "the default must go through the cap, not bypass it")
	})
}

func TestGetBlockHeadersToCommonAncestor(t *testing.T) {
	initPrometheusMetrics()

	t.Run("negative n never reaches the repository as a huge uint32", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/headersToCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "-1")

		err := httpServer.GetBlockHeadersToCommonAncestor(JSON)(echoContext)

		require.Error(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): number of headers must not be negative", echoErr.Message)
	})

	t.Run("JSON success with default n", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeadersToCommonAncestor", mock.Anything, mock.Anything, uint32(100)).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headersToCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockHeadersToCommonAncestor(JSON)(echoContext)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		mockRepo.AssertExpectations(t)
	})

	t.Run("catchup n of 10000 is preserved end to end", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeadersToCommonAncestor", mock.Anything, mock.Anything, uint32(10_000)).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headersToCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "10000")

		err := httpServer.GetBlockHeadersToCommonAncestor(JSON)(echoContext)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		mockRepo.AssertExpectations(t)
	})

	t.Run("invalid hash string", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/headersToCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalid-hash")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockHeadersToCommonAncestor(JSON)(echoContext)

		require.Error(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
	})

	t.Run("too many block locator hashes is rejected", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		hash := "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"

		locator := ""
		for i := 0; i < maxBlockLocatorHashes+1; i++ {
			locator += hash
		}

		echoContext.SetPath("/block/headersToCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", locator)

		err := httpServer.GetBlockHeadersToCommonAncestor(JSON)(echoContext)

		require.Error(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): too many block locator hashes", echoErr.Message)
	})

	t.Run("repository not found error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeadersToCommonAncestor", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.ErrNotFound)

		echoContext.SetPath("/block/headersToCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockHeadersToCommonAncestor(JSON)(echoContext)

		require.Error(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		mockRepo.AssertExpectations(t)
	})
}
