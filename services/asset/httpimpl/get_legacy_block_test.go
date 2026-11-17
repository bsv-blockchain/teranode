package httpimpl

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetLegacyBlock(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid hash", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		reader, writer := io.Pipe()

		go func() {
			defer writer.Close()
			_, _ = writer.Write([]byte("test"))
		}()

		// set mock response
		mockRepo.On("GetLegacyBlockReader", mock.Anything, mock.Anything, mock.Anything).Return(reader, nil)

		// set echo context
		echoContext.SetPath("/block/legacy/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetLegacyBlock handler
		err := httpServer.GetLegacyBlock()(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		assert.Equal(t, "test", responseRecorder.Body.String())
	})

	t.Run("Invalid hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/block/legacy/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalid")

		// Call GetLegacyBlock handler
		err := httpServer.GetLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid block hash length", echoErr.Message)
	})

	t.Run("Invalid hash format", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/block/legacy/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99t")

		// Call GetLegacyBlock handler
		err := httpServer.GetLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid block hash format -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("Block not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLegacyBlockReader", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("block not found"))

		// set echo context
		echoContext.SetPath("/block/legacy/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetLegacyBlock handler
		err := httpServer.GetLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)
		assert.Equal(t, "NOT_FOUND (3): block not found", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLegacyBlockReader", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewProcessingError("error getting block"))

		// set echo context
		echoContext.SetPath("/block/legacy/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetLegacyBlock handler
		err := httpServer.GetLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "PROCESSING (4): error getting block", echoErr.Message)
	})
}

func TestGetRestLegacyBlock(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid hash with .bin extension", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		reader, writer := io.Pipe()

		go func() {
			defer writer.Close()
			_, _ = writer.Write([]byte("test"))
		}()

		// set mock response
		mockRepo.On("GetLegacyBlockReader", mock.Anything, mock.Anything).Return(reader, nil)

		// set echo context
		echoContext.SetPath("/block/legacy/:hash.bin")
		echoContext.SetParamNames("hash.bin")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetRestLegacyBlock handler
		err := httpServer.GetRestLegacyBlock()(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		assert.Equal(t, "test", responseRecorder.Body.String())
	})

	t.Run("Invalid hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/block/legacy/:hash.bin")
		echoContext.SetParamNames("hash.bin")
		echoContext.SetParamValues("short")

		// Call GetRestLegacyBlock handler
		err := httpServer.GetRestLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid block hash length", echoErr.Message)
	})

	t.Run("Invalid hash string", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/block/legacy/:hash.bin")
		echoContext.SetParamNames("hash.bin")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99t")

		// Call GetRestLegacyBlock handler
		err := httpServer.GetRestLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid block hash string -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("Invalid hash extension", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/block/legacy/:hash.bin")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99t")

		// Call GetRestLegacyBlock handler
		err := httpServer.GetRestLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid block hash extension", echoErr.Message)
	})

	t.Run("Block not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLegacyBlockReader", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("block not found"))

		// set echo context
		echoContext.SetPath("/block/legacy/:hash.bin")
		echoContext.SetParamNames("hash.bin")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995.bin")

		// Call GetRestLegacyBlock handler
		err := httpServer.GetRestLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)
		assert.Equal(t, "NOT_FOUND (3): block not found -> NOT_FOUND (3): block not found", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLegacyBlockReader", mock.Anything, mock.Anything).Return(nil, errors.NewProcessingError("error getting block"))

		// set echo context
		echoContext.SetPath("/block/legacy/:hash.bin")
		echoContext.SetParamNames("hash.bin")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995.bin")

		// Call GetRestLegacyBlock handler
		err := httpServer.GetRestLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "PROCESSING (4): error getting block -> PROCESSING (4): error getting block", echoErr.Message)
	})
}

// TestGetLegacyBlockUsesLegacyPeerPool pins the security-critical decision that
// gates the internal legacy-peer-server pool
// (asset_concurrency_get_legacy_block_reader_peer): it must key off the shared
// secret asset_legacyPeerPoolToken, presented in the X-Teranode-Internal-Token
// header, never off network origin or the wire=1 query parameter alone. A mock
// repository observes the ctx GetLegacyBlock passes to GetLegacyBlockReader and
// reports whether it was marked with repository.WithLegacyBlockReaderPeerPool.
func TestGetLegacyBlockUsesLegacyPeerPool(t *testing.T) {
	const validHash = "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995"

	setup := func(t *testing.T, configuredToken string) (*HTTP, *repository.Mock, echo.Context, *bool) {
		t.Helper()

		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)
		httpServer.settings.Asset.LegacyPeerPoolToken = configuredToken

		reader, writer := io.Pipe()
		go func() {
			defer writer.Close()
			_, _ = writer.Write([]byte("test"))
		}()

		usedPeerPool := false

		mockRepo.On("GetLegacyBlockReader", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				ctx, ok := args.Get(0).(context.Context)
				require.True(t, ok, "GetLegacyBlockReader must be called with a context.Context as its first argument")
				usedPeerPool = repository.LegacyBlockReaderUsesPeerPool(ctx)
			}).
			Return(reader, nil)

		echoContext.SetPath("/block_legacy/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(validHash)

		return httpServer, mockRepo, echoContext, &usedPeerPool
	}

	t.Run("wire=1 with the correct token uses the peer pool", func(t *testing.T) {
		httpServer, _, echoContext, usedPeerPool := setup(t, "correct-token")
		echoContext.Request().URL.RawQuery = "wire=1"
		echoContext.Request().Header.Set(legacyInternalTokenHeader, "correct-token")

		require.NoError(t, httpServer.GetLegacyBlock()(echoContext))
		require.True(t, *usedPeerPool, "wire=1 with the correct token must use the peer pool")
	})

	t.Run("wire=1 with the wrong token uses the anonymous pool", func(t *testing.T) {
		httpServer, _, echoContext, usedPeerPool := setup(t, "correct-token")
		echoContext.Request().URL.RawQuery = "wire=1"
		echoContext.Request().Header.Set(legacyInternalTokenHeader, "wrong-token")

		require.NoError(t, httpServer.GetLegacyBlock()(echoContext))
		require.False(t, *usedPeerPool, "wire=1 with the wrong token must not use the peer pool")
	})

	t.Run("wire=1 with no token header uses the anonymous pool", func(t *testing.T) {
		httpServer, _, echoContext, usedPeerPool := setup(t, "correct-token")
		echoContext.Request().URL.RawQuery = "wire=1"

		require.NoError(t, httpServer.GetLegacyBlock()(echoContext))
		require.False(t, *usedPeerPool, "wire=1 with no token header must not use the peer pool")
	})

	t.Run("the correct token without wire=1 uses the anonymous pool", func(t *testing.T) {
		httpServer, _, echoContext, usedPeerPool := setup(t, "correct-token")
		echoContext.Request().Header.Set(legacyInternalTokenHeader, "correct-token")

		require.NoError(t, httpServer.GetLegacyBlock()(echoContext))
		require.False(t, *usedPeerPool, "a correct token without wire=1 must not use the peer pool")
	})

	t.Run("an empty configured token makes the peer pool unreachable regardless of any header", func(t *testing.T) {
		httpServer, _, echoContext, usedPeerPool := setup(t, "")
		echoContext.Request().URL.RawQuery = "wire=1"
		echoContext.Request().Header.Set(legacyInternalTokenHeader, "anything-at-all")

		require.NoError(t, httpServer.GetLegacyBlock()(echoContext))
		require.False(t, *usedPeerPool, "an empty configured token must make the peer pool unreachable")
	})
}
