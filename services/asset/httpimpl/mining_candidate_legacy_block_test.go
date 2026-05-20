package httpimpl

import (
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHandleMiningCandidateLegacyBlock(t *testing.T) {
	initPrometheusMetrics()

	validCandidateID := "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995"

	t.Run("block assembly client not configured", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)
		// blockAssemblyClient is nil by default

		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(validCandidateID)
		echoContext.QueryParams().Set("type", "miningcandidate")

		err := httpServer.GetLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		require.Equal(t, http.StatusNotImplemented, echoErr.Code)
	})

	t.Run("invalid candidate ID format", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)
		mockBA := blockassembly.NewMock()
		httpServer.blockAssemblyClient = mockBA

		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("not-hex!")
		echoContext.QueryParams().Set("type", "miningcandidate")

		err := httpServer.GetLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		require.Equal(t, http.StatusBadRequest, echoErr.Code)
	})

	t.Run("candidate not found", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)
		mockBA := blockassembly.NewMock()
		httpServer.blockAssemblyClient = mockBA

		mockBA.On("GetCandidateBlock", mock.Anything, mock.Anything).Return(
			nil, errors.NewNotFoundError("candidate not found"),
		)

		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(validCandidateID)
		echoContext.QueryParams().Set("type", "miningcandidate")

		err := httpServer.GetLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		require.Equal(t, http.StatusNotFound, echoErr.Code)
	})

	t.Run("block assembly internal error", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)
		mockBA := blockassembly.NewMock()
		httpServer.blockAssemblyClient = mockBA

		mockBA.On("GetCandidateBlock", mock.Anything, mock.Anything).Return(
			nil, errors.NewProcessingError("internal error"),
		)

		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(validCandidateID)
		echoContext.QueryParams().Set("type", "miningcandidate")

		err := httpServer.GetLegacyBlock()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		require.Equal(t, http.StatusInternalServerError, echoErr.Code)
	})

	t.Run("successful streaming", func(t *testing.T) {
		httpServer, mockRepo, echoContext, rec := GetMockHTTP(t, nil)
		mockBA := blockassembly.NewMock()
		httpServer.blockAssemblyClient = mockBA

		// Mock block assembly response
		mockBA.On("GetCandidateBlock", mock.Anything, mock.Anything).Return(
			&blockassembly_api.GetCandidateBlockResponse{
				Header:           make([]byte, 80),
				CoinbaseTx:       []byte{0x01, 0x02, 0x03},
				SubtreeHashes:    [][]byte{},
				TransactionCount: 1,
			}, nil,
		)

		// Mock repository streaming
		reader, writer := io.Pipe()
		go func() {
			defer writer.Close()
			_, _ = writer.Write([]byte("block-data"))
		}()
		mockRepo.On("GetMiningCandidateLegacyBlockReader").Return(reader, nil)

		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(validCandidateID)
		echoContext.QueryParams().Set("type", "miningcandidate")

		err := httpServer.GetLegacyBlock()(echoContext)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "block-data", rec.Body.String())
	})

	t.Run("normal block request without type param still works", func(t *testing.T) {
		httpServer, mockRepo, echoContext, rec := GetMockHTTP(t, nil)

		reader, writer := io.Pipe()
		go func() {
			defer writer.Close()
			_, _ = writer.Write([]byte("legacy-block"))
		}()
		mockRepo.On("GetLegacyBlockReader", mock.Anything, mock.Anything, mock.Anything).Return(reader, nil)

		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(validCandidateID)
		// No ?type= param — should go through the normal block path

		err := httpServer.GetLegacyBlock()(echoContext)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "legacy-block", rec.Body.String())
	})
}

// setQueryParams is a helper to set query parameters on an echo context.
func setQueryParams(c echo.Context, params map[string]string) {
	q := make(url.Values)
	for k, v := range params {
		q.Set(k, v)
	}
	c.Request().URL.RawQuery = q.Encode()
}
