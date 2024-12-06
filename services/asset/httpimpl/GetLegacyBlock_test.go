package httpimpl

import (
	"io"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
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
		assert.Equal(t, "Error: INVALID_ARGUMENT (error code: 1), Message: invalid block hash length", echoErr.Message)
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
		assert.Equal(t, "Error: INVALID_ARGUMENT (error code: 1), Message: invalid block hash format, Wrapped err: Error: UNKNOWN (error code: 0), Message: encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
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
		assert.Equal(t, "Error: NOT_FOUND (error code: 3), Message: block not found", echoErr.Message)
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
		assert.Equal(t, "Error: PROCESSING (error code: 4), Message: error getting block", echoErr.Message)
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
		assert.Equal(t, "Error: INVALID_ARGUMENT (error code: 1), Message: invalid block hash length", echoErr.Message)
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
		assert.Equal(t, "Error: INVALID_ARGUMENT (error code: 1), Message: invalid block hash string, Wrapped err: Error: UNKNOWN (error code: 0), Message: encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
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
		assert.Equal(t, "Error: INVALID_ARGUMENT (error code: 1), Message: invalid block hash extension", echoErr.Message)
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
		assert.Equal(t, "Error: NOT_FOUND (error code: 3), Message: block not found, Wrapped err: Error: NOT_FOUND (error code: 3), Message: block not found", echoErr.Message)
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
		assert.Equal(t, "Error: PROCESSING (error code: 4), Message: error getting block, Wrapped err: Error: PROCESSING (error code: 4), Message: error getting block", echoErr.Message)
	})
}
