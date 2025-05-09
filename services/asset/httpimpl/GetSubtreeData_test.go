package httpimpl

import (
	"io"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetSubtreeData(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid hash", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		reader, writer := io.Pipe()

		go func() {
			defer writer.Close()
			_, _ = writer.Write([]byte("test"))
		}()

		// set mock response
		mockRepo.On("GetSubtreeDataReader", mock.Anything, mock.Anything, mock.Anything).Return(reader, nil)

		// set echo context
		echoContext.SetPath("/subtree_data/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetSubtreeData handler
		err := httpServer.GetSubtreeData()(echoContext)
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
		echoContext.SetPath("/subtree_data/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalid")

		// Call GetSubtreeData handler
		err := httpServer.GetSubtreeData()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid subtree hash length", echoErr.Message)
	})

	t.Run("Invalid hash format", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/subtree_data/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99t")

		// Call GetSubtreeData handler
		err := httpServer.GetSubtreeData()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid subtree hash format -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("subtree not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetSubtreeDataReader", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("subtree not found"))

		// set echo context
		echoContext.SetPath("/subtree_data/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetSubtreeData handler
		err := httpServer.GetSubtreeData()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)
		assert.Equal(t, "NOT_FOUND (3): subtree not found", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetSubtreeDataReader", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewProcessingError("error getting subtree"))

		// set echo context
		echoContext.SetPath("/subtree_data/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetSubtreeData handler
		err := httpServer.GetSubtreeData()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "PROCESSING (4): error getting subtree", echoErr.Message)
	})
}
