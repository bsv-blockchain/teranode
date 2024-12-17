package httpimpl

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetTransactions(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid transaction hashes", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(test.TX1RawBytes, nil).Once()
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(test.TX2RawBytes, nil).Once()

		transactionHashes := test.TX1Hash.CloneBytes()
		transactionHashes = append(transactionHashes, test.TX1Hash.CloneBytes()...)

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(transactionHashes))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		reader := bytes.NewReader(responseRecorder.Body.Bytes())

		// read transactions from body, using go-bt
		// check that the transactions are the same as what we sent
		tx1 := bt.Tx{}
		_, err = tx1.ReadFrom(reader)
		require.NoError(t, err)
		assert.Equal(t, test.TX1Hash.String(), tx1.TxIDChainHash().String())

		tx2 := bt.Tx{}
		_, err = tx2.ReadFrom(reader)
		require.NoError(t, err)
		assert.Equal(t, test.TX2Hash.String(), tx2.TxIDChainHash().String())

		// make sure no more data is on the reader
		assert.Equal(t, 0, reader.Len())
	})

	t.Run("Invalid transaction hash length", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(test.TX1RawBytes, nil)

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): error reading request body -> UNKNOWN (0): unexpected EOF", echoErr.Message)
	})

	t.Run("Transaction not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("transaction not found"))

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(test.TX1Hash.CloneBytes()))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): transaction not found -> NOT_FOUND (3): transaction not found", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("error getting transaction"))

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(test.TX1Hash.CloneBytes()))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): error getting transaction -> STORAGE_ERROR (59): error getting transaction", echoErr.Message)
	})
}
