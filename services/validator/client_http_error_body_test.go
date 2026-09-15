package validator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

type httpFallbackCountingReader struct {
	reader io.Reader
	read   int
}

func (r *httpFallbackCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += n

	return n, err
}

type httpFallbackPartialReader struct{}

func (httpFallbackPartialReader) Read(p []byte) (int, error) {
	return copy(p, "partial diagnostic"), io.ErrUnexpectedEOF
}

func TestReadHTTPFallbackErrorBody(t *testing.T) {
	for _, size := range []int{0, 13, 2047, 2048, 2049, 2080} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			input := strings.Repeat("x", size)
			reader := &httpFallbackCountingReader{reader: strings.NewReader(input)}
			prefix, truncated := readHTTPFallbackErrorBody(reader)
			require.Equal(t, input[:min(size, 2048)], string(prefix))
			require.Equal(t, size > 2048, truncated)
			require.Equal(t, min(size, 2049), reader.read)
		})
	}

	t.Run("partial read error", func(t *testing.T) {
		prefix, truncated := readHTTPFallbackErrorBody(httpFallbackPartialReader{})
		require.Equal(t, "partial diagnostic", string(prefix))
		require.False(t, truncated)
	})
}

type httpFallbackWarningLogger struct {
	*testLogger
	warning string
}

func (l *httpFallbackWarningLogger) Warnf(format string, args ...interface{}) {
	l.warning = fmt.Sprintf(format, args...)
}

func TestValidateTransactionViaHTTP_BoundedDiagnostics(t *testing.T) {
	prefix := "body-only-" + strings.Repeat("p", 2048-len("body-only-"))
	longBody := prefix + "tail-marker"
	verdict := errors.NewTxPolicyError("insufficient-fee")

	tests := []struct {
		name       string
		body       string
		header     string
		mediaType  string
		chunked    bool
		incomplete bool
		success    bool
	}{
		{name: "empty", mediaType: "text/plain"},
		{name: "short", body: "short diagnostic", mediaType: "text/plain"},
		{name: "exact limit", body: prefix, mediaType: "text/plain"},
		{name: "known length", body: longBody, mediaType: "text/plain"},
		{name: "unknown length", body: longBody, chunked: true},
		{name: "malformed header", body: longBody, header: "malformed", mediaType: "application/json"},
		{name: "public verdict", body: longBody, header: "valid", mediaType: "text/plain"},
		{name: "escaped warning", body: "body-only-\r\n\x1b\x00", header: "valid", mediaType: "application/octet-stream"},
		{name: "escaped service error", body: "body-only-\r\n\x1b\x00", mediaType: "application/octet-stream"},
		{name: "partial service error", body: "partial diagnostic", incomplete: true},
		{name: "partial verdict", body: "body-only-partial", header: "valid", incomplete: true},
		{name: "success ignores verdict", body: longBody, header: "valid", success: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.header == "valid" {
					errors.AttachHTTPError(w.Header(), verdict)
				} else if tc.header == "malformed" {
					w.Header().Set(errors.HTTPErrorHeader, "not-base64!")
				}

				if tc.mediaType != "" {
					w.Header().Set("Content-Type", tc.mediaType)
				}

				if !tc.chunked {
					length := len(tc.body)
					if tc.incomplete {
						length++
					}

					w.Header().Set("Content-Length", strconv.Itoa(length))
				}

				status := http.StatusBadRequest
				if tc.success {
					status = http.StatusOK
				}

				w.WriteHeader(status)

				if tc.chunked {
					w.(http.Flusher).Flush()
				}

				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(server.Close)

			addr, err := url.Parse(server.URL)
			require.NoError(t, err)

			logger := &httpFallbackWarningLogger{testLogger: &testLogger{t: t}}
			client := &Client{validatorHTTPAddr: addr, logger: logger}
			tx := createTestTransaction(t)

			err = client.validateTransactionViaHTTP(context.Background(), tx, 100, NewDefaultOptions())
			if tc.success {
				require.NoError(t, err)
				require.Empty(t, logger.warning)

				return
			}

			require.Error(t, err)
			require.IsType(t, &errors.Error{}, err)

			actual := err.(*errors.Error)

			diagnostic := fmt.Sprintf("%q", tc.body[:min(len(tc.body), 2048)])
			if len(tc.body) > 2048 {
				diagnostic += " (truncated)"
			}

			require.LessOrEqual(t, len(diagnostic), 4*2048+2+len(" (truncated)"))

			if tc.header == "valid" {
				require.Equal(t, verdict.Code(), actual.Code())
				require.Equal(t, fmt.Sprintf("[ValidateWithOptions][%s] %s", tx.TxID(), verdict.Message()), actual.Message())
				require.Nil(t, actual.WrappedErr())
				require.NotContains(t, errors.UserMessage(err), "body-only-")
				require.Equal(t, fmt.Sprintf("[ValidateWithOptions][%s] validator /tx endpoint rejected transaction: status=400 body=%s", tx.TxID(), diagnostic), logger.warning)
				require.NotContains(t, logger.warning, "tail-marker")
				require.NotContains(t, logger.warning, "\r")
				require.NotContains(t, logger.warning, "\n")
				require.NotContains(t, logger.warning, "\x1b")
			} else {
				require.Equal(t, errors.ERR_SERVICE_ERROR, actual.Code())
				require.Equal(t, fmt.Sprintf("[ValidateWithOptions][%s] validator /tx endpoint returned non-OK status: 400, body: %s", tx.TxID(), diagnostic), actual.Message())
				require.NotContains(t, actual.Message(), "tail-marker")
				require.NotContains(t, actual.Message(), "\r")
				require.NotContains(t, actual.Message(), "\n")
				require.NotContains(t, actual.Message(), "\x1b")
				require.Empty(t, logger.warning)
			}
		})
	}
}
