package util

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/require"
)

// TestMain disables SSRF protection for the entire package during tests because
// httptest.NewServer binds to 127.0.0.1, which the dial-level guard would otherwise
// block. Tests that specifically exercise SSRF protection re-enable it locally.
func TestMain(m *testing.M) {
	SetSSRFProtection(false)
	os.Exit(m.Run())
}

func TestDoHTTPRequestGET(t *testing.T) {
	// Create a test server that returns JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"message": "success"}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	response, err := DoHTTPRequest(ctx, server.URL)

	require.NoError(t, err)
	require.Equal(t, `{"message": "success"}`, string(response))
}

func TestDoHTTPRequestPOST(t *testing.T) {
	requestBody := []byte(`{"data": "test"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, requestBody, body)

		w.WriteHeader(http.StatusCreated)
		_, err = w.Write([]byte(`{"created": true}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	response, err := DoHTTPRequest(ctx, server.URL, requestBody)

	require.NoError(t, err)
	require.Equal(t, `{"created": true}`, string(response))
}

func TestDoHTTPRequestWithTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a slow server
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("slow response"))
		require.NoError(t, err)
	}))
	defer server.Close()

	// Test with context timeout shorter than server delay
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := DoHTTPRequest(ctx, server.URL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context deadline exceeded")
}

func TestDoHTTPRequestDefaultTimeoutInMilliseconds(t *testing.T) {
	// This test verifies that the default timeout is in milliseconds, not seconds
	// The http_timeout setting is 1000, which should be 1000ms (1 second), not 1000 seconds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a server that responds after 500ms - should succeed with 1000ms timeout
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("response after 500ms"))
		require.NoError(t, err)
	}))
	defer server.Close()

	// Use context without deadline to trigger default timeout
	ctx := context.Background()

	start := time.Now()
	response, err := DoHTTPRequest(ctx, server.URL)
	duration := time.Since(start)

	// Should succeed because server responds in 500ms and default timeout is 1000ms
	require.NoError(t, err)
	require.Equal(t, "response after 500ms", string(response))

	// Verify the request completed in a reasonable time (under 2 seconds)
	// If timeout was 1000 seconds, this test would pass but take forever
	require.Less(t, duration, 2*time.Second, "Request should complete quickly with millisecond timeout")
}

func TestDoHTTPRequestWithExistingDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("response"))
		require.NoError(t, err)
	}))
	defer server.Close()

	// Create context with existing deadline
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := DoHTTPRequest(ctx, server.URL)
	require.NoError(t, err)
	require.Equal(t, "response", string(response))
}

func TestDoHTTPRequestNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte("not found"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, server.URL)

	require.Error(t, err)
	require.Contains(t, err.Error(), "404")
	require.NotContains(t, err.Error(), "not found", "peer-controlled body must not be echoed into the classified error")
}

// TestBuildHTTPErrorBodyIsBoundedAndNotEchoed pins two properties of the non-2xx error body: the
// read is bounded (a hostile peer that answers with an error status then streams indefinitely
// cannot defeat the cap), and the peer body is NEVER embedded in the message (it would otherwise
// feed substring-based classification at the catchup gates).
func TestBuildHTTPErrorBodyIsBoundedAndNotEchoed(t *testing.T) {
	// headMarker is in the first bytes (drained, so its ABSENCE proves no-echo); tailMarker is past
	// the drain cap (so its absence proves the read was bounded, not run to completion).
	const headMarker = "ERRBODYHEAD"
	const tailMarker = "ERRBODYTAIL"

	oversizedBody := headMarker + strings.Repeat("A", 4*maxErrorBodyBytes) + tailMarker

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := w.Write([]byte(oversizedBody))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()

	t.Run("unbounded caller", func(t *testing.T) {
		_, err := DoHTTPRequest(ctx, server.URL)
		require.Error(t, err)
		require.NotContains(t, err.Error(), headMarker, "peer body must never be echoed into the classified error")
		require.NotContains(t, err.Error(), tailMarker, "the read must be bounded, not run to completion")
		require.Less(t, len(err.Error()), 512, "message carries only status + URL + byte count, not the body")
	})

	t.Run("bounded caller keeps its cap on an error status", func(t *testing.T) {
		// A non-2xx status short-circuits into buildHTTPError before DoHTTPRequestBounded's own cap.
		_, err := DoHTTPRequestBounded(ctx, server.URL, 1024)
		require.Error(t, err)
		require.NotContains(t, err.Error(), headMarker)
		require.NotContains(t, err.Error(), tailMarker)
	})
}

// TestBuildHTTPErrorBodyNotEchoed pins that a hostile body's content never reaches the error
// string at all — stronger than escaping. Because the body is not embedded, there is nothing to
// forge log lines with, and nothing feeds the catchup substring classifiers.
func TestBuildHTTPErrorBodyNotEchoed(t *testing.T) {
	// A forged log line, an ANSI escape, and a NUL.
	hostileBody := "real\nERROR forged log line\x1b[31m\x00tail"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, err := w.Write([]byte(hostileBody))
		require.NoError(t, err)
	}))
	defer server.Close()

	_, err := DoHTTPRequest(context.Background(), server.URL)
	require.Error(t, err)

	msg := err.Error()
	require.NotContains(t, msg, "\n", "no newline can reach the log line")
	require.NotContains(t, msg, "\x1b", "no terminal escape can reach operator logs")
	require.NotContains(t, msg, "\x00", "no control byte can reach operator logs")
	require.NotContains(t, msg, "forged log line", "peer body content must not appear in the classified error at all")
}

func TestDoHTTPRequestServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("internal server error"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, server.URL)

	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
	require.NotContains(t, err.Error(), "internal server error", "peer-controlled body must not be echoed into the classified error")
}

func TestDoHTTPRequestServerErrorNoBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		// There is no "body" written
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, server.URL)

	require.Error(t, err)
	require.Contains(t, err.Error(), "400")
	// An empty body adds no "(N body bytes...)" suffix and never echoes body content.
	require.NotContains(t, err.Error(), "with body")
}

func TestDoHTTPRequestHTMLResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("<html><body>Error page</body></html>"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, server.URL)

	require.Error(t, err)
	require.Contains(t, err.Error(), "returned HTML")
	require.Contains(t, err.Error(), "assume bad URL")
}

func TestDoHTTPRequestInvalidURL(t *testing.T) {
	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, "invalid-url")

	require.Error(t, err)
	// Non-http URL passes SSRF validation but fails at HTTP client level
	require.Contains(t, err.Error(), "failed to do http request")
}

func TestDoHTTPRequestConnectionError(t *testing.T) {
	ctx := context.Background()
	// Use a port that should be closed
	_, err := DoHTTPRequest(ctx, "http://localhost:99999")

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to do http request")
}

func TestDoHTTPRequestBodyReaderGET(t *testing.T) {
	responseData := `{"message": "success"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(responseData))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	reader, err := DoHTTPRequestBodyReader(ctx, server.URL)
	require.NoError(t, err)
	defer func(reader io.ReadCloser) {
		_ = reader.Close()
	}(reader)

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, responseData, string(body))
}

func TestDoHTTPRequestBodyReaderPOST(t *testing.T) {
	requestBody := []byte(`{"data": "test"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, requestBody, body)

		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte(`{"processed": true}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	reader, err := DoHTTPRequestBodyReader(ctx, server.URL, requestBody)
	require.NoError(t, err)
	defer func(reader io.ReadCloser) {
		_ = reader.Close()
	}(reader)

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, `{"processed": true}`, string(body))
}

func TestDoHTTPRequestBodyReaderError(t *testing.T) {
	ctx := context.Background()
	reader, err := DoHTTPRequestBodyReader(ctx, "invalid-url")

	require.Nil(t, reader)
	require.Error(t, err)
	// Non-http URL passes SSRF validation but fails at HTTP client level
	require.Contains(t, err.Error(), "failed to do http request")
}

func TestDoHTTPRequestBodyReaderServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("server error"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	reader, err := DoHTTPRequestBodyReader(ctx, server.URL)

	require.Nil(t, reader)
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
}

func TestDoHTTPRequestBodyReaderNoTimeoutOnSlowStream(t *testing.T) {
	// This test verifies that DoHTTPRequestBodyReader successfully completes
	// for quick responses, proving the streaming timeout is long enough
	responseData := []byte("streaming response data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(responseData)
		require.NoError(t, err)
	}))
	defer server.Close()

	// Context without timeout - function should apply streaming timeout (5 min default)
	ctx := context.Background()
	reader, err := DoHTTPRequestBodyReader(ctx, server.URL)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer func(reader io.ReadCloser) {
		_ = reader.Close()
	}(reader)

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, responseData, body)
}

func TestDoHTTPRequestBodyReaderWithShortTimeout(t *testing.T) {
	// This test uses a custom short streaming timeout to actually verify timeout behavior
	// Save and restore the original timeout
	originalTimeout := httpStreamingTimeout
	httpStreamingTimeout = 500 // 500ms for testing - enough for connection but not for slow read
	defer func() {
		httpStreamingTimeout = originalTimeout
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		// Write some data immediately
		flusher, ok := w.(http.Flusher)
		require.True(t, ok, "Expected http.ResponseWriter to be an http.Flusher")
		_, _ = w.Write([]byte("starting..."))
		flusher.Flush()

		// Sleep longer than timeout - this should cause the read to fail
		time.Sleep(1 * time.Second)
		_, _ = w.Write([]byte("this should not be received"))
	}))
	defer server.Close()

	ctx := context.Background()
	reader, err := DoHTTPRequestBodyReader(ctx, server.URL)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer func(reader io.ReadCloser) {
		_ = reader.Close()
	}(reader)

	// Reading should fail due to timeout while reading the body
	_, err = io.ReadAll(reader)
	// Should get an error because the server is too slow
	require.Error(t, err, "Should get timeout error on slow stream")
}

func TestDoHTTPRequestBodyReaderRespectsExistingDeadline(t *testing.T) {
	// This test verifies that DoHTTPRequestBodyReader respects an existing context deadline
	// We do this by setting a very short deadline and ensuring it times out quickly
	// (rather than waiting for the 5-minute streaming timeout)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		// Write initial data
		_, _ = w.Write([]byte("start"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Sleep for 2 seconds - longer than our 100ms deadline
		time.Sleep(2 * time.Second)
		_, _ = w.Write([]byte("should timeout before this"))
	}))
	defer server.Close()

	// Context with very short 100ms timeout (much shorter than 5-minute streaming default)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	reader, err := DoHTTPRequestBodyReader(ctx, server.URL)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer func(reader io.ReadCloser) {
		_ = reader.Close()
	}(reader)

	// This should timeout due to our 100ms deadline, not the 5-minute streaming timeout
	_, err = io.ReadAll(reader)
	elapsed := time.Since(start)

	// Should fail with timeout
	require.Error(t, err, "Should timeout with custom deadline")
	// Should have timed out quickly (within 1 second), not after 5 minutes
	require.Less(t, elapsed, 2*time.Second, "Should respect short custom deadline, not wait for streaming timeout")
}

func TestDoHTTPRequestEmptyRequestBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Empty slice still triggers POST because len(requestBody) > 0
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, []byte{}, body)

		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte("ok"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	// Pass empty byte slice - becomes POST because len > 0
	response, err := DoHTTPRequest(ctx, server.URL, []byte{})
	require.NoError(t, err)
	require.Equal(t, "ok", string(response))
}

func TestDoHTTPRequestNilRequestBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("ok"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	// Pass nil byte slice - should still be GET
	response, err := DoHTTPRequest(ctx, server.URL, nil)
	require.NoError(t, err)
	require.Equal(t, "ok", string(response))
}

func TestDoHTTPRequestMultipleRequestBodies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		// Should use the first non-nil body
		require.Equal(t, "first", string(body))

		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte("ok"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	// Pass multiple request bodies - should use first one
	response, err := DoHTTPRequest(ctx, server.URL, []byte("first"), []byte("second"))
	require.NoError(t, err)
	require.Equal(t, "ok", string(response))
}

func TestDoHTTPRequestServerErrorWithNilBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		// Empty body which results in b == nil after ReadAll of empty body
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, server.URL)

	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
	// Should not contain "with body" since body is effectively nil
	require.NotContains(t, err.Error(), "with body")
}

func TestDoHTTPRequestLargeResponse(t *testing.T) {
	// Create a large response (1MB)
	largeData := strings.Repeat("x", 1024*1024)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(largeData))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	response, err := DoHTTPRequest(ctx, server.URL)
	require.NoError(t, err)
	require.Equal(t, largeData, string(response))
}

func TestDoHTTPRequestJSONResponse(t *testing.T) {
	testData := map[string]interface{}{
		"id":      123,
		"name":    "test",
		"active":  true,
		"details": map[string]string{"key": "value"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(testData)
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	response, err := DoHTTPRequest(ctx, server.URL)
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(response, &parsed)
	require.NoError(t, err)
	require.Equal(t, float64(123), parsed["id"]) // JSON numbers become float64
	require.Equal(t, "test", parsed["name"])
	require.Equal(t, true, parsed["active"])
}

func TestDoHTTPRequestBounded_HappyPath(t *testing.T) {
	responseData := []byte(`{"message": "success"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(responseData)
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	response, err := DoHTTPRequestBounded(ctx, server.URL, 1024)

	require.NoError(t, err)
	require.Equal(t, responseData, response)
}

func TestDoHTTPRequestBounded_POST(t *testing.T) {
	requestBody := []byte(`{"data": "test"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, requestBody, body)

		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte(`{"processed": true}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	response, err := DoHTTPRequestBounded(ctx, server.URL, 1024, requestBody)

	require.NoError(t, err)
	require.Equal(t, `{"processed": true}`, string(response))
}

func TestDoHTTPRequestBounded_BodyEqualToLimit(t *testing.T) {
	// Boundary case: server returns exactly maxBytes — must succeed.
	const limit = 32
	responseData := []byte(strings.Repeat("a", limit))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(responseData)
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	response, err := DoHTTPRequestBounded(ctx, server.URL, int64(limit))

	require.NoError(t, err)
	require.Equal(t, responseData, response)
	require.Len(t, response, limit)
}

func TestDoHTTPRequestBounded_BodyExceedsLimit(t *testing.T) {
	// Server returns more than maxBytes — must return a typed error and not allocate the whole body.
	const limit = 16
	responseData := []byte(strings.Repeat("x", limit*4))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(responseData)
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	response, err := DoHTTPRequestBounded(ctx, server.URL, int64(limit))

	require.Error(t, err)
	require.Nil(t, response)
	require.True(t, errors.Is(err, errors.ErrExternal), "expected ErrExternal, got %v", err)
	require.Contains(t, err.Error(), "exceeds")
}

func TestDoHTTPRequestBounded_BodyOneByteOverLimit(t *testing.T) {
	// Off-by-one boundary: maxBytes+1 bytes must fail.
	const limit = 32
	responseData := []byte(strings.Repeat("b", limit+1))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(responseData)
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	response, err := DoHTTPRequestBounded(ctx, server.URL, int64(limit))

	require.Error(t, err)
	require.Nil(t, response)
	require.True(t, errors.Is(err, errors.ErrExternal))
}

func TestDoHTTPRequestBounded_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte("not found"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := DoHTTPRequestBounded(ctx, server.URL, 1024)

	require.Error(t, err)
	require.Contains(t, err.Error(), "404")
}

func TestDoHTTPRequestBounded_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("slow response"))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := DoHTTPRequestBounded(ctx, server.URL, 1024)
	require.Error(t, err)
}

// Benchmark tests
func BenchmarkDoHTTPRequest_GET(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("benchmark response"))
		require.NoError(b, err)
	}))
	defer server.Close()

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := DoHTTPRequest(ctx, server.URL)
		require.NoError(b, err)
	}
}

func BenchmarkDoHTTPRequest_POST(b *testing.B) {
	requestBody := []byte(`{"benchmark": true}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("benchmark response"))
		require.NoError(b, err)
	}))
	defer server.Close()

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := DoHTTPRequest(ctx, server.URL, requestBody)
		require.NoError(b, err)
	}
}

func BenchmarkDoHTTPRequestBodyReader_GET(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("benchmark response"))
		require.NoError(b, err)
	}))
	defer server.Close()

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader, err := DoHTTPRequestBodyReader(ctx, server.URL)
		require.NoError(b, err)
		_, err = io.ReadAll(reader)
		require.NoError(b, err)
		_ = reader.Close()
	}
}

func TestDoHTTPRequest_ReadBodyError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Force connection close to cause read error
		w.Header().Set("Connection", "close")
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, _ := hj.Hijack()
			_ = conn.Close()
		}
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, server.URL)

	// This might pass or fail depending on timing, but exercises the read error path
	if err != nil {
		require.Contains(t, err.Error(), "failed to read body")
	}
}

func TestDoHTTPRequest_ErrorResponseBodyReadError(t *testing.T) {
	// This test is tricky - we need to create a server that returns an error status
	// but has a body that will fail to read. This is hard to simulate reliably.
	// For now, we'll test the normal error path which is already well covered.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("server error"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, server.URL)

	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
	require.NotContains(t, err.Error(), "server error", "peer-controlled body must not be echoed into the classified error")
}

func TestDoHTTPRequest_NilFirstRequestBody(t *testing.T) {
	// Test with first element nil but slice not empty
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method) // Should be GET because first body is nil
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("ok"))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	// Pass nil as first element - should be GET
	response, err := DoHTTPRequest(ctx, server.URL, nil)
	require.NoError(t, err)
	require.Equal(t, "ok", string(response))
}

func TestDoHTTPRequest_ErrorResponseNilBody(t *testing.T) {
	// Test error response with nil body to cover that path
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		// Write nothing - body will be non-nil but ReadAll will return empty slice
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, server.URL)

	require.Error(t, err)
	require.Contains(t, err.Error(), "400")
	// An empty body adds no "(N body bytes...)" suffix and never echoes body content.
	require.NotContains(t, err.Error(), "with body")
}

func TestDoHTTPRequest_CreateRequestError(t *testing.T) {
	SetSSRFProtection(true)
	defer SetSSRFProtection(false)
	// Test with malformed URL that will fail validation/request creation
	ctx := context.Background()
	_, err := DoHTTPRequest(ctx, "ht\ttp://invalid-url-with-control-char")

	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid URL")
}

func TestValidateURL(t *testing.T) {
	SetSSRFProtection(true)
	defer SetSSRFProtection(false)

	tests := []struct {
		name    string
		url     string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid https URL",
			url:     "https://example.com/path",
			wantErr: false,
		},
		{
			name:    "valid http URL",
			url:     "http://example.com/path",
			wantErr: false,
		},
		{
			name:    "skip ftp scheme",
			url:     "ftp://example.com/file",
			wantErr: false,
		},
		{
			name:    "skip file scheme",
			url:     "file:///etc/passwd",
			wantErr: false,
		},
		{
			name:    "allow loopback IPv4",
			url:     "http://127.0.0.1:8080/path",
			wantErr: false,
		},
		{
			name:    "allow localhost",
			url:     "http://localhost:8080/path",
			wantErr: false,
		},
		{
			name:    "allow private 10.x",
			url:     "http://10.0.0.1/path",
			wantErr: false,
		},
		{
			name:    "allow private 192.168.x",
			url:     "http://192.168.1.1/path",
			wantErr: false,
		},
		{
			name:    "allow private 172.16.x",
			url:     "http://172.16.0.1/path",
			wantErr: false,
		},
		{
			name:    "reject link-local 169.254.x",
			url:     "http://169.254.169.254/latest/meta-data",
			wantErr: true,
			errMsg:  "blocked IP",
		},
		{
			name:    "skip non-http scheme",
			url:     "no-scheme-url",
			wantErr: false,
		},
		{
			name:    "reject empty hostname",
			url:     "http:///path",
			wantErr: true,
			errMsg:  "no hostname",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateURL(tt.url)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateURL_Disabled(t *testing.T) {
	// Verify that disabling SSRF protection allows all URLs
	SetSSRFProtection(false)
	defer SetSSRFProtection(false) // restore the package default set by TestMain

	err := ValidateURL("http://127.0.0.1:8080/path")
	require.NoError(t, err)
}

// TestValidateURL_NonHTTPSentinelsPassThrough guards the "legacy" baseURL default (and the
// empty string) used by block/subtree validation: they have no http/https scheme, so
// ValidateURL must return nil and never reject them. Regression test for the early
// ValidateURL guards added to the peer-supplied baseURL entry points.
func TestValidateURL_NonHTTPSentinelsPassThrough(t *testing.T) {
	SetSSRFProtection(true)
	defer SetSSRFProtection(false)

	for _, s := range []string{"legacy", ""} {
		require.NoError(t, ValidateURL(s), "ValidateURL(%q) must pass through", s)
	}
}

func TestValidateURL_RejectsUserinfo(t *testing.T) {
	SetSSRFProtection(true)
	defer SetSSRFProtection(false)
	tests := []struct {
		name string
		url  string
	}{
		{"username only", "http://user@example.com/path"},
		{"username and password", "http://user:pass@example.com/path"},
		{"empty username with colon", "http://:secret@example.com/path"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateURL(tt.url)
			require.Error(t, err)
			require.Contains(t, err.Error(), "userinfo")
		})
	}
}

func TestIsBlockedDialIP(t *testing.T) {
	// isBlockedDialIP is a pure function; no SSRF toggle needed.
	// Only link-local (incl. the metadata endpoint), loopback and unspecified are blocked.
	blocked := []string{
		"127.0.0.1",
		"::1",
		"169.254.1.1",
		"169.254.169.254", // cloud metadata endpoint
		"fe80::1",
		"0.0.0.0",
		"::",
	}
	for _, ipStr := range blocked {
		t.Run("blocked_"+ipStr, func(t *testing.T) {
			ip := net.ParseIP(ipStr)
			require.NotNil(t, ip)
			require.True(t, isBlockedDialIP(ip), "expected %s to be blocked", ipStr)
		})
	}

	// RFC1918 / ULA ranges are intentionally allowed: teranode peers, k8s pods and
	// private miner interconnects all live on private networks.
	allowed := []string{
		"8.8.8.8",
		"1.1.1.1",
		"203.0.113.1",
		"2001:db8::1",
		"10.0.0.1",
		"10.255.255.255",
		"172.16.0.1",
		"172.31.255.255",
		"192.168.0.1",
		"192.168.255.255",
		"fc00::1",
	}
	for _, ipStr := range allowed {
		t.Run("allowed_"+ipStr, func(t *testing.T) {
			ip := net.ParseIP(ipStr)
			require.NotNil(t, ip)
			require.False(t, isBlockedDialIP(ip), "expected %s to be allowed", ipStr)
		})
	}
}

func TestSSRFDialContext_RejectsPrivateHostname(t *testing.T) {
	SetSSRFProtection(true)
	defer SetSSRFProtection(false)
	// ssrfDialContext resolves hostnames; loopback should be blocked.
	// We use "localhost" which always resolves to 127.0.0.1 / ::1.
	ctx := context.Background()
	_, err := ssrfDialContext(ctx, "tcp", "localhost:80")
	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked IP")
}

func TestSSRFDialContext_DisabledAllowsPrivate(t *testing.T) {
	SetSSRFProtection(false)
	defer SetSSRFProtection(false) // restore the package default set by TestMain

	// With protection disabled the dialer should attempt the connection normally.
	// Use a closed port so we get a connection-refused rather than hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := ssrfDialContext(ctx, "tcp", "127.0.0.1:1")
	// Any error here is a real network error (connection refused), NOT our SSRF guard.
	if err != nil {
		require.NotContains(t, err.Error(), "blocked IP")
	}
}

// TestSSRFDialContext_RejectsDNSRebinding reproduces the DNS-rebinding TOCTOU attack and
// proves the dial guard blocks it. A peer-controlled name "looks" public but resolves to a
// private/loopback address; the guard must reject the dial rather than connect to the
// internal target. We inject the resolver because the real attack relies on a hostile
// authoritative server we cannot stand up in a unit test.
func TestSSRFDialContext_RejectsDNSRebinding(t *testing.T) {
	SetSSRFProtection(true)
	defer SetSSRFProtection(false)

	orig := ssrfLookupHost
	defer func() { ssrfLookupHost = orig }()
	// Resolver returns a loopback address for a "public-looking" hostname.
	ssrfLookupHost = func(_ context.Context, host string) ([]string, error) {
		require.Equal(t, "evil.example.com", host)
		return []string{"127.0.0.1"}, nil
	}

	_, err := ssrfDialContext(context.Background(), "tcp", "evil.example.com:80")
	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked IP")
}

// TestSSRFDialContext_RejectsMixedPublicBlocked ensures a resolver answer mixing a public
// and a blocked (link-local metadata) address is rejected outright, so failover cannot
// smuggle in the internal IP.
func TestSSRFDialContext_RejectsMixedPublicBlocked(t *testing.T) {
	SetSSRFProtection(true)
	defer SetSSRFProtection(false)

	orig := ssrfLookupHost
	defer func() { ssrfLookupHost = orig }()
	ssrfLookupHost = func(_ context.Context, _ string) ([]string, error) {
		return []string{"8.8.8.8", "169.254.169.254"}, nil
	}

	_, err := ssrfDialContext(context.Background(), "tcp", "evil.example.com:80")
	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked IP")
}

func TestHTTPClient_RejectsRedirectToLinkLocal(t *testing.T) {
	SetSSRFProtection(true)
	defer SetSSRFProtection(false)
	linkLocalURL := "http://169.254.169.254/latest/meta-data/"
	req := &http.Request{URL: func() *url.URL { u, _ := url.Parse(linkLocalURL); return u }()}
	err := httpClient.CheckRedirect(req, []*http.Request{{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "SSRF redirect check")
}

func TestHTTPClient_RedirectLimitEnforced(t *testing.T) {
	// Verify that CheckRedirect rejects chains longer than 10 hops.
	req := &http.Request{URL: func() *url.URL { u, _ := url.Parse("http://example.com/"); return u }()}
	via := make([]*http.Request, 10)
	err := httpClient.CheckRedirect(req, via)
	require.Error(t, err)
	require.Contains(t, err.Error(), "10 redirects")
}

// testRetryConfig is a fast retry config for tests: short delays, low attempt count.
var testRetryConfig = retryConfig{
	maxAttempts:  4,
	initialDelay: 10 * time.Millisecond,
	maxDelay:     50 * time.Millisecond,
}

func TestDoHTTPRequestBodyReaderWithRetry_SuccessOnFirstTry(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	}))
	defer server.Close()

	body, err := doHTTPRequestBodyReaderWithRetry(context.Background(), server.URL, testRetryConfig, nil)
	require.NoError(t, err)
	defer body.Close()

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, "body", string(got))
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts), "should not retry on success")
}

func TestDoHTTPRequestBodyReaderWithRetry_RetriesOn503ThenSucceeds(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			// First two attempts: 503
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("busy"))
			return
		}
		// Third attempt: success
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok-after-retry"))
	}))
	defer server.Close()

	body, err := doHTTPRequestBodyReaderWithRetry(context.Background(), server.URL, testRetryConfig, nil)
	require.NoError(t, err)
	defer body.Close()

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, "ok-after-retry", string(got), "should return body from successful retry, not earlier 503 body")
	require.Equal(t, int32(3), atomic.LoadInt32(&attempts), "exactly two retries before success")
}

func TestDoHTTPRequestBodyReaderWithRetry_ExhaustsAttemptsOnPersistent503(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	body, err := doHTTPRequestBodyReaderWithRetry(context.Background(), server.URL, testRetryConfig, nil)
	require.Error(t, err)
	require.Nil(t, body)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable),
		"final error must be ErrServiceUnavailable so callers can branch on it; got %T: %v", err, err)
	require.Equal(t, int32(testRetryConfig.maxAttempts), atomic.LoadInt32(&attempts),
		"should have exactly maxAttempts attempts")
}

func TestDoHTTPRequestBodyReaderWithRetry_HonorsRetryAfter(t *testing.T) {
	var attempts int32
	const retryAfterSeconds = 1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "1") // 1 second per RFC 7231 delta-seconds
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	// Tight initialDelay (10ms) so the only thing that can produce a >=1s wait is honoring Retry-After.
	cfg := retryConfig{maxAttempts: 4, initialDelay: 10 * time.Millisecond, maxDelay: 5 * time.Second}

	start := time.Now()
	body, err := doHTTPRequestBodyReaderWithRetry(context.Background(), server.URL, cfg, nil)
	elapsed := time.Since(start)

	require.NoError(t, err)
	defer body.Close()
	require.GreaterOrEqual(t, elapsed, time.Duration(retryAfterSeconds)*time.Second-100*time.Millisecond,
		"server's Retry-After=1 must be honored over the much smaller initialDelay")
	require.Less(t, elapsed, 3*time.Second, "should not wait significantly longer than Retry-After")
}

func TestDoHTTPRequestBodyReaderWithRetry_NoRetryOnNon503(t *testing.T) {
	cases := []struct {
		name string
		code int
	}{
		{"500_internal_server_error", http.StatusInternalServerError},
		{"502_bad_gateway", http.StatusBadGateway},
		{"504_gateway_timeout", http.StatusGatewayTimeout},
		{"404_not_found", http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var attempts int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&attempts, 1)
				w.WriteHeader(tc.code)
			}))
			defer server.Close()

			_, err := doHTTPRequestBodyReaderWithRetry(context.Background(), server.URL, testRetryConfig, nil)
			require.Error(t, err)
			require.Equal(t, int32(1), atomic.LoadInt32(&attempts),
				"non-503 status %d must fail immediately, not retry", tc.code)
		})
	}
}

func TestDoHTTPRequestBodyReaderWithRetry_ContextCancelAbortsRetries(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	// Generous per-attempt delay so the cancel must be the thing that ends the loop.
	cfg := retryConfig{maxAttempts: 6, initialDelay: 200 * time.Millisecond, maxDelay: time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := doHTTPRequestBodyReaderWithRetry(ctx, server.URL, cfg, nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 500*time.Millisecond,
		"ctx cancel should short-circuit the retry loop well before exhausting attempts")
	// At least one attempt happened; we don't assert exactly because timing is racy.
	require.GreaterOrEqual(t, atomic.LoadInt32(&attempts), int32(1))
	require.LessOrEqual(t, atomic.LoadInt32(&attempts), int32(2),
		"should not fire all 6 attempts if cancelled at 50ms with 200ms+ backoffs")
}

func TestDoHTTPRequestBodyReaderWithRetry_RetriesOn429ThenSucceeds(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			// First two attempts: 429 rate limited with no Retry-After header, exercising
			// the jittered exponential-backoff path (the Retry-After path is covered
			// separately by TestRetryHTTP_RetryAfterClampedToCeiling / _RetryAfterAboveMaxDelayHonored).
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"rate limit exceeded"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok-after-429"))
	}))
	defer server.Close()

	body, err := doHTTPRequestBodyReaderWithRetry(context.Background(), server.URL, testRetryConfig, nil)
	require.NoError(t, err)
	defer body.Close()

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, "ok-after-429", string(got))
	require.Equal(t, int32(3), atomic.LoadInt32(&attempts), "429 must be retried, not failed immediately")
}

func TestDoHTTPRequestBodyReaderWithRetry_ExhaustsAttemptsOnPersistent429(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	body, err := doHTTPRequestBodyReaderWithRetry(context.Background(), server.URL, testRetryConfig, nil)
	require.Error(t, err)
	require.Nil(t, body)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable),
		"persistent 429 must surface as ErrServiceUnavailable (retryable class); got %T: %v", err, err)
	require.Equal(t, int32(testRetryConfig.maxAttempts), atomic.LoadInt32(&attempts))
}

// TestBuildHTTPError_429MapsToServiceUnavailable proves a single (non-retrying)
// request maps HTTP 429 to the retryable ErrServiceUnavailable class, so any
// caller using errors.Is can branch on it.
func TestBuildHTTPError_429MapsToServiceUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"message":"rate limit exceeded"}`))
	}))
	defer server.Close()

	_, err := DoHTTPRequest(context.Background(), server.URL)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable),
		"429 must map to ErrServiceUnavailable; got %T: %v", err, err)
	require.Contains(t, err.Error(), "429")
}

func TestDoHTTPRequestWithRetry_RetriesOn429ThenSucceeds(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("batch-ok"))
	}))
	defer server.Close()

	got, err := doHTTPRequestWithRetry(context.Background(), server.URL, testRetryConfig, nil)
	require.NoError(t, err)
	require.Equal(t, "batch-ok", string(got))
	require.Equal(t, int32(2), atomic.LoadInt32(&attempts))
}

func TestDoHTTPRequestWithRetry_NoRetryOn404(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	_, err := doHTTPRequestWithRetry(context.Background(), server.URL, testRetryConfig, nil)
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts), "404 must not be retried (peer lacks data)")
}

func TestDoHTTPRequestBoundedWithRetry_RetriesOn429ThenSucceeds(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("subtree-bytes"))
	}))
	defer server.Close()

	got, err := doHTTPRequestBoundedWithRetry(context.Background(), server.URL, 1024, testRetryConfig, nil)
	require.NoError(t, err)
	require.Equal(t, "subtree-bytes", string(got))
	require.Equal(t, int32(2), atomic.LoadInt32(&attempts))
}

func TestDoHTTPRequestBoundedWithRetry_EnforcesCap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("0123456789"))
	}))
	defer server.Close()

	_, err := doHTTPRequestBoundedWithRetry(context.Background(), server.URL, 4, testRetryConfig, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrExternal), "over-cap body must return ErrExternal; got %v", err)
}

// TestReadBodyWithRetry_MidStreamStallIsPeerFault proves a peer that stalls mid-body
// (tripping the per-request transport timeout) surfaces as a NON-local error, so the
// catchup reputation gate attributes it to the peer rather than silently absolving it.
func TestReadBodyWithRetry_MidStreamStallIsPeerFault(t *testing.T) {
	// Shrink the per-request timeout for the duration of this test.
	saved := httpRequestTimeout
	httpRequestTimeout = 150 // ms
	t.Cleanup(func() { httpRequestTimeout = saved })

	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release // hang past the transport timeout
	}))
	// LIFO: close(release) runs before server.Close(), so the parked handler exits and
	// Close() (which waits for outstanding connections) doesn't deadlock.
	defer server.Close()
	defer close(release)

	// No parent deadline → the transport timeout (httpRequestTimeout) governs the read.
	_, err := doHTTPRequestWithRetry(context.Background(), server.URL, defaultRetryConfig, nil)
	require.Error(t, err)
	require.False(t, errors.IsLocalError(err),
		"a peer stalling mid-body must be a peer fault (non-local), not absolved as local; got %T: %v", err, err)
}

// TestReadBodyWithRetry_ShutdownCancelIsLocal proves the opposite-direction case:
// a parent-context CANCEL mid-body-read (e.g. node shutdown) is a LOCAL condition,
// not a peer fault, so it must not ding peer reputation.
func TestReadBodyWithRetry_ShutdownCancelIsLocal(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release
	}))
	defer server.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel() // simulate shutdown mid-read
	}()

	_, err := doHTTPRequestWithRetry(ctx, server.URL, defaultRetryConfig, nil)
	require.Error(t, err)
	require.True(t, errors.IsLocalError(err),
		"a shutdown cancel mid-read must be local (not a peer fault); got %T: %v", err, err)
}

// TestReadBodyWithRetry_PreResponseStallIsPeerFault proves a peer that stalls BEFORE
// sending response headers (connect/TLS/header phase) is attributed to the peer
// (non-local), not absolved as local — otherwise the failover gate's break-on-local
// would halt all alternative-peer attempts and re-wedge catchup.
func TestReadBodyWithRetry_PreResponseStallIsPeerFault(t *testing.T) {
	saved := httpRequestTimeout
	httpRequestTimeout = 150 // ms
	t.Cleanup(func() { httpRequestTimeout = saved })

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hang before writing any header/status
	}))
	defer server.Close()
	defer close(release)

	_, err := doHTTPRequestWithRetry(context.Background(), server.URL, defaultRetryConfig, nil)
	require.Error(t, err)
	require.False(t, errors.IsLocalError(err),
		"a peer stalling before headers must be a peer fault (non-local); got %T: %v", err, err)
}

// TestWithRetryHelpers_SignRequests guards against the regression where the retry
// request-builder diverged from executeHTTPRequest and dropped request signing —
// which would send every catchup fetch unsigned, losing the asset rate-limit
// exemption. All *WithRetry helpers must sign when a signer is configured.
func TestWithRetryHelpers_SignRequests(t *testing.T) {
	suiteOriginal := loadHTTPRequestSigner()
	priorSigner := NewEd25519RequestSigner(nil)
	SetHTTPRequestSigner(priorSigner)
	t.Cleanup(func() {
		if suiteOriginal != nil {
			SetHTTPRequestSigner(suiteOriginal)
			return
		}
		SetHTTPRequestSigner(NewEd25519RequestSigner(nil))
	})

	t.Run("signs all retry helpers", func(t *testing.T) {
		privKey, _, err := crypto.GenerateEd25519Key(crand.Reader)
		require.NoError(t, err)
		original := loadHTTPRequestSigner()
		SetHTTPRequestSigner(NewEd25519RequestSigner(privKey))
		t.Cleanup(func() { SetHTTPRequestSigner(original) })

		var gotSig atomic.Bool
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Peer-Signature") != "" && r.Header.Get("X-Peer-PubKey") != "" {
				gotSig.Store(true)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		defer server.Close()

		t.Run("doHTTPRequestWithRetry", func(t *testing.T) {
			gotSig.Store(false)
			_, err := doHTTPRequestWithRetry(context.Background(), server.URL, defaultRetryConfig, nil)
			require.NoError(t, err)
			require.True(t, gotSig.Load())
		})
		t.Run("DoHTTPRequestBoundedWithRetry", func(t *testing.T) {
			gotSig.Store(false)
			_, err := DoHTTPRequestBoundedWithRetry(context.Background(), server.URL, 1024, nil)
			require.NoError(t, err)
			require.True(t, gotSig.Load())
		})
		t.Run("DoHTTPRequestBodyReaderWithRetry", func(t *testing.T) {
			gotSig.Store(false)
			body, err := DoHTTPRequestBodyReaderWithRetry(context.Background(), server.URL)
			require.NoError(t, err)
			require.NoError(t, body.Close())
			require.True(t, gotSig.Load())
		})
	})

	require.Same(t, priorSigner, loadHTTPRequestSigner())
}

// TestRetryHTTP_DeadlineAfterPeerFaultIsPeerError proves that when the context deadline
// expires while backing off from a real retryable peer fault (503/429), the error is
// attributed to the peer (non-local network timeout, matched by error CODE) — not a bare
// local context error — so a peer that stalls us out cannot evade a reputation penalty.
// Driven white-box with an instant attempt so the deadline can only land during the
// backoff sleep (deterministic), not mid-HTTP-call (which would be flaky).
func TestRetryHTTP_DeadlineAfterPeerFaultIsPeerError(t *testing.T) {
	cfg := retryConfig{maxAttempts: 6, initialDelay: 500 * time.Millisecond, maxDelay: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	attempt := func(context.Context) (int, time.Duration, error) {
		return 0, 0, errors.NewServiceUnavailableError("peer 503") // instant retryable fault
	}
	_, err := retryHTTP(ctx, cfg, attempt)
	require.Error(t, err)
	require.False(t, errors.IsLocalError(err),
		"deadline after a peer fault must be a peer fault (non-local); got %T: %v", err, err)
	require.True(t, errors.IsNetworkError(err),
		"should classify as a network timeout (peer fault) by code; got %T: %v", err, err)
}

// TestRetryHTTP_CancelStaysLocal proves the complementary case: an explicit cancel
// (e.g. shutdown), even after a peer fault, stays a local context error so the peer is
// not blamed for our teardown.
func TestRetryHTTP_CancelStaysLocal(t *testing.T) {
	cfg := retryConfig{maxAttempts: 6, initialDelay: 500 * time.Millisecond, maxDelay: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()

	attempt := func(context.Context) (int, time.Duration, error) {
		return 0, 0, errors.NewServiceUnavailableError("peer 503")
	}
	_, err := retryHTTP(ctx, cfg, attempt)
	require.Error(t, err)
	require.True(t, errors.IsLocalError(err), "an explicit cancel must stay local; got %T: %v", err, err)
	require.True(t, errors.Is(err, context.Canceled), "should be the canceled sentinel; got %T: %v", err, err)
}

// TestBuildHTTPError_DoesNotEchoOrTrustPeerBody proves a non-OK response body is neither
// echoed into the error (no unbounded allocation) NOR allowed to forge the error
// classification: a peer whose body contains "context deadline exceeded" must NOT make the
// HTTP error classify as local — otherwise it would clear its reputation penalty and halt
// catchup failover (a #1174 regression / availability DoS).
func TestBuildHTTPError_DoesNotEchoOrTrustPeerBody(t *testing.T) {
	// A body that both is huge AND contains the context-sentinel poison token.
	body := "context deadline exceeded " + strings.Repeat("A", 1<<20)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	_, err := DoHTTPRequest(context.Background(), server.URL)
	require.Error(t, err)
	require.Less(t, len(err.Error()), maxErrorBodyBytes, "error must not echo the peer body")
	require.NotContains(t, err.Error(), "context deadline exceeded", "peer body must not reach the classified message")
	require.False(t, errors.IsLocalError(err),
		"a peer's HTTP error must never classify as local, even if its body contains context-sentinel text; got %v", err)
}

// TestRetryHTTP_RetryAfterClampedToCeiling proves a Retry-After hint larger than the ceiling
// (maxRetryAfter) is clamped to that ceiling — honored, not discarded to the smaller jittered
// backoff, and not stalling for the hostile 999s. maxDelay is set tiny so the wait pinning at
// ~maxRetryAfter proves the clamp uses the ceiling, not the backoff cap.
func TestRetryHTTP_RetryAfterClampedToCeiling(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "999") // far above the ceiling
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	cfg := retryConfig{maxAttempts: 4, initialDelay: time.Millisecond, maxDelay: 20 * time.Millisecond, maxRetryAfter: 200 * time.Millisecond}
	start := time.Now()
	body, err := doHTTPRequestBodyReaderWithRetry(context.Background(), server.URL, cfg, nil)
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer body.Close()
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "clamped Retry-After should wait ~maxRetryAfter, not the tiny maxDelay")
	require.Less(t, elapsed, 3*time.Second, "must not wait the full 999s Retry-After")
}

// TestRetryHTTP_RetryAfterAboveMaxDelayHonored proves a Retry-After between maxDelay and
// maxRetryAfter is honored in full (waits ~the hint) rather than clamped down to maxDelay, which
// would re-hit an honest-but-busy peer every maxDelay and re-open the #1174 wedge.
func TestRetryHTTP_RetryAfterAboveMaxDelayHonored(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "1") // 1s: above maxDelay, below maxRetryAfter
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	cfg := retryConfig{maxAttempts: 4, initialDelay: time.Millisecond, maxDelay: 50 * time.Millisecond, maxRetryAfter: 5 * time.Second}
	start := time.Now()
	body, err := doHTTPRequestBodyReaderWithRetry(context.Background(), server.URL, cfg, nil)
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer body.Close()
	require.GreaterOrEqual(t, elapsed, time.Second, "a Retry-After above maxDelay must be honored, not clamped to maxDelay")
	require.Less(t, elapsed, 3*time.Second, "but bounded well under a hostile hint")
}

// TestRetryHTTP_CumulativeBackoffBudget proves a peer answering every attempt with a max-ceiling
// Retry-After cannot pin one fetch for maxAttempts*ceiling: the loop stops once the cumulative
// sleep would breach maxBackoffTotal, and reports the real attempt count.
func TestRetryHTTP_CumulativeBackoffBudget(t *testing.T) {
	cfg := retryConfig{maxAttempts: 6, initialDelay: time.Millisecond, maxDelay: 4 * time.Millisecond, maxRetryAfter: 40 * time.Millisecond, maxBackoffTotal: 60 * time.Millisecond}
	var calls int32
	attempt := func(context.Context) (int, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return 0, 40 * time.Millisecond, errors.NewServiceUnavailableError("peer 429")
	}
	start := time.Now()
	_, err := retryHTTP(context.Background(), cfg, attempt)
	elapsed := time.Since(start)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable))
	// Two ~40ms waits fit the 60ms budget for only the first; the loop abandons well before the
	// 6-attempt cap would allow ~200ms of sleeping.
	require.Less(t, elapsed, 150*time.Millisecond, "cumulative backoff must be bounded by maxBackoffTotal")
	require.Less(t, atomic.LoadInt32(&calls), int32(6), "must fail over before exhausting all attempts")
}

// TestRetryHTTP_ZeroBudgetRetainsAttemptOnlyBehavior guards shrunk test configs: with
// maxBackoffTotal == 0 every attempt runs (attempt-count only, no cumulative bound).
func TestRetryHTTP_ZeroBudgetRetainsAttemptOnlyBehavior(t *testing.T) {
	cfg := retryConfig{maxAttempts: 4, initialDelay: time.Millisecond, maxDelay: 2 * time.Millisecond} // maxBackoffTotal defaults to 0
	var calls int32
	attempt := func(context.Context) (int, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return 0, 0, errors.NewServiceUnavailableError("peer 503")
	}
	_, err := retryHTTP(context.Background(), cfg, attempt)
	require.Error(t, err)
	require.Equal(t, int32(4), atomic.LoadInt32(&calls), "all maxAttempts must run when no cumulative budget is set")
}

// TestRetryHTTP_HostileRetryAfterNearOverflowStillBacksOff proves a Retry-After near int64
// overflow is clamped to the ceiling (not wrapped negative → immediate re-fire): the loop still
// sleeps a bounded, positive amount between attempts.
func TestRetryHTTP_HostileRetryAfterNearOverflowStillBacksOff(t *testing.T) {
	cfg := retryConfig{maxAttempts: 3, initialDelay: time.Millisecond, maxDelay: 5 * time.Millisecond, maxRetryAfter: 20 * time.Millisecond, maxBackoffTotal: time.Second}
	attempt := func(context.Context) (int, time.Duration, error) {
		return 0, time.Duration(math.MaxInt64 - 1), errors.NewServiceUnavailableError("hostile 429")
	}
	start := time.Now()
	_, err := retryHTTP(context.Background(), cfg, attempt)
	elapsed := time.Since(start)
	require.Error(t, err)
	// 2 backoff gaps clamped to the 20ms ceiling → ≥ ~40ms; NOT the ~0ms an overflow wrap would give.
	require.GreaterOrEqual(t, elapsed, 30*time.Millisecond, "clamped hostile hint must still back off, not fire immediately")
	require.Less(t, elapsed, 500*time.Millisecond, "and stay bounded by the ceiling")
}

func TestParseRetryAfter_HugeSeconds(t *testing.T) {
	// int64-max seconds: clamped to a positive duration (not wrapped negative).
	require.Positive(t, parseRetryAfter("9223372036854775807"))
	// Atoi overflow → treated as no hint.
	require.Equal(t, time.Duration(0), parseRetryAfter("99999999999999999999"))
	// Existing behavior preserved.
	require.Equal(t, time.Duration(0), parseRetryAfter("-5"))
	require.Equal(t, time.Duration(0), parseRetryAfter("0"))
	require.Equal(t, 3*time.Second, parseRetryAfter("3"))
}

// TestDoHTTPRequestWithRetry_BeforeAttemptFiresEveryAttempt guards the anti-re-burst
// wiring: the per-peer rate-limit hook must run before EVERY attempt (including retries),
// not just the first. A future refactor hoisting the hook out of the per-attempt closure
// would silently revert it and this test would catch it.
func TestDoHTTPRequestWithRetry_BeforeAttemptFiresEveryAttempt(t *testing.T) {
	var attempts, hookCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	hook := func(context.Context) error { atomic.AddInt32(&hookCalls, 1); return nil }
	got, err := doHTTPRequestWithRetry(context.Background(), server.URL, testRetryConfig, hook)
	require.NoError(t, err)
	require.Equal(t, "ok", string(got))
	require.Equal(t, int32(3), atomic.LoadInt32(&attempts))
	require.Equal(t, atomic.LoadInt32(&attempts), atomic.LoadInt32(&hookCalls),
		"the rate-limit hook must fire on every attempt incl. retries")
}

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"0", 0},
		{"-1", 0},
		{"1", time.Second},
		{"30", 30 * time.Second},
		{"not-a-number", 0},
		// HTTP-date format must be treated as "no retry hint", not silently
		// reinterpreted. Same for any non-integer the server might emit.
		{"Wed, 21 Oct 2015 07:28:00 GMT", 0},
		{"1.5", 0}, // time.ParseDuration would have accepted "1.5s"
		{"1m", 0},  // and "1ms", "1h" etc — none are valid delta-seconds
		{" 5", 0},  // surrounding whitespace
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			require.Equal(t, c.want, parseRetryAfter(c.in))
		})
	}
}

func TestRedactPeerURL(t *testing.T) {
	// The F3 forgery vector: a peer path containing "context canceled" must not survive redaction.
	require.Equal(t, "http://evil.example", RedactPeerURL("http://evil.example/context canceled"))
	require.Equal(t, "https://peer:8080", RedactPeerURL("https://peer:8080/subtree/abc?cachebust=1"))
	require.NotContains(t, RedactPeerURL("http://evil.example/context canceled"), "context canceled")
	// Unparseable / hostless inputs collapse to a safe placeholder.
	require.Equal(t, "[redacted url]", RedactPeerURL("/no/scheme/or/host"))
}
