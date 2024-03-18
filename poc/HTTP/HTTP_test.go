package HTTP

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/bitcoin-sv/ubsv/util"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const addr = "localhost:7666"

func TestHTTPDownloadWithCancelledContext(t *testing.T) {

	// ---------------------------- Server ----------------------------
	e := echo.New()
	e.HideBanner = true

	// Create a slice with random bytes in it
	binaryData := make([]byte, 10*1024)
	_, err := rand.Read(binaryData)
	require.NoError(t, err)

	// Handler to serve the binary file
	e.GET("/file", func(c echo.Context) error {
		return c.Blob(http.StatusOK, "application/octet-stream", binaryData)
	})

	// Start echo server in a new goroutine
	go func() {
		if err := e.Start(addr); err != nil && err != http.ErrServerClosed {
			t.Logf("shutting down the server: %v", err)
		}
	}()

	defer func() {
		err := e.Shutdown(context.Background())
		require.NoError(t, err)
	}()

	// ---------------------------- Client ----------------------------

	ctx, cancel := context.WithCancel(context.Background())

	// Get the file
	resp, err := get(ctx)
	require.NoError(t, err)

	assert.Equal(t, resp.StatusCode, http.StatusOK)

	time.Sleep(100 * time.Millisecond)

	// Read the response body before we cancel the context.  This should work.
	data, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	require.NoError(t, err)

	assert.Equal(t, binaryData, data)

	// Get the file again
	resp, err = get(ctx)
	require.NoError(t, err)

	assert.Equal(t, resp.StatusCode, http.StatusOK)

	// Cancel the context
	cancel()

	time.Sleep(100 * time.Millisecond)

	defer resp.Body.Close()

	// Read the response body after we cancel the context.  This should fail.
	_, err = io.ReadAll(resp.Body)
	require.ErrorIs(t, err, context.Canceled)
}

func get(ctx context.Context) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/file", addr), nil)
	if err != nil {
		return &http.Response{}, err
	}

	httpClient := &http.Client{}

	return httpClient.Do(req)
}

func TestHTTPGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := util.DoHTTPRequest(ctx, "http://www.google.com")
	require.NoError(t, err)
}

func TestHTTPGetReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body, err := util.DoHTTPRequestBodyReader(ctx, "http://www.google.com")
	require.NoError(t, err)

	defer body.Close()

	_, err = io.ReadAll(body)
	require.NoError(t, err)
}
