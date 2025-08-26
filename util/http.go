package util

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/ordishs/gocore"
)

var (
	// httpRequestTimeout defines the default HTTP request timeout in seconds
	// when no deadline is set on the context.
	httpRequestTimeout, _ = gocore.Config().GetInt("http_timeout", 60)
)

// DoHTTPRequest performs an HTTP GET or POST request and returns the response body as bytes.
// Uses GET by default, switches to POST if requestBody is provided.
// Automatically handles timeouts and validates response status codes.
func DoHTTPRequest(ctx context.Context, url string, requestBody ...[]byte) ([]byte, error) {
	bodyReaderCloser, cancelFn, err := doHTTPRequest(ctx, url, requestBody...)
	defer cancelFn()

	if err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := bodyReaderCloser.Close(); closeErr != nil {
			// Log the error but don't override the main return value
		}
	}()

	// Read body with context deadline support
	// Create a channel to handle the read operation
	done := make(chan struct{})
	var blockBytes []byte
	var readErr error

	go func() {
		blockBytes, readErr = io.ReadAll(bodyReaderCloser)
		close(done)
	}()

	// Wait for either read completion or context timeout
	select {
	case <-ctx.Done():
		return nil, errors.NewNetworkTimeoutError("http request [%s] timed out while reading body", url)
	case <-done:
		if readErr != nil {
			return nil, errors.NewServiceError("http request [%s] failed to read body", url, readErr)
		}
		return blockBytes, nil
	}
}

// DoHTTPRequestBodyReader performs an HTTP request and returns the response body as a ReadCloser.
// This is more memory-efficient for large responses as it streams the data.
// Caller is responsible for closing the returned ReadCloser.
func DoHTTPRequestBodyReader(ctx context.Context, url string, requestBody ...[]byte) (io.ReadCloser, error) {
	bodyReaderCloser, cancelFn, err := doHTTPRequest(ctx, url, requestBody...)
	if err != nil {
		cancelFn()
		return nil, err
	}

	return bodyReaderCloser, nil
}

func doHTTPRequest(ctx context.Context, url string, requestBody ...[]byte) (io.ReadCloser, context.CancelFunc, error) {
	cancelFn := func() {
		// noop
	}

	if _, ok := ctx.Deadline(); !ok {
		ctx, cancelFn = context.WithTimeout(ctx, time.Duration(httpRequestTimeout)*time.Second)
	}

	httpClient := http.DefaultClient

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, cancelFn, errors.NewServiceError("failed to create http request", err)
	}

	// If there is a request body assume we want a POST and write request body
	if len(requestBody) > 0 && requestBody[0] != nil {
		req.Body = io.NopCloser(bytes.NewReader(requestBody[0]))
		req.Method = http.MethodPost
		req.Header.Set("Content-Type", "application/json")
	}

	var resp *http.Response
	resp, err = httpClient.Do(req)
	if err != nil {
		return nil, cancelFn, errors.NewServiceError("failed to do http request", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		errFn := errors.NewServiceError
		if resp.StatusCode == http.StatusNotFound {
			errFn = errors.NewNotFoundError
		}

		if resp.Body != nil {
			defer func() {
				if bodyCloseErr := resp.Body.Close(); bodyCloseErr != nil {
					// Log the error but don't override the main return value
				}
			}()

			b, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return nil, cancelFn, errFn("http request [%s] returned status code [%d]", url, resp.StatusCode, readErr)
			}

			if b != nil {
				return nil, cancelFn, errFn("http request [%s] returned status code [%d] with body [%s]", url, resp.StatusCode, string(b))
			}
		}

		return nil, cancelFn, errFn("http request [%s] returned status code [%d]", url, resp.StatusCode)
	}

	isHTML := resp.Header.Get("content-type") == "text/html"
	if isHTML {
		return nil, cancelFn, errors.NewServiceError("http request [%s] returned HTML - assume bad URL", url)
	}

	return resp.Body, cancelFn, nil
}
