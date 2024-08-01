package util

import (
	"bytes"
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"io"
	"net/http"
	"time"

	"github.com/ordishs/gocore"
)

var (
	httpRequestTimeout, _ = gocore.Config().GetInt("http_timeout", 60)
)

func DoHTTPRequest(ctx context.Context, url string, requestBody ...[]byte) ([]byte, error) {
	bodyReaderCloser, cancelFn, err := doHTTPRequest(ctx, url, requestBody...)
	defer cancelFn()

	if err != nil {
		return nil, err
	}

	defer bodyReaderCloser.Close()

	blockBytes, err := io.ReadAll(bodyReaderCloser)
	if err != nil {
		return nil, errors.NewServiceError("http request [%s] failed to read body", url, err)
	}

	return blockBytes, nil
}

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

	httpClient := &http.Client{}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, cancelFn, errors.NewServiceError("failed to create http request", err)
	}

	// If there is a request body assume we want a POST and write request body
	if len(requestBody) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(requestBody[0]))
		req.Method = http.MethodPost
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, cancelFn, errors.NewServiceError("failed to do http request", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		if resp.Body != nil {
			defer resp.Body.Close()

			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, cancelFn, errors.NewServiceError("http request [%s] returned status code [%d]", url, resp.StatusCode, err)
			}

			if b != nil {
				return nil, cancelFn, errors.NewServiceError("http request [%s] returned status code [%d] with body [%s]", url, resp.StatusCode, string(b))
			}
		}
		return nil, cancelFn, errors.NewServiceError("http request [%s] returned status code [%d]", url, resp.StatusCode)
	}

	return resp.Body, cancelFn, nil
}
