package util

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func DoHTTPRequest(ctx context.Context, url string, requestBody ...[]byte) ([]byte, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Join(errors.New("failed to create http request"), err)
	}

	// write request body
	if len(requestBody) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(requestBody[0]))
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.Join(errors.New("failed to do http request"), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request [%s] returned status code [%d]", url, resp.StatusCode)
	}

	blockBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Join(errors.New("failed to read http response body"), err)
	}

	return blockBytes, nil
}

func DoHTTPRequestBodyReader(ctx context.Context, url string, requestBody ...[]byte) (io.ReadCloser, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Join(errors.New("failed to create http request"), err)
	}

	// write request body
	if len(requestBody) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(requestBody[0]))
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.Join(errors.New("failed to do http request"), err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request [%s] returned status code [%d]", url, resp.StatusCode)
	}

	return resp.Body, nil
}
