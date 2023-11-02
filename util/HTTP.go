package util

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func DoHTTPRequest(ctx context.Context, url string) ([]byte, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Join(errors.New("failed to create http request"), err)
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
