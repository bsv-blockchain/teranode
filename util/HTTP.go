package util

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

func DoHTTPRequest(ctx context.Context, url string) ([]byte, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request [%s]", err.Error())
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to do http request [%s]", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request [%s] returned status code [%d]", url, resp.StatusCode)
	}

	blockBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read http response body [%s]", err.Error())
	}

	return blockBytes, nil
}
