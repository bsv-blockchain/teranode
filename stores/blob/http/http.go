package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
)

const (
	blobURLFormat        = "%s/blob/%s?%s"
	blobURLFormatWithDAH = blobURLFormat + "&dah=%d"
	blobURLFormatGetDAH  = blobURLFormat + "&getDAH=1"
)

type HTTPStore struct {
	baseURL    string
	httpClient *http.Client
	logger     ulogger.Logger
	options    *options.Options
}

func New(logger ulogger.Logger, storeURL *url.URL, opts ...options.StoreOption) (*HTTPStore, error) {
	logger = logger.New("http")

	if storeURL == nil {
		return nil, errors.NewConfigurationError("storeURL is nil")
	}

	options := options.NewStoreOptions(opts...)

	return &HTTPStore{
		baseURL:    storeURL.String(),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		logger:     logger,
		options:    options,
	}, nil
}

func (s *HTTPStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	resp, err := s.httpClient.Get(fmt.Sprintf("%s/health", s.baseURL))
	if err != nil {
		return http.StatusServiceUnavailable, "HTTP Store: Service Unavailable", errors.NewStorageError("[HTTPStore] Health check failed", err)
	}
	defer resp.Body.Close()

	return resp.StatusCode, "HTTP Store", nil
}

func (s *HTTPStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode())

	resp, err := s.httpClient.Head(url)
	if err != nil {
		return false, errors.NewStorageError("[HTTPStore] Exists check failed", err)
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}

func (s *HTTPStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	rc, err := s.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		return nil, errors.NewStorageError("[HTTPStore] Get failed", err)
	}
	defer rc.Close()

	return io.ReadAll(rc)
}

func (s *HTTPStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode())

	resp, err := s.httpClient.Get(url)
	if err != nil {
		return nil, errors.NewStorageError("[HTTPStore] GetIoReader failed", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, errors.ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, errors.NewStorageError(fmt.Sprintf("[HTTPStore] GetIoReader failed with status code %d", resp.StatusCode), nil)
	}

	return resp.Body, nil
}

func (s *HTTPStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	rc := io.NopCloser(bytes.NewReader(value))
	defer rc.Close()

	return s.SetFromReader(ctx, key, fileType, rc, opts...)
}

func (s *HTTPStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, value io.ReadCloser, opts ...options.FileOption) error {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode())

	req, err := http.NewRequestWithContext(ctx, "POST", url, value)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetFromReader failed to create request", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetFromReader failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return errors.NewStorageError(fmt.Sprintf("[HTTPStore] SetFromReader failed with status code %d", resp.StatusCode), nil)
	}

	return nil
}

func (s *HTTPStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormatWithDAH, s.baseURL, encodedKey, query.Encode(), dah)

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, nil)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetTTL failed to create request", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetTTL failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.NewStorageError(fmt.Sprintf("[HTTPStore] SetTTL failed with status code %d", resp.StatusCode), nil)
	}

	return nil
}

func (s *HTTPStore) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormatGetDAH, s.baseURL, encodedKey, query.Encode())

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, nil)
	if err != nil {
		return 0, errors.NewStorageError("[HTTPStore] GetTTL failed to create request", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, errors.NewStorageError("[HTTPStore] GetTTL failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return 0, errors.ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return 0, errors.NewStorageError(fmt.Sprintf("[HTTPStore] GetDAH failed with status code %d", resp.StatusCode), nil)
	}

	dah, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.NewStorageError("[HTTPStore] GetDAH failed to read response body", err)
	}

	// Parse the DAH from the response body, the dah will be a string representation of an int
	dahInt, err := strconv.Atoi(string(dah))
	if err != nil {
		return 0, errors.NewStorageError("[HTTPStore] GetDAH failed to parse response body int", err)
	}

	// nolint: gosec
	return uint32(dahInt), nil
}

func (s *HTTPStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode())

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] Del failed to create request", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] Del failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return errors.NewStorageError(fmt.Sprintf("[HTTPStore] Del failed with status code %d", resp.StatusCode), nil)
	}

	return nil
}

func (s *HTTPStore) Close(ctx context.Context) error {
	// No need to close anything for HTTP client
	return nil
}
