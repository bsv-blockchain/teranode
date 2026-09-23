// Package http provides an HTTP client implementation of the blob.Store interface.
// It allows Teranode components to interact with remote blob stores via HTTP requests,
// enabling distributed storage architectures where blob data can be stored and retrieved
// from separate services or nodes.
//
// The HTTP blob store communicates with a remote server that implements the blob store
// HTTP API (typically an HTTPBlobServer). It supports all standard blob operations including
// Get, Set, Exists, Delete, and Delete-At-Height (DAH) functionality.
//
// This implementation is particularly useful for:
// - Cross-service blob access within a Teranode deployment
// - Accessing blob stores on remote nodes
// - Creating redundant or distributed blob storage architectures
//
// All operations are performed via standard HTTP methods with appropriate status codes
// and error handling to maintain compatibility with the blob.Store interface contract.
package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
)

const (
	blobURLFormat        = "%s/blob/%s?%s"
	blobURLFormatWithDAH = blobURLFormat + "&dah=%d"
)

// HTTPStore implements the blob.Store interface by making HTTP requests to a remote
// blob storage server. It translates blob operations into appropriate HTTP requests
// and handles response parsing, error handling, and connection management.
type HTTPStore struct {
	// baseURL is the base URL of the remote blob server (e.g., "http://localhost:8080")
	baseURL string
	// authToken is the shared secret presented on mutating requests. Empty means none is sent.
	authToken string
	// httpClient is the HTTP client used for making requests with configurable timeout
	httpClient *http.Client
	// logger provides structured logging for HTTP operations and errors
	logger ulogger.Logger
	// options contains configuration options for the HTTP blob store
	options *options.Options
}

// refuseBlobRedirect refuses every redirect. A blob server does not redirect, so a redirect
// can only be something else answering on that address - and this client carries a bearer
// token and, for writes, a body. Neither should be handed to a destination we did not choose.
func refuseBlobRedirect(req *http.Request, _ []*http.Request) error {
	return errors.NewInvalidArgumentError("blob http store: refusing to follow a redirect to %s", req.URL.Redacted())
}

// New creates a new HTTP blob store client that connects to a remote blob server.
//
// The HTTP blob store translates blob operations into HTTP requests to the specified
// server URL. It handles serialization, error handling, and connection management to
// provide a seamless blob.Store interface implementation.
//
// Parameters:
//   - logger: Logger for recording operations and errors
//   - storeURL: URL of the remote blob server (e.g., "http://localhost:8080")
//   - opts: Optional store configuration options
//
// The shared secret the remote server requires on POST, PATCH and DELETE comes from
// options.WithHTTPAuthToken - even an empty one - or, when that option is not given at all,
// from the blob_httpAuthToken setting read in the process's own settings context. Stores the
// daemon builds pass Settings.BlobHTTPAuthToken, so they resolve per context; a caller that
// builds its settings with an alternative context must pass the option itself.
// It must never be placed in storeURL: store URLs are logged verbatim.
//
// Returns:
//   - *HTTPStore: Configured HTTP blob store client
//   - error: Configuration error if storeURL is nil or carries a token
func New(logger ulogger.Logger, storeURL *url.URL, opts ...options.StoreOption) (*HTTPStore, error) {
	logger = logger.New("http")

	if storeURL == nil {
		return nil, errors.NewConfigurationError("storeURL is nil")
	}

	storeOpts := options.NewStoreOptions(opts...)

	authToken := storeOpts.HTTPAuthToken
	if !storeOpts.HTTPAuthTokenSet {
		// Fallback for the standalone tools that build a store without passing the option. It
		// reads the process context only, which is the context those tools build settings with.
		authToken, _ = gocore.Config().Get("blob_httpAuthToken", "")
	}

	// A token in the URL would be logged: store URLs are printed verbatim by callers. Refuse
	// rather than silently accept a credential in a place that leaks.
	if storeURL.Query().Get("authToken") != "" {
		return nil, errors.NewConfigurationError("blob http store URL must not carry an authToken query parameter - set blob_httpAuthToken or pass options.WithHTTPAuthToken")
	}

	// baseURL is formatted into "%s/blob/%s?%s", so it must carry no query of its own -
	// otherwise every request URL comes out malformed. Strip query and fragment.
	base := *storeURL
	base.RawQuery = ""
	base.ForceQuery = false
	base.Fragment = ""
	base.RawFragment = ""

	return &HTTPStore{
		baseURL:   strings.TrimSuffix(base.String(), "/"),
		authToken: authToken,
		httpClient: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: refuseBlobRedirect,
		},
		logger:  logger,
		options: storeOpts,
	}, nil
}

// Health checks the health status of the remote blob server.
// It makes an HTTP GET request to the /health endpoint of the remote server
// and returns the status code, message, and any error encountered.
//
// Parameters:
//   - ctx: Context for the health check operation
//   - checkLiveness: Whether to perform a more thorough liveness check (passed as query parameter)
//
// Returns:
//   - int: HTTP status code indicating health status
//   - string: Description of the health status
//   - error: Any error that occurred during the health check
func (s *HTTPStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	resp, err := s.httpClient.Get(fmt.Sprintf("%s/health", s.baseURL))
	if err != nil {
		return http.StatusServiceUnavailable, "HTTP Store: Service Unavailable", errors.NewStorageError("[HTTPStore] Health check failed", err)
	}
	defer resp.Body.Close()

	return resp.StatusCode, "HTTP Store", nil
}

// Exists checks if a blob exists in the remote blob store.
// It makes an HTTP HEAD request to the blob endpoint with the specified key and file type.
// The existence is determined by the HTTP status code of the response.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - bool: True if the blob exists, false otherwise
//   - error: Any error that occurred during the operation
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

// Get retrieves a blob from the remote blob store.
// It makes an HTTP GET request to the blob endpoint with the specified key and file type,
// and returns the blob data as a byte slice.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - []byte: The blob data
//   - error: Any error that occurred during the operation
func (s *HTTPStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	rc, err := s.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		return nil, errors.NewStorageError("[HTTPStore] Get failed", err)
	}
	defer rc.Close()

	return io.ReadAll(rc)
}

// GetIoReader retrieves a blob from the remote blob store as a streaming reader.
// It makes an HTTP GET request to the blob endpoint with the specified key and file type,
// and returns an io.ReadCloser for streaming the blob data. This is more memory-efficient
// than Get for large blobs as it doesn't require loading the entire blob into memory.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - io.ReadCloser: Reader for streaming the blob data
//   - error: Any error that occurred during the operation
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

// Set stores a blob in the remote blob store.
// It makes an HTTP POST request to the blob endpoint with the specified key, file type, and blob data.
// The blob data is sent in the request body.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - value: The blob data to store
//   - opts: Optional file options
//
// Overwrite is not available over the HTTP blob API: a call with WithAllowOverwrite(true) fails
// with a configuration error before anything is sent, and a blob that already exists is
// reported as ErrBlobAlreadyExists. See SetFromReader.
//
// Returns:
//   - error: Any error that occurred during the operation
func (s *HTTPStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	rc := io.NopCloser(bytes.NewReader(value))
	defer rc.Close()

	return s.SetFromReader(ctx, key, fileType, rc, opts...)
}

// SetFromReader stores a blob in the remote blob store from a streaming reader.
// It makes an HTTP POST request to the blob endpoint with the specified key and file type,
// streaming the blob data from the provided reader. This is more memory-efficient than Set
// for large blobs as it doesn't require loading the entire blob into memory.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - value: Reader providing the blob data
//   - opts: Optional file options
//
// The server does not take an overwrite request from the caller: whether an existing blob may be
// replaced is the receiving store's policy. Rather than drop WithAllowOverwrite(true) silently -
// which lets the first write succeed and refuses every later one - this returns a configuration
// error before anything is sent. A 409 from the server is returned as ErrBlobAlreadyExists.
//
// Returns:
//   - error: Any error that occurred during the operation
func (s *HTTPStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, value io.ReadCloser, opts ...options.FileOption) error {
	if options.NewFileOptions(opts...).AllowOverwrite {
		return errors.NewConfigurationError("[HTTPStore] overwrite is not available over the HTTP blob API: whether an existing blob may be replaced is the receiving store's policy")
	}

	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	// NOTE: Any WithDeleteAt(dah) in opts is serialized as the "dah" query param for
	// diagnostics only. The receiving node does NOT use the sender's DAH — it applies its
	// own retention policy via its local BlockHeightRetention setting. See QueryToFileOptions.
	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode())

	req, err := http.NewRequestWithContext(ctx, "POST", url, value)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetFromReader failed to create request", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	if s.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.authToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetFromReader failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return errors.NewBlobAlreadyExistsError("[HTTPStore] SetFromReader: blob already exists")
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return errors.NewStorageError(fmt.Sprintf("[HTTPStore] SetFromReader failed with status code %d", resp.StatusCode), nil)
	}

	return nil
}

// SetDAH sets the Delete-At-Height (DAH) value for a blob in the remote blob store.
// It makes an HTTP PATCH request to the blob endpoint with the specified key, file type, and DAH value.
// The DAH value determines at which blockchain height the blob will be automatically deleted.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - dah: The delete at height value
//   - opts: Optional file options
//
// Returns:
//   - error: Any error that occurred during the operation
func (s *HTTPStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormatWithDAH, s.baseURL, encodedKey, query.Encode(), dah)

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, nil)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetTTL failed to create request", err)
	}

	if s.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.authToken)
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

// Del deletes a blob from the remote blob store.
// This operation is idempotent - deleting a non-existent blob (404) is treated as success.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob to delete
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - error: Any error that occurred during deletion
func (s *HTTPStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode())

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] Del failed to create request", err)
	}

	if s.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.authToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] Del failed", err)
	}
	defer resp.Body.Close()

	// Treat 404 Not Found as success (idempotent deletion)
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return errors.NewStorageError(fmt.Sprintf("[HTTPStore] Del failed with status code %d", resp.StatusCode), nil)
	}

	return nil
}

// Close performs any necessary cleanup for the HTTP blob store.
// In the current implementation, this is a no-op as HTTP connections are managed by the HTTP client.
//
// Parameters:
//   - ctx: Context for the operation
//
// Returns:
//   - error: Always returns nil
func (s *HTTPStore) Close(ctx context.Context) error {
	// No need to close anything for HTTP client
	return nil
}

// SetCurrentBlockHeight is a no-op in the HTTP blob store implementation.
// The remote blob server is responsible for tracking the current block height.
//
// Parameters:
//   - height: The current block height (ignored)
func (s *HTTPStore) SetCurrentBlockHeight(_ uint32) {
	// noop
}
