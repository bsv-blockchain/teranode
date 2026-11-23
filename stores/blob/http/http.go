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
	"github.com/bsv-blockchain/teranode/util"
	"github.com/ordishs/gocore"
)

const (
	blobURLFormat        = "%s/blob/%s?%s"
	blobURLFormatWithDAH = blobURLFormat + "&dah=%d"

	// bearerScheme prefixes the shared secret in the Authorization header of every mutating request.
	bearerScheme = "Bearer "

	// NotFoundMsg is the body the blob server answers a miss with, through http.Error, so it
	// arrives with a trailing newline. Every version of the server has sent it. A 404 with any
	// other body comes from something else answering on that address.
	NotFoundMsg = "Not found"

	// notFoundBodyLimit bounds how much of a 404 body is read to compare it with NotFoundMsg.
	// It is longer than the body it looks for, so a longer body cannot be cut down to a match.
	notFoundBodyLimit = 64
)

// HTTPStore implements the blob.Store interface by making HTTP requests to a remote
// blob storage server. It translates blob operations into appropriate HTTP requests
// and handles response parsing, error handling, and connection management.
type HTTPStore struct {
	// baseURL is the base URL of the remote blob server (e.g., "http://localhost:8080")
	baseURL string
	// redactedBaseURL is baseURL with any password masked, for the Warn line logged with a
	// fault. Error text never names the server: the asset service answers a transaction request
	// with it, a 404 for a miss and a 500 for a fault.
	redactedBaseURL string
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

// isBlobServerMiss reports whether a 404 came from the blob server answering a miss, which it
// does with NotFoundMsg. Anything else answering 404 on that address - a wrong base URL, a proxy -
// is not a verdict on the blob. It reads from the body, so the caller closes it afterwards.
func isBlobServerMiss(resp *http.Response) bool {
	body, err := io.ReadAll(io.LimitReader(resp.Body, notFoundBodyLimit))
	if err != nil {
		return false
	}

	return strings.TrimSuffix(string(body), "\n") == NotFoundMsg
}

// errUnauthorized reports a 401 from the blob server. It sends one only when this client and
// the server do not share a token - none configured on the server, or none or a different one
// here - so it is a configuration error, never a transient failure a caller should retry.
func errUnauthorized(op string) error {
	return errors.NewConfigurationError("[HTTPStore] %s refused with status code 401: blob_httpAuthToken must match the blob server's blockpersister_httpAuthToken", op)
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
		// Trimmed as Settings.BlobHTTPAuthToken is, so both sides agree on a token read from a file.
		authToken, _ = gocore.Config().Get("blob_httpAuthToken", "")
		authToken = strings.TrimSpace(authToken)
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
		baseURL:         strings.TrimSuffix(base.String(), "/"),
		redactedBaseURL: strings.TrimSuffix(base.Redacted(), "/"),
		authToken:       authToken,
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/health", s.baseURL), nil) // urlsafe: builds the request URL sent to the blob server, not a log line; the credentials in baseURL are the point
	if err != nil {
		return http.StatusServiceUnavailable, "HTTP Store: Service Unavailable", errors.NewStorageError("[HTTPStore] Health check failed to create request", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return http.StatusServiceUnavailable, "HTTP Store: Service Unavailable", errors.NewStorageError("[HTTPStore] Health check failed", err)
	}
	defer resp.Body.Close()

	return resp.StatusCode, "HTTP Store", nil
}

// Exists checks if a blob exists in the remote blob store.
// It makes an HTTP HEAD request to the blob endpoint with the specified key and file type.
// The existence is determined by the HTTP status code of the response: 200 is held, 404 is not
// held, and any other status is a storage error. A HEAD response carries no body, so unlike a
// read, a 404 here cannot be checked against NotFoundMsg.
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
	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode()) // urlsafe: builds the request URL sent to the blob server, not a log line; the credentials in baseURL are the point

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, errors.NewStorageError("[HTTPStore] Exists failed to create request", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return false, errors.NewStorageError("[HTTPStore] Exists check failed", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		s.logger.Warnf("[HTTPStore] Exists %s %s: %s answered status code %d", fileType, util.ReverseAndHexEncodeSlice(key), s.redactedBaseURL, resp.StatusCode) // urlsafe: redactedBaseURL is base.Redacted() output, set in New
		return false, errors.NewStorageError("[HTTPStore] Exists failed with status code %d", resp.StatusCode)
	}
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
//   - error: a not-found if the blob server has no such blob, a storage error for any other failure
func (s *HTTPStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	rc, err := s.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		// A miss is returned as it is, as GetIoReader and the other blob stores return
		// it. Wrapped in a storage error it reads as the store failing to look, and
		// callers that tell the two apart by ErrStorageError treat it as a fault.
		if errors.Is(err, errors.ErrNotFound) {
			return nil, err
		}

		return nil, errors.NewStorageError("[HTTPStore] Get failed", err)
	}
	defer rc.Close()

	value, err := io.ReadAll(rc)
	if err != nil {
		s.logger.Warnf("[HTTPStore] Get %s %s: reading the body from %s failed: %v", fileType, util.ReverseAndHexEncodeSlice(key), s.redactedBaseURL, err) // urlsafe: redactedBaseURL is base.Redacted() output, set in New
		return nil, errors.NewStorageError("[HTTPStore] Get failed to read %s %s", fileType, util.ReverseAndHexEncodeSlice(key), err)
	}

	return value, nil
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
// Only a 404 carrying the blob server's NotFoundMsg is a miss, returned as a not-found naming
// the blob. Any other 404 comes from something else on that address, so it is
// a storage error, as is every other failure. Neither names the server; a fault logs it at
// Warn instead.
//
// Returns:
//   - io.ReadCloser: Reader for streaming the blob data
//   - error: a not-found if the blob server has no such blob, a storage error for any other failure
func (s *HTTPStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	query := options.FileOptionsToQuery(fileType, opts...)
	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode()) // urlsafe: builds the request URL sent to the blob server, not a log line; the credentials in baseURL are the point

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.NewStorageError("[HTTPStore] GetIoReader failed to create request", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, errors.NewStorageError("[HTTPStore] GetIoReader failed", err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Body, nil
	case http.StatusNotFound:
		miss := isBlobServerMiss(resp)
		resp.Body.Close()

		if miss {
			return nil, errors.NewNotFoundError("[HTTPStore] %s %s not found", fileType, util.ReverseAndHexEncodeSlice(key))
		}

		s.logger.Warnf("[HTTPStore] GetIoReader %s %s: %s answered 404 without the blob server's not-found body", fileType, util.ReverseAndHexEncodeSlice(key), s.redactedBaseURL) // urlsafe: redactedBaseURL is base.Redacted() output, set in New

		return nil, errors.NewStorageError("[HTTPStore] GetIoReader failed with status code 404 without the blob server's not-found body")
	default:
		resp.Body.Close()
		s.logger.Warnf("[HTTPStore] GetIoReader %s %s: %s answered status code %d", fileType, util.ReverseAndHexEncodeSlice(key), s.redactedBaseURL, resp.StatusCode) // urlsafe: redactedBaseURL is base.Redacted() output, set in New

		return nil, errors.NewStorageError("[HTTPStore] GetIoReader failed with status code %d", resp.StatusCode)
	}
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
// WithAllowOverwrite(true) asks the server to replace an existing blob, and the server honours
// it only for an authenticated caller. Without it, a 409 is returned as ErrBlobAlreadyExists;
// a 401 is returned as a configuration error. See SetFromReader.
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
// WithAllowOverwrite(true) asks the server to replace an existing blob, and the server honours
// it only for an authenticated caller. Without it, a 409 from the server is returned as
// ErrBlobAlreadyExists. A 401 - this client and the server do not share a token - is returned
// as a configuration error.
//
// Returns:
//   - error: Any error that occurred during the operation
func (s *HTTPStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, value io.ReadCloser, opts ...options.FileOption) error {
	encodedKey := base64.URLEncoding.EncodeToString(key) + "." + fileType.String()

	// NOTE: Any WithDeleteAt(dah) in opts is serialized as the "dah" query param for
	// diagnostics only. The receiving node does NOT use the sender's DAH — it applies its
	// own retention policy via its local BlockHeightRetention setting. See QueryToFileOptions.
	query := options.FileOptionsToQuery(fileType, opts...)
	if options.NewFileOptions(opts...).AllowOverwrite {
		// Only a POST carries it, and the server honours it only for an authenticated caller.
		query.Set(options.AllowOverwriteQueryParam, "true")
	}

	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode()) // urlsafe: builds the request URL sent to the blob server, not a log line; the credentials in baseURL are the point

	req, err := http.NewRequestWithContext(ctx, "POST", url, value)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetFromReader failed to create request", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	if s.authToken != "" {
		req.Header.Set("Authorization", bearerScheme+s.authToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetFromReader failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return errors.NewBlobAlreadyExistsError("[HTTPStore] SetFromReader: blob already exists")
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return errUnauthorized("SetFromReader")
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return errors.NewStorageError("[HTTPStore] SetFromReader failed with status code %d", resp.StatusCode)
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
	url := fmt.Sprintf(blobURLFormatWithDAH, s.baseURL, encodedKey, query.Encode(), dah) // urlsafe: builds the request URL sent to the blob server, not a log line; the credentials in baseURL are the point

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, nil)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetTTL failed to create request", err)
	}

	if s.authToken != "" {
		req.Header.Set("Authorization", bearerScheme+s.authToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] SetTTL failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return errUnauthorized("SetDAH")
	}

	if resp.StatusCode != http.StatusOK {
		return errors.NewStorageError("[HTTPStore] SetTTL failed with status code %d", resp.StatusCode)
	}

	return nil
}

// Del deletes a blob from the remote blob store.
// This operation is idempotent - deleting a non-existent blob is success. The blob server
// answers that with 204; a 404 counts as success only when it carries the server's NotFoundMsg,
// since any other 404 is something else on that address and the blob may still be there.
// A 401 - this client and the server do not share a token - is returned as a configuration
// error, not a storage error: retrying cannot fix it.
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
	url := fmt.Sprintf(blobURLFormat, s.baseURL, encodedKey, query.Encode()) // urlsafe: builds the request URL sent to the blob server, not a log line; the credentials in baseURL are the point

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] Del failed to create request", err)
	}

	if s.authToken != "" {
		req.Header.Set("Authorization", bearerScheme+s.authToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return errors.NewStorageError("[HTTPStore] Del failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		if isBlobServerMiss(resp) {
			return nil
		}

		s.logger.Warnf("[HTTPStore] Del %s %s: %s answered 404 without the blob server's not-found body", fileType, util.ReverseAndHexEncodeSlice(key), s.redactedBaseURL) // urlsafe: redactedBaseURL is base.Redacted() output, set in New

		return errors.NewStorageError("[HTTPStore] Del failed with status code 404 without the blob server's not-found body")
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return errUnauthorized("Del")
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return errors.NewStorageError("[HTTPStore] Del failed with status code %d", resp.StatusCode)
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
