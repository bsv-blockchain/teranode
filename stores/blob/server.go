// Package blob provides a comprehensive blob storage system with multiple backend implementations.
// The blob package is designed to store, retrieve, and manage arbitrary binary data (blobs) with
// features such as customizable storage backends, Delete-At-Height (DAH) functionality for automatic
// data expiration, and a standardized HTTP API.
//
// Key features:
// - Multiple storage backends (memory, file, S3, HTTP, etc.) behind a common interface
// - HTTP API for interacting with blob stores over the network
// - Batching capabilities for efficient bulk operations
// - Delete-At-Height (DAH) support for blockchain-based data expiration
// - Range-based content retrieval for partial data access
// - Streaming data access through io.Reader interfaces
//
// This package integrates with the broader Teranode system to provide reliable data storage
// with features specifically designed for blockchain data management. The HTTP server component
// provides a RESTful API that follows standard HTTP conventions:
//   - GET /blob/{key}.{fileType} - Retrieve a blob
//   - HEAD /blob/{key}.{fileType} - Check if a blob exists
//   - POST /blob/{key}.{fileType} - Store a new blob (requires Authorization: Bearer)
//   - PATCH /blob/{key}.{fileType} - Update blob's Delete-At-Height value (requires Authorization: Bearer)
//   - DELETE /blob/{key}.{fileType} - Delete a blob (requires Authorization: Bearer)
//   - GET /health - Health check endpoint
package blob

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const NotFoundMsg = "Not found"

// HTTPBlobServer provides an HTTP interface to a blob storage backend.
// It implements the http.Handler interface and exposes blob operations as RESTful endpoints.
// The server supports standard CRUD operations plus specialized features like health checks,
// range requests, and DAH management.
//
// The server follows RESTful principles and uses standard HTTP methods for operations:
// - GET: Retrieve blobs with support for HTTP Range headers for partial content
// - HEAD: Check blob existence without retrieving content
// - POST: Store new blobs with streaming support for large data
// - PATCH: Update blob metadata (specifically DAH values)
// - DELETE: Remove blobs from storage
//
// The server automatically handles content negotiation, status codes, and error responses
// according to HTTP standards. It's designed to be deployed as part of a microservice
// architecture where other services can interact with blob storage over HTTP.
type HTTPBlobServer struct {
	// store is the underlying blob storage implementation
	store Store
	// logger provides structured logging for server operations
	logger ulogger.Logger
	// authToken is the shared secret a caller must present to mutate the store. Empty
	// means this server is read-only: POST, PATCH and DELETE are refused outright.
	authToken string
}

// NewHTTPBlobServer creates a new HTTP blob server instance.
// Parameters:
//   - logger: Logger instance for server operations
//   - storeURL: URL containing the store configuration
//   - authToken: Shared secret callers must present to POST, PATCH or DELETE a blob.
//     Empty makes the server read-only.
//   - opts: Optional store configuration options
//
// Returns:
//   - *HTTPBlobServer: The configured server instance
//   - error: Any error that occurred during creation
func NewHTTPBlobServer(logger ulogger.Logger, storeURL *url.URL, authToken string, opts ...options.StoreOption) (*HTTPBlobServer, error) {
	store, err := NewStore(logger, storeURL, opts...)
	if err != nil {
		return nil, err
	}

	return &HTTPBlobServer{
		store:     store,
		logger:    logger,
		authToken: authToken,
	}, nil
}

// Start begins serving HTTP requests on the specified address.
// Parameters:
//   - ctx: Context for server lifecycle
//   - addr: Address to listen on
//
// Returns:
//   - error: Any error that occurred during server startup
func (s *HTTPBlobServer) Start(ctx context.Context, addr string) error {
	s.logger.Infof("Starting HTTP blob server on %s", addr)

	srv := &http.Server{
		Addr:         addr,
		Handler:      s,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		s.logger.Infof("Shutting down HTTP blob server")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			s.logger.Errorf("HTTP blob server shutdown error: %v", err)
		}
	}()

	return srv.ListenAndServe()
}

// ServeHTTP handles HTTP requests to the blob server, implementing the http.Handler interface.
// It routes requests to the appropriate handler function based on the HTTP method and path.
//
// The server supports the following endpoints:
// - GET /health: Health check endpoint
// - GET /blob/{key}.{fileType}: Retrieve a blob
// - HEAD /blob/{key}.{fileType}: Check if a blob exists
// - POST /blob/{key}.{fileType}: Store a new blob
// - PATCH /blob/{key}.{fileType}: Update blob's Delete-At-Height value
// - DELETE /blob/{key}.{fileType}: Delete a blob
//
// POST, PATCH and DELETE change the store, so they require an authenticated caller.
//
// Parameters:
//   - w: HTTP response writer for sending the response
//   - r: HTTP request containing the client's request details
func (s *HTTPBlobServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		s.handleHealth(w, r)
		return
	}

	switch r.Method {
	case http.MethodPost, http.MethodPatch, http.MethodDelete:
		if !s.authorizeMutation(r) {
			// The path is quoted: it is percent-decoded and caller-controlled, so it can carry a newline.
			s.logger.Warnf("[HTTPBlobServer] refused unauthenticated %s %q from %s", r.Method, r.URL.Path, r.RemoteAddr)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}
	}

	opts := options.QueryToFileOptions(r.URL.Query())

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, opts...)
	case http.MethodHead:
		s.handleExists(w, r, opts...)
	case http.MethodPost:
		s.handleSet(w, r, opts...)
	case http.MethodPatch:
		s.handleSetDAH(w, r, opts...)
	case http.MethodDelete:
		s.handleDelete(w, r, opts...)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// authorizeMutation reports whether a request may change the store. With no token
// configured the server is read-only - a blob server reachable on the network with no
// shared secret must not accept writes at all. Comparison is constant-time.
//
// The token is presented in the clear: this endpoint is plain HTTP. It stops a caller
// that cannot see our traffic, which is the case this guards against; it is not a
// defence against anything on the path. Keep the listener on loopback or a trusted segment.
func (s *HTTPBlobServer) authorizeMutation(r *http.Request) bool {
	if s.authToken == "" {
		return false
	}

	const prefix = "Bearer "

	// The auth-scheme is case-insensitive (RFC 7235); the token itself is not.
	header := r.Header.Get("Authorization")
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return false
	}

	presented := []byte(header[len(prefix):])
	expected := []byte(s.authToken)

	return len(presented) == len(expected) && subtle.ConstantTimeCompare(presented, expected) == 1
}

// setCurrentBlockHeight removed - DAH cleanup now handled by pruner service

// handleHealth processes health check requests to verify the blob store's operational status.
// It queries the underlying store's health status and returns the appropriate HTTP response.
//
// Parameters:
//   - w: HTTP response writer for sending the health status response
//   - r: HTTP request containing the health check request details
func (s *HTTPBlobServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	status, msg, err := s.store.Health(r.Context(), false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(status)

	_, _ = w.Write([]byte(msg))
}

// handleExists processes blob existence check requests (HTTP HEAD).
// It checks if a blob exists in the store without retrieving the actual content,
// making it an efficient way to verify blob availability. The method extracts the
// blob key and file type from the request path and queries the underlying store.
//
// The function returns appropriate HTTP status codes based on the result:
// - 200 OK if the blob exists
// - 404 Not Found if the blob doesn't exist
// - 500 Internal Server Error if an error occurs during the check
//
// Parameters:
//   - w: HTTP response writer for sending the existence check response
//   - r: HTTP request containing the blob key in the path
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleExists(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, fileType, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	exists, err := s.store.Exists(r.Context(), key, fileType, opts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if exists {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// handleGet processes blob retrieval requests (HTTP GET).
// It supports both full blob retrieval and partial retrieval via Range headers.
// For Range requests, it delegates to handleRangeRequest for specialized handling.
//
// The function follows these steps:
// 1. Extract the blob key and file type from the request path
// 2. Check for Range headers and delegate to handleRangeRequest if present
// 3. For full retrievals, stream the blob directly from the store to the HTTP response
// 4. Set appropriate Content-Type headers based on the file type
//
// The function handles errors by returning appropriate HTTP status codes:
// - 200 OK for successful retrievals
// - 404 Not Found if the blob doesn't exist
// - 500 Internal Server Error for other errors
//
// For large blobs, the function uses streaming to minimize memory usage by not
// loading the entire blob into memory at once.
//
// The function streams data directly from the store to the HTTP response to minimize
// memory usage when handling large blobs.
//
// Parameters:
//   - w: HTTP response writer for sending the blob data response
//   - r: HTTP request containing the blob key in the path
//   - fileType: The file type of the blob
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleGet(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, fileType, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		s.handleRangeRequest(w, r, key, fileType, opts...)
		return
	}

	rc, err := s.store.GetIoReader(r.Context(), key, fileType, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			http.Error(w, NotFoundMsg, http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}
	defer rc.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// handleRangeRequest processes partial content requests using HTTP Range headers.
// It serves a single byte range of a blob with 206 Partial Content.
//
// The function follows these steps:
// 1. Parse the Range header strictly (parseByteRange)
// 2. Open the blob and, when the reader is seekable, determine its payload size
// 3. Resolve the range against that size before reading anything
// 4. Set Content-Range and Content-Length
// 5. Stream exactly the resolved span to the client
//
// A reader that cannot seek has no size, so the handler cannot promise which bytes it
// will send. It ignores the Range header, as RFC 9110 section 14.2 allows, and streams
// the whole blob with 200 like a plain GET.
//
// The response body is streamed with io.CopyN, so memory use does not depend on the
// requested span. Allocating a buffer sized from the header let a tiny request make the
// server allocate gigabytes for a one-byte blob (bitcoin-sv/teranode issue 4853).
//
// Status codes:
//   - 400 Bad Request for a malformed Range header
//   - 416 Range Not Satisfiable for multiple ranges, a reversed range, a zero-length
//     suffix, or a first byte at or past the end of the blob (with "bytes */size" when
//     the size is known)
//   - 200 with the whole blob when the store cannot seek
//
// Parameters:
//   - w: HTTP response writer for sending the partial content response
//   - r: HTTP request containing the Range header
//   - key: The blob key to retrieve partial content from
//   - fileType: The type of the blob
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleRangeRequest(w http.ResponseWriter, r *http.Request, key []byte, fileType fileformat.FileType, opts ...options.FileOption) {
	br, status := parseByteRange(r.Header.Get("Range"))
	switch status {
	case http.StatusOK:
	case http.StatusRequestedRangeNotSatisfiable:
		http.Error(w, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	default:
		http.Error(w, "Invalid Range header", http.StatusBadRequest)
		return
	}

	dataReader, err := s.store.GetIoReader(r.Context(), key, fileType, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			http.Error(w, NotFoundMsg, http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}
	// Close on every return path - the underlying file-store reader holds
	// a read permit. Previously this handler leaked the permit on success
	// AND on all four mid-function error returns (seek failure, store does
	// not support seeking, Read failure, and the fall-through after Write).
	// handleGet just above has the same defer rc.Close() pattern.
	defer dataReader.Close()

	// Range requests address payload bytes, not raw file bytes. The file
	// blob store's GetIoReader has already consumed any fileformat magic
	// header from the reader, so the current position is the start of the
	// payload (offset 8 for header-bearing files, 0 otherwise). We anchor
	// all subsequent Seeks against this post-header position, otherwise:
	//   - Seek(first, SeekStart) for first=2 would seek to byte 2 of the
	//     raw file, which is INSIDE the magic header, and Read would return
	//     header bytes instead of payload.
	//   - SeekEnd would report the raw file length (payload + header)
	//     rather than the payload total the client cares about.
	seeker, isSeeker := dataReader.(io.Seeker)
	if !isSeeker {
		// Without a size, a 206 would have to advertise a Content-Range before knowing
		// whether the blob holds those bytes. Ignore the range and serve the whole blob.
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)

		if n, err := io.Copy(w, dataReader); err != nil {
			s.logger.Errorf("[BlobServer] range request fallback read failed after %d bytes for key %x: %v", n, key, err)
		}

		return
	}

	payloadStart, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		http.Error(w, "Failed to read blob position", http.StatusInternalServerError)
		return
	}

	rawEnd, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		http.Error(w, "Failed to determine blob size", http.StatusInternalServerError)
		return
	}

	size := rawEnd - payloadStart

	first, last, ok := br.resolve(size)
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(w, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)

		return
	}

	if _, err = seeker.Seek(payloadStart+first, io.SeekStart); err != nil {
		http.Error(w, "Failed to seek in blob", http.StatusInternalServerError)
		return
	}

	span := last - first + 1

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(span, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, size))
	w.WriteHeader(http.StatusPartialContent)

	// The headers are already sent, so a short copy cannot change the status. The client
	// detects it against Content-Length; log it so the operator sees the store fault too.
	if n, err := io.CopyN(w, dataReader, span); err != nil {
		s.logger.Errorf("[BlobServer] range read failed after %d/%d bytes for key %x: %v", n, span, key, err)
	}
}

// byteRange is one parsed "bytes=" range spec, not yet resolved against a blob size.
type byteRange struct {
	first     int64 // first byte position; unused for a suffix range
	last      int64 // last byte position, inclusive; unused when openEnded or suffix
	suffixLen int64 // number of trailing bytes for a suffix range ("bytes=-N")
	openEnded bool  // "bytes=N-"
	suffix    bool  // "bytes=-N"
}

// resolve converts the range into absolute, inclusive [first, last] positions within a
// payload of size bytes, clamping an over-long last position to the end. ok is false when
// the range cannot be satisfied: the first position is at or past the end, or a suffix
// range asks for zero bytes. Every comparison happens before any addition, so no
// attacker-chosen value can overflow.
func (br byteRange) resolve(size int64) (first, last int64, ok bool) {
	if size <= 0 {
		return 0, 0, false
	}

	switch {
	case br.suffix:
		if br.suffixLen == 0 {
			return 0, 0, false
		}

		first = 0
		if br.suffixLen < size {
			first = size - br.suffixLen
		}

		return first, size - 1, true
	case br.first >= size:
		return 0, 0, false
	case br.openEnded || br.last >= size:
		return br.first, size - 1, true
	default:
		return br.first, br.last, true
	}
}

// parseByteRange parses a Range header value holding a single RFC 7233 byte range:
//   - "bytes=0-499"  - bytes 0 through 499
//   - "bytes=500-"   - byte 500 to the end
//   - "bytes=-500"   - the last 500 bytes
//
// Positions must be plain decimal digits that fit in an int64; signs, whitespace and
// overflowing values are malformed.
//
// Parameters:
//   - rangeHeader: The Range header value from the HTTP request
//
// Returns:
//   - byteRange: the parsed range, to be resolved against the blob size
//   - int: http.StatusOK when parsed, http.StatusRequestedRangeNotSatisfiable for
//     multiple ranges or a reversed range (last < first), http.StatusBadRequest for a
//     malformed header
func parseByteRange(rangeHeader string) (byteRange, int) {
	spec, found := strings.CutPrefix(rangeHeader, "bytes=")
	if !found {
		return byteRange{}, http.StatusBadRequest
	}

	if strings.Contains(spec, ",") {
		return byteRange{}, http.StatusRequestedRangeNotSatisfiable
	}

	firstStr, lastStr, found := strings.Cut(spec, "-")
	if !found {
		return byteRange{}, http.StatusBadRequest
	}

	if firstStr == "" {
		n, ok := parseRangePosition(lastStr)
		if !ok {
			return byteRange{}, http.StatusBadRequest
		}

		return byteRange{suffix: true, suffixLen: n}, http.StatusOK
	}

	first, ok := parseRangePosition(firstStr)
	if !ok {
		return byteRange{}, http.StatusBadRequest
	}

	if lastStr == "" {
		return byteRange{first: first, openEnded: true}, http.StatusOK
	}

	last, ok := parseRangePosition(lastStr)
	if !ok {
		return byteRange{}, http.StatusBadRequest
	}

	if last < first {
		return byteRange{}, http.StatusRequestedRangeNotSatisfiable
	}

	return byteRange{first: first, last: last}, http.StatusOK
}

// parseRangePosition parses one byte position: decimal digits only, fitting in an int64.
func parseRangePosition(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}

	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}

	n, err := strconv.ParseInt(s, 10, 64)

	return n, err == nil
}

// handleSet processes blob storage requests (HTTP POST).
// It reads the request body as the blob content and stores it in the blob store
// using the key extracted from the URL path. The function uses streaming via
// SetFromReader to efficiently handle large blob uploads without excessive memory usage.
//
// The function follows these steps:
// 1. Extract the blob key and file type from the request path
// 2. Stream the request body directly to the underlying store
// 3. Return appropriate HTTP status codes based on the result
//
// The function handles errors by returning appropriate HTTP status codes:
// - 201 Created for successful storage operations
// - 400 Bad Request if the key cannot be extracted from the path
// - 500 Internal Server Error for storage failures
//
// Parameters:
//   - w: HTTP response writer for sending the storage operation response
//   - r: HTTP request containing the blob key in the path and content in the body
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleSet(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, fileType, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// The opts from QueryToFileOptions intentionally do not include DAH from the sender.
	// Each teranode applies its own DAH via its local file store's BlockHeightRetention
	// setting in constructFilename(). A peer's DAH is irrelevant to our retention policy.
	err = s.store.SetFromReader(r.Context(), key, fileType, r.Body, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			http.Error(w, "Blob already exists", http.StatusConflict)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	w.WriteHeader(http.StatusCreated)
}

// handleSetDAH processes Delete-At-Height (DAH) setting requests (HTTP PATCH).
// It updates the DAH value for an existing blob, which determines when the blob
// will be automatically deleted based on blockchain height. The DAH value is provided
// as a query parameter in the request URL.
//
// The function follows these steps:
// 1. Extract the blob key and file type from the request path
// 2. Parse the DAH value from the 'dah' query parameter
// 3. Update the DAH value in the underlying store
// 4. Return appropriate HTTP status codes based on the result
//
// The function handles errors by returning appropriate HTTP status codes:
// - 204 No Content for successful DAH updates
// - 400 Bad Request if the key cannot be extracted or the DAH value is invalid
// - 404 Not Found if the blob doesn't exist
// - 500 Internal Server Error for other failures
//
// Parameters:
//   - w: HTTP response writer for sending the DAH update response
//   - r: HTTP request containing the blob key in the path and DAH value in query parameters
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleSetDAH(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, fileType, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dahStr := r.URL.Query().Get("dah")

	dah, err := strconv.ParseUint(dahStr, 10, 32)
	if err != nil {
		http.Error(w, "Invalid DAH", http.StatusBadRequest)
		return
	}

	err = s.store.SetDAH(r.Context(), key, fileType, uint32(dah), opts...)
	if err != nil {
		if err == errors.ErrNotFound {
			http.Error(w, NotFoundMsg, http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleDelete processes blob deletion requests (HTTP DELETE).
// It permanently removes a blob from the store based on the key in the URL path.
// Upon successful deletion, it returns HTTP 204 No Content status.
//
// The function follows these steps:
// 1. Extract the blob key and file type from the request path
// 2. Delete the blob from the underlying store
// 3. Return appropriate HTTP status codes based on the result
//
// The function handles errors by returning appropriate HTTP status codes:
// - 204 No Content for successful deletions
// - 400 Bad Request if the key cannot be extracted from the path
// - 404 Not Found if the blob doesn't exist
// - 500 Internal Server Error for other failures
//
// Parameters:
//   - w: HTTP response writer for sending the deletion response
//   - r: HTTP request containing the blob key in the path
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleDelete(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, fileType, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.store.Del(r.Context(), key, fileType, opts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// getKeyFromPath extracts the blob key and file type from the request path.
// It parses paths in the format "/blob/{key}.{fileType}" where {key} is a base64-encoded
// blob key and {fileType} is a string representation of the file type.
//
// The function performs these steps:
// 1. Validate the path starts with "/blob/"
// 2. Extract the key and file type portions from the path
// 3. Decode the base64-encoded key
// 4. Convert the file type string to a fileformat.FileType enum
//
// Parameters:
//   - path: The HTTP request path to parse
//
// Returns:
//   - []byte: Decoded binary key that identifies the blob
//   - fileType: Enumerated file type value from the fileformat package
//   - error: Any error that occurred during extraction, such as invalid path format,
//     invalid base64 encoding, or unrecognized file type
func getKeyFromPath(path string) ([]byte, fileformat.FileType, error) {
	const blobPathPrefix = "/blob/"

	// The path is caller-controlled: a dot before the end of the prefix (e.g. "/.tx") would
	// otherwise slice out of range and panic the handler.
	pos := strings.LastIndex(path, ".")
	if !strings.HasPrefix(path, blobPathPrefix) || pos < len(blobPathPrefix) {
		return nil, "", errors.NewInvalidArgumentError("invalid path format")
	}

	ext := path[pos+1:]

	fileType, err := fileformat.FileTypeFromExtension(ext)
	if err != nil {
		return nil, "", errors.NewInvalidArgumentError("invalid file type", err)
	}

	encodedKey := path[len(blobPathPrefix):pos]

	key, err := base64.URLEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, "", errors.NewInvalidArgumentError("invalid key format")
	}

	return key, fileType, nil
}
