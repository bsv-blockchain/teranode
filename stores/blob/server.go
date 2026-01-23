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
//   - POST /blob/{key}.{fileType} - Store a new blob
//   - PATCH /blob/{key}.{fileType} - Update blob's Delete-At-Height value
//   - DELETE /blob/{key}.{fileType} - Delete a blob
//   - GET /health - Health check endpoint
package blob

import (
	"context"
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
}

// NewHTTPBlobServer creates a new HTTP blob server instance.
// Parameters:
//   - logger: Logger instance for server operations
//   - storeURL: URL containing the store configuration
//   - opts: Optional store configuration options
//
// Returns:
//   - *HTTPBlobServer: The configured server instance
//   - error: Any error that occurred during creation
func NewHTTPBlobServer(logger ulogger.Logger, storeURL *url.URL, opts ...options.StoreOption) (*HTTPBlobServer, error) {
	store, err := NewStore(logger, storeURL, opts...)
	if err != nil {
		return nil, err
	}

	return &HTTPBlobServer{
		store:  store,
		logger: logger,
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
// Parameters:
//   - w: HTTP response writer for sending the response
//   - r: HTTP request containing the client's request details
func (s *HTTPBlobServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		s.handleHealth(w, r)
		return
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
// It implements the HTTP/1.1 Range request specification to return only a portion of a blob.
// This is particularly useful for large blobs where the client only needs a specific section,
// such as resumable downloads or media streaming.
//
// The function follows these steps:
// 1. Parse the Range header to determine the requested byte range
// 2. Retrieve the full blob from the store
// 3. Validate the requested range against the actual blob size
// 4. Set appropriate Content-Range and Content-Length headers
// 5. Return the requested portion with 206 Partial Content status
//
// The function handles various edge cases including:
// - Invalid range formats
// - Ranges that exceed the blob size
// - Missing or malformed Range headers
//
// Parameters:
//   - w: HTTP response writer for sending the partial content response
//   - r: HTTP request containing the Range header
//   - key: The blob key to retrieve partial content from
//   - fileType: The type of the blob
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleRangeRequest(w http.ResponseWriter, r *http.Request, key []byte, fileType fileformat.FileType, opts ...options.FileOption) {
	rangeHeader := r.Header.Get("Range")

	start, end, err := parseRange(rangeHeader)
	if err != nil {
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

	// seek the reader to the start position
	if start > 0 {
		if seeker, ok := dataReader.(io.Seeker); ok {
			_, err = seeker.Seek(int64(start), io.SeekStart)
			if err != nil {
				http.Error(w, "Failed to seek in blob", http.StatusInternalServerError)
				return
			}
		} else {
			http.Error(w, "Store does not support seeking", http.StatusInternalServerError)
			return
		}
	}

	// read the requested range
	data := make([]byte, end-start)

	_, err = io.ReadFull(dataReader, data)
	if err != nil && err != io.EOF {
		http.Error(w, "Failed to read blob data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(data)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(data)
}

// parseRange parses the Range header from HTTP requests according to RFC 7233.
// It extracts the start and end byte positions from a Range header value in the format
// "bytes=start-end". This function is used to support partial content requests for large blobs.
//
// The function handles various range formats:
// - "bytes=0-499" - First 500 bytes
// - "bytes=500-999" - Second 500 bytes
// - "bytes=-500" - Last 500 bytes (converted to absolute positions internally)
// - "bytes=500-" - All bytes from position 500 to the end
//
// Parameters:
//   - rangeHeader: The Range header value from the HTTP request
//
// Returns:
//   - start: Starting byte position (inclusive)
//   - end: Ending byte position (inclusive)
//   - error: Any error that occurred during parsing, such as invalid format
func parseRange(rangeHeader string) (int, int, error) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, errors.NewInvalidArgumentError("invalid range header format")
	}

	rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")

	rangeParts := strings.Split(rangeStr, "-")
	if len(rangeParts) != 2 {
		return 0, 0, errors.NewInvalidArgumentError("invalid range header format")
	}

	start, err := strconv.Atoi(rangeParts[0])
	if err != nil {
		return 0, 0, errors.NewInvalidArgumentError("invalid start range")
	}

	var end int
	if rangeParts[1] != "" {
		end, err = strconv.Atoi(rangeParts[1])
		if err != nil {
			return 0, 0, errors.NewInvalidArgumentError("invalid end range")
		}
	}

	return start, end + 1, nil
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

	// Use SetFromReader to handle streaming data
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
	// Assuming the path is in the format "/blob/{key}.{fileType}"
	pos := strings.LastIndex(path, ".")
	if pos == -1 {
		return nil, "", errors.NewInvalidArgumentError("invalid path format")
	}

	ext := path[pos+1:]

	fileType, err := fileformat.FileTypeFromExtension(ext)
	if err != nil {
		return nil, "", errors.NewInvalidArgumentError("invalid file type", err)
	}

	encodedKey := path[6:pos]

	key, err := base64.URLEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, "", errors.NewInvalidArgumentError("invalid key format")
	}

	return key, fileType, nil
}
