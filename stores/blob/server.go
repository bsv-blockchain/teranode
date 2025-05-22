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
// with features specifically designed for blockchain data management.
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

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
)

const NotFoundMsg = "Not found"

// HTTPBlobServer provides an HTTP interface to a blob storage backend.
// It implements the http.Handler interface and exposes blob operations as RESTful endpoints.
// The server supports standard CRUD operations plus specialized features like health checks,
// range requests, and DAH management.
type HTTPBlobServer struct {
	// store is the underlying blob storage implementation
	store  Store
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
// - GET /blob/{key}: Retrieve a blob
// - HEAD /blob/{key}: Check if a blob exists
// - POST /blob/{key}: Store a new blob
// - PATCH /blob/{key}: Update blob's Delete-At-Height value
// - DELETE /blob/{key}: Delete a blob
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

// setCurrentBlockHeight updates the current block height in the underlying store if it supports this operation.
// This is used for DAH (Delete-At-Height) functionality to determine when blobs should be deleted.
//
// Parameters:
//   - height: The current blockchain height to set
//
// Returns:
//   - error: Error if the underlying store doesn't support the SetCurrentBlockHeight operation
func (s *HTTPBlobServer) setCurrentBlockHeight(height uint32) error {
	store, ok := s.store.(interface {
		SetCurrentBlockHeight(height uint32)
	})

	if !ok {
		return errors.NewStorageError("[HTTPBlobServer] store does not support SetCurrentBlockHeight", nil)
	}

	store.SetCurrentBlockHeight(height)

	return nil
}

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
// It checks if a blob exists in the store and returns an appropriate HTTP status code:
// - 200 OK if the blob exists
// - 404 Not Found if the blob doesn't exist
//
// Parameters:
//   - w: HTTP response writer for sending the existence check response
//   - r: HTTP request containing the blob key in the path
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleExists(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	exists, err := s.store.Exists(r.Context(), key, opts...)
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
// The function streams data directly from the store to the HTTP response to minimize
// memory usage when handling large blobs.
//
// Parameters:
//   - w: HTTP response writer for sending the blob data response
//   - r: HTTP request containing the blob key in the path
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleGet(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		s.handleRangeRequest(w, r, key, opts...)
		return
	}

	rc, err := s.store.GetIoReader(r.Context(), key, opts...)
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
// The function parses the Range header, retrieves the requested byte range, and returns
// the data with appropriate Content-Range headers and 206 Partial Content status.
//
// Parameters:
//   - w: HTTP response writer for sending the partial content response
//   - r: HTTP request containing the Range header
//   - key: The blob key to retrieve partial content from
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleRangeRequest(w http.ResponseWriter, r *http.Request, key []byte, opts ...options.FileOption) {
	rangeHeader := r.Header.Get("Range")

	start, end, err := parseRange(rangeHeader)
	if err != nil {
		http.Error(w, "Invalid Range header", http.StatusBadRequest)
		return
	}

	data, err := s.store.GetHead(r.Context(), key, end-start, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			http.Error(w, NotFoundMsg, http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	if start >= len(data) {
		http.Error(w, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if end > len(data) || end == 0 {
		end = len(data)
	}

	rangeData := data[start:end]

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(rangeData)))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(data)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(rangeData)
}

// parseRange parses the Range header from HTTP requests.
// Parameters:
//   - rangeHeader: The Range header value
//
// Returns:
//   - start: Starting byte position
//   - end: Ending byte position
//   - error: Any error that occurred during parsing
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
// Parameters:
//   - w: HTTP response writer for sending the storage operation response
//   - r: HTTP request containing the blob key in the path and content in the body
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleSet(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Use SetFromReader to handle streaming data
	err = s.store.SetFromReader(r.Context(), key, r.Body, opts...)
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
// Parameters:
//   - w: HTTP response writer for sending the DAH update response
//   - r: HTTP request containing the blob key in the path and DAH value in query parameters
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleSetDAH(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dahStr := r.URL.Query().Get("dah")

	dah, err := strconv.Atoi(dahStr)
	if err != nil {
		http.Error(w, "Invalid DAH", http.StatusBadRequest)
		return
	}

	err = s.store.SetDAH(r.Context(), key, uint32(dah), opts...) // nolint: gosec
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
// Parameters:
//   - w: HTTP response writer for sending the deletion response
//   - r: HTTP request containing the blob key in the path
//   - opts: Optional file options derived from the query parameters
func (s *HTTPBlobServer) handleDelete(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.store.Del(r.Context(), key, opts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// getKeyFromPath extracts the blob key from the request path.
// Parameters:
//   - path: The request path
//
// Returns:
//   - []byte: Decoded key
//   - error: Any error that occurred during extraction
func getKeyFromPath(path string) ([]byte, error) {
	// Assuming the path is in the format "/blob/{key}"
	encodedKey := path[6:]

	return base64.URLEncoding.DecodeString(encodedKey)
}
