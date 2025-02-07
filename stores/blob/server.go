// Package blob provides blob storage functionality with various storage backend implementations.
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

type HTTPBlobServer struct {
	store  Store
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

// ServeHTTP handles HTTP requests to the blob server.
// Implements http.Handler interface.
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
		s.handleSetTTL(w, r, opts...)
	case http.MethodDelete:
		s.handleDelete(w, r, opts...)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleHealth processes health check requests.
func (s *HTTPBlobServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	status, msg, err := s.store.Health(r.Context(), false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(status)

	_, _ = w.Write([]byte(msg))
}

// handleExists processes blob existence check requests.
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

// handleGet processes blob retrieval requests.
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

// handleRangeRequest processes partial content requests.
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

// handleSet processes blob storage requests.
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

// handleSetTTL processes TTL setting requests.
func (s *HTTPBlobServer) handleSetTTL(w http.ResponseWriter, r *http.Request, opts ...options.FileOption) {
	key, err := getKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ttlStr := r.URL.Query().Get("ttl")

	ttl, err := time.ParseDuration(ttlStr)
	if err != nil {
		http.Error(w, "Invalid TTL", http.StatusBadRequest)
		return
	}

	err = s.store.SetTTL(r.Context(), key, ttl, opts...)
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

// handleDelete processes blob deletion requests.
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
