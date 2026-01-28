package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
)

func setupTestServer() (*httptest.Server, *HTTPStore, error) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			if r.URL.Path == "/health" {
				w.WriteHeader(http.StatusOK)
				return
			}

			// Handle range requests for GetHead
			if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
				w.WriteHeader(http.StatusPartialContent)

				if _, err := w.Write([]byte("partial")); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				return
			}

			w.WriteHeader(http.StatusOK)

			if _, err := w.Write([]byte("test data")); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

		case "HEAD":
			w.WriteHeader(http.StatusOK)

		case "POST":
			w.WriteHeader(http.StatusCreated)

		case "PATCH":
			// Handle GetTTL request
			if r.URL.Query().Get("getDAH") == "1" {
				w.WriteHeader(http.StatusOK)

				if _, err := w.Write([]byte("300")); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				return
			}

			w.WriteHeader(http.StatusOK)

		case "DELETE":
			w.WriteHeader(http.StatusNoContent)
		}
	}))

	// Parse server URL
	storeURL, err := url.Parse(server.URL)
	if err != nil {
		return nil, nil, err
	}

	// Create store
	store, err := New(ulogger.TestLogger{}, storeURL)
	if err != nil {
		return nil, nil, err
	}

	return server, store, nil
}

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		storeURL  *url.URL
		wantError bool
	}{
		{
			name:      "nil URL",
			storeURL:  nil,
			wantError: true,
		},
		{
			name: "valid URL",
			storeURL: func() *url.URL {
				u, _ := url.Parse("http://localhost:8080")
				return u
			}(),
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := New(ulogger.TestLogger{}, tt.storeURL)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				if store == nil {
					t.Error("expected store, got nil")
				}
			}
		})
	}
}

func TestHealth(t *testing.T) {
	server, store, err := setupTestServer()
	if err != nil {
		t.Fatalf("failed to setup test server: %v", err)
	}
	defer server.Close()

	status, msg, err := store.Health(context.Background(), true)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if status != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, status)
	}

	if msg != "HTTP Store" {
		t.Errorf("expected message 'HTTP Store', got %s", msg)
	}
}

func TestExists(t *testing.T) {
	server, store, err := setupTestServer()
	if err != nil {
		t.Fatalf("failed to setup test server: %v", err)
	}
	defer server.Close()

	exists, err := store.Exists(context.Background(), []byte("test-key"), fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !exists {
		t.Error("expected exists to be true")
	}
}

func TestGet(t *testing.T) {
	server, store, err := setupTestServer()
	if err != nil {
		t.Fatalf("failed to setup test server: %v", err)
	}
	defer server.Close()

	data, err := store.Get(context.Background(), []byte("test-key"), fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if string(data) != "test data" {
		t.Errorf("expected 'test data', got %s", string(data))
	}
}

func TestSet(t *testing.T) {
	server, store, err := setupTestServer()
	if err != nil {
		t.Fatalf("failed to setup test server: %v", err)
	}
	defer server.Close()

	err = store.Set(context.Background(), []byte("test-key"), fileformat.FileTypeTesting, []byte("test data"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSetTTL(t *testing.T) {
	server, store, err := setupTestServer()
	if err != nil {
		t.Fatalf("failed to setup test server: %v", err)
	}
	defer server.Close()

	err = store.SetDAH(context.Background(), []byte("test-key"), fileformat.FileTypeTesting, 100)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGetTTL(t *testing.T) {
	// GetDAH has been removed from the blob.Store interface
	// DAH functionality is now centralized in the pruner service
	t.Skip("GetDAH removed from interface - see e2e pruner tests")
}

func TestDel(t *testing.T) {
	server, store, err := setupTestServer()
	if err != nil {
		t.Fatalf("failed to setup test server: %v", err)
	}
	defer server.Close()

	err = store.Del(context.Background(), []byte("test-key"), fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestClose(t *testing.T) {
	server, store, err := setupTestServer()
	if err != nil {
		t.Fatalf("failed to setup test server: %v", err)
	}
	defer server.Close()

	err = store.Close(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
