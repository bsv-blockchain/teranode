package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
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

// redirectingPair starts a target server that counts its hits and records any Authorization
// header it sees, plus a server that answers every request with a 302 to it. The status is
// 302 deliberately: a 307 would prove nothing here, because this client's request bodies are
// not of a type net/http can replay, so net/http declines to follow a 307 even with no
// redirect policy at all. A 302 is followed by the default client, and because both servers
// are on 127.0.0.1 Go treats them as the same host and does not strip Authorization.
func redirectingPair(t *testing.T) (redirector *httptest.Server, targetHits *atomic.Int64, targetAuth *atomic.Value) {
	t.Helper()

	targetHits = &atomic.Int64{}
	targetAuth = &atomic.Value{}

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		targetAuth.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)

	redirector = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/blob/elsewhere", http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	return redirector, targetHits, targetAuth
}

// TestHTTPStore_RefusesRedirectOnGet covers issue 4841: a blob server does not redirect, so a
// redirect can only be something else answering on that address.
func TestHTTPStore_RefusesRedirectOnGet(t *testing.T) {
	redirector, targetHits, _ := redirectingPair(t)

	storeURL, err := url.Parse(redirector.URL)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	_, err = store.Get(context.Background(), []byte("k"), fileformat.FileTypeTesting)
	require.Error(t, err)
	require.Zero(t, targetHits.Load(), "the redirect target must never be contacted")
}

// TestHTTPStore_RefusesRedirectOnSetAndDoesNotLeakToken is the write-side half: without the
// redirect policy the bearer token would be handed to whatever the redirect names.
func TestHTTPStore_RefusesRedirectOnSetAndDoesNotLeakToken(t *testing.T) {
	redirector, targetHits, targetAuth := redirectingPair(t)

	storeURL, err := url.Parse(redirector.URL)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, options.WithHTTPAuthToken("leak-me-not"))
	require.NoError(t, err)

	err = store.Set(context.Background(), []byte("k"), fileformat.FileTypeTesting, []byte("payload"))
	require.Error(t, err)
	require.Zero(t, targetHits.Load(), "the redirect target must never be contacted")
	require.Nil(t, targetAuth.Load(), "the token must never leave for a destination we did not choose")
}

// TestNew_RejectsTokenInURL pins that a credential cannot be smuggled into a store URL,
// which callers log verbatim.
func TestNew_RejectsTokenInURL(t *testing.T) {
	storeURL, err := url.Parse("http://h:1/?authToken=secret")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL)
	require.Error(t, err)
	require.Nil(t, store)
	require.NotContains(t, err.Error(), "secret", "the error must not echo the credential")
}

// TestNew_StripsQueryFromBaseURL covers the request URL format "%s/blob/%s?%s": a base URL
// carrying a query of its own produced a malformed request URL.
func TestNew_StripsQueryFromBaseURL(t *testing.T) {
	storeURL, err := url.Parse("http://localhost:8080/base?testId=42#frag")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)
	require.Equal(t, "http://localhost:8080/base", store.baseURL)

	parsed, err := url.Parse(fmt.Sprintf(blobURLFormat, store.baseURL, "key.testing", "fileType=testing"))
	require.NoError(t, err)
	require.Equal(t, "/base/blob/key.testing", parsed.Path)
	require.Equal(t, "testing", parsed.Query().Get("fileType"))
}

// TestHTTPStore_SendsTokenOnWritesOnly pins where the credential goes: on the requests that
// change the store, and nowhere else.
func TestHTTPStore_SendsTokenOnWritesOnly(t *testing.T) {
	const token = "write-token"

	var mu sync.Mutex

	seen := map[string]string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Method] = r.Header.Get("Authorization")
		mu.Unlock()

		switch r.Method {
		case http.MethodPost:
			w.WriteHeader(http.StatusCreated)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("test data"))
		}
	}))
	defer server.Close()

	storeURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, options.WithHTTPAuthToken(token))
	require.NoError(t, err)

	key := []byte("k")

	require.NoError(t, store.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("v")))
	require.NoError(t, store.SetDAH(context.Background(), key, fileformat.FileTypeTesting, 1000))
	require.NoError(t, store.Del(context.Background(), key, fileformat.FileTypeTesting))

	_, err = store.Get(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	_, err = store.Exists(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, "Bearer "+token, seen[http.MethodPost])
	require.Equal(t, "Bearer "+token, seen[http.MethodPatch])
	require.Equal(t, "Bearer "+token, seen[http.MethodDelete])
	require.Empty(t, seen[http.MethodGet], "reads must not carry the credential")
	require.Empty(t, seen[http.MethodHead], "reads must not carry the credential")
}
