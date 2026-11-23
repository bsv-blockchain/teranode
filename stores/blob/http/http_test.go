package http

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
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

// TestHTTPStore_AllowOverwriteSentOnPostOnly pins where the overwrite flag goes on the wire: on
// a POST that asked for it, and nowhere else. The server honours it only for an authenticated
// caller.
func TestHTTPStore_AllowOverwriteSentOnPostOnly(t *testing.T) {
	var mu sync.Mutex

	seen := map[string]string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Method] = r.URL.Query().Get(options.AllowOverwriteQueryParam)
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

	seenFor := func(method string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()

		value, ok := seen[method]
		delete(seen, method)

		return value, ok
	}

	storeURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, options.WithHTTPAuthToken("t"))
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("k")
	overwrite := options.WithAllowOverwrite(true)

	require.NoError(t, store.Set(ctx, key, fileformat.FileTypeTesting, []byte("v"), overwrite))

	value, ok := seenFor(http.MethodPost)
	require.True(t, ok)
	require.Equal(t, "true", value, "a POST that asks for overwrite must carry the flag")

	require.NoError(t, store.Set(ctx, key, fileformat.FileTypeTesting, []byte("v")))

	value, ok = seenFor(http.MethodPost)
	require.True(t, ok)
	require.Empty(t, value, "a POST that does not ask for overwrite must not carry the flag")

	_, err = store.Get(ctx, key, fileformat.FileTypeTesting, overwrite)
	require.NoError(t, err)

	_, err = store.Exists(ctx, key, fileformat.FileTypeTesting, overwrite)
	require.NoError(t, err)

	require.NoError(t, store.SetDAH(ctx, key, fileformat.FileTypeTesting, 1000, overwrite))
	require.NoError(t, store.Del(ctx, key, fileformat.FileTypeTesting, overwrite))

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPatch, http.MethodDelete} {
		value, ok := seenFor(method)
		require.True(t, ok, "%s must have reached the server", method)
		require.Empty(t, value, "%s must never carry the overwrite flag", method)
	}
}

// TestHTTPStore_UnauthorizedIsConfigurationError pins that a 401 is classifiable: it means this
// client and the server do not share a token, which no retry can fix. The token never appears in
// the error.
func TestHTTPStore_UnauthorizedIsConfigurationError(t *testing.T) {
	const token = "secret-value"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	storeURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, options.WithHTTPAuthToken(token))
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("k")

	calls := map[string]func() error{
		"Set":    func() error { return store.Set(ctx, key, fileformat.FileTypeTesting, []byte("v")) },
		"SetDAH": func() error { return store.SetDAH(ctx, key, fileformat.FileTypeTesting, 1000) },
		"Del":    func() error { return store.Del(ctx, key, fileformat.FileTypeTesting) },
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			require.ErrorIs(t, err, errors.ErrConfiguration)
			require.NotContains(t, err.Error(), token, "the error must not echo the credential")
		})
	}
}

// TestHTTPStore_ConflictIsBlobAlreadyExists pins that a 409 is classifiable by callers.
func TestHTTPStore_ConflictIsBlobAlreadyExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	storeURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, options.WithHTTPAuthToken("t"))
	require.NoError(t, err)

	err = store.Set(context.Background(), []byte("k"), fileformat.FileTypeTesting, []byte("v"))
	require.ErrorIs(t, err, errors.ErrBlobAlreadyExists)
}

// newStoreFor returns a client for server with no auth token, so the blob_httpAuthToken
// fallback cannot leak into the test.
func newStoreFor(t *testing.T, server *httptest.Server) *HTTPStore {
	t.Helper()

	storeURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, options.WithHTTPAuthToken(""))
	require.NoError(t, err)

	return store
}

// answerMiss answers the way the blob server answers a miss.
func answerMiss(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, NotFoundMsg, http.StatusNotFound)
}

// TestHTTPStore_GetMissIsNotFound pins that a read reports a miss the way the other blob
// stores do - a not-found with no storage error in the chain - and only for a 404 the blob
// server sent. Callers that tell a miss from a store fault by ErrStorageError - the asset tx
// 404 marker, for one - would otherwise read every miss behind an http:// store as a fault,
// and a 404 from a wrong base URL or a proxy as a miss.
func TestHTTPStore_GetMissIsNotFound(t *testing.T) {
	key := []byte{0x01, 0x02, 0xab}

	reads := map[string]func(*HTTPStore) error{
		"Get": func(s *HTTPStore) error {
			_, err := s.Get(context.Background(), key, fileformat.FileTypeTx)
			return err
		},
		"GetIoReader": func(s *HTTPStore) error {
			rc, err := s.GetIoReader(context.Background(), key, fileformat.FileTypeTx)
			if rc != nil {
				_ = rc.Close()
			}

			return err
		},
	}

	for name, tc := range map[string]struct {
		handler http.HandlerFunc
		miss    bool
	}{
		"the blob server's 404 is a miss":     {answerMiss, true},
		"a 404 with no body is a store fault": {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }, false},
		"a mux's 404 is a store fault":        {http.NotFound, false},
		"a 404 that only starts with the server's body is a store fault": {func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(NotFoundMsg + "\nfrom a proxy"))
		}, false},
		"a 500 is a store fault": {func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", http.StatusInternalServerError) }, false},
	} {
		for op, read := range reads {
			t.Run(name+"/"+op, func(t *testing.T) {
				server := httptest.NewServer(tc.handler)
				t.Cleanup(server.Close)

				err := read(newStoreFor(t, server))
				require.Error(t, err)

				if tc.miss {
					require.ErrorIs(t, err, errors.ErrNotFound)
					require.NotErrorIs(t, err, errors.ErrStorageError)
					require.NotSame(t, errors.ErrNotFound, err, "the miss names what was missing")
					require.Contains(t, err.Error(), "ab0201", "the key, as the other stores print it")
					require.Contains(t, err.Error(), fileformat.FileTypeTx.String())
					require.NotContains(t, err.Error(), server.Listener.Addr().String(), "a miss does not name the blob server")
					require.NotContains(t, err.Error(), "EXTRA")
				} else {
					require.ErrorIs(t, err, errors.ErrStorageError)
					require.NotErrorIs(t, err, errors.ErrNotFound)
				}
			})
		}
	}
}

const storePassword = "s3cret-password"

// logRecorder is the testing.T a ulogger.UnifiedTestLogger writes to. It keeps every line, so a
// test can read what was logged.
type logRecorder struct {
	t     *testing.T
	mu    sync.Mutex
	lines []string
}

func (r *logRecorder) Errorf(format string, args ...interface{}) { r.t.Errorf(format, args...) }
func (r *logRecorder) FailNow()                                  { r.t.FailNow() }

func (r *logRecorder) Logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.lines = append(r.lines, fmt.Sprintf(format, args...))
}

// warnings returns the lines logged at Warn.
func (r *logRecorder) warnings() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var warnings []string

	for _, line := range r.lines {
		if strings.Contains(line, " WARN: ") {
			warnings = append(warnings, line)
		}
	}

	return warnings
}

// newStoreWithPassword returns a client for a server running handler, through a store URL that
// carries a password, with the server's host and what the client logs.
func newStoreWithPassword(t *testing.T, handler http.HandlerFunc) (*HTTPStore, string, *logRecorder) {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	storeURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	storeURL.User = url.UserPassword("blob", storePassword)

	recorder := &logRecorder{t: t}
	logger := ulogger.NewUnifiedTestLogger(recorder, t.Name(), "test")

	store, err := New(logger, storeURL, options.WithHTTPAuthToken(""))
	require.NoError(t, err)

	return store, storeURL.Host, recorder
}

// TestHTTPStore_AddressGoesToTheLogNotTheError pins where the blob server's address goes. Not
// into an error: the asset service answers a transaction request with the error text, a 404
// for a miss and a 500 for a fault, so the address - and any password in the store URL - would
// reach whoever asked. The operator still needs it to tell which server misbehaved, so every
// fault logs it at Warn with the password masked. A miss is routine and logs nothing.
func TestHTTPStore_AddressGoesToTheLogNotTheError(t *testing.T) {
	ctx := context.Background()
	key := []byte("k")

	get := func(s *HTTPStore) error {
		_, err := s.Get(ctx, key, fileformat.FileTypeTx)
		return err
	}
	getIoReader := func(s *HTTPStore) error {
		rc, err := s.GetIoReader(ctx, key, fileformat.FileTypeTx)
		if rc != nil {
			_ = rc.Close()
		}

		return err
	}
	answer := func(status int) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }
	}

	for name, tc := range map[string]struct {
		handler http.HandlerFunc
		call    func(*HTTPStore) error
		fault   bool
	}{
		"a miss":                  {answerMiss, get, false},
		"a foreign 404 on Get":    {answer(http.StatusNotFound), get, true},
		"a foreign 404 on reader": {answer(http.StatusNotFound), getIoReader, true},
		"a 500 on Get":            {answer(http.StatusInternalServerError), get, true},
		"a 500 on reader":         {answer(http.StatusInternalServerError), getIoReader, true},
		"a body cut short on Get": {truncatedBody, get, true},
		"a foreign 404 on Del":    {answer(http.StatusNotFound), func(s *HTTPStore) error { return s.Del(ctx, key, fileformat.FileTypeTx) }, true},
		"a 500 on Exists":         {answer(http.StatusInternalServerError), func(s *HTTPStore) error { _, err := s.Exists(ctx, key, fileformat.FileTypeTx); return err }, true},
	} {
		t.Run(name, func(t *testing.T) {
			store, host, recorder := newStoreWithPassword(t, tc.handler)

			err := tc.call(store)
			require.Error(t, err)
			require.NotContains(t, err.Error(), host, "the error text reaches API responses")
			require.NotContains(t, err.Error(), storePassword)

			warnings := recorder.warnings()

			if !tc.fault {
				require.ErrorIs(t, err, errors.ErrNotFound)
				require.Empty(t, warnings, "a miss is routine, not worth a warning")

				return
			}

			require.ErrorIs(t, err, errors.ErrStorageError)
			require.Len(t, warnings, 1)
			require.Contains(t, warnings[0], host, "the operator needs to know which server misbehaved")
			require.NotContains(t, warnings[0], storePassword)
		})
	}
}

// truncatedBody declares more bytes than it sends, so the client's body read fails.
func truncatedBody(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Length", "100")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("short"))
}

// TestHTTPStore_GetTruncatedBodyIsStorageError pins that Get reports a body cut short as a
// storage error, as its doc says, not as the bare transport error.
func TestHTTPStore_GetTruncatedBodyIsStorageError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(truncatedBody))
	t.Cleanup(server.Close)

	_, err := newStoreFor(t, server).Get(context.Background(), []byte("k"), fileformat.FileTypeTx)
	require.ErrorIs(t, err, errors.ErrStorageError)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NotErrorIs(t, err, errors.ErrNotFound)
	require.NotContains(t, err.Error(), "EXTRA")
}

// TestHTTPStore_ReadsHonourContext pins that cancelling the caller's context stops a request in
// flight. Without it a read runs until the client's own 30s timeout, whatever the caller wants.
func TestHTTPStore_ReadsHonourContext(t *testing.T) {
	for op, call := range map[string]func(context.Context, *HTTPStore) error{
		"GetIoReader": func(ctx context.Context, s *HTTPStore) error {
			rc, err := s.GetIoReader(ctx, []byte("k"), fileformat.FileTypeTx)
			if rc != nil {
				_ = rc.Close()
			}

			return err
		},
		"Get": func(ctx context.Context, s *HTTPStore) error {
			_, err := s.Get(ctx, []byte("k"), fileformat.FileTypeTx)
			return err
		},
		"Exists": func(ctx context.Context, s *HTTPStore) error {
			_, err := s.Exists(ctx, []byte("k"), fileformat.FileTypeTx)
			return err
		},
		"Health": func(ctx context.Context, s *HTTPStore) error {
			_, _, err := s.Health(ctx, false)
			return err
		},
	} {
		t.Run(op, func(t *testing.T) {
			arrived := make(chan struct{}, 1)
			release := make(chan struct{})

			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				// Never block here: a retried request would hold Close up forever.
				select {
				case arrived <- struct{}{}:
				default:
				}

				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			// Cleanups run last-in first-out: release the handler before Close waits for it.
			t.Cleanup(server.Close)
			t.Cleanup(func() { close(release) })

			store := newStoreFor(t, server)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := make(chan error, 1)

			go func() { done <- call(ctx, store) }()

			select {
			case <-arrived:
			case <-time.After(5 * time.Second):
				t.Fatal("the request never reached the server")
			}

			cancel()

			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatalf("%s kept running after its context was cancelled", op)
			}
		})
	}
}

// TestHTTPStore_ExistsTellsMissFromFault pins that Exists answers false only for a 404. A HEAD
// response carries no body, so its 404 cannot be checked against NotFoundMsg; every other
// status is a storage error, not an absent blob.
func TestHTTPStore_ExistsTellsMissFromFault(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		exists bool
		fault  bool
	}{
		"200 is held":          {http.StatusOK, true, false},
		"404 is not held":      {http.StatusNotFound, false, false},
		"500 is a store fault": {http.StatusInternalServerError, false, true},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(server.Close)

			exists, err := newStoreFor(t, server).Exists(context.Background(), []byte("k"), fileformat.FileTypeTx)
			require.Equal(t, tc.exists, exists)

			if tc.fault {
				require.ErrorIs(t, err, errors.ErrStorageError)
				require.Contains(t, err.Error(), "500")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestHTTPStore_DelTreatsOnlyTheServersMissAsGone pins that Del reads a 404 as "already gone"
// only when the blob server sent it. The server has never answered DELETE with 404 - a missing
// blob is a 204 - so any other 404 is something else on that address, and calling it success
// would report a delete that never happened.
func TestHTTPStore_DelTreatsOnlyTheServersMissAsGone(t *testing.T) {
	for name, tc := range map[string]struct {
		handler http.HandlerFunc
		fault   bool
	}{
		"204 is deleted":                      {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }, false},
		"the blob server's 404 is gone":       {answerMiss, false},
		"a 404 with no body is a store fault": {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }, true},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			t.Cleanup(server.Close)

			err := newStoreFor(t, server).Del(context.Background(), []byte("k"), fileformat.FileTypeTx)
			if tc.fault {
				require.ErrorIs(t, err, errors.ErrStorageError)
				require.Contains(t, err.Error(), "404")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestHTTPStore_StatusErrorHasNoStrayParam pins that a non-2xx write reports its status code
// without a formatting artefact such as "%!(EXTRA <nil>)".
func TestHTTPStore_StatusErrorHasNoStrayParam(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	storeURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, options.WithHTTPAuthToken("t"))
	require.NoError(t, err)

	err = store.Set(context.Background(), []byte("k"), fileformat.FileTypeTesting, []byte("v"))
	require.Error(t, err)
	require.NotContains(t, err.Error(), "EXTRA")
	require.Contains(t, err.Error(), "500")
	require.False(t, errors.Is(err, errors.ErrConfiguration), "only a 401 is a configuration error")
}

// TestNew_ExplicitEmptyTokenSuppressesFallback pins that the blob_httpAuthToken fallback only
// applies when no token option was given: an explicit option, even an empty one, wins.
func TestNew_ExplicitEmptyTokenSuppressesFallback(t *testing.T) {
	t.Setenv("blob_httpAuthToken", "leak")

	storeURL, err := url.Parse("http://localhost:8080")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, options.WithHTTPAuthToken(""))
	require.NoError(t, err)
	require.Empty(t, store.authToken)

	store, err = New(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)
	require.Equal(t, "leak", store.authToken)
}

// TestNew_FallbackTokenIsTrimmed pins that the standalone-tool fallback trims the token the
// same way Settings.BlobHTTPAuthToken does, so it still matches a server that read the same
// secret file.
func TestNew_FallbackTokenIsTrimmed(t *testing.T) {
	t.Setenv("blob_httpAuthToken", "  fallback-token\n")

	storeURL, err := url.Parse("http://localhost:8080")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)
	require.Equal(t, "fallback-token", store.authToken)
}
