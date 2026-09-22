package blob

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newAuthTestServer builds an HTTP blob server over an in-memory store, plus the backing
// store, so a test can create fixtures without going through the HTTP API it is testing.
// Requests are driven with a plain http.Client rather than the blob HTTP client: the wire
// contract is what is under test, and a client that always attaches the token would mask
// every unauthenticated case.
//
// The store is in-memory rather than a file store because PATCH exercises SetDAH, which the
// file store refuses without a configured blob deletion scheduler - a 500 that would hide
// what these tests are asserting.
func newAuthTestServer(t *testing.T, authToken string) (*httptest.Server, Store) {
	t.Helper()

	logger := ulogger.New("blob-auth-test")

	storeURL, err := url.Parse("memory://")
	require.NoError(t, err)

	blobServer, err := NewHTTPBlobServer(logger, storeURL, authToken)
	require.NoError(t, err)

	httpServer := httptest.NewServer(blobServer)
	t.Cleanup(httpServer.Close)

	return httpServer, blobServer.store
}

// blobPath is the request path for a key, matching what the HTTP blob client sends.
func blobPath(key []byte) string {
	return "/blob/" + base64.URLEncoding.EncodeToString(key) + "." + fileformat.FileTypeTesting.String()
}

// doBlobRequest issues one request, optionally with an Authorization header, and returns
// the status code. The body is always drained and closed.
func doBlobRequest(t *testing.T, server *httptest.Server, method, path, authHeader string, body io.Reader) int {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, server.URL+path, body)
	require.NoError(t, err)

	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := server.Client().Do(req)
	require.NoError(t, err)

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	return resp.StatusCode
}

// TestHTTPBlobServer_MutationsRequireAuth is the regression test issue 4841 asks for: an
// unauthenticated caller must not be able to write to, re-date or delete a blob. Every case
// uses its own key and creates its own fixture, so a successful DELETE cannot remove the blob
// a later case needs.
func TestHTTPBlobServer_MutationsRequireAuth(t *testing.T) {
	const token = "test-token"

	server, store := newAuthTestServer(t, token)

	mutations := []struct {
		name          string
		method        string
		query         string
		successStatus int
		body          func() io.Reader
	}{
		{name: "POST", method: http.MethodPost, successStatus: http.StatusCreated, body: func() io.Reader { return bytes.NewReader([]byte("payload")) }},
		{name: "PATCH", method: http.MethodPatch, query: "?dah=1000", successStatus: http.StatusOK},
		{name: "DELETE", method: http.MethodDelete, successStatus: http.StatusNoContent},
	}

	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			for caseIdx, authHeader := range []string{"", "Bearer wrong", "Basic " + token} {
				key := []byte(fmt.Sprintf("%s-refused-%d", mutation.name, caseIdx))

				// PATCH and DELETE need the blob to exist, so a 401 cannot be confused
				// with a 404. POST needs the key to be free.
				if mutation.method != http.MethodPost {
					require.NoError(t, store.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("fixture")))
				}

				var body io.Reader
				if mutation.body != nil {
					body = mutation.body()
				}

				status := doBlobRequest(t, server, mutation.method, blobPath(key)+mutation.query, authHeader, body)
				require.Equal(t, http.StatusUnauthorized, status, "auth header %q must be refused", authHeader)
			}

			key := []byte(mutation.name + "-accepted")

			if mutation.method != http.MethodPost {
				require.NoError(t, store.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("fixture")))
			}

			var body io.Reader
			if mutation.body != nil {
				body = mutation.body()
			}

			status := doBlobRequest(t, server, mutation.method, blobPath(key)+mutation.query, "Bearer "+token, body)
			require.Equal(t, mutation.successStatus, status, "the configured token must be accepted")
		})
	}

	t.Run("reads need no credential", func(t *testing.T) {
		key := []byte("readable")
		require.NoError(t, store.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("fixture")))

		require.Equal(t, http.StatusOK, doBlobRequest(t, server, http.MethodGet, blobPath(key), "", nil))
		require.Equal(t, http.StatusOK, doBlobRequest(t, server, http.MethodHead, blobPath(key), "", nil))
	})
}

// TestHTTPBlobServer_ReadOnlyWithoutConfiguredToken pins the deny-by-default behaviour: a
// blob server with no shared secret configured must not accept writes at all, presented
// credential or not.
func TestHTTPBlobServer_ReadOnlyWithoutConfiguredToken(t *testing.T) {
	server, store := newAuthTestServer(t, "")

	mutations := []struct {
		name   string
		method string
		query  string
		body   func() io.Reader
	}{
		{name: "POST", method: http.MethodPost, body: func() io.Reader { return bytes.NewReader([]byte("payload")) }},
		{name: "PATCH", method: http.MethodPatch, query: "?dah=1000"},
		{name: "DELETE", method: http.MethodDelete},
	}

	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			for caseIdx, authHeader := range []string{"", "Bearer anything"} {
				key := []byte(fmt.Sprintf("readonly-%s-%d", mutation.name, caseIdx))
				require.NoError(t, store.Set(context.Background(), key, fileformat.FileTypeTesting, []byte("fixture")))

				var body io.Reader
				if mutation.body != nil {
					body = mutation.body()
				}

				status := doBlobRequest(t, server, mutation.method, blobPath(key)+mutation.query, authHeader, body)
				require.Equal(t, http.StatusUnauthorized, status, "auth header %q must be refused on a read-only server", authHeader)

				require.Equal(t, http.StatusOK, doBlobRequest(t, server, http.MethodGet, blobPath(key), "", nil), "reads must still work")
			}
		})
	}
}

// TestHTTPBlobServer_QueryAllowOverwriteDoesNotOverwrite is the other regression test issue
// 4841 asks for: allowOverwrite in the query string must not let a caller replace a blob the
// store already holds, even with a valid credential.
func TestHTTPBlobServer_QueryAllowOverwriteDoesNotOverwrite(t *testing.T) {
	const token = "overwrite-token"

	server, store := newAuthTestServer(t, token)

	key := []byte("overwrite-key")

	status := doBlobRequest(t, server, http.MethodPost, blobPath(key), "Bearer "+token, bytes.NewReader([]byte("first")))
	require.Equal(t, http.StatusCreated, status)

	status = doBlobRequest(t, server, http.MethodPost, blobPath(key)+"?allowOverwrite=true", "Bearer "+token, bytes.NewReader([]byte("second")))
	require.Equal(t, http.StatusConflict, status, "allowOverwrite from the query must not be honoured")

	stored, err := store.Get(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), stored, "the original blob must survive")
}
