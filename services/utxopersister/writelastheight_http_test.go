package utxopersister

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobhttp "github.com/bsv-blockchain/teranode/stores/blob/http"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newMarkerBlobServer starts an HTTP blob server over a fresh in-memory store that requires
// token for writes, and counts the POSTs it receives.
func newMarkerBlobServer(t *testing.T, token string) (*url.URL, *atomic.Int64) {
	t.Helper()

	storeURL, err := url.Parse("memory://")
	require.NoError(t, err)

	blobServer, err := blob.NewHTTPBlobServer(ulogger.TestLogger{}, storeURL, token)
	require.NoError(t, err)

	posts := &atomic.Int64{}

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}

		blobServer.ServeHTTP(w, r)
	}))
	t.Cleanup(httpServer.Close)

	clientURL, err := url.Parse(httpServer.URL)
	require.NoError(t, err)

	return clientURL, posts
}

// TestWriteLastHeight_OverHTTPBlobStore pins what an http:// block store does to the
// lastProcessed marker, which is replaced on every block. With the shared token the marker
// advances block after block - block one used to succeed and block two get 409. Without a
// token the very first write fails loudly as a configuration error.
func TestWriteLastHeight_OverHTTPBlobStore(t *testing.T) {
	const token = "persister-token"

	t.Run("with the shared token the marker advances every block", func(t *testing.T) {
		clientURL, posts := newMarkerBlobServer(t, token)

		client, err := blobhttp.New(ulogger.TestLogger{}, clientURL, options.WithHTTPAuthToken(token))
		require.NoError(t, err)

		s := &Server{blockStore: client, logger: ulogger.TestLogger{}}

		require.NoError(t, s.writeLastHeight(context.Background(), 1))
		require.NoError(t, s.writeLastHeight(context.Background(), 2), "the second block must replace the marker")

		height, err := s.readLastHeight(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint32(2), height)
		require.Equal(t, int64(2), posts.Load())
	})

	t.Run("without a token the first write is a configuration error", func(t *testing.T) {
		clientURL, _ := newMarkerBlobServer(t, token)

		client, err := blobhttp.New(ulogger.TestLogger{}, clientURL, options.WithHTTPAuthToken(""))
		require.NoError(t, err)

		s := &Server{blockStore: client, logger: ulogger.TestLogger{}}

		err = s.writeLastHeight(context.Background(), 1)
		require.ErrorIs(t, err, errors.ErrConfiguration)

		exists, err := client.Exists(context.Background(), nil, fileformat.FileTypeDat, options.WithFilename("lastProcessed"))
		require.NoError(t, err)
		require.False(t, exists, "no marker must have been written on the server")
	})
}
