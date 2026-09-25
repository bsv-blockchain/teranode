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

// TestWriteLastHeight_OverHTTPBlobStoreIsConfigurationError pins what an http:// block store
// does to the lastProcessed marker: the marker is replaced on every block, the HTTP blob API
// cannot replace a blob, so the very first write must fail loudly - not succeed once and then
// be refused on every later block.
func TestWriteLastHeight_OverHTTPBlobStoreIsConfigurationError(t *testing.T) {
	const token = "persister-token"

	storeURL, err := url.Parse("memory://")
	require.NoError(t, err)

	blobServer, err := blob.NewHTTPBlobServer(ulogger.TestLogger{}, storeURL, token)
	require.NoError(t, err)

	var posts atomic.Int64

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}

		blobServer.ServeHTTP(w, r)
	}))
	t.Cleanup(httpServer.Close)

	clientURL, err := url.Parse(httpServer.URL)
	require.NoError(t, err)

	client, err := blobhttp.New(ulogger.TestLogger{}, clientURL, options.WithHTTPAuthToken(token))
	require.NoError(t, err)

	s := &Server{blockStore: client, logger: ulogger.TestLogger{}}

	err = s.writeLastHeight(context.Background(), 1)
	require.ErrorIs(t, err, errors.ErrConfiguration)
	require.Zero(t, posts.Load(), "nothing must be sent for a write that cannot be honoured")

	exists, err := client.Exists(context.Background(), nil, fileformat.FileTypeDat, options.WithFilename("lastProcessed"))
	require.NoError(t, err)
	require.False(t, exists, "no marker must have been written on the server")
}
