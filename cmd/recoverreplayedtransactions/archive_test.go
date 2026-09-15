package recoverreplayedtransactions

import (
	"context"
	"io"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyArchiveMatchesDaemonPrefix(t *testing.T) {
	for _, query := range []string{"", "hashPrefix=3", "hashPrefix=0"} {
		t.Run(query, func(t *testing.T) {
			ctx := context.Background()
			location := &url.URL{Scheme: "file", Path: t.TempDir(), RawQuery: query}
			native, err := blob.NewStore(ulogger.TestLogger{}, location, options.WithHashPrefix(2), options.WithDisableDAH(true))
			require.NoError(t, err)
			defer native.Close(ctx)
			key := chainhash.DoubleHashH([]byte("archive fixture"))
			payload := []byte("retained subtree")
			require.NoError(t, native.Set(ctx, key[:], fileformat.FileTypeSubtree, payload))
			archive, err := readOnlyArchive(ulogger.TestLogger{}, location)
			require.NoError(t, err)
			defer archive.Close(ctx)
			reader, err := archive.GetIoReader(ctx, key[:], fileformat.FileTypeSubtree)
			require.NoError(t, err)
			defer reader.Close()
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, payload, got)
			require.Equal(t, query, location.RawQuery, "read-only options must not alter configured URL")
		})
	}
}
