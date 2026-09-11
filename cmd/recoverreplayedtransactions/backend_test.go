package recoverreplayedtransactions

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestExternalTransactionReaderUsesNativeLayout(t *testing.T) {
	for _, mode := range []string{"standard", "extended", "wrong-identity", "trailing"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			external := &url.URL{Scheme: "file", Path: t.TempDir()}
			native, err := blob.NewStore(ulogger.TestLogger{}, external, options.WithDisableDAH(true))
			require.NoError(t, err)
			defer native.Close(ctx)
			raw := "0100000001" + strings.Repeat("00", 32) + "ffffffff0100ffffffff010100000000000000015100000000"
			tx, err := bt.NewTxFromString(raw)
			require.NoError(t, err)
			payload := tx.Bytes()
			if mode == "extended" {
				payload = tx.ExtendedBytes()
			}
			if mode == "wrong-identity" {
				other := tx.Clone()
				other.LockTime++
				payload = other.Bytes()
			}
			if mode == "trailing" {
				payload = append(payload, 0)
			}
			require.NoError(t, native.Set(ctx, tx.TxIDChainHash()[:], fileformat.FileTypeTx, payload))
			configured := &url.URL{Scheme: "aerospike", Host: "localhost", RawQuery: url.Values{"externalStore": []string{external.String()}}.Encode()}
			read, closeReader, err := openExternalTransactionReader(ctx, ulogger.TestLogger{}, configured)
			require.NoError(t, err)
			defer func() { require.NoError(t, closeReader()) }()
			result, err := read(ctx, tx.TxID())
			if mode == "wrong-identity" || mode == "trailing" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tx.Bytes(), result.Bytes())
		})
	}
}

func TestExternalTransactionReaderOptionalAndLazy(t *testing.T) {
	configured := &url.URL{Scheme: "aerospike", Host: "localhost"}
	read, closeReader, err := openExternalTransactionReader(context.Background(), ulogger.TestLogger{}, configured)
	require.NoError(t, err)
	require.Nil(t, read)
	require.NoError(t, closeReader())
	configured.RawQuery = url.Values{"externalStore": []string{"file:///nonexistent/replay-recovery-test"}}.Encode()
	_, closeReader, err = openExternalTransactionReader(context.Background(), ulogger.TestLogger{}, configured)
	require.NoError(t, err, "unused optional store need not open")
	require.NoError(t, closeReader())
}
