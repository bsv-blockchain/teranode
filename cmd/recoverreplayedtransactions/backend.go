package recoverreplayedtransactions

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// openExternalTransactionReader matches Aerospike's externalStore URL and its
// default unprefixed layout. Open lazily: missing optional local bytes must not
// prevent use of independently available canonical archive evidence.
func openExternalTransactionReader(ctx context.Context, logger ulogger.Logger, utxoURL *url.URL) (replayrecovery.TransactionReader, func() error, error) {
	closeEmpty := func() error { return nil }
	if utxoURL == nil || utxoURL.Query().Get("externalStore") == "" {
		return nil, closeEmpty, nil
	}
	configured, err := url.Parse(utxoURL.Query().Get("externalStore"))
	if err != nil {
		return nil, closeEmpty, commandError("invalid external transaction store URL")
	}
	query := configured.Query()
	if query.Get("hashPrefix") == "" && query.Get("hashSuffix") == "" {
		query.Set("hashPrefix", "0")
	}
	configured.RawQuery = query.Encode()
	var once sync.Once
	var store blob.Store
	var openErr error
	read := func(readCtx context.Context, id string) (*bt.Tx, error) {
		if err := readCtx.Err(); err != nil {
			return nil, err
		}
		hash, err := chainhash.NewHashFromStr(id)
		if err != nil || len(id) != 64 || hash.String() != id {
			return nil, commandError("invalid external transaction identity")
		}
		once.Do(func() { store, openErr = readOnlyArchive(logger, configured) })
		if openErr != nil {
			return nil, commandError("external transaction archive unavailable")
		}
		reader, err := store.GetIoReader(readCtx, hash[:], fileformat.FileTypeTx)
		if err != nil {
			return nil, commandError("retained raw transaction unavailable")
		}
		const maxTransactionBytes int64 = 64 << 20
		raw, readErr := io.ReadAll(io.LimitReader(reader, maxTransactionBytes+1))
		closeErr := reader.Close()
		if readCtx.Err() != nil {
			return nil, readCtx.Err()
		}
		if readErr != nil || closeErr != nil {
			return nil, commandError("retained raw transaction read failed")
		}
		if int64(len(raw)) > maxTransactionBytes {
			return nil, commandError("retained raw transaction exceeds byte limit")
		}
		data := bytes.NewReader(raw)
		tx, err := replayrecovery.ReadBoundedTransaction(data)
		if err != nil || data.Len() != 0 || tx.TxID() != id {
			return nil, commandError("retained raw transaction identity or encoding mismatch")
		}
		return tx, nil
	}
	closeReader := func() error {
		if store == nil {
			return nil
		}
		return store.Close(ctx)
	}
	return read, closeReader, nil
}
