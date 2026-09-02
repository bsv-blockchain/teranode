package utxopersister

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
)

func persistSetHash(ctx context.Context, store blob.Store, blockHash *chainhash.Hash, digest [32]byte) error {
	if err := store.Set(ctx, blockHash[:], fileformat.FileTypeUtxoSetHash, digest[:], options.WithAllowOverwrite(true)); err != nil {
		return errors.NewStorageError("error writing utxo set hash for %s", blockHash.String(), err)
	}

	return nil
}

func readSetHash(ctx context.Context, store blob.Store, blockHash *chainhash.Hash) ([32]byte, error) {
	var out [32]byte

	b, err := store.Get(ctx, blockHash[:], fileformat.FileTypeUtxoSetHash)
	if err != nil {
		return out, err
	}

	if len(b) != 32 {
		return out, errors.NewProcessingError("utxo set hash for %s has wrong length %d", blockHash.String(), len(b))
	}

	copy(out[:], b)

	return out, nil
}
