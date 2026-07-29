package utxopersister

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/seedcheckpoint"
	"github.com/bsv-blockchain/teranode/pkg/utxoseed"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
)

// BuildSignedCheckpoint reads the persisted set hash for blockHash, signs a
// checkpoint over (height, blockHash, setHash) with priv — bound to netMagic
// (the local network's wire magic) — persists the signed checkpoint as a
// sidecar blob, and returns it.
func BuildSignedCheckpoint(ctx context.Context, store blob.Store, blockHash chainhash.Hash, height uint32, priv *bec.PrivateKey, netMagic uint32) (*seedcheckpoint.SignedCheckpoint, error) {
	setHash, err := readSetHash(ctx, store, &blockHash)
	if err != nil {
		return nil, errors.NewProcessingError("cannot build checkpoint: no set hash for %s", blockHash.String(), err)
	}

	sc, err := seedcheckpoint.Sign(priv, seedcheckpoint.Checkpoint{
		CommitmentVersion: utxoseed.CommitmentVersion,
		Height:            height,
		BlockHash:         blockHash,
		SetHash:           setHash,
	}, netMagic)
	if err != nil {
		return nil, err
	}

	if err := store.Set(ctx, blockHash[:], fileformat.FileTypeSeedCheckpoint, sc.Serialize(), options.WithAllowOverwrite(true)); err != nil {
		return nil, errors.NewStorageError("error writing signed checkpoint for %s", blockHash.String(), err)
	}

	return sc, nil
}

func readSignedCheckpoint(ctx context.Context, store blob.Store, blockHash chainhash.Hash) (*seedcheckpoint.SignedCheckpoint, error) {
	b, err := store.Get(ctx, blockHash[:], fileformat.FileTypeSeedCheckpoint)
	if err != nil {
		return nil, err
	}

	return seedcheckpoint.ParseSignedCheckpoint(b)
}
