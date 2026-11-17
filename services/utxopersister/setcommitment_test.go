package utxopersister

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

func TestSetHashRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	blockHash := chainhash.HashH([]byte("block-for-sethash-roundtrip"))

	var digest [32]byte
	for i := range digest {
		digest[i] = byte(i + 1)
	}

	require.NoError(t, persistSetHash(ctx, store, &blockHash, digest))

	got, err := readSetHash(ctx, store, &blockHash)
	require.NoError(t, err)
	require.Equal(t, digest, got)
}

func TestReadSetHashMissing(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	blockHash := chainhash.HashH([]byte("absent-block"))

	_, err := readSetHash(ctx, store, &blockHash)
	require.Error(t, err)
}
