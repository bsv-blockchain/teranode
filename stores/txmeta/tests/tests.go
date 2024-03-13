package tests

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Store(t *testing.T, db txmeta.Store) {
	ctx := context.Background()

	hash1 := Tx1.TxIDChainHash()

	t.Run("simple smoke test", func(t *testing.T) {
		_ = db.Delete(ctx, hash1)

		_, err := db.Create(ctx, Tx1)
		require.NoError(t, err)

		resp, err := db.Get(ctx, hash1)
		require.NoError(t, err)
		require.Equal(t, uint64(215), resp.Fee)
		require.Equal(t, uint64(328), resp.SizeInBytes)
		assert.Len(t, resp.ParentTxHashes, 1)

		_, err = db.Create(ctx, Tx1)
		require.Error(t, err, txmeta.NewErrTxmetaAlreadyExists)
	})

	t.Run("extended tests", func(t *testing.T) {
		_ = db.Delete(ctx, hash1)

		parentTxHashes := make([]chainhash.Hash, len(Tx1.Inputs))
		for index, input := range Tx1.Inputs {
			parentTxHashes[index] = *input.PreviousTxIDChainHash()
		}

		_, err := db.Create(ctx, Tx1)
		require.NoError(t, err)

		resp, err := db.Get(ctx, hash1)
		require.NoError(t, err)
		require.Equal(t, uint64(215), resp.Fee)
		require.Equal(t, uint64(328), resp.SizeInBytes)
		assert.Len(t, resp.ParentTxHashes, 1)
		for i, h := range resp.ParentTxHashes {
			assert.Equal(t, parentTxHashes[i], h)
		}

		_, err = db.Create(ctx, Tx1)
		require.Error(t, err, txmeta.NewErrTxmetaAlreadyExists)
	})

	t.Run("mined", func(t *testing.T) {
		_ = db.Delete(ctx, hash1)

		_, err := db.Create(ctx, Tx1)
		require.NoError(t, err)

		err = db.SetMined(ctx, hash1, 2)
		require.NoError(t, err)

		resp, err := db.Get(ctx, hash1)
		require.NoError(t, err)

		require.Len(t, resp.BlockIDs, 1)
		assert.Equal(t, uint32(2), resp.BlockIDs[0])

		// set mined again
		err = db.SetMined(ctx, hash1, 1)
		require.NoError(t, err)

		resp, err = db.Get(ctx, hash1)
		require.NoError(t, err)

		require.Len(t, resp.BlockIDs, 2)
		assert.Equal(t, uint32(2), resp.BlockIDs[0])
		assert.Equal(t, uint32(1), resp.BlockIDs[1])
	})
}

func Sanity(t *testing.T, db txmeta.Store) {
	util.SkipLongTests(t)
}

func Benchmark(b *testing.B, db txmeta.Store) {
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := make([]byte, 32)
			_, err := rand.Read(buf)
			require.NoError(b, err)

			bHash, _ := chainhash.NewHash(buf)

			/* todo */
			_, err = db.Create(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}

			_, err = db.Get(ctx, bHash)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
