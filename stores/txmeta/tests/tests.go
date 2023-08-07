package tests

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/TAAL-GmbH/ubsv/stores/txmeta"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Store(t *testing.T, db txmeta.Store) {
	ctx := context.Background()

	t.Run("simple smoke test", func(t *testing.T) {
		_ = db.Delete(ctx, Hash)

		err := db.Create(ctx, Hash, 100, nil, nil, 0)
		require.NoError(t, err)

		resp, err := db.Get(ctx, Hash)
		require.NoError(t, err)
		require.Equal(t, txmeta.Validated, resp.Status)
		require.Equal(t, uint64(100), resp.Fee)
		assert.Len(t, resp.ParentTxHashes, 0)
		assert.Len(t, resp.UtxoHashes, 0)
		assert.Equal(t, uint32(0), resp.LockTime)

		err = db.Create(ctx, Hash, 100, nil, nil, 0)
		require.Error(t, err, txmeta.ErrAlreadyExists)
	})

	t.Run("extended tests", func(t *testing.T) {
		_ = db.Delete(ctx, Hash)

		parentTxHashes := []*chainhash.Hash{
			Hash2,
			Hash,
		}
		utxoHashes := []*chainhash.Hash{
			Hash,
			Hash2,
		}
		err := db.Create(ctx, Hash, 123, parentTxHashes, utxoHashes, 101)
		require.NoError(t, err)

		resp, err := db.Get(ctx, Hash)
		require.NoError(t, err)
		require.Equal(t, txmeta.Validated, resp.Status)
		require.Equal(t, uint64(123), resp.Fee)
		assert.Len(t, resp.ParentTxHashes, 2)
		for i, h := range resp.ParentTxHashes {
			assert.Equal(t, parentTxHashes[i], h)
		}
		assert.Len(t, resp.UtxoHashes, 2)
		for i, h := range resp.UtxoHashes {
			assert.Equal(t, utxoHashes[i], h)
		}
		assert.Equal(t, uint32(101), resp.LockTime)

		err = db.Create(ctx, Hash, 100, nil, nil, 0)
		require.Error(t, err, txmeta.ErrAlreadyExists)
	})

	t.Run("mined", func(t *testing.T) {
		_ = db.Delete(ctx, Hash)

		err := db.Create(ctx, Hash, 100, nil, nil, 0)
		require.NoError(t, err)

		err = db.SetMined(ctx, Hash, Hash2)
		require.NoError(t, err)

		resp, err := db.Get(ctx, Hash)
		require.NoError(t, err)

		require.Equal(t, txmeta.Confirmed, resp.Status)
		require.Len(t, resp.BlockHashes, 1)
		assert.Equal(t, Hash2, resp.BlockHashes[0])
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

			err = db.Create(ctx, bHash, 100, nil, nil, 0)
			if err != nil {
				b.Fatal(err)
			}

			status, err := db.Get(ctx, bHash)
			if err != nil {
				b.Fatal(err)
			}
			if status.Status != txmeta.Validated {
				b.Fatal(status)
			}
		}
	})
}
