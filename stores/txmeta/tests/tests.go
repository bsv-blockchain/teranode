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
	hash2 := Tx2.TxIDChainHash()

	t.Run("simple smoke test", func(t *testing.T) {
		_ = db.Delete(ctx, hash1)

		_, err := db.Create(ctx, Tx1)
		require.NoError(t, err)

		resp, err := db.Get(ctx, hash1)
		require.NoError(t, err)
		require.Equal(t, uint64(215), resp.Fee)
		require.Equal(t, uint64(328), resp.SizeInBytes)
		assert.Len(t, resp.ParentTxHashes, 1)
		assert.Len(t, resp.UtxoHashes, 5)

		_, err = db.Create(ctx, Tx1)
		require.Error(t, err, txmeta.ErrAlreadyExists)
	})

	t.Run("extended tests", func(t *testing.T) {
		_ = db.Delete(ctx, hash1)

		parentTxHashes := make([]*chainhash.Hash, len(Tx1.Inputs))
		for index, input := range Tx1.Inputs {
			parentTxHash, _ := util.UTXOHashFromInput(input)
			parentTxHashes[index] = parentTxHash
		}

		utxoHashes := make([]*chainhash.Hash, len(Tx1.Outputs))
		for index, output := range Tx1.Outputs {
			utxoHash, _ := util.UTXOHashFromOutput(hash1, output, uint32(index))
			utxoHashes[index] = utxoHash
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
		assert.Len(t, resp.UtxoHashes, 5)
		for i, h := range resp.UtxoHashes {
			assert.Equal(t, utxoHashes[i], h)
		}

		_, err = db.Create(ctx, Tx1)
		require.Error(t, err, txmeta.ErrAlreadyExists)
	})

	t.Run("mined", func(t *testing.T) {
		_ = db.Delete(ctx, hash1)

		_, err := db.Create(ctx, Tx1)
		require.NoError(t, err)

		err = db.SetMined(ctx, hash1, hash2)
		require.NoError(t, err)

		resp, err := db.Get(ctx, hash1)
		require.NoError(t, err)

		require.Len(t, resp.BlockHashes, 1)
		assert.Equal(t, hash2, resp.BlockHashes[0])
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
