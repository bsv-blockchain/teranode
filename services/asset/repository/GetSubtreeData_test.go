package repository

import (
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

// TestGetSubtreeDataWithReader tests subtree data retrieval from storage backends.
func TestGetSubtreeDataWithReader(t *testing.T) {
	tracing.SetupMockTracer()

	t.Run("get subtree from subtree store", func(t *testing.T) {
		ctx, subtree, txs := setupSubtreeReaderTest(t)

		// create the block-store .subtree file
		storer, err := filestorer.NewFileStorer(t.Context(), ctx.logger, ctx.settings, ctx.repo.SubtreeStore, subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)

		for _, tx := range txs {
			_, err = storer.Write(tx.Bytes())
			require.NoError(t, err)
		}

		require.NoError(t, storer.Close(t.Context()))

		// should be able to get the subtree from the block-store (should NOT be looking at subtree-store)
		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r, true)

		// close the reader
		require.NoError(t, r.Close())
	})

	t.Run("get subtree from utxo store", func(t *testing.T) {
		ctx, subtree, _ := setupSubtreeReaderTest(t)

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		// write the subtree to the subtree store
		err = ctx.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		// should be able to get the subtree from the block-store (should NOT be looking at subtree-store)
		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r, false)

		// close the reader
		require.NoError(t, r.Close())
	})
}

func setupSubtreeReaderTest(t *testing.T) (*testContext, *subtreepkg.Subtree, []*bt.Tx) {
	ctx := setup(t)
	ctx.logger.Debugf("test")

	_, subtree := newBlock(ctx, t, params)

	txs := make([]*bt.Tx, 0, len(params.txs))

	// Create the txs in the utxo store
	for i, tx := range params.txs {
		if i != 0 {
			_, err := ctx.repo.UtxoStore.Create(t.Context(), tx, params.height)
			require.NoError(t, err)
		}

		txs = append(txs, tx)
	}

	return ctx, subtree, txs
}

func checkSubtreeTransactions(t *testing.T, r io.ReadCloser, includeCoinbase bool) {
	// read the transactions from the subtree data
	txCount := 0

	offset := 1
	if !includeCoinbase {
		offset = 0
	}

	for {
		tx := &bt.Tx{}

		_, err := tx.ReadFrom(r)
		if err != nil {
			break
		}

		txCount++
		require.Equal(t, params.txs[txCount-offset].TxID(), tx.TxID())
	}

	if includeCoinbase {
		require.Equal(t, len(params.txs), txCount)
	} else {
		require.Equal(t, len(params.txs)-1, txCount)
	}
}
