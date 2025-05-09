package repository

import (
	"io"
	"testing"

	"github.com/bitcoin-sv/teranode/services/blockpersister"
	"github.com/bitcoin-sv/teranode/services/utxopersister/filestorer"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/require"
)

func TestGetSubtreeDataWithReader(t *testing.T) {
	tracing.SetGlobalMockTracer()

	t.Run("get subtree from block store", func(t *testing.T) {
		ctx, subtree, metaDatas := setupSubtreeReaderTest(t)

		// create the block-store .subtree file
		storer, err := filestorer.NewFileStorer(t.Context(), ctx.logger, ctx.settings, ctx.repo.BlockPersisterStore, subtree.RootHash()[:], "subtree")
		require.NoError(t, err)

		err = blockpersister.WriteTxs(t.Context(), ctx.logger, storer, metaDatas, nil)
		require.NoError(t, err)

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
		err = ctx.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		// should be able to get the subtree from the block-store (should NOT be looking at subtree-store)
		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r, false)

		// close the reader
		require.NoError(t, r.Close())
	})
}

func setupSubtreeReaderTest(t *testing.T) (*testContext, *util.Subtree, []*meta.Data) {
	ctx := setup(t)
	ctx.logger.Debugf("test")

	_, subtree := newBlock(ctx, t, params)

	metaDatas := make([]*meta.Data, 0, len(params.txs))

	// Create the txs in the utxo store
	for i, tx := range params.txs {
		if i != 0 {
			_, err := ctx.repo.UtxoStore.Create(t.Context(), tx, params.height)
			require.NoError(t, err)
		}

		metaDatas = append(metaDatas, &meta.Data{
			Tx: tx,
		})
	}

	return ctx, subtree, metaDatas
}

func checkSubtreeTransactions(t *testing.T, r *io.PipeReader, includeCoinbase bool) {
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
