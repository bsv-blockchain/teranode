package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func TestParentOutputsWideParentReadsRequestedOutputs(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	parent := newExtendedTx(t, 150, 700_000)
	_, err := store.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 99}))
	require.NoError(t, err)

	hash := *parent.TxIDChainHash()
	vouts := []uint32{0, 63, 64, 149, 150, 1 << 31, 4_000_000_000}

	outpoints := make([]utxo.Outpoint, len(vouts))
	for i, v := range vouts {
		outpoints[i] = utxo.Outpoint{TxID: hash, Vout: v}
	}

	answers, err := store.ParentOutputsForValidation(ctx, outpoints)
	require.NoError(t, err)
	require.Len(t, answers, len(vouts))

	for i, v := range vouts {
		require.NoError(t, answers[i].Err, "vout %d", v)

		if v >= 150 {
			require.Equal(t, utxo.ParentOutputNoSuchIndex, answers[i].Status, "vout %d", v)
			continue
		}

		require.Equal(t, utxo.ParentOutputMined, answers[i].Status, "vout %d", v)
		require.Equal(t, uint32(99), answers[i].Height, "vout %d", v)
		require.Equal(t, parent.Outputs[v].Satoshis, answers[i].Satoshis, "vout %d", v)
		require.Equal(t, []byte(*parent.Outputs[v].LockingScript), []byte(*answers[i].LockingScript), "vout %d", v)
	}
}

func TestParentOutputsStoreFaultIsErrNotNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	parent := newExtendedTx(t, 2, 710_000)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	store.pool.Close()

	answers, err := store.ParentOutputsForValidation(ctx, []utxo.Outpoint{
		{TxID: *parent.TxIDChainHash(), Vout: 0},
		{TxID: chainhash.HashH([]byte("other")), Vout: 0},
	})
	require.NoError(t, err)
	require.Len(t, answers, 2)

	for _, a := range answers {
		require.Error(t, a.Err)
		require.Equal(t, utxo.ParentOutputUnknown, a.Status)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	_, err = store.ParentOutputsForValidation(cancelled, []utxo.Outpoint{{TxID: *parent.TxIDChainHash()}})
	require.Error(t, err, "a cancelled context fails the whole call")
}
