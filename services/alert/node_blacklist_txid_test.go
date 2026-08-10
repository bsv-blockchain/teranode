package alert

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bn/models"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestAddToConsensusBlacklist_UsesRequestedTxidForUtxoHash pins the identity used
// to compute the UTXO hash of the output being frozen.
//
// The hash preimage is txid || vout || script || satoshis, and the resulting
// Spend is filed under the txid the caller asked to freeze. Deriving the hash
// from the stored transaction's own TxIDChainHash() instead only works while the
// two agree. On a node bootstrapped from a UTXO-set snapshot they do not: the
// stored transaction is an input-less reconstruction that hashes to something
// else, so the freeze computes a UTXO hash matching nothing in the store and
// reports success while blacklisting nothing. A court-ordered freeze that quietly
// does nothing is the worst available outcome for this subsystem.
//
// The bounds and nil guards this relies on landed in #1328; this covers the
// identity that fix left untouched.
func TestAddToConsensusBlacklist_UsesRequestedTxidForUtxoHash(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	// A stored transaction carrying the right output but hashing to a different
	// txid than the one being blacklisted — what a snapshot reconstruction looks
	// like from this call site's point of view.
	storedTx := &bt.Tx{
		Version: 1,
		Inputs:  []*bt.Input{{PreviousTxOutIndex: 0}},
		Outputs: []*bt.Output{
			{Satoshis: 4000, LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14})},
		},
	}

	requested := tx.TxIDChainHash() // a real, different txid
	require.False(t, storedTx.TxIDChainHash().IsEqual(requested),
		"fixture must have a stored txid that differs from the requested one")

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).
		Return(&meta.Data{Tx: storedTx}, nil)
	mockStore.On("GetBlockHeight").Return(uint32(101))

	var frozen []*utxo.Spend

	mockStore.On("FreezeUTXOs", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			frozen = args.Get(1).([]*utxo.Spend)
		}).Return(nil)

	node := NewNodeConfig(ulogger.TestLogger{}, nil, mockStore, nil, nil, nil, tSettings)

	funds := []models.Fund{{
		TxOut:           models.TxOut{TxId: requested.String(), Vout: 0},
		EnforceAtHeight: []models.Enforce{{Start: 101, Stop: 999999999}},
	}}

	response, err := node.AddToConsensusBlacklist(ctx, funds)
	require.NoError(t, err)
	require.Empty(t, response.NotProcessed)

	require.Len(t, frozen, 1)
	require.True(t, frozen[0].TxID.IsEqual(requested),
		"the Spend is filed under the requested txid")

	want, err := util.UTXOHashFromOutput(requested, storedTx.Outputs[0], 0)
	require.NoError(t, err)
	require.Equal(t, want[:], frozen[0].UTXOHash[:],
		"the UTXO hash must be derived from the same txid the Spend is filed under, "+
			"otherwise it matches nothing in the store and the freeze is a silent no-op")
}
