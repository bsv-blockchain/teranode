package alert

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bn/models"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// spendOutput returns an unsigned transaction spending parent:vout. The store does not
// evaluate scripts, so that is enough to record the spend.
func spendOutput(t *testing.T, parent *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	spender := bt.NewTx()
	require.NoError(t, spender.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))
	spender.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})
	require.NoError(t, spender.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[vout].Satoshis-1))

	return spender
}

func newFreezeTestStore(t *testing.T) *sql.Store {
	t.Helper()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	store, err := sql.New(context.Background(), ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(context.Background()) })

	require.NoError(t, store.SetBlockHeight(101))

	return store
}

// Freezing an output already spent by an unmined transaction takes that spender, and its
// descendants first, out of block assembly: otherwise this node mines a block inside the
// freeze window that it and every peer holding the alert reject.
func TestAddToConsensusBlacklist_EvictsUnminedSpender(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	store := newFreezeTestStore(t)

	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	spender := spendOutput(t, tx, 0)
	_, _, err = store.SpendAndCreate(ctx, spender, 101)
	require.NoError(t, err)

	child := spendOutput(t, spender, 0)
	_, _, err = store.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)

	var removed []chainhash.Hash

	ba := blockassembly.NewMock()
	ba.On("RemoveTx", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		removed = append(removed, *args.Get(1).(*chainhash.Hash))
	}).Return(nil)

	node := NewNodeConfig(ulogger.TestLogger{}, nil, store, ba, nil, nil, tSettings)

	response, err := node.AddToConsensusBlacklist(ctx, []models.Fund{{
		TxOut:           models.TxOut{TxId: tx.TxIDChainHash().String(), Vout: 0},
		EnforceAtHeight: []models.Enforce{{Start: 102, Stop: 200}},
	}})
	require.NoError(t, err)
	require.Empty(t, response.NotProcessed)

	require.Equal(t, []chainhash.Hash{*child.TxIDChainHash(), *spender.TxIDChainHash()}, removed,
		"the descendant goes first, then the spender")
}

// A release (an empty window whose policy tier expires with it) removes nothing, and an
// unspent output has no spender to remove.
func TestAddToConsensusBlacklist_NoEvictionWithoutActiveFreezeOrSpender(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	store := newFreezeTestStore(t)

	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	spender := spendOutput(t, tx, 0)
	_, _, err = store.SpendAndCreate(ctx, spender, 101)
	require.NoError(t, err)

	ba := blockassembly.NewMock()
	node := NewNodeConfig(ulogger.TestLogger{}, nil, store, ba, nil, nil, tSettings)

	response, err := node.AddToConsensusBlacklist(ctx, []models.Fund{
		{
			TxOut:                      models.TxOut{TxId: tx.TxIDChainHash().String(), Vout: 0},
			EnforceAtHeight:            []models.Enforce{{Start: 150, Stop: 150}},
			PolicyExpiresWithConsensus: true,
		},
		{
			TxOut:           models.TxOut{TxId: tx.TxIDChainHash().String(), Vout: 1},
			EnforceAtHeight: []models.Enforce{{Start: 102, Stop: 200}},
		},
	})
	require.NoError(t, err)
	require.Empty(t, response.NotProcessed)

	ba.AssertNotCalled(t, "RemoveTx", mock.Anything, mock.Anything)
}

// A fund whose transaction this node does not hold yet fails the alert, so the alert system
// retries it, while the funds it can freeze are frozen now.
func TestAddToConsensusBlacklist_MissingTransactionIsRetried(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	store := newFreezeTestStore(t)

	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	unknown := chainhash.HashH([]byte("not stored yet"))

	node := NewNodeConfig(ulogger.TestLogger{}, nil, store, nil, nil, nil, tSettings)

	response, err := node.AddToConsensusBlacklist(ctx, []models.Fund{
		{TxOut: models.TxOut{TxId: unknown.String(), Vout: 0}, EnforceAtHeight: []models.Enforce{{Start: 102, Stop: 200}}},
		{TxOut: models.TxOut{TxId: tx.TxIDChainHash().String(), Vout: 0}, EnforceAtHeight: []models.Enforce{{Start: 102, Stop: 200}}},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrTxNotFound)
	require.ErrorContains(t, err, unknown.String())
	require.NotNil(t, response)
	require.Len(t, response.NotProcessed, 1)

	spend, err := store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_FROZEN), spend.Status, "the fund this node holds is frozen now")
}
