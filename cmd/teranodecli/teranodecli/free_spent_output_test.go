package teranodecli

import (
	"bytes"
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	utxoaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	aerospiketest "github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// freeSpentOutputStore is a store the test can also write P and C's spend into. Both stores have
// these methods; the store interface reaches them through SpendAndCreate.
type freeSpentOutputStore interface {
	utxostore.Store
	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxostore.CreateOption) (*meta.Data, error)
	Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxostore.IgnoreFlags) ([]*utxostore.Spend, error)
}

// freeSpentOutputFixture is a store holding parent P, whose output 0 is spent by child C. C's
// record exists only when the caller creates it.
type freeSpentOutputFixture struct {
	store  freeSpentOutputStore
	parent *bt.Tx
	child  *bt.Tx
	spend  *utxostore.Spend
}

// newFreeSpentOutputFixture writes P and C's spend of P:0 into store. seed makes P distinct, so
// several fixtures can share one store.
func newFreeSpentOutputFixture(t *testing.T, store freeSpentOutputStore, seed uint32) *freeSpentOutputFixture {
	t.Helper()

	ctx := context.Background()

	parent := bt.NewTx()
	require.NoError(t, parent.From("4ff7de8f0b6d5c4a3e2f1d0c9b8a7968574635241302f1e0d9c8b7a695847362", seed, "51", 10_000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 9_000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 8_000))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	// The spend is written and C is never created: what a failed attempt leaves behind.
	_, err = store.Spend(ctx, child, store.GetBlockHeight()+1)
	require.NoError(t, err)

	utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(t, err)

	return &freeSpentOutputFixture{
		store:  store,
		parent: parent,
		child:  child,
		spend:  &utxostore.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash},
	}
}

// spentByChild reports whether P:0 is spent, failing if it names anything but C.
func (f *freeSpentOutputFixture) spentByChild(t *testing.T) bool {
	t.Helper()

	resp, err := f.store.GetSpend(context.Background(), f.spend)
	require.NoError(t, err)

	if resp.SpendingData == nil {
		return false
	}

	require.Equal(t, f.child.TxIDChainHash().String(), resp.SpendingData.TxID.String())

	return true
}

// freeSpentOutputCases run against every store: the command uses only the store interface, and
// each store's Unspend checks the stored spend before clearing it.
var freeSpentOutputCases = []struct {
	name string
	run  func(t *testing.T, f *freeSpentOutputFixture)
}{
	{
		// A spend left behind by a transaction with no record is freed, so C can spend P:0 again,
		// as the block that mines it will.
		name: "frees a spend whose spender has no record",
		run: func(t *testing.T, f *freeSpentOutputFixture) {
			require.True(t, f.spentByChild(t), "precondition: C spends P:0")

			var out bytes.Buffer
			require.NoError(t, freeSpentOutput(context.Background(), f.store, f.parent.TxIDChainHash(), 0, f.child.TxIDChainHash(), &out))

			require.False(t, f.spentByChild(t), "P:0 is free")
			require.Contains(t, out.String(), "Freed "+f.parent.TxIDChainHash().String()+":0")

			spends, err := f.store.Spend(context.Background(), f.child, f.store.GetBlockHeight()+1)
			require.NoError(t, err)
			require.Len(t, spends, 1)
			require.True(t, f.spentByChild(t), "C spends P:0 again")
		},
	},
	{
		// A stored spender's spend is a real one: freeing it would let a second spender take P:0.
		name: "refuses when the spender is stored",
		run: func(t *testing.T, f *freeSpentOutputFixture) {
			_, err := f.store.Create(context.Background(), f.child, 0)
			require.NoError(t, err)

			err = freeSpentOutput(context.Background(), f.store, f.parent.TxIDChainHash(), 0, f.child.TxIDChainHash(), &bytes.Buffer{})
			require.ErrorContains(t, err, "has a record in this store")
			require.True(t, f.spentByChild(t), "P:0 stays spent by C")
		},
	},
	{
		name: "refuses when the output names another spender",
		run: func(t *testing.T, f *freeSpentOutputFixture) {
			other := chainhash.Hash{9} // no record, and P:0 does not name it

			err := freeSpentOutput(context.Background(), f.store, f.parent.TxIDChainHash(), 0, &other, &bytes.Buffer{})
			require.ErrorContains(t, err, "is not spent by")
			require.True(t, f.spentByChild(t), "P:0 stays spent by C")
		},
	},
	{
		name: "refuses an output the parent does not have",
		run: func(t *testing.T, f *freeSpentOutputFixture) {
			err := freeSpentOutput(context.Background(), f.store, f.parent.TxIDChainHash(), 5, f.child.TxIDChainHash(), &bytes.Buffer{})
			require.ErrorContains(t, err, "has no output 5")
			require.True(t, f.spentByChild(t), "P:0 stays spent by C")
		},
	},
}

func TestFreeSpentOutput_SQL(t *testing.T) {
	for i, tc := range freeSpentOutputCases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse("sqlitememory:///free_spent_output_" + string(rune('a'+i)))
			require.NoError(t, err)

			store, err := utxosql.New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), u)
			require.NoError(t, err)

			tc.run(t, newFreeSpentOutputFixture(t, store, uint32(i))) //nolint:gosec // small index
		})
	}
}

// TestFreeSpentOutput_Aerospike is the store the command exists for: on Aerospike no SQL statement
// can free an output by hand. The unspend runs through the Lua UDF, which clears the spend and its
// recorded height together.
func TestFreeSpentOutput_Aerospike(t *testing.T) {
	containerURL, cleanup, err := aerospiketest.InitAerospikeContainer()
	if err != nil {
		if cleanup != nil {
			_ = cleanup()
		}

		t.Skipf("Aerospike container not available: %v", err)
	}

	t.Cleanup(func() { _ = cleanup() })

	u, err := url.Parse(containerURL)
	require.NoError(t, err)

	store, err := utxoaerospike.New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), u)
	require.NoError(t, err)

	for i, tc := range freeSpentOutputCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, newFreeSpentOutputFixture(t, store, uint32(100+i))) //nolint:gosec // small index
		})
	}
}
