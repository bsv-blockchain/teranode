package validator

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// These tests read parents from a real sqlitememory store. Where a failure the
// store cannot be made to produce is needed (a fault, a short answer list), a
// wrapper over that same store replaces ParentOutputsForValidation's answer.

// parentOutputsStore opens a sqlitememory UTXO store for t.
func parentOutputsStore(t *testing.T) *sql.Store {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	store, err := sql.New(ctx, ulogger.NewErrorTestLogger(t), tSettings, storeURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(200))

	return store
}

// storedParent creates a parent with one output per script, each worth
// 100*(index+1) satoshis, mined at minedAt, or unmined when minedAt is 0.
func storedParent(t *testing.T, store utxostore.Store, seed byte, minedAt uint32, scripts ...*bscript.Script) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	parent.LockTime = uint32(seed)

	in := &bt.Input{PreviousTxOutIndex: 0, UnlockingScript: bscript.NewFromBytes([]byte{0x00}), SequenceNumber: 0xffffffff}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{0xaa, seed}))
	parent.Inputs = append(parent.Inputs, in)

	for i, s := range scripts {
		parent.AddOutput(&bt.Output{Satoshis: uint64(100 * (i + 1)), LockingScript: s})
	}

	opts := []utxostore.CreateOption{utxostore.WithCreateOnly(), utxostore.WithSkipExtendedInputs(true)}
	if minedAt > 0 {
		opts = append(opts, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: uint32(seed), BlockHeight: minedAt}))
	}

	_, _, err := store.SpendAndCreate(context.Background(), parent, minedAt, opts...)
	require.NoError(t, err)

	return parent
}

// parentOutputsChild builds a child spending (parent[i], vout[i]) for each i, with
// forged previous-output fields on every input, so a test can tell whether the
// validator overwrote them from the store.
func parentOutputsChild(t *testing.T, parents []chainhash.Hash, vouts []uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for i := range parents {
		in := &bt.Input{
			PreviousTxOutIndex: vouts[i],
			PreviousTxScript:   bscript.NewFromBytes([]byte{0x51}), // forged OP_TRUE
			PreviousTxSatoshis: 21_000_000_00000000,
			UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
		}
		require.NoError(t, in.PreviousTxIDAdd(&parents[i]))
		tx.Inputs = append(tx.Inputs, in)
	}

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))

	return tx
}

func parentOutputsValidator(t *testing.T, store utxostore.Store) *Validator {
	return &Validator{settings: test.CreateBaseTestSettings(t), utxoStore: store}
}

// countingParentStore counts ParentOutputsForValidation calls and the outpoints
// they carry, and can replace the real store's answer.
type countingParentStore struct {
	utxostore.Store

	calls     int
	outpoints []utxostore.Outpoint
	override  func([]utxostore.ParentOutput) ([]utxostore.ParentOutput, error)
	callErr   error
}

func (c *countingParentStore) ParentOutputsForValidation(ctx context.Context, outpoints []utxostore.Outpoint, opts ...utxostore.ParentOutputOption) ([]utxostore.ParentOutput, error) {
	c.calls++
	c.outpoints = append(c.outpoints, outpoints...)

	if c.callErr != nil {
		return nil, c.callErr
	}

	answers, err := c.Store.ParentOutputsForValidation(ctx, outpoints, opts...)
	if err != nil || c.override == nil {
		return answers, err
	}

	return c.override(answers)
}

func TestGetUtxoBlockHeightsAndExtendTx_UsesParentOutputs(t *testing.T) {
	ctx := context.Background()
	base := parentOutputsStore(t)

	s0 := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x00})
	s1 := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x01})
	s2 := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x02})
	s3 := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x03})

	mined := storedParent(t, base, 1, 120, s0, s1, s2, s3)
	unmined := storedParent(t, base, 2, 0, s1)

	tx := parentOutputsChild(t, []chainhash.Hash{*mined.TxIDChainHash(), *unmined.TxIDChainHash(), *mined.TxIDChainHash()}, []uint32{3, 0, 1})

	store := &countingParentStore{Store: base}

	heights, err := parentOutputsValidator(t, store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), nil)
	require.NoError(t, err)
	require.Equal(t, []uint32{120, unconfirmedParentHeight, 120}, heights)
	require.Equal(t, 1, store.calls, "one read for the whole transaction")

	// Every input is overwritten from the stored outputs, never trusted as
	// supplied (GHSA-v76m-6vc7-g7c7).
	require.Equal(t, uint64(400), tx.Inputs[0].PreviousTxSatoshis)
	require.Equal(t, s3.Bytes(), tx.Inputs[0].PreviousTxScript.Bytes())
	require.Equal(t, uint64(100), tx.Inputs[1].PreviousTxSatoshis)
	require.Equal(t, s1.Bytes(), tx.Inputs[1].PreviousTxScript.Bytes())
	require.Equal(t, uint64(200), tx.Inputs[2].PreviousTxSatoshis)
	require.Equal(t, s1.Bytes(), tx.Inputs[2].PreviousTxScript.Bytes())
}

func TestGetUtxoBlockHeightsAndExtendTx_ParentOutputFailures(t *testing.T) {
	ctx := context.Background()
	script := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x00})

	t.Run("missing transaction is a missing parent", func(t *testing.T) {
		store := parentOutputsStore(t)
		tx := parentOutputsChild(t, []chainhash.Hash{{0x0e}}, []uint32{0})

		_, err := parentOutputsValidator(t, store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), nil)
		require.ErrorIs(t, err, errors.ErrTxMissingParent)
	})

	t.Run("no such index is invalid", func(t *testing.T) {
		store := parentOutputsStore(t)
		parent := storedParent(t, store, 3, 120, script)
		tx := parentOutputsChild(t, []chainhash.Hash{*parent.TxIDChainHash()}, []uint32{5})

		_, err := parentOutputsValidator(t, store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), nil)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
	})

	injected := []struct {
		name     string
		override func([]utxostore.ParentOutput) ([]utxostore.ParentOutput, error)
		callErr  error
	}{
		{name: "a store fault is a processing error", override: func(a []utxostore.ParentOutput) ([]utxostore.ParentOutput, error) {
			a[0] = utxostore.ParentOutput{Err: errors.NewStorageError("timeout")}
			return a, nil
		}},
		{name: "an unknown answer is a processing error", override: func(a []utxostore.ParentOutput) ([]utxostore.ParentOutput, error) {
			a[0] = utxostore.ParentOutput{}
			return a, nil
		}},
		{name: "a short answer list is a processing error", override: func([]utxostore.ParentOutput) ([]utxostore.ParentOutput, error) {
			return []utxostore.ParentOutput{}, nil
		}},
		{name: "a call-level error is a processing error", callErr: errors.NewServiceUnavailableError("down")},
	}

	for _, tc := range injected {
		t.Run(tc.name, func(t *testing.T) {
			base := parentOutputsStore(t)
			parent := storedParent(t, base, 4, 120, script)
			tx := parentOutputsChild(t, []chainhash.Hash{*parent.TxIDChainHash()}, []uint32{0})

			store := &countingParentStore{Store: base, override: tc.override, callErr: tc.callErr}

			_, err := parentOutputsValidator(t, store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), nil)
			require.ErrorIs(t, err, errors.ErrProcessing)
			require.NotErrorIs(t, err, errors.ErrTxMissingParent, "a fault must never read as a missing parent")
			require.NotErrorIs(t, err, errors.ErrTxInvalid, "a fault must never read as an invalid transaction")
		})
	}
}

// Prefetched parents are read from the map and never sent to the store; they
// report the lowest recorded height, as the store does.
func TestGetUtxoBlockHeightsAndExtendTx_PrefetchedAndStoreMix(t *testing.T) {
	ctx := context.Background()
	base := parentOutputsStore(t)

	stored := storedParent(t, base, 5, 90, bscript.NewFromBytes([]byte{0x6a}))
	p0 := chainhash.Hash{0x01}

	tx := parentOutputsChild(t, []chainhash.Hash{p0, *stored.TxIDChainHash()}, []uint32{0, 0})

	prefetched := map[chainhash.Hash]*meta.Data{
		p0: {BlockHeights: []uint32{126, 125}, Tx: prefetchParentTx(1000)},
	}

	store := &countingParentStore{Store: base}

	heights, err := parentOutputsValidator(t, store).getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), prefetched)
	require.NoError(t, err)
	require.Equal(t, []uint32{125, 90}, heights)
	require.Equal(t, uint64(1000), tx.Inputs[0].PreviousTxSatoshis)
	require.Equal(t, uint64(100), tx.Inputs[1].PreviousTxSatoshis)
	require.Equal(t, []utxostore.Outpoint{{TxID: *stored.TxIDChainHash(), Vout: 0}}, store.outpoints, "the prefetched parent is never sent to the store")
}

// extendTransaction reads through the same method, and overwrites every input.
func TestExtendTransaction_UsesParentOutputs(t *testing.T) {
	ctx := context.Background()
	base := parentOutputsStore(t)

	s := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x05})
	parent := storedParent(t, base, 6, 0, s, s, s)
	tx := parentOutputsChild(t, []chainhash.Hash{*parent.TxIDChainHash()}, []uint32{2})

	store := &countingParentStore{Store: base}

	require.NoError(t, parentOutputsValidator(t, store).extendTransaction(ctx, tx))
	require.Equal(t, uint64(300), tx.Inputs[0].PreviousTxSatoshis)
	require.Equal(t, s.Bytes(), tx.Inputs[0].PreviousTxScript.Bytes())
	require.True(t, tx.IsExtended())
	require.Equal(t, 1, store.calls)
}
