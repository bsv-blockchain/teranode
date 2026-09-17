// Package alert implements the BSV Blockchain alert system server and related functionality.
package alert

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bn/models"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestEnforceAtHeightWindow pins the mapping from an alert's enforceAtHeight range onto
// the stored window, through utxo.NormalizeFreezeWindow. Before issue #1422 only Stop
// was read, and only to decide freeze-vs-unfreeze against the live tip; Start was
// discarded entirely, which is what let alert-gossip timing decide whether a block was
// valid. The mapping follows SV Node: stop is an exclusive end, stop <= start is an empty
// interval, and "no end" is a stop the chain never reaches.
func TestEnforceAtHeightWindow(t *testing.T) {
	fundWith := func(enforce ...models.Enforce) models.Fund {
		return models.Fund{
			TxOut:           models.TxOut{TxId: "00", Vout: 0},
			EnforceAtHeight: enforce,
		}
	}

	t.Run("start is carried through, not discarded", func(t *testing.T) {
		from, until, err := enforceAtHeightWindow(fundWith(models.Enforce{Start: 800_000, Stop: 800_100}))
		require.NoError(t, err)
		require.Equal(t, uint32(800_000), from, "the start height must reach the store")
		require.Equal(t, uint32(800_100), until)
	})

	t.Run("a stop the chain never reaches is effectively unbounded", func(t *testing.T) {
		from, until, err := enforceAtHeightWindow(fundWith(models.Enforce{Start: 800_000, Stop: 1 << 40}))
		require.NoError(t, err)
		require.Equal(t, uint32(800_000), from)
		require.Equal(t, uint32(utxo.FreezeWindowNever), until)
		require.True(t, utxo.FreezeWindowActiveAt(from, until, 1_000_000_000))
	})

	// The pre-#1422 unfreeze idiom, and SV Node's empty interval: never consensus-active.
	// Whether the policy tier lifts too is the alert's policyExpiresWithConsensus, applied
	// by the store, not here.
	for _, tc := range []struct {
		name        string
		start, stop int
	}{
		{name: "(0, 0)", start: 0, stop: 0},
		{name: "stop equal to start", start: 100, stop: 100},
		{name: "stop below start", start: 200, stop: 100},
		{name: "a zero stop with a positive start", start: 500, stop: 0},
	} {
		t.Run("an empty interval is never consensus-active: "+tc.name, func(t *testing.T) {
			from, until, err := enforceAtHeightWindow(fundWith(models.Enforce{Start: tc.start, Stop: tc.stop}))
			require.NoError(t, err)
			require.Equal(t, uint32(utxo.FreezeWindowNever), from)
			require.Equal(t, uint32(utxo.FreezeWindowNever), until)

			for _, h := range []uint32{0, 1, 100, 500, 1_000_000} {
				require.False(t, utxo.FreezeWindowActiveAt(from, until, h), "active at %d", h)
			}
		})
	}

	t.Run("no range at all is an empty interval, never enforce-everywhere", func(t *testing.T) {
		from, until, err := enforceAtHeightWindow(fundWith())
		require.NoError(t, err)
		require.Equal(t, uint32(utxo.FreezeWindowNever), from)
		require.Equal(t, uint32(utxo.FreezeWindowNever), until)
	})

	// The alert wire format carries exactly one range per fund; more than one cannot be
	// represented as a single window, and silently keeping the first would enforce a
	// freeze over heights the authority did not ask for.
	t.Run("more than one range is rejected", func(t *testing.T) {
		_, _, err := enforceAtHeightWindow(fundWith(
			models.Enforce{Start: 100, Stop: 200},
			models.Enforce{Start: 300, Stop: 400},
		))
		require.Error(t, err)
	})

	t.Run("a negative bound is rejected", func(t *testing.T) {
		_, _, err := enforceAtHeightWindow(fundWith(models.Enforce{Start: -1, Stop: 100}))
		require.Error(t, err)
	})
}

// TestAddToConsensusBlacklistRecordsWindow checks the end-to-end alert path: a freeze
// whose window starts above the current tip is recorded rather than applied outright,
// and it bites only for blocks at or after its start height.
func TestAddToConsensusBlacklistRecordsWindow(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	const (
		tipHeight   = 101
		windowStart = 150
		windowStop  = 250
	)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.NewErrorTestLogger(t), tSettings, utxoStoreURL)
	require.NoError(t, err)

	require.NoError(t, utxoStore.SetBlockHeight(tipHeight))

	_, err = utxoStore.Create(ctx, tx, tipHeight)
	require.NoError(t, err)

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	node := NewNodeConfig(ulogger.TestLogger{}, nil, utxoStore, nil, nil, nil, tSettings)

	// A freeze whose enforcement starts well above the tip. Before #1422 this was applied
	// immediately and Start was thrown away.
	response, err := node.AddToConsensusBlacklist(ctx, []models.Fund{{
		TxOut:           models.TxOut{TxId: tx.TxIDChainHash().String(), Vout: 0},
		EnforceAtHeight: []models.Enforce{{Start: windowStart, Stop: windowStop}},
	}})
	require.NoError(t, err)
	require.Empty(t, response.NotProcessed)

	// A transaction spending the frozen output. The store does not evaluate scripts, so
	// an unsigned spender is enough to exercise the spend path.
	spendingTx := bt.NewTx()
	require.NoError(t, spendingTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
	}))
	spendingTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})
	require.NoError(t, spendingTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", tx.Outputs[0].Satoshis-1))

	spendAtHeight := func(blockHeight uint32) error {
		_, spends, spendErr := utxoStore.SpendAndCreate(ctx, spendingTx, blockHeight,
			utxo.WithSpendOnly(), utxo.WithIgnorePolicyFreeze(true))
		if spendErr == nil {
			require.NoError(t, utxoStore.Unspend(ctx, spends, false))
			return nil
		}

		require.NotEmpty(t, spends)

		return spends[0].Err
	}

	require.NoError(t, spendAtHeight(windowStart-1),
		"a block below the freeze's start height must still be accepted")
	require.ErrorIs(t, spendAtHeight(windowStart), errors.ErrUtxoConsensusFrozen,
		"a block at the freeze's start height must be rejected")
	require.ErrorIs(t, spendAtHeight(windowStop-1), errors.ErrUtxoConsensusFrozen,
		"a block at the last enforced height must be rejected")
	require.NoError(t, spendAtHeight(windowStop),
		"a block at the freeze's stop height must be accepted again")

	// The policy tier is unchanged: the coin stays out of this node's mempool at every
	// height for as long as it is frozen.
	spendResp, err := utxoStore.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_FROZEN), spendResp.Status)
}
