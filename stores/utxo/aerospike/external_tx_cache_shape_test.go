package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// newCachedExternalStore builds a Store with the external tx cache enabled (its
// production default) whose external blob store holds the full .tx blob of a
// transaction with one spendable and one provably-unspendable output.
//
// The unspendable output is what makes the two reconstructions observably
// different: getExternalOutpoints applies the era-aware ShouldStoreOutputAsUTXO
// rule and nils it out, while the full-transaction read keeps it.
func newCachedExternalStore(t *testing.T) (*Store, *bt.Tx, uint32) {
	t.Helper()

	chainParams := chaincfg.RegressionNetParams // GenesisActivationHeight = 10000
	tSettings := &settings.Settings{}
	tSettings.ChainCfgParams = &chainParams

	mem := memory.New()

	s := &Store{
		ctx:           context.Background(),
		externalStore: mem,
		settings:      tSettings,
		logger:        ulogger.TestLogger{},
	}
	s.SetExternalTxCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](10 * time.Second))
	s.SetExternalOutpointsCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](10 * time.Second))

	mkScript := func(b []byte) *bscript.Script {
		sc := bscript.Script(b)
		return &sc
	}

	tx := bt.NewTx()
	require.NoError(t, tx.From(
		"0000000000000000000000000000000000000000000000000000000000000001",
		7,
		"76a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac",
		5000,
	))

	// 0 and 2 are spendable; 1 is OP_FALSE OP_RETURN, provably unspendable in
	// every era, so create never stored it as a UTXO and the outpoint
	// reconstruction must nil it.
	tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: mkScript([]byte{0x76, 0xa9, 0x14, 0x01, 0x02, 0x03})})
	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: mkScript([]byte{bscript.OpFALSE, bscript.OpRETURN, 0xbe, 0xef})})
	tx.AddOutput(&bt.Output{Satoshis: 2000, LockingScript: mkScript([]byte{0x76, 0xa9, 0x14, 0x04, 0x05, 0x06})})

	txHash := *tx.TxIDChainHash()
	require.NoError(t, mem.Set(context.Background(), txHash[:], fileformat.FileTypeTx, tx.ExtendedBytes()))

	// Post-Genesis on regtest.
	return s, tx, 20000
}

// TestExternalTxCache_ShapesDoNotAlias pins that the two external-store
// reconstructions never see each other's results.
//
// GetTxFromExternalStore returns the transaction. GetOutpointsFromExternalStore
// returns a deliberately mutilated copy for outpoint resolution: inputs stripped
// and every era-unspendable output nil'd. Both used to GetOrSet the same cache
// under the same txid, so whichever ran first won for the TTL and the other
// caller silently received the wrong shape.
//
// This is not prevented by the numberOfActiveOutputs < 2 "do not cache" guard:
// util.ExpiringConcurrentCache.GetOrSet also shares the in-flight waiter map, and
// a waiter returns the other goroutine's value regardless of whether that fetch
// asked for it to be cached.
func TestExternalTxCache_ShapesDoNotAlias(t *testing.T) {
	t.Run("outpoints read first must not poison the full-transaction read", func(t *testing.T) {
		s, tx, creationHeight := newCachedExternalStore(t)
		txHash := *tx.TxIDChainHash()

		outpoints, err := s.GetOutpointsFromExternalStore(context.Background(), txHash, creationHeight)
		require.NoError(t, err)
		require.Empty(t, outpoints.Inputs, "the outpoint reconstruction strips inputs")

		full, err := s.GetTxFromExternalStore(context.Background(), txHash)
		require.NoError(t, err)

		require.Len(t, full.Inputs, 1,
			"the full-transaction read must not receive the inputs-stripped outpoint reconstruction")
		require.Equal(t, uint32(7), full.Inputs[0].PreviousTxOutIndex)
		require.True(t, full.TxIDChainHash().IsEqual(&txHash),
			"the full-transaction read must return something that hashes to the requested txid")
	})

	t.Run("full-transaction read first must not poison the outpoints read", func(t *testing.T) {
		s, tx, creationHeight := newCachedExternalStore(t)
		txHash := *tx.TxIDChainHash()

		full, err := s.GetTxFromExternalStore(context.Background(), txHash)
		require.NoError(t, err)
		require.Len(t, full.Inputs, 1)

		outpoints, err := s.GetOutpointsFromExternalStore(context.Background(), txHash, creationHeight)
		require.NoError(t, err)

		require.Len(t, outpoints.Outputs, 3)
		require.Nil(t, outpoints.Outputs[1],
			"the outpoints read must apply the era-unspendable filter, not inherit the unfiltered full transaction")
		require.NotNil(t, outpoints.Outputs[0])
		require.NotNil(t, outpoints.Outputs[2])
	})
}
