package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestCreationHeightFromBlockHeights pins the era-height derivation used when
// reconstructing an externally-stored parent's spendable outputs: the earliest
// block the tx was mined in, or (for an unmined parent with no block heights)
// the Genesis activation height — unmined txs are necessarily post-Genesis.
func TestCreationHeightFromBlockHeights(t *testing.T) {
	const genesis = uint32(620538)

	require.Equal(t, genesis, creationHeightFromBlockHeights(nil, genesis), "unmined => post-Genesis")
	require.Equal(t, genesis, creationHeightFromBlockHeights([]uint32{}, genesis), "unmined => post-Genesis")
	require.Equal(t, uint32(700000), creationHeightFromBlockHeights([]uint32{700000}, genesis))
	require.Equal(t, uint32(700000), creationHeightFromBlockHeights([]uint32{700002, 700000, 700001}, genesis), "earliest mining height")
}

// TestGetExternalOutpoints_EraAware verifies the external-tx outpoint
// reconstruction nils exactly the provably-unspendable outputs for the parent's
// creation-height era — matching ShouldStoreOutputAsUTXO (era-aware,
// value-agnostic) so the reconstructed set agrees with what create stored.
func TestGetExternalOutpoints_EraAware(t *testing.T) {
	ctx := context.Background()
	chainParams := chaincfg.RegressionNetParams
	tSettings := &settings.Settings{}
	tSettings.ChainCfgParams = &chainParams

	mem := memory.New()
	s := &Store{
		ctx:           ctx,
		externalStore: mem,
		settings:      tSettings,
		logger:        ulogger.TestLogger{},
	}

	mkScript := func(b []byte) *bscript.Script {
		sc := bscript.Script(b)
		return &sc
	}

	tx := bt.NewTx()
	// 0: p2pkh, value-bearing                  -> always kept
	tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: mkScript([]byte{0x76, 0xa9, 0x14, 0x01, 0x02, 0x03})})
	// 1: bare OP_RETURN, 0 value               -> kept post-Genesis, nil'd pre-Genesis
	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: mkScript([]byte{bscript.OpRETURN, 0xde, 0xad})})
	// 2: OP_FALSE OP_RETURN, 0 value           -> nil'd in every era (provably unspendable)
	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: mkScript([]byte{bscript.OpFALSE, bscript.OpRETURN, 0xbe, 0xef})})
	// 3: OP_FALSE OP_TRUE (0x00 0x51), 0 value -> kept (NOT OP_RETURN; the old
	//    era-blind filter wrongly nil'd any 0x00-led 0-value script)
	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: mkScript([]byte{bscript.OpFALSE, bscript.OpTRUE})})

	txHash := *tx.TxIDChainHash()
	require.NoError(t, mem.Set(ctx, txHash[:], fileformat.FileTypeTx, tx.Bytes()))

	// Derive the two probe heights from the params rather than hardcoding them:
	// the regtest activation height has moved before (10000 -> 100) and a fixed
	// literal silently flips this test's era instead of failing.
	genesis := chainParams.GenesisActivationHeight
	require.Positive(t, genesis, "params must leave room for a pre-Genesis height")

	preGenesis, postGenesis := genesis-1, genesis+1

	t.Run("post-genesis", func(t *testing.T) {
		got, n, err := s.getExternalOutpoints(ctx, txHash, postGenesis)
		require.NoError(t, err)
		require.NotNil(t, got.Outputs[0], "p2pkh kept")
		require.NotNil(t, got.Outputs[1], "post-Genesis bare OP_RETURN is spendable -> kept (the fix)")
		require.Nil(t, got.Outputs[2], "OP_FALSE OP_RETURN burned -> nil'd")
		require.NotNil(t, got.Outputs[3], "OP_FALSE OP_TRUE is not OP_RETURN -> kept")
		require.Equal(t, 3, n)
	})

	t.Run("pre-genesis", func(t *testing.T) {
		got, n, err := s.getExternalOutpoints(ctx, txHash, preGenesis)
		require.NoError(t, err)
		require.NotNil(t, got.Outputs[0], "p2pkh kept")
		require.Nil(t, got.Outputs[1], "pre-Genesis bare OP_RETURN is unspendable -> nil'd")
		require.Nil(t, got.Outputs[2], "OP_FALSE OP_RETURN burned -> nil'd")
		require.NotNil(t, got.Outputs[3], "OP_FALSE OP_TRUE is not OP_RETURN -> kept")
		require.Equal(t, 2, n)
	})
}
