package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bec"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// overpayingCanaryCoinbase builds a coinbase that encodes the given height, carries the markup
// payload, and pays far more than the block subsidy — a consensus failure that block.Valid reaches
// BELOW the merkle binding, so the verdict is about the miner's own committed body.
func overpayingCanaryCoinbase(t *testing.T, height uint32) *bt.Tx {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	scriptSig := util.EncodeCoinbaseHeightPush(height)
	scriptSig = append(scriptSig, minerMarkupCanary...)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 1000*100000000))
	require.True(t, coinbaseTx.IsCoinbase())

	return coinbaseTx
}

// TestValidateBlock_BoundInvalidBody_IsPersistedInvalid is the positive control for the binding
// gate, and it is not optional (bitcoin-sv/teranode#4844). Without it the gate could be over-broad —
// declining to persist EVERY invalid block — with every other test still green.
//
// A body that does reconcile to the header's merkle root IS the miner's committed body. A consensus
// failure in it is genuine invalidity, the hash is condemned, and the full record is written so
// RevalidateBlock can still reconsider it.
func TestValidateBlock_BoundInvalidBody_IsPersistedInvalid(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	// Single-transaction block: the header merkle root IS the coinbase txid, which is a real
	// binding.
	coinbaseTx := overpayingCanaryCoinbase(t, blockHeight)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound body's consensus failure condemns the hash, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err))

	exists, err := client.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, exists, "a bound invalid body must still be persisted, or the verdict is forgotten")

	_, meta, err := client.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, meta.Invalid, "the stored row must carry the invalid verdict")

	stored, err := client.GetBlock(ctx, block.Hash())
	require.NoError(t, err)
	require.Equal(t, coinbaseTx.TxIDChainHash().String(), stored.CoinbaseTx.TxIDChainHash().String(),
		"the full body is kept so RevalidateBlock can reconsider it")
}

// TestValidateBlock_UnboundContextualFailure_IsNotPersisted closes the conditionally-unbound
// persist (bitcoin-sv/teranode#4844). block.Valid runs its contextual header checks BEFORE the
// merkle binding, so a contextual failure reaches the catch-all with a body THIS validation has not
// reconciled to the header — and the two-hours-in-the-future rule it can fail is reversible on the
// wall clock alone, so a stored verdict would be doubly wrong.
//
// Asserted only through what this layer can observe. ValidateBlockWithOptions returns just an error,
// and the binding fact is deliberately local to the ValidWithBinding call inside it; the value
// itself is asserted one level down by TestBlock_ValidWithBinding_ReportsTheBinding. This test
// proves the gate that consumes it actually took effect.
func TestValidateBlock_UnboundContextualFailure_IsNotPersisted(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	// Non-optimistic: the contextual checks then run only inside block.Valid, above the binding,
	// which is the path under test.
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Add(3 * time.Hour).Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
	require.Error(t, err)
	require.ErrorContains(t, err, "two hours in the future")

	requireNothingPersisted(ctx, t, client, block.Hash())
}
