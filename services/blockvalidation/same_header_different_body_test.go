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
	p2pconstants "github.com/bsv-blockchain/teranode/interfaces/p2p"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// oversizedCoinbase builds a coinbase whose unlocking script is 101 bytes — one past the consensus
// bound — while remaining a well-formed coinbase in every other respect.
func oversizedCoinbase(t *testing.T, height uint32) *bt.Tx {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	scriptSig := util.EncodeCoinbaseHeightPush(height)
	for len(scriptSig) < 101 {
		scriptSig = append(scriptSig, 'A')
	}

	require.Len(t, scriptSig, 101)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))
	require.True(t, coinbaseTx.IsCoinbase())

	return coinbaseTx
}

// TestValidateBlock_SameHeaderDifferentBody_HonestBodyStillAccepted is the empirical
// re-verification of the audit's same-header/different-body proof (bitcoin-sv/teranode#4844).
//
// A peer keeps a genuine header — the hash nobody can substitute — and serves a body that does not
// reconcile to it. That verdict is about the DELIVERY, not the block, so it must not condemn the
// hash: the peer is struck, nothing is persisted, and the honest body for the same header is still
// accepted on a later delivery.
func TestValidateBlock_SameHeaderDifferentBody_HonestBodyStillAccepted(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	fake := &corruptStrikeP2PClient{}
	bv.p2pClient = fake

	// Height 1 on genesis: the height the store derives from the parent, so the accepted delivery
	// below passes the store's own coinbase-height guard.
	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	// The genuine header commits the honest coinbase-only body: for a single-transaction block the
	// header merkle root IS the coinbase txid.
	honestCoinbase := coinbaseAtHeight(t, blockHeight)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, honestCoinbase.TxIDChainHash(), *expected, timestamp)

	honest, err := model.NewBlock(hdr, honestCoinbase, []*chainhash.Hash{}, 1, uint64(honestCoinbase.Size()), blockHeight, 0)
	require.NoError(t, err)

	// The doctored delivery: same header, so the same block hash and the same proof of work, but a
	// coinbase one byte past the consensus scriptSig bound.
	attackerCoinbase := oversizedCoinbase(t, blockHeight)
	tampered, err := model.NewBlock(hdr, attackerCoinbase, []*chainhash.Hash{}, 1, uint64(attackerCoinbase.Size()), blockHeight, 0)
	require.NoError(t, err)
	require.True(t, tampered.Hash().IsEqual(honest.Hash()), "the tampering must not change the block hash")

	err = bv.ValidateBlockWithOptions(ctx, tampered, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "an unbound body is a verdict on the delivery, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "the genuine hash must never be condemned on an unbound body")

	exists, err := client.GetBlockExists(ctx, tampered.Hash())
	require.NoError(t, err)
	require.False(t, exists, "nothing may be persisted, or the honest body is later refused as already-invalid")

	calls := fake.recorded()
	require.Len(t, calls, 1, "the serving peer must be struck exactly once")
	require.Equal(t, "peer-serving", calls[0].peerID)
	require.Equal(t, p2pconstants.ReasonCorruptBlockBody.String(), calls[0].reason)

	// The second delivery must be a fresh validation, not the previous result replayed from the
	// once-per-block grace window.
	time.Sleep(2 * validationResultGrace)

	err = bv.ValidateBlockWithOptions(ctx, honest, "http://localhost", &ValidateBlockOptions{PeerID: "peer-honest"})
	require.NoError(t, err, "the honest body for the same header must still be accepted")

	exists, err = client.GetBlockExists(ctx, honest.Hash())
	require.NoError(t, err)
	require.True(t, exists)

	_, meta, err := client.GetBlockHeader(ctx, honest.Hash())
	require.NoError(t, err)
	require.False(t, meta.Invalid, "the accepted block must not carry an invalid verdict")
}
