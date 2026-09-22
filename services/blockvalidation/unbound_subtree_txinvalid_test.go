package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	p2pconstants "github.com/bsv-blockchain/teranode/interfaces/p2p"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_SubtreeTxInvalid_IsCorruptNotPersisted closes the cheapest entry point of
// bitcoin-sv/teranode#4844: relay a genuine, fully-worked header and serve a doctored subtree list
// containing an invalid transaction.
//
// Subtree validation runs ABOVE block.Valid, so nothing has reconciled the subtree list against the
// header's merkle root — the body is still whatever the serving peer chose. Persisting on that
// verdict stored the peer's coinbase as a miner tag AND poisoned a real block hash, so the honest
// body was afterwards refused as already-invalid. The verdict must therefore strike the peer and
// leave no record at all.
func TestValidateBlock_SubtreeTxInvalid_IsCorruptNotPersisted(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newHarnessWithSubtreeResult(ctx, t, tSettings,
		errors.NewTxInvalidError("transaction in subtree is invalid"))

	fake := &corruptStrikeP2PClient{}
	bv.p2pClient = fake

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	// The genuine header commits an honest coinbase-only body.
	honestCoinbase := coinbaseAtHeight(t, blockHeight)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, honestCoinbase.TxIDChainHash(), *expected, timestamp)

	honest, err := model.NewBlock(hdr, honestCoinbase, []*chainhash.Hash{}, 1, uint64(honestCoinbase.Size()), blockHeight, 0)
	require.NoError(t, err)

	// The doctored delivery: the same header, an attacker-chosen subtree list, and a coinbase
	// carrying the markup payload. Subtree validation reports an invalid transaction in it.
	attackerCoinbase := canaryCoinbaseAtHeight(t, blockHeight)
	tampered, err := model.NewBlock(hdr, attackerCoinbase, []*chainhash.Hash{{0xAB}}, 2,
		uint64(attackerCoinbase.Size()), blockHeight, 0)
	require.NoError(t, err)
	require.True(t, tampered.Hash().IsEqual(honest.Hash()), "the tampering must not change the block hash")

	err = bv.ValidateBlockWithOptions(ctx, tampered, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err),
		"an invalid transaction in an unbound subtree list is a verdict on the delivery, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "the genuine hash must never be condemned")

	requireNothingPersisted(ctx, t, client, tampered.Hash())

	calls := fake.recorded()
	require.Len(t, calls, 1, "the serving peer must be struck exactly once")
	require.Equal(t, "peer-serving", calls[0].peerID)
	require.Equal(t, p2pconstants.ReasonCorruptBlockBody.String(), calls[0].reason)

	// The second delivery must be a fresh validation, not the previous result replayed from the
	// once-per-block grace window.
	time.Sleep(2 * validationResultGrace)

	// The honest body carries no subtrees, so subtree validation is never consulted for it — the
	// genuine hash was never poisoned and the block is accepted.
	err = bv.ValidateBlockWithOptions(ctx, honest, "http://localhost", &ValidateBlockOptions{PeerID: "peer-honest"})
	require.NoError(t, err, "the honest body for the same header must still be accepted")

	_, meta, err := client.GetBlockHeader(ctx, honest.Hash())
	require.NoError(t, err)
	require.False(t, meta.Invalid)
}

// TestValidateBlock_SubtreeTxInvalid_InfrastructureErrorDoesNotStrikePeer covers the guard inside
// that branch. processTransactionsInLevels wraps its failures, so an invalid-transaction verdict can
// arrive carrying a purely local cause — a cancelled context, a storage failure. Neither is evidence
// of peer misconduct: the peer must not be struck, the verdict must not be read as a corrupt body,
// and nothing may be persisted.
func TestValidateBlock_SubtreeTxInvalid_InfrastructureErrorDoesNotStrikePeer(t *testing.T) {
	initPrometheusMetrics()

	for _, tc := range []struct {
		name  string
		cause error
		is    error
	}{
		{name: "cancelled context", cause: context.Canceled, is: context.Canceled},
		{name: "storage failure", cause: errors.NewStorageError("utxo store unavailable"), is: errors.ErrStorageError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.OptimisticMining = false
			tSettings.ChainCfgParams.Checkpoints = nil

			bv, client := newHarnessWithSubtreeResult(ctx, t, tSettings,
				errors.NewTxInvalidError("transaction validation failed", tc.cause))

			fake := &corruptStrikeP2PClient{}
			bv.p2pClient = fake

			const blockHeight = uint32(1)

			timestamp := uint32(time.Now().Unix()) //nolint:gosec

			expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
			require.NoError(t, err)
			require.NotNil(t, expected)

			coinbaseTx := coinbaseAtHeight(t, blockHeight)
			hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

			block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{{0xAB}}, 2,
				uint64(coinbaseTx.Size()), blockHeight, 0)
			require.NoError(t, err)

			err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
			require.Error(t, err)
			require.True(t, errors.Is(err, tc.is), "the local cause must survive so callers can see it is not the peer's fault, got: %v", err)
			require.False(t, errors.IsBlockCorrupt(err), "a local failure must not be read as a corrupt body")
			require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a local failure must never condemn the hash")

			require.Empty(t, fake.recorded(), "a local failure must not strike the serving peer")

			requireNothingPersisted(ctx, t, client, block.Hash())
		})
	}
}
