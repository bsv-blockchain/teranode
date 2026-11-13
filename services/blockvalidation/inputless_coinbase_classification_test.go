//go:build bvgaps

package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

const (
	// inputlessCoinbasePayoutAddress receives the single output of the inputless coinbase.
	inputlessCoinbasePayoutAddress = "1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"
	// inputlessCoinbasePayoutSatoshis is the 50 BSV paid by that single output.
	inputlessCoinbasePayoutSatoshis = 50 * 100_000_000
	// inputlessCoinbaseServingPeerID and inputlessCoinbaseServingPeerURL name the peer the block came from.
	inputlessCoinbaseServingPeerID  = "peer-serving"
	inputlessCoinbaseServingPeerURL = "http://localhost"
)

// inputlessCoinbaseSettings is the default non-optimistic configuration both paths run under.
func inputlessCoinbaseSettings(t *testing.T) *settings.Settings {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	return tSettings
}

// boundBlockWithInputlessCoinbase builds a coinbase-only block whose only transaction has ZERO
// inputs and one output, with a header (PoW against the chain's expected bits, current timestamp)
// that commits to exactly that transaction (merkle root = its txid). The body is therefore bound:
// the header's author committed to a coinbase with no inputs (bitcoin-sv: bad-txns-vin-empty).
func boundBlockWithInputlessCoinbase(ctx context.Context, t *testing.T, tSettings *settings.Settings, server *BlockValidation, height uint32) *model.Block {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(inputlessCoinbasePayoutAddress, inputlessCoinbasePayoutSatoshis))
	require.Empty(t, coinbaseTx.Inputs, "fixture precondition: a present coinbase with no inputs")

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expectedBits, err := server.blockchainClient.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)

	header := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expectedBits, timestamp)

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	// The header commits to this body: the coinbase-only binding holds.
	require.NoError(t, block.CheckCoinbaseOnlyBodyBound(), "fixture precondition: the body is bound to the header")

	return block
}

// requireInputlessCoinbaseClassedInvalid asserts that a present, bound coinbase with no inputs was
// rejected as ERR_BLOCK_INVALID and not as ERR_BLOCK_INCOMPLETE.
func requireInputlessCoinbaseClassedInvalid(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrBlockIncomplete),
		"only an absent coinbase is INCOMPLETE, got: %v", err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid),
		"a present, bound, malformed coinbase must be ERR_BLOCK_INVALID, got: %v", err)
}

// TestKnownDefect_InputlessCoinbase_IsInvalidNotIncomplete checks that a coinbase which is present
// but has no inputs is classed as an invalid block, not an incomplete one.
//
// Spec: BV-BODY-005 and AC-BV-BODY-005.1 (tracked as GAP-BV-046).
//
// Scenario: Given a block at height 100 whose only transaction is a coinbase with zero inputs and
// one 50 BSV output, and whose header commits to that coinbase (the body is bound), When the block
// is validated on the full path or on the quick path, Then it is ERR_BLOCK_INVALID and not
// ERR_BLOCK_INCOMPLETE. Only an ABSENT coinbase is incomplete; a present coinbase with no inputs is
// malformed (BV-BODY-007 "at least one input").
//
// Today: on the full path BlockValidation.go ValidateBlockWithOptions (~1778-1791) tests
// `len(block.CoinbaseTx.Inputs) == 0` together with the nil check and returns
// NewBlockIncompleteError before block.Valid (whose IsConsensusCoinbase check, model/Block.go step
// 4a, would have classified it INVALID) ever runs. The quick path (quick_validate.go
// quickValidateBlock ~268) has the same `len(Inputs) == 0` test.
func TestKnownDefect_InputlessCoinbase_IsInvalidNotIncomplete(t *testing.T) {
	initPrometheusMetrics()

	const blockHeight = uint32(100)

	t.Run("full validation rejects it as invalid, not incomplete", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Given a bound block whose coinbase has no inputs
		tSettings := inputlessCoinbaseSettings(t)
		server, _ := newNoPersistHarness(ctx, t, tSettings)
		block := boundBlockWithInputlessCoinbase(ctx, t, tSettings, server, blockHeight)

		// When it goes through full validation
		err := server.ValidateBlockWithOptions(ctx, block, inputlessCoinbaseServingPeerURL, &ValidateBlockOptions{PeerID: inputlessCoinbaseServingPeerID})

		// Then it is invalid, not incomplete
		requireInputlessCoinbaseClassedInvalid(t, err)
	})

	t.Run("quick validation rejects it as invalid, not incomplete", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Given a bound block whose coinbase has no inputs
		tSettings := inputlessCoinbaseSettings(t)
		server, _ := newNoPersistHarness(ctx, t, tSettings)
		block := boundBlockWithInputlessCoinbase(ctx, t, tSettings, server, blockHeight)

		// When it goes through quick validation
		err := server.quickValidateBlock(ctx, block, inputlessCoinbaseServingPeerID, inputlessCoinbaseServingPeerURL)

		// Then it is invalid, not incomplete
		requireInputlessCoinbaseClassedInvalid(t, err)
	})
}
