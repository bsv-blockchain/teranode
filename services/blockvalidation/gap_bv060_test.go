//go:build bvgaps

package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// gap060Settings is the default non-optimistic configuration with the BIP34 version floor active
// from height 1, so a version-1 block at height 1 fails BV-HEADER-008. CreateBaseTestSettings hands
// out its own copy of the chain parameters, so the override does not leak into other tests.
func gap060Settings(t *testing.T) *settings.Settings {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil
	tSettings.ChainCfgParams.BIP0034Height = 1

	return tSettings
}

// TestGapBV060_UnboundHeaderFinalVerdictPublishesNotification covers GAP-BV-060 (BV-HEADER-009,
// BV-VERDICT-011).
//
// Spec-correct behaviour: a header verdict under BV-HEADER-008 (version floor) depends on the header
// alone, so it is HEADER-FINAL: not persisted, and the invalid-block notification is published
// whether or not the body was bound. Expected to fail today: on the non-optimistic path the version
// check runs inside block.Valid before the merkle binding, and the catch-all in BlockValidation.go
// (~2559-2575) only logs "not persisting invalid verdict" without publishing.
func TestGapBV060_UnboundHeaderFinalVerdictPublishesNotification(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := gap060Settings(t)
	bv, client := newNoPersistHarness(ctx, t, tSettings)

	producer := &SafeMockKafkaProducer{}
	bv.invalidBlockKafkaProducer = producer

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

	// Re-mine at version 1, below the BIP34 floor.
	hdr.Version = 1
	for hdr.Nonce = 0; ; hdr.Nonce++ {
		if ok, _, _ := hdr.HasMetTargetDifficulty(); ok {
			break
		}

		require.Less(t, hdr.Nonce, uint32(50_000_000), "could not mine the fixture header")
	}

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})

	// Preconditions: the failure is the version-floor verdict, and it is (correctly) not persisted.
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "precondition: expected the version-floor INVALID verdict, got: %v", err)
	require.ErrorContains(t, err, "bad-version")
	requireNothingPersisted(ctx, t, client, block.Hash())

	require.True(t, producer.IsPublishCalled(),
		"GAP-BV-060: the HEADER-FINAL version-floor verdict was not published as an invalid-block notification")
}

// TestGapBV060_Control_BoundInvalidBodyPublishesNotification is the control for the test above: on
// the same harness and path, a bound invalid body is persisted and published, so the producer
// wiring the GAP-BV-060 assertion relies on works.
func TestGapBV060_Control_BoundInvalidBodyPublishesNotification(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := gap060Settings(t)
	bv, client := newNoPersistHarness(ctx, t, tSettings)

	producer := &SafeMockKafkaProducer{}
	bv.invalidBlockKafkaProducer = producer

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)

	coinbaseTx := overpayingCanaryCoinbase(t, blockHeight)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected a bound INVALID verdict, got: %v", err)
	require.True(t, producer.IsPublishCalled(), "control: a bound invalid body must publish the notification")
}
