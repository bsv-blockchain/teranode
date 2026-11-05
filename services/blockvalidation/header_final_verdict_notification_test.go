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

const (
	// versionFloorActivationHeight is where the BIP34 version floor starts to apply.
	versionFloorActivationHeight = 1
	// versionFloorBlockHeight is the height of the block under test, inside the version floor.
	versionFloorBlockHeight = uint32(1)
	// versionBelowFloor is a header version the BIP34 floor rejects.
	versionBelowFloor = uint32(1)
	// versionFloorMaxNonce bounds the search when re-mining the fixture header.
	versionFloorMaxNonce = uint32(50_000_000)
	// versionFloorServingPeerID and versionFloorServingPeerURL name the peer the block came from.
	versionFloorServingPeerID  = "peer-serving"
	versionFloorServingPeerURL = "http://localhost"
)

// versionFloorSettings is the default non-optimistic configuration with the BIP34 version floor
// active from height 1, so a version-1 block at height 1 fails BV-HEADER-008. CreateBaseTestSettings
// hands out its own copy of the chain parameters, so the override does not leak into other tests.
func versionFloorSettings(t *testing.T) *settings.Settings {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil
	tSettings.ChainCfgParams.BIP0034Height = versionFloorActivationHeight

	return tSettings
}

// remineHeaderAtVersion sets the header's version and searches nonces from zero until the header
// meets its target again.
func remineHeaderAtVersion(t *testing.T, header *model.BlockHeader, version uint32) {
	t.Helper()

	header.Version = version
	for header.Nonce = 0; ; header.Nonce++ {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}

		require.Less(t, header.Nonce, versionFloorMaxNonce, "could not mine the fixture header")
	}
}

// TestKnownDefect_HeaderFinalVerdict_PublishesInvalidBlockNotification checks that a verdict which
// depends on the header alone is announced to the network even when the body was never bound.
//
// Spec: BV-HEADER-009 and BV-VERDICT-011 (tracked as GAP-BV-060).
//
// Scenario: Given the non-optimistic path with the BIP34 version floor active from height 1, and a
// version-1 block at height 1 (below the floor), When the block is validated, Then the
// version-floor verdict is ERR_BLOCK_INVALID ("bad-version"), nothing is persisted, and the
// invalid-block notification is published. A header verdict under BV-HEADER-008 is HEADER-FINAL:
// not persisted, but published whether or not the body was bound.
//
// Today: on the non-optimistic path the version check runs inside block.Valid before the merkle
// binding, and the catch-all in BlockValidation.go ValidateBlockWithOptions (~2559-2575) only logs
// "not persisting invalid verdict" without publishing.
//
// Controls: on the same harness and path, a bound block whose coinbase overpays is persisted and
// published, so the producer wiring the main assertion relies on works.
func TestKnownDefect_HeaderFinalVerdict_PublishesInvalidBlockNotification(t *testing.T) {
	initPrometheusMetrics()

	t.Run("a version-floor verdict on an unbound body publishes the notification without persisting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Given the non-optimistic path with the version floor active, and a recording producer
		tSettings := versionFloorSettings(t)
		server, chain := newNoPersistHarness(ctx, t, tSettings)

		producer := &SafeMockKafkaProducer{}
		server.invalidBlockKafkaProducer = producer

		timestamp := uint32(time.Now().Unix()) //nolint:gosec

		expectedBits, err := chain.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
		require.NoError(t, err)
		require.NotNil(t, expectedBits)

		// and a block at the floor height whose header is mined at a version below the floor
		coinbaseTx := canaryCoinbaseAtHeight(t, versionFloorBlockHeight)
		header := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expectedBits, timestamp)
		remineHeaderAtVersion(t, header, versionBelowFloor)

		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), versionFloorBlockHeight, 0)
		require.NoError(t, err)

		// When the block is validated
		err = server.ValidateBlockWithOptions(ctx, block, versionFloorServingPeerURL, &ValidateBlockOptions{PeerID: versionFloorServingPeerID})

		// Then the failure is the version-floor verdict, and it is (correctly) not persisted
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "precondition: expected the version-floor INVALID verdict, got: %v", err)
		require.ErrorContains(t, err, "bad-version")
		requireNothingPersisted(ctx, t, chain, block.Hash())

		// and the invalid-block notification is published
		require.True(t, producer.IsPublishCalled(),
			"the header-final version-floor verdict was not published as an invalid-block notification")
	})

	t.Run("control: a bound invalid body publishes the notification", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Given the same harness and a recording producer
		tSettings := versionFloorSettings(t)
		server, chain := newNoPersistHarness(ctx, t, tSettings)

		producer := &SafeMockKafkaProducer{}
		server.invalidBlockKafkaProducer = producer

		timestamp := uint32(time.Now().Unix()) //nolint:gosec

		expectedBits, err := chain.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
		require.NoError(t, err)

		// and a bound block whose coinbase overpays
		coinbaseTx := overpayingCanaryCoinbase(t, versionFloorBlockHeight)
		header := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expectedBits, timestamp)

		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), versionFloorBlockHeight, 0)
		require.NoError(t, err)

		// When the block is validated
		err = server.ValidateBlockWithOptions(ctx, block, versionFloorServingPeerURL, &ValidateBlockOptions{PeerID: versionFloorServingPeerID})

		// Then it is a bound INVALID verdict and the notification is published
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected a bound INVALID verdict, got: %v", err)
		require.True(t, producer.IsPublishCalled(), "a bound invalid body must publish the notification")
	})
}
