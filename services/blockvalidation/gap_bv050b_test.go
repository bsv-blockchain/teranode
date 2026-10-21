//go:build bvgaps

package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// gap050bPublishRecorder is a KafkaAsyncProducerI that records every published message, so a test
// can observe whether an invalid-block notification (which drives p2p ban scoring) was emitted.
type gap050bPublishRecorder struct {
	mu   sync.Mutex
	msgs []*kafka.Message
}

func (r *gap050bPublishRecorder) Start(_ context.Context, _ chan *kafka.Message) {}
func (r *gap050bPublishRecorder) Stop() error                                    { return nil }
func (r *gap050bPublishRecorder) BrokersURL() []string                           { return nil }

func (r *gap050bPublishRecorder) Publish(m *kafka.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.msgs = append(r.msgs, m)
}

func (r *gap050bPublishRecorder) TryPublish(m *kafka.Message) bool {
	r.Publish(m)

	return true
}

func (r *gap050bPublishRecorder) published() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.msgs)
}

// gap050bBanRecorder records AddBanScore calls made directly on the P2P client.
type gap050bBanRecorder struct {
	maliciousAbortP2PClient

	mu   sync.Mutex
	bans int
}

func (r *gap050bBanRecorder) AddBanScore(_ context.Context, _ string, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.bans++

	return nil
}

func (r *gap050bBanRecorder) banCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.bans
}

// TestGapBV050b_FutureDatedBlockIsNotPersistedNotifiedNorStruck asserts BV-HEADER-006 /
// AC-BV-HEADER-006.1.
//
// GAP-BV-050(b): a block timestamped now+3h depends on the local clock, so it is not a verdict on
// the block: it MUST NOT be persisted, MUST NOT publish the invalid-block notification (the p2p
// consumer turns that into a peer strike), and MUST NOT strike or report the peer. Fails today:
// the block is rejected ERR_BLOCK_INVALID by model/Block.go CheckHeaderContextual (~699), and
// BlockValidation.go treats it as a final header verdict: rejectFinalHeaderVerdict ->
// kafkaNotifyBlockInvalid publishes the notification (optimistic branch ~2246), while the
// non-optimistic branch persists it via storeInvalidBlock (which also notifies). Both
// optimistic-mining modes are asserted as subtests. Observed: the non-optimistic subtest PASSES
// (Valid fails before the merkle binding, so bodyBoundToHeader is false and nothing is persisted or
// notified); only the optimistic subtest fails.
func TestGapBV050b_FutureDatedBlockIsNotPersistedNotifiedNorStruck(t *testing.T) {
	initPrometheusMetrics()

	for _, optimistic := range []bool{false, true} {
		name := "non-optimistic"
		if optimistic {
			name = "optimistic"
		}

		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.OptimisticMining = optimistic
			tSettings.BlockValidation.OptimisticMiningPeerBlocks = optimistic

			bv, client := newNoPersistHarness(ctx, t, tSettings)

			kafkaRec := &gap050bPublishRecorder{}
			bv.invalidBlockKafkaProducer = kafkaRec

			banRec := &gap050bBanRecorder{}
			bv.p2pClient = banRec

			const blockHeight = uint32(100)

			// Three hours ahead: past the 7200 s bound, and reversible by the wall clock alone.
			timestamp := uint32(time.Now().Add(3 * time.Hour).Unix()) //nolint:gosec

			expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
			require.NoError(t, err)

			coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
			hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

			block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0) //nolint:gosec
			require.NoError(t, err)

			err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
			require.Error(t, err, "the block must not be accepted at this time")
			require.ErrorContains(t, err, "future", "the rejection must come from the 2h-future timestamp rule (not setup)")

			exists, existsErr := client.GetBlockExists(ctx, block.Hash())
			require.NoError(t, existsErr)

			require.False(t, exists, "BV-HEADER-006: a clock-deferred block must not be persisted")
			require.Zero(t, kafkaRec.published(), "BV-HEADER-006: no invalid-block notification (it drives a peer strike)")
			require.Zero(t, banRec.banCount(), "BV-HEADER-006: the peer must not be struck")
		})
	}
}
