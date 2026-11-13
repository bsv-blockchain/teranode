//go:build bvgaps

package blockvalidation

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// futureDatedAheadOfClock is how far ahead of the local clock the scenario timestamps its header:
// past the 7200 s (2 h) future bound, and reversible by the wall clock alone.
const futureDatedAheadOfClock = 3 * time.Hour

// futureDatedTimestamp returns a header timestamp futureDatedAheadOfClock ahead of now.
func futureDatedTimestamp() uint32 {
	return uint32(time.Now().Add(futureDatedAheadOfClock).Unix()) //nolint:gosec
}

// futureDatedMaliciousReportRecorder is the existing no-op P2P client plus a record of every
// RecordCatchupMalicious call (the malicious report).
type futureDatedMaliciousReportRecorder struct {
	maliciousAbortP2PClient

	mu        sync.Mutex
	malicious []string
}

func (r *futureDatedMaliciousReportRecorder) RecordCatchupMalicious(_ context.Context, peerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.malicious = append(r.malicious, peerID)

	return nil
}

func (r *futureDatedMaliciousReportRecorder) reportedPeers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.malicious...)
}

// futureDatedInvalidBlockNotificationRecorder is a KafkaAsyncProducerI that records every published
// message, so a test can observe whether an invalid-block notification (which drives p2p ban
// scoring) was emitted.
type futureDatedInvalidBlockNotificationRecorder struct {
	mu   sync.Mutex
	msgs []*kafka.Message
}

func (r *futureDatedInvalidBlockNotificationRecorder) Start(_ context.Context, _ chan *kafka.Message) {
}
func (r *futureDatedInvalidBlockNotificationRecorder) Stop() error          { return nil }
func (r *futureDatedInvalidBlockNotificationRecorder) BrokersURL() []string { return nil }

func (r *futureDatedInvalidBlockNotificationRecorder) Publish(m *kafka.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.msgs = append(r.msgs, m)
}

func (r *futureDatedInvalidBlockNotificationRecorder) TryPublish(m *kafka.Message) bool {
	r.Publish(m)

	return true
}

func (r *futureDatedInvalidBlockNotificationRecorder) publishedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.msgs)
}

// futureDatedBanScoreRecorder records AddBanScore calls made directly on the P2P client.
type futureDatedBanScoreRecorder struct {
	maliciousAbortP2PClient

	mu   sync.Mutex
	bans int
}

func (r *futureDatedBanScoreRecorder) AddBanScore(_ context.Context, _ string, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.bans++

	return nil
}

func (r *futureDatedBanScoreRecorder) banCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.bans
}

// TestKnownDefect_FutureDatedCatchupHeader_AbortsWithoutMaliciousReport checks that a catchup
// aborts on a future-dated header without reporting the peer as malicious.
//
// Spec: BV-CATCHUP-007 and AC-BV-CATCHUP-007.2, also BV-HEADER-006 (tracked as GAP-BV-050, part a).
//
// Scenario: Given a local tip at height 1000 and a peer that serves a PoW-valid, well-formed header
// linked to that tip but stamped 3 hours ahead of the local clock (past the 7200 s bound), When the
// node fetches headers from that peer during catchup for a target block at height 1001, Then the
// catchup aborts because of the future timestamp, but the peer is NOT reported as malicious and the
// error does not carry the malicious-response class. A future timestamp depends on the validating
// node's own clock, so bitcoin-sv attaches no penalty to it.
//
// Today: catchup/header_validation.go ValidateHeaderTimestamp returns NewNetworkInvalidResponseError
// (a "malicious response" class), and catchup_get_block_headers.go (the validateBatchHeaders
// branch) calls reportCatchupMalicious -> P2PClient.RecordCatchupMalicious for any
// IsMaliciousResponseError, so the peer is reported.
func TestKnownDefect_FutureDatedCatchupHeader_AbortsWithoutMaliciousReport(t *testing.T) {
	const (
		tipHeight         = uint32(1000)
		targetBlockHeight = uint32(1001)
		peerID            = "peer-honest-clock"
		peerURL           = "http://peer-honest-clock"
		regtestBits       = "207fffff"
	)

	ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
	defer cancel()

	// Given: a node whose tip is at tipHeight, recording every malicious report it makes.
	server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	mockUTXOStore.On("GetBlockHeight").Return(tipHeight).Maybe()

	maliciousReports := &futureDatedMaliciousReportRecorder{}
	server.p2pClient = maliciousReports

	mainnetHeaders := testhelpers.GetMainnetHeaders(t, 1)
	tipHeader := mainnetHeaders[0]
	targetBlock := &model.Block{Header: mainnetHeaders[0], Height: targetBlockHeight}

	nBits, err := model.NewNBitFromString(regtestBits)
	require.NoError(t, err)

	// A header well-formed in every respect (PoW against its own bits, non-zero merkle root, linked
	// to the tip) except that its timestamp is 3 hours ahead of the local clock.
	futureDatedHeader := &model.BlockHeader{
		Version:        4,
		HashPrevBlock:  tipHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{1},
		Timestamp:      futureDatedTimestamp(),
		Bits:           *nBits,
	}
	testhelpers.MineHeader(futureDatedHeader)

	mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).Return(false, nil)
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(tipHeader, &model.BlockHeaderMeta{Height: tipHeight}, nil)
	mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{tipHeader.Hash()}, nil)

	// And: the peer serves the tip followed by the future-dated header.
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `=~^http://peer-honest-clock/headers_from_common_ancestor/.*`,
		func(*http.Request) (*http.Response, error) {
			return httpmock.NewBytesResponse(200, append(tipHeader.Bytes(), futureDatedHeader.Bytes()...)), nil
		})

	// When: the catchup fetches headers from that peer.
	result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, peerID, peerURL)

	// Then: the catchup aborts on the timestamp rule ...
	require.Positive(t, httpmock.GetTotalCallCount(), "fixture precondition: the header was actually fetched")
	require.Error(t, err, "the catchup must abort on a future-dated header")
	require.NotNil(t, result)
	require.False(t, result.Success)
	require.Contains(t, err.Error(), "future", "the abort must come from the timestamp rule, not another check")

	// ... but the peer is neither reported nor tagged as malicious.
	require.Empty(t, maliciousReports.reportedPeers(),
		"a future-dated header is clock-dependent and must NOT be reported as a malicious response")
	require.False(t, errors.IsMaliciousResponseError(err),
		"the abort error must not carry the malicious-response class, which callers turn into a ban score: %v", err)
}

// TestKnownDefect_FutureDatedBlock_IsNotPersistedNotifiedNorStruck checks that a block rejected only
// because its timestamp is too far in the future leaves no trace: no stored block, no invalid-block
// notification, no ban score for the peer.
//
// Spec: BV-HEADER-006 and AC-BV-HEADER-006.1 (tracked as GAP-BV-050, part b).
//
// Scenario: Given a block at height 100 on top of genesis, with correct difficulty bits, timestamped
// 3 hours ahead of the local clock (past the 7200 s bound), When it is validated from peer
// "peer-serving", Then it is rejected because of the future timestamp, and it is NOT persisted, NO
// invalid-block notification is published (the p2p consumer turns that into a peer strike), and the
// peer is NOT struck. The verdict depends on the local clock, so it is not a verdict on the block.
// The scenario runs once with optimistic mining disabled and once with it enabled.
//
// Today: the block is rejected ERR_BLOCK_INVALID by model/Block.go CheckHeaderContextual, and
// BlockValidation.go treats it as a final header verdict: rejectFinalHeaderVerdict ->
// kafkaNotifyBlockInvalid publishes the notification on the optimistic branch, so the
// optimistic-mining subtest fails. The non-optimistic branch would persist it via storeInvalidBlock
// (which also notifies), but that subtest passes today: Valid fails before the merkle binding, so
// bodyBoundToHeader is false and nothing is persisted or notified.
func TestKnownDefect_FutureDatedBlock_IsNotPersistedNotifiedNorStruck(t *testing.T) {
	initPrometheusMetrics()

	const (
		blockHeight = uint32(100)
		peerID      = "peer-serving"
		peerBaseURL = "http://localhost"
	)

	miningModes := []struct {
		name             string
		optimisticMining bool
	}{
		{name: "with optimistic mining disabled the block is not persisted, notified or struck", optimisticMining: false},
		{name: "with optimistic mining enabled the block is not persisted, notified or struck", optimisticMining: true},
	}

	for _, mode := range miningModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Given: a node in this optimistic-mining mode, recording notifications and ban scores.
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.OptimisticMining = mode.optimisticMining
			tSettings.BlockValidation.OptimisticMiningPeerBlocks = mode.optimisticMining

			blockValidation, blockchainClient := newNoPersistHarness(ctx, t, tSettings)

			invalidBlockNotifications := &futureDatedInvalidBlockNotificationRecorder{}
			blockValidation.invalidBlockKafkaProducer = invalidBlockNotifications

			banScores := &futureDatedBanScoreRecorder{}
			blockValidation.p2pClient = banScores

			// And: a block at blockHeight with correct bits but a timestamp 3 hours in the future.
			timestamp := futureDatedTimestamp()

			expectedBits, err := blockchainClient.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
			require.NoError(t, err)

			coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
			header := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expectedBits, timestamp)

			block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0) //nolint:gosec
			require.NoError(t, err)

			// When: the block is validated from the peer.
			err = blockValidation.ValidateBlockWithOptions(ctx, block, peerBaseURL, &ValidateBlockOptions{PeerID: peerID})

			// Then: it is rejected by the future-timestamp rule ...
			require.Error(t, err, "the block must not be accepted at this time")
			require.ErrorContains(t, err, "future", "the rejection must come from the 2h-future timestamp rule (not setup)")

			// ... and leaves no trace: not stored, not notified, peer not struck.
			exists, existsErr := blockchainClient.GetBlockExists(ctx, block.Hash())
			require.NoError(t, existsErr)

			require.False(t, exists, "a clock-deferred block must not be persisted")
			require.Zero(t, invalidBlockNotifications.publishedCount(), "no invalid-block notification (it drives a peer strike)")
			require.Zero(t, banScores.banCount(), "the peer must not be struck")
		})
	}
}
