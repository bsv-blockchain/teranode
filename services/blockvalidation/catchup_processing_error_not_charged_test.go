package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

// localProcessingErr is the shape conflict resolution produces when the counter-spend
// lookup misses a record the pruner already removed: ProcessingError links over a
// TxNotFound, with no peer-attributable code anywhere in the chain.
func localProcessingErr() error {
	return errors.NewProcessingError("[blessMissingTransaction] failed to check counter conflicting tx on current chain",
		errors.NewProcessingError("[checkCounterConflictingOnCurrentChain] failed to get counter conflicting tx meta",
			errors.NewTxNotFoundError("tx not found")))
}

// TestReleaseCatchupLock_LocalProcessingErrorIsNotChargedToThePrimary pins that a terminal
// ProcessingError carrying no peer code is classified local: no malicious report, no failure
// charge and no peer error stored against the primary.
func TestReleaseCatchupLock_LocalProcessingErrorIsNotChargedToThePrimary(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	recorder := newPeerFailureRecordingP2PClient()
	server.p2pClient = recorder

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	ctx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://honest-primary:8000",
		peerID:    "honest-primary",
		startTime: time.Now(),
	}
	require.NoError(t, server.acquireCatchupLock(ctx))

	relErr := localProcessingErr()
	server.releaseCatchupLock(ctx, &relErr)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	require.Zero(t, recorder.maliciousByPeer["honest-primary"])
	require.Zero(t, recorder.failuresByPeer["honest-primary"])
	require.Empty(t, recorder.errorMsgsByPeer["honest-primary"],
		"a local processing fault must not be stored as the peer's catchup error")

	require.NotNil(t, server.previousCatchupAttempt)
	require.Equal(t, "local_processing_fault", server.previousCatchupAttempt.ErrorType)
}

// TestRecordCatchupPeerFailure_IgnoresLocalProcessingError pins the mid-cycle path: a local
// ProcessingError must not land the peer in failedPeers, while a peer-coded one still does.
func TestRecordCatchupPeerFailure_IgnoresLocalProcessingError(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	ctx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://primary:8000",
		peerID:    "primary-peer",
		startTime: time.Now(),
	}
	require.NoError(t, server.acquireCatchupLock(ctx))

	server.recordCatchupPeerFailure("local-peer", localProcessingErr())
	server.recordCatchupPeerFailure("lying-peer", errors.NewProcessingError("fetch failed",
		errors.NewNetworkInvalidResponseError("subtree bytes hash to a different hash")))

	ctx.failedPeersMu.Lock()
	defer ctx.failedPeersMu.Unlock()

	require.NotContains(t, ctx.failedPeers, "local-peer")
	require.Contains(t, ctx.failedPeers, "lying-peer")
}

// TestReportAnnouncedBlockFetchFailure_IgnoresLocalProcessingError pins the announced-block
// fetch path: a local ProcessingError is not charged, a peer-coded one is.
func TestReportAnnouncedBlockFetchFailure_IgnoresLocalProcessingError(t *testing.T) {
	client := &failureCountingP2PClient{}
	u := &Server{p2pClient: client, logger: ulogger.TestLogger{}}

	require.False(t, u.reportAnnouncedBlockFetchFailure(context.Background(), "peer-1", localProcessingErr()))
	require.Equal(t, 0, client.failures)

	require.True(t, u.reportAnnouncedBlockFetchFailure(context.Background(), "peer-1",
		errors.NewNetworkInvalidResponseError("peer served block for a different hash")))
	require.Equal(t, 1, client.failures)
}

// TestReportCatchupFailureForError_SkipsLocalProcessingError pins the generic tail's own
// exemption, with a peer-coded positive control.
func TestReportCatchupFailureForError_SkipsLocalProcessingError(t *testing.T) {
	client := &failureCountingP2PClient{}
	u := &Server{p2pClient: client, logger: ulogger.TestLogger{}}

	u.reportCatchupFailureForError(context.Background(), "peer-1", localProcessingErr())
	require.Equal(t, 0, client.failures)

	u.reportCatchupFailureForError(context.Background(), "peer-1",
		errors.NewProcessingError("catchup failed", errors.NewNetworkInvalidResponseError("no common ancestor in peer headers")))
	require.Equal(t, 1, client.failures)
}

// TestProcessCatchupChItem_LocalProcessingErrorIsNotCharged pins the end-to-end catchup
// consumer: a terminal local ProcessingError takes the local-fault branch, so no reputation
// failure, no malicious report, no ban score and no ReportPeerFailure (whose p2p handler
// records a sync failure against the peer before rotating). The processing marker is cleared
// so the hash can be retried.
func TestProcessCatchupChItem_LocalProcessingErrorIsNotCharged(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = 3

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	realClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	bcRecorder := &reportPeerFailureRecorder{ClientI: realClient}
	p2pRecorder := &subtreeAttributionP2PClient{}

	prev := chainhash.HashH([]byte("local-processing-prev"))
	mr := chainhash.HashH([]byte("local-processing-merkle"))
	block := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr}}

	const peerID = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"

	catchupCalls := 0

	u := &Server{
		settings:            tSettings,
		logger:              ulogger.TestLogger{},
		blockchainClient:    bcRecorder,
		p2pClient:           p2pRecorder,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
			ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
		),
		blockPolicyDeclineAttempts: ttlcache.New[blockAttemptKey, int](
			ttlcache.WithTTL[blockAttemptKey, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[blockAttemptKey, int](),
		),
	}
	u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
		catchupCalls++

		return localProcessingErr()
	}

	u.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)

	u.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: peerID, baseURL: "http://peer:8000"})

	require.Equal(t, 1, catchupCalls)
	require.Equal(t, 0, p2pRecorder.totalCharges(), "a local processing fault must not be charged to any peer")
	require.Equal(t, 0, p2pRecorder.totalMaliciousReports())
	require.Empty(t, p2pRecorder.struck())
	require.Equal(t, 0, bcRecorder.reportCount(), "ReportPeerFailure records a sync failure in p2p, so it must not fire")
	require.Nil(t, u.processBlockNotify.Get(*block.Hash()), "the processing marker must be cleared for a retry")
}

// TestMarkCatchupFailureReported_KeepsThePeerAttribution pins the marker wrapper. It marks only
// failures already charged to the peer at their source, so it must not read as a local processing
// fault: an uncoded peer error (the InvalidArgument util's SSRF dial check returns for a peer host
// that resolves to a blocked address) would otherwise send processCatchupChItem down its local
// branch, which neither rotates away from the peer nor tries the alternative announcers. A local
// cause stays local.
func TestMarkCatchupFailureReported_KeepsThePeerAttribution(t *testing.T) {
	peerErr := markCatchupFailureReported(errors.NewInvalidArgumentError("SSRF dial check: resolved address is a private address"))
	require.True(t, catchupFailureAlreadyReported(peerErr))
	require.False(t, isLocalCatchupFault(peerErr), "a marked peer failure must not become a local fault")

	localErr := markCatchupFailureReported(errors.NewServiceError("failed to do http request"))
	require.True(t, catchupFailureAlreadyReported(localErr))
	require.True(t, isLocalCatchupFault(localErr), "the marker must not turn a local cause into a peer fault")
}

// consensusSpendRejectionChain is the shape a block-context spend rejection takes when nothing
// re-codes it: the store's code under the validator's, blessMissing's and the level loop's
// ProcessingError wrappers.
func consensusSpendRejectionChain(cause error) error {
	return errors.NewProcessingError("[processTransactionsInLevels] Completed processing with 1 errors (0 missing-parent)",
		errors.NewProcessingError("[blessMissingTransaction] failed to validate transaction",
			errors.NewProcessingError("[Validate] error spending utxos", cause)))
}

// TestConsensusSpendRejectionIsChargedToThePeer pins that a consensus spend rule broken by a
// block's transaction stays the peer's fault even under bare ProcessingError wrappers: it is
// charged, not retried as local. A frozen UTXO depends on this node's alert-system state and a
// UTXO hash mismatch on this node's own store, so both stay local.
func TestConsensusSpendRejectionIsChargedToThePeer(t *testing.T) {
	for name, cause := range map[string]error{
		"coinbase immature": errors.NewTxCoinbaseImmatureError("coinbase is not spendable yet"),
		"non final":         errors.NewUtxoNonFinalError("transaction is not final", errors.NewTxLockTimeError("lock time (1100) as block height is not less than block height (1000)")),
	} {
		t.Run(name, func(t *testing.T) {
			err := consensusSpendRejectionChain(cause)

			require.False(t, isLocalProcessingFault(err))
			require.False(t, isLocalCatchupFault(err))
			require.False(t, isRecoverableBlockMessageError(err), "a consensus-invalid block must not be retried as a local fault")

			client := &failureCountingP2PClient{}
			u := &Server{p2pClient: client, logger: ulogger.TestLogger{}}
			u.reportCatchupFailureForError(context.Background(), "peer-1", err)
			require.Equal(t, 1, client.failures)
		})
	}

	for name, cause := range map[string]error{
		"frozen utxo":        errors.NewUtxoFrozenError("utxo is frozen"),
		"utxo hash mismatch": errors.NewUtxoHashMismatchError("utxo hash mismatch"),
	} {
		t.Run(name+" stays local", func(t *testing.T) {
			err := consensusSpendRejectionChain(cause)

			require.True(t, isLocalProcessingFault(err))
			require.True(t, isLocalCatchupFault(err))
		})
	}
}
