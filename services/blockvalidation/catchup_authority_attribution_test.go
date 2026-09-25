package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testAdmissionFailure(t *testing.T, failure error) error {
	t.Helper()
	return waitForCatchupAdmissionWithBudget(context.Background(), func(context.Context) error {
		return failure
	}, time.Second, time.Millisecond, 3*time.Millisecond)
}

func TestCatchupAuthorityFailureDoesNotChargeCachedAlternative(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
	}{
		{"unavailable budget", status.Error(codes.Unavailable, "blockchain restarting")},
		{"rolling upgrade budget", status.Error(codes.Unimplemented, "old blockchain")},
		{"permanent refusal", status.Error(codes.PermissionDenied, "authority denied")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newCatchupAdmissionAuthority(t, "RUNNING")
			peerReports := &failureCountingP2PClient{}
			server.p2pClient = peerReports
			block := testhelpers.CreateTestBlockChain(t, 2)[1]
			server.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, peerID: "alternative", baseURL: "http://alternative"}}, time.Minute)
			admissionErr := testAdmissionFailure(t, tc.failure)
			server.catchupFunc = func(_ context.Context, _ *model.Block, peerID, _ string) error {
				if peerID == "primary" {
					return errors.NewNetworkTimeoutError("primary fetch timed out")
				}
				return errors.NewProcessingError("alternative attempt", admissionErr)
			}
			server.processCatchupChItem(context.Background(), processBlockCatchup{block: block, peerID: "primary", baseURL: "http://primary"})
			require.Equal(t, 1, peerReports.failures, "primary network failure counts; local authority failure does not charge alternative")
		})
	}
}

func TestCatchupAuthorityFailureDoesNotWritePeerDiagnostic(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()
	peerReports := newPeerFailureRecordingP2PClient()
	server.p2pClient = peerReports
	block := testhelpers.CreateTestBlockChain(t, 2)[1]
	catchupCtx := &CatchupContext{blockUpTo: block, baseURL: "http://honest", peerID: "honest", startTime: time.Now()}
	require.NoError(t, server.acquireCatchupLock(catchupCtx))
	err := error(errors.NewProcessingError("fork repair", testAdmissionFailure(t, status.Error(codes.Unavailable, "blockchain restarting"))))
	server.releaseCatchupLock(catchupCtx, &err)
	require.Equal(t, "local_catchup_authority", server.previousCatchupAttempt.ErrorType)
	require.Zero(t, peerReports.failuresByPeer["honest"])
	require.Empty(t, peerReports.errorMsgsByPeer["honest"])
}

func TestCatchupRemoteServiceFailureStillChargesAlternative(t *testing.T) {
	server, _ := newCatchupAdmissionAuthority(t, "RUNNING")
	peerReports := &failureCountingP2PClient{}
	server.p2pClient = peerReports
	block := testhelpers.CreateTestBlockChain(t, 2)[1]
	server.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, peerID: "alternative", baseURL: "http://alternative"}}, time.Minute)
	server.catchupFunc = func(_ context.Context, _ *model.Block, peerID, _ string) error {
		if peerID == "primary" {
			return errors.NewNetworkTimeoutError("primary fetch timed out")
		}
		return errors.NewServiceError("peer data request failed", errors.NewNetworkTimeoutError("alternative fetch timed out"))
	}
	server.processCatchupChItem(context.Background(), processBlockCatchup{block: block, peerID: "primary", baseURL: "http://primary"})
	require.Equal(t, 2, peerReports.failures, "both real peer network failures must be charged")
}
