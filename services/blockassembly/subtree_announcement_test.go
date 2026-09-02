package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSubtreeAnnouncementContent pins BA-SUBTREE-005: a completed subtree is
// announced to the network via the Blockchain service, and the announcement
// identifies the subtree by its content-derived root hash.
//
// The existing TestSendSubtreeNotification only checks that SendNotification is
// called (with mock.Anything); this strengthens it to assert the notification
// actually carries Type == Subtree and Hash == the subtree root hash, so a peer
// can locate the subtree it refers to.
func TestSubtreeAnnouncementContent(t *testing.T) {
	server, _, subtree, _ := setup(t)

	var captured *blockchain.Notification

	mockClient := &blockchain.Mock{}
	mockClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)
	mockClient.On("SendNotification", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			captured, _ = args.Get(1).(*blockchain.Notification)
		}).
		Return(nil)
	server.blockchainClient = mockClient

	server.sendSubtreeNotification(context.Background(), *subtree.RootHash())

	mockClient.AssertExpectations(t)

	require.NotNil(t, captured, "a notification must be sent")
	require.Equal(t, model.NotificationType_Subtree, captured.Type,
		"BA-SUBTREE-005: the announcement must be a subtree notification")
	require.Equal(t, subtree.RootHash()[:], captured.Hash,
		"BA-SUBTREE-005: the announcement must identify the subtree by its root hash")
}

// TestSubtreeAnnouncementFailureIsNotACorrectnessGate pins BA-SUBTREE-006:
// subtree announcement is a network optimization, not a correctness gate. A
// failure to announce (Blockchain service / P2P fan-out unavailable) MUST NOT
// undo or block the subtree's persistence — the subtree remains retrievable from
// the Subtree Store so peers can fetch it on demand once they see a referencing
// block.
func TestSubtreeAnnouncementFailureIsNotACorrectnessGate(t *testing.T) {
	server, subtreeStore, subtree, txMap := setup(t)

	ctx := context.Background()

	// Persist the subtree through the normal assembly storage path. Storage is
	// structurally independent of announcement.
	subtreeRetryChan := make(chan *subtreeRetrySend, 1000)
	subtreeDone, allDone, err := server.storeSubtreeData(ctx, subtreeprocessor.NewSubtreeRequest{
		Subtree:     subtree,
		ParentTxMap: txMap,
	}, subtreeRetryChan)
	require.NoError(t, err)
	require.True(t, <-subtreeDone, "subtree storage must succeed")
	<-allDone

	// Announcement now fails (FSM running, but SendNotification errors).
	mockClient := &blockchain.Mock{}
	mockClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)
	mockClient.On("SendNotification", mock.Anything, mock.Anything).
		Return(errors.NewProcessingError("p2p fan-out unavailable"))
	server.blockchainClient = mockClient

	// sendSubtreeNotification returns nothing and must not panic: the failure is
	// swallowed (logged), never propagated as a pipeline error.
	require.NotPanics(t, func() {
		server.sendSubtreeNotification(ctx, *subtree.RootHash())
	})

	mockClient.AssertExpectations(t)

	// The subtree is still persisted and retrievable despite the failed
	// announcement — exactly the on-demand-fetch fallback BA-SUBTREE-006 relies on.
	stored, err := subtreeStore.Get(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree)
	require.NoError(t, err, "BA-SUBTREE-006: the subtree must remain retrievable after a failed announcement")
	require.NotEmpty(t, stored)
}
