package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestReleaseCatchupLock_OperatorIdleIsLocal(t *testing.T) {
	for _, failedPeer := range []string{"", "honest-primary", "failed-fallback"} {
		name := failedPeer
		if name == "" {
			name = "no earlier peer failure"
		}
		t.Run(name, func(t *testing.T) {
			server, client, store, catchup := newPromotionAuthority(t)
			ctx := context.Background()
			_, err := client.authority.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
			require.NoError(t, err)
			_, err = client.authority.Idle(ctx, &emptypb.Empty{})
			require.NoError(t, err)
			recorder := newPeerFailureRecordingP2PClient()
			server.p2pClient = recorder
			catchup.peerID = "honest-primary"
			catchup.baseURL = "http://honest-primary:8000"
			catchup.startTime = time.Now()
			if failedPeer != "" {
				catchup.failedPeers = map[string]string{failedPeer: "earlier data fetch failed"}
			}
			require.NoError(t, server.acquireCatchupLock(catchup))
			var size atomic.Int64
			err = server.setFSMCatchingBlocks(ctx, catchup, &size)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrStateError))
			// Exercise the production defer boundary omitted by a direct call to
			// fetchAndValidateBlocks: it must not overwrite a peer's error with IDLE.
			server.releaseCatchupLock(catchup, &err)
			requirePromotionState(t, client, store, blockchain.FSMStateIDLE)
			require.False(t, server.isCatchingUp.Load())
			require.NotNil(t, server.previousCatchupAttempt)

			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			require.Empty(t, recorder.maliciousByPeer)
			if failedPeer == "" {
				require.Empty(t, recorder.failuresByPeer)
				require.Empty(t, recorder.errorMsgsByPeer, "operator IDLE must not be reported as a peer error")
			} else {
				require.Equal(t, map[string]int{failedPeer: 1}, recorder.failuresByPeer)
				require.Equal(t, map[string]string{failedPeer: "earlier data fetch failed"}, recorder.errorMsgsByPeer,
					"earlier real peer failures must remain charged and keep their own error")
			}
			require.Equal(t, "local_fsm_refusal", server.previousCatchupAttempt.ErrorType)
		})
	}
}
