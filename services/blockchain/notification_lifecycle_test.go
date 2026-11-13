package blockchain

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSendNotification_ServiceShutdown(t *testing.T) {
	b := newTestBlockchainForNotifications(t, 1)
	appCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.AppCtx = appCtx
	b.notifications <- &blockchain_api.Notification{}
	done := make(chan error, 1)
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	go func() {
		_, err := b.SendNotification(ctx, &blockchain_api.Notification{Type: model.NotificationType_Block, Hash: make([]byte, 32)})
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("notification send remained blocked after shutdown")
	}
}

func TestFSMNotification_FullQueueIsBounded(t *testing.T) {
	for _, shutdown := range []bool{true, false} {
		name := "timeout"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			b := setup(t).server // real sqlitememory store
			b.finiteStateMachine = b.NewFiniteStateMachine()
			appCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.AppCtx = appCtx
			b.notifications = make(chan *blockchain_api.Notification, 1)
			b.notifications <- &blockchain_api.Notification{}
			if shutdown {
				cancel()
			}
			done := make(chan error, 1)
			go func() {
				_, err := b.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_CATCHUPBLOCKS})
				done <- err
			}()
			// Also release the old unbounded implementation if this regression fails.
			defer func() { <-b.notifications; <-done }()
			limit := 7 * time.Second // five-second notification deadline plus scheduling allowance
			if shutdown {
				limit = time.Second
			}
			select {
			case err := <-done:
				close(done)
				require.NoError(t, err)
			case <-time.After(limit):
				t.Fatal("FSM transition remained blocked on a full notification queue")
			}
			require.True(t, b.fsmMu.TryLock(), "notification delivery must release the FSM control-plane lock")
			b.fsmMu.Unlock()
		})
	}
}
