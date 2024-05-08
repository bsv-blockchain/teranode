package blockchain

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/util/test/mock_logger"
	"github.com/stretchr/testify/require"
)

func Test_NewFiniteStateMachine(t *testing.T) {
	ctx := context.Background()
	logger := mock_logger.NewTestLogger()
	blockchainClient, err := New(ctx, logger)
	require.NoError(t, err)

	fsm := blockchainClient.NewFiniteStateMachine()
	require.NotNil(t, fsm)
	require.Equal(t, "Stopped", fsm.Current())
	require.True(t, fsm.Can(FiniteStateMachineEvent_Run))

	// Test transitions
	t.Run("Transition from Stopped to Running", func(t *testing.T) {
		err := fsm.Event(ctx, "Run")
		require.NoError(t, err)
		require.Equal(t, "Running", fsm.Current())
		require.True(t, fsm.Can(FiniteStateMachineEvent_Mine))
		require.True(t, fsm.Can(FiniteStateMachineEvent_Stop))
	})

	t.Run("Transition from Running to Mining", func(t *testing.T) {
		// Try to set the state to Runningm again
		err := fsm.Event(ctx, "Run")
		require.Error(t, err)

		// Transition to Mining
		err = fsm.Event(ctx, "Mine")
		require.NoError(t, err)
		require.Equal(t, "Mining", fsm.Current())
		require.True(t, fsm.Can(FiniteStateMachineEvent_StopMining))
		require.True(t, fsm.Can(FiniteStateMachineEvent_Stop))
	})

	t.Run("Transition from Mining to Running", func(t *testing.T) {
		// Stop mining, transition to Running
		err = fsm.Event(ctx, "StopMining")
		require.NoError(t, err)
		require.Equal(t, "Running", fsm.Current())
		require.True(t, fsm.Can(FiniteStateMachineEvent_Mine))
		require.True(t, fsm.Can(FiniteStateMachineEvent_Stop))
	})

	t.Run("Transition from Running to Stopped", func(t *testing.T) {
		// Transition to Stopped
		err = fsm.Event(ctx, "Stop")
		require.NoError(t, err)
		require.Equal(t, "Stopped", fsm.Current())
		require.True(t, fsm.Can(FiniteStateMachineEvent_Run))
	})

}

/*
type subscriber struct {
	subscription blockchain_api.BlockchainAPI_SubscribeServer
	source       string
	done         chan struct{}
}

type MockBlockchain struct {
	logger              ulogger.Logger
	subscribers         map[subscriber]bool
	deadSubscriptions   chan subscriber
	notifications       chan *blockchain_api.Notification
	notificationCounter int
}

// New will return a server instance with the logger stored within it
func NewBlockchainService(ctx context.Context, logger ulogger.Logger) (*MockBlockchain, error) {
	return &MockBlockchain{
		logger:              logger,
		subscribers:         make(map[subscriber]bool),
		deadSubscriptions:   make(chan subscriber, 10),
		notifications:       make(chan *blockchain_api.Notification, 100),
		notificationCounter: 0,
	}, nil
}

func (b *MockBlockchain) SendNotification(_ context.Context, notification *model.Notification) error {

	b.notifications <- &blockchain_api.Notification{
		Type: notification.Type,
		Hash: notification.Hash[:],
	}

	return nil
}

func (b *MockBlockchain) Start(ctx context.Context) error {

	go func() {
		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("[Blockchain] Stopping channel listeners go routine")
				// for sub := range b.subscribers {
				// 	safeClose(sub.done)
				// }
				return
			case notification := <-b.notifications:
				func() {
					b.logger.Debugf("[Blockchain] Sending notification: %s", notification.Stringify())

					for sub := range b.subscribers {
						b.logger.Debugf("[Blockchain] Sending notification to %s in background: %s", sub.source, notification.Stringify())
						go func(s subscriber) {
							b.logger.Debugf("[Blockchain] Sending notification to %s: %s", s.source, notification.Stringify())
							if err := s.subscription.Send(notification); err != nil {
								b.deadSubscriptions <- s
							}
						}(sub)
					}
				}()
			}
		}
	}()

	return nil
}
*/
