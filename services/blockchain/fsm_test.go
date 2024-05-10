package blockchain

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/util/test/mock_logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
func TestFiniteStateMachine_Transitions(t *testing.T) {
	ctx := context.Background()
	logger := mock_logger.NewTestLogger()
	blockchainClient, err := New(ctx, logger)
	require.NoError(t, err)

	// Track whether the stopped state has been entered
	// enteredStopped := false

	//testFSM := NewFiniteStateMachine(callbacks)

	// testFSM := blockchainClient.NewFiniteStateMachine(logger, &fsm.Callbacks{
	// 	"enter_state": func(_ context.Context, e *fsm.Event) {
	// 		//logger.Infof("[BlockFound][%s] too many blocks in queue, sending STOPMINING", hash.String())

	// 		// make make(map[string]string) with one element "event" : "STOPMINING"
	// 		metadata := make(map[string]string)
	// 		metadata["event"] = e.Event

	// 		// create a new blockchain notification
	// 		fsmTransitionNotification := &model.Notification{
	// 			Type:    model.NotificationType_FSMEvent,
	// 			Hash:    nil,
	// 			BaseURL: "",
	// 			Metadata: model.NotificationMetadata{
	// 				Metadata: metadata,
	// 			},
	// 		}

	// 		err := blockchainClient.SendNotification(ctx, fsmTransitionNotification)
	// 		require.NoError(t, err)
	// 	},
	// })

	testFSM := blockchainClient.NewFiniteStateMachine(logger)

	// Test transition from Stopped to Running
	err = testFSM.Event(ctx, FiniteStateMachineEvent_Run)
	assert.Nil(t, err, "should transition from Stopped to Running without error")
	assert.Equal(t, FiniteStateMachineState_Running, testFSM.Current(), "FSM did not transition to Running state")

	// // Test transition from Running to Mining
	// err = testFSM.Event(ctx, FiniteStateMachineEvent_Mine)
	// assert.Nil(t, err, "should transition from Running to Mining without error")
	// assert.Equal(t, FiniteStateMachineState_Mining, testFSM.Current(), "FSM did not transition to Mining state")

	// // Test transition from Mining to Running
	// err = testFSM.Event(ctx, FiniteStateMachineEvent_StopMining)
	// assert.Nil(t, err, "should transition from Mining to Running without error")
	// assert.Equal(t, FiniteStateMachineState_Running, testFSM.Current(), "FSM did not transition back to Running state")

	// // Test transition from Running to Stopped
	// err = testFSM.Event(ctx, FiniteStateMachineEvent_Stop)
	// assert.Nil(t, err, "should transition from Running to Stopped without error")
	// assert.Equal(t, FiniteStateMachineState_Stopped, testFSM.Current(), "FSM did not transition to Stopped state")
	// assert.True(t, enteredStopped, "Enter Stopped callback was not called")
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

func Test_NewFiniteStateMachine(t *testing.T) {
	ctx := context.Background()
	logger := mock_logger.NewTestLogger()
	blockchainClient, err := New(ctx, logger)
	require.NoError(t, err)

	fsm := blockchainClient.NewFiniteStateMachine(logger)

	// Test transitions
	t.Run("Transition from Stopped to Running", func(t *testing.T) {
		err := fsm.Event(ctx, "Run")
		assert.NoError(t, err)
		assert.Equal(t, "Running", fsm.Current())
	})

	t.Run("Transition from Running to Mining", func(t *testing.T) {
		// Try to set the state to Runningm again
		err := fsm.Event(ctx, "Run")
		require.Error(t, err)

		// Now, transition to Mining
		err = fsm.Event(ctx, "Mine")
		assert.NoError(t, err)
		assert.Equal(t, "Mining", fsm.Current())
	})

	t.Run("Transition from Mining to Running", func(t *testing.T) {
		// Transition back to Running
		err = fsm.Event(ctx, "StopMining")
		assert.NoError(t, err)
		assert.Equal(t, "Running", fsm.Current())
	})

	t.Run("Transition from Running to Stopped", func(t *testing.T) {

		// Now, transition to Stopped
		err = fsm.Event(ctx, "Stop")
		assert.NoError(t, err)
		assert.Equal(t, "Stopped", fsm.Current())
	})

}
