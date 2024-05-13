package blockchain

// func Test_SendNotification(t *testing.T) {
// 	// Create a new server instance
// 	logger := ulogger.New("blockchain")
// 	ctx := context.Background()

// 	server, err := New(context.Background(), logger)
// 	if err != nil {
// 		t.Fatalf("Failed to create server: %v", err)
// 	}

// 	// Start the server
// 	go func() {
// 		if err := server.Start(ctx); err != nil {
// 			t.Errorf("Failed to start server: %v", err)
// 		}
// 	}()

// 	// client, err := NewClient(ctx, logger)
// 	// if err != nil {
// 	// 	t.Fatalf("Failed to create server: %v", err)
// 	// }

// 	// Send FSM event to start mining
// 	// create a new blockchain notification with Mine event
// 	notification := &model.Notification{
// 		Type:    model.NotificationType_FSMEvent,
// 		Hash:    nil, // not relevant for FSMEvent notifications
// 		BaseURL: "",  // not relevant for FSMEvent notifications
// 		Metadata: model.NotificationMetadata{
// 			Metadata: map[string]string{
// 				"event": FiniteStateMachineEvent_Mine,
// 			},
// 		},
// 	}

// 	// send FSMEvent Mine notification to the blockchain client. FSM will transition to state Mining
// 	//require.NoError(t, server.SendNotification(ctx, notification))

// 	// notification := &blockchain_api.Notification{
// 	// 	Hash: []byte{1},
// 	// }

// 	_, err = server.SendNotification(context.Background(), notification)
// 	require.Error(t, err)

// }
