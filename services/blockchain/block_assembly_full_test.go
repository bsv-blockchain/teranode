package blockchain

import (
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// TestBlockAssemblyFullNotificationRoundTrip checks that what the producer writes is what the
// consumers read.
//
// Block assembly builds the notification with NewBlockAssemblyFullNotification and both clients
// read it back with blockAssemblyFullFromNotification, so this is the contract that stops the two
// ends drifting apart on the metadata key or its encoding.
func TestBlockAssemblyFullNotificationRoundTrip(t *testing.T) {
	for _, full := range []bool{true, false} {
		notification := NewBlockAssemblyFullNotification(full)

		require.Equal(t, model.NotificationType_BlockAssemblyFull, notification.GetType())
		require.Equal(t, full, blockAssemblyFullFromNotification(notification))
	}
}

// TestBlockAssemblyFullFromNotification checks how the flag is parsed, including the values that
// must fail safe.
//
// A value the node cannot parse has to read as "not full". Failing the other way would refuse
// transactions on a malformed notification, which turns a bad message into an outage.
func TestBlockAssemblyFullFromNotification(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		expected bool
	}{
		{name: "true", metadata: map[string]string{"full": "true"}, expected: true},
		{name: "false", metadata: map[string]string{"full": "false"}, expected: false},
		{name: "parses the alternate bool spellings", metadata: map[string]string{"full": "1"}, expected: true},
		{name: "missing key fails safe", metadata: map[string]string{}, expected: false},
		{name: "unparseable value fails safe", metadata: map[string]string{"full": "yes please"}, expected: false},
		{name: "wrong key fails safe", metadata: map[string]string{"blockAssemblyFull": "true"}, expected: false},
		{name: "nil metadata fails safe", metadata: nil, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notification := &Notification{
				Type:     model.NotificationType_BlockAssemblyFull,
				Metadata: &NotificationMetadata{Metadata: tt.metadata},
			}

			require.Equal(t, tt.expected, blockAssemblyFullFromNotification(notification))
		})
	}
}

// TestClientIsBlockAssemblyFull covers the gRPC Client's copy of the flag.
//
// Every other test in this feature drives a LocalClient, so without this the Client path — the one
// that actually runs in a deployed node — is only covered by the shared parser above. This drives
// the same cached field the subscription handler writes.
func TestClientIsBlockAssemblyFull(t *testing.T) {
	c := &Client{}

	require.False(t, c.IsBlockAssemblyFull(),
		"a client must default to accepting transactions before it hears anything")

	c.blockAssemblyFull.Store(blockAssemblyFullFromNotification(NewBlockAssemblyFullNotification(true)))
	require.True(t, c.IsBlockAssemblyFull())

	c.blockAssemblyFull.Store(blockAssemblyFullFromNotification(NewBlockAssemblyFullNotification(false)))
	require.False(t, c.IsBlockAssemblyFull())
}

// TestLocalClientIsBlockAssemblyFull checks that the LocalClient picks the flag up from the
// notifications passed through SendNotification, and ignores unrelated notification types.
func TestLocalClientIsBlockAssemblyFull(t *testing.T) {
	c := &LocalClient{}

	require.False(t, c.IsBlockAssemblyFull())

	require.NoError(t, c.SendNotification(t.Context(), NewBlockAssemblyFullNotification(true)))
	require.True(t, c.IsBlockAssemblyFull())

	// an unrelated notification must not disturb the flag
	require.NoError(t, c.SendNotification(t.Context(), &Notification{Type: model.NotificationType_Block}))
	require.True(t, c.IsBlockAssemblyFull())

	require.NoError(t, c.SendNotification(t.Context(), NewBlockAssemblyFullNotification(false)))
	require.False(t, c.IsBlockAssemblyFull())
}
