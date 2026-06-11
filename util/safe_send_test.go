package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSafeSend_Blocking(t *testing.T) {
	ch := make(chan int, 1)
	require.True(t, SafeSend(ch, 1))
	require.Equal(t, 1, <-ch)
}

func TestSafeSend_ClosedChannelDoesNotPanic(t *testing.T) {
	ch := make(chan int, 1)
	close(ch)
	// The send panics, the deferred recover swallows it, and the (unnamed) bool
	// return is left at its zero value: false. The point is no panic escapes.
	require.False(t, SafeSend(ch, 1))
}

func TestSafeSend_Timeout(t *testing.T) {
	ch := make(chan int) // unbuffered, no receiver
	require.False(t, SafeSend(ch, 1, 10*time.Millisecond))
}

func TestTrySend_SendsWhenRoom(t *testing.T) {
	ch := make(chan int, 1)
	require.True(t, TrySend(ch, 42))
	require.Equal(t, 42, <-ch)
}

func TestTrySend_DropsWhenFull(t *testing.T) {
	ch := make(chan int, 1)
	require.True(t, TrySend(ch, 1))  // fills the buffer
	require.False(t, TrySend(ch, 2)) // buffer full -> dropped, non-blocking
	require.Equal(t, 1, <-ch)        // original value retained, second was dropped
}

func TestTrySend_DropsOnClosedChannel(t *testing.T) {
	ch := make(chan int, 1)
	close(ch)
	require.False(t, TrySend(ch, 1)) // recovers from send-on-closed panic, reports not sent
}
