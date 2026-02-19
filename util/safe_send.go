package util

import "time"

// SafeSend sends a value on a channel, recovering from panics if the channel is closed.
// Returns true if the value was sent successfully.
func SafeSend[T any](ch chan T, t T, timeoutOption ...time.Duration) bool {
	defer func() {
		_ = recover()
	}()

	if len(timeoutOption) == 0 {
		ch <- t
		return true
	}

	timer := time.NewTimer(timeoutOption[0])
	defer timer.Stop()

	select {
	case ch <- t:
		return true
	case <-timer.C:
		return false
	}
}
