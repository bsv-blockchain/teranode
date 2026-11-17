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

// TrySend performs a non-blocking send on a channel, recovering from panics if the
// channel is closed. It returns true if the value was sent, and false if the channel
// was full or closed. Use this for best-effort sends where blocking the caller on a
// full buffer is worse than dropping the value.
func TrySend[T any](ch chan T, t T) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()

	select {
	case ch <- t:
		return true
	default:
		return false
	}
}
