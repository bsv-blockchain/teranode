package file

import "time"

// SetKeyLockedSchedulerTimeout shortens the bound on scheduler calls made under a blob's key lock
// for a test, and returns the function that restores it. Not safe while a store is in use.
func SetKeyLockedSchedulerTimeout(d time.Duration) func() {
	previous := keyLockedSchedulerTimeout
	keyLockedSchedulerTimeout = d

	return func() { keyLockedSchedulerTimeout = previous }
}
