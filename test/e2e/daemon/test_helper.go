package smoke

import (
	"sync"
	"testing"
)

// SharedTestLock is a mutex that ensures all tests in this package run sequentially
// This prevents port conflicts when multiple tests try to start daemons simultaneously
var SharedTestLock sync.Mutex

// RunSequentialTest is a helper function that ensures a test runs sequentially
// by acquiring the shared test lock before running the test function
func RunSequentialTest(t *testing.T, testFunc func(*testing.T)) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	testFunc(t)
}
