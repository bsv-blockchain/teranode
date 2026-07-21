package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// TestReValidateBlock_DoesNotBlockAfterWorkerStopped is the regression guard for
// the remaining half of issue 1151: once the sole revalidateBlockChan consumer has
// stopped (shutdown), ReValidateBlock must not block the caller forever on a full
// channel — it drops the request via the revalidateWorkerStopped signal.
func TestReValidateBlock_DoesNotBlockAfterWorkerStopped(t *testing.T) {
	u := &BlockValidation{
		logger:                  ulogger.TestLogger{},
		revalidateBlockChan:     make(chan revalidateBlockData, 2),
		revalidateWorkerStopped: make(chan struct{}),
	}

	// Fill the channel so any further send would block.
	u.revalidateBlockChan <- revalidateBlockData{}
	u.revalidateBlockChan <- revalidateBlockData{}

	// Simulate the consumer having exited at shutdown.
	close(u.revalidateWorkerStopped)

	block := testhelpers.CreateTestBlocks(t, 1)[0]

	done := make(chan struct{})
	go func() {
		defer close(done)
		u.ReValidateBlock(block, "")
	}()

	select {
	case <-done:
		// returned promptly — dropped instead of blocking
	case <-time.After(2 * time.Second):
		t.Fatal("ReValidateBlock blocked after the revalidation worker stopped")
	}
}
