package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// Stop waits a bounded time for the processor goroutine. When that wait times
// out, a handler (a long moveForwardBlock, a reset's reload) is still running
// and still using the disk tx maps and subtrees. Closing them under it closed
// each disk's writeCh, so the handler's next write panicked with "send on
// closed channel" (and its rollback's Clear panicked again). Stop must leave
// them open for process exit instead.
func TestStop_TimeoutLeavesInFlightHandlerStateOpen(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	stp.stopWaitTimeout = 50 * time.Millisecond
	stp.Start(context.Background())

	entered := make(chan struct{})
	release := make(chan struct{})
	result := make(chan any, 1)

	go stp.Reset(resetTestHeader(9401), nil, nil, false, func() error {
		close(entered)
		<-release

		func() {
			defer func() { result <- recover() }()

			stp.diskTxMap.Set(batchTestHash(1), &subtreepkg.TxInpoints{})
			_ = stp.diskTxMap.Flush()
		}()

		return nil
	})

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reset's reload never started")
	}

	stp.Stop(context.Background()) // times out: the reload is still blocked

	close(release)

	select {
	case r := <-result:
		require.Nil(t, r, "the in-flight handler must still be able to write to the disk tx map after Stop timed out")
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight handler never finished")
	}

	require.Eventually(t, stp.stopped.Load, 5*time.Second, 5*time.Millisecond)

	// Stop gave up on them, so release them here.
	_ = stp.diskTxMap.Close()
	_ = stp.diskTxMapShadow.Close()
}
