package util

import (
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// DefaultBatcherDrainTimeout bounds a single batcher drain during shutdown. It
// is comfortably under the service-manager's per-service Stop() timeout
// (servicemanager.DefaultStopTimeout, ~30s) so a hung flush fn cannot stall the
// whole shutdown.
const DefaultBatcherDrainTimeout = 5 * time.Second

// DrainBatcher runs a go-batcher Close() (passed as closeFn) under a bounded
// timeout. The pinned go-batcher v2.0.4 Close() blocks until the queued items
// have been drained into the batch fn and the worker has unwound, and it is
// idempotent — so this runs Close() in a goroutine and waits for it OR the
// timeout, whichever comes first. On timeout it logs and returns; the Close()
// goroutine is left to finish (a late return is safe given Close()'s
// idempotency), so a wedged flush fn cannot block shutdown indefinitely.
//
// Parameters:
//   - logger:  logger for the timeout warning
//   - name:    human-readable batcher name for the log line
//   - timeout: maximum time to wait for the drain (use DefaultBatcherDrainTimeout)
//   - closeFn: the batcher's Close method, e.g. b.Close
func DrainBatcher(logger ulogger.Logger, name string, timeout time.Duration, closeFn func()) {
	if closeFn == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		closeFn()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		// drain completed within the window
	case <-timer.C:
		if logger != nil {
			logger.Errorf("[DrainBatcher] %s drain did not complete within %s; continuing shutdown", name, timeout)
		}
	}
}
