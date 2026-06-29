package kafka

import (
	"context"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// StopProducerCtx stops an async Kafka producer under a deadline. A producer's
// Stop() ends in publishWg.Wait() followed by client.Flush(context.Background()),
// neither of which honours a deadline — so against a wedged or unreachable broker
// Stop() can block well past a bounded shutdown window and stall the whole serial
// shutdown. This runs Stop() in a goroutine and waits for it OR ctx.Done(),
// whichever comes first. On timeout it logs and returns, leaving the outstanding
// Stop() (and the producer's own ctx-cancel self-close) to finish the flush later
// if it can — it is not guaranteed to finish, but shutdown no longer blocks on a
// dead broker.
//
// It nil-guards the producer (a no-op when p is nil) and logs an Errorf on a
// Stop() error, preserving the previous per-site behaviour. The name identifies
// the producer in log lines.
func StopProducerCtx(ctx context.Context, logger ulogger.Logger, name string, p KafkaAsyncProducerI) {
	if p == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		if err := p.Stop(); err != nil && logger != nil {
			logger.Errorf("[StopProducerCtx] failed to stop %s kafka producer gracefully: %v", name, err)
		}
	}()

	select {
	case <-done:
		// stop completed within the window
	case <-ctx.Done():
		if logger != nil {
			logger.Errorf("[StopProducerCtx] %s kafka producer stop exceeded shutdown window; relying on async self-close: %v", name, ctx.Err())
		}
	}
}
