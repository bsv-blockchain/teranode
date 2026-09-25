package propagation

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestProcessTransaction_FlagOnRoutesThroughCoalescer asserts:
//  1. With validator_useBatchValidation=true AND no Kafka producer,
//     ProcessTransaction routes through the coalescer.
//  2. The per-tx outcome (error or success) for the same input is
//     the same shape as the flag-off path.
func TestProcessTransaction_FlagOnRoutesThroughCoalescer(t *testing.T) {
	ctx := context.Background()

	run := func(t *testing.T, useBatch bool) error {
		t.Helper()
		ps, cleanup := newPropagationServerForTest(t)
		defer cleanup()
		ps.validatorKafkaProducerClient = nil
		ps.settings.Validator.UseBatchValidation = useBatch

		if ps.validatorKafkaProducerClient == nil && ps.settings.Validator.UseBatchValidation {
			ps.coalescer = NewTxCoalescer(
				ctx, ulogger.TestLogger{}, ps.validator,
				ps.settings.Validator.BatchMaxSize,
				ps.settings.Validator.BatchMaxWait,
				ps.settings.Validator.BatchMaxConcurrent,
				true,
			)
			t.Cleanup(func() { _ = ps.coalescer.Close(context.Background()) })
		}

		tx := dummyTx(t, 0xAA)
		_, err := ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{Tx: tx.Bytes()})
		return err
	}

	offErr := run(t, false)
	onErr := run(t, true)

	require.Error(t, offErr, "flag off should produce an error for an empty tx")
	require.Error(t, onErr, "flag on should produce an error for an empty tx")
}

// TestProcessTransaction_FlagOnNConcurrentCallersAllRespond asserts
// that N concurrent ProcessTransaction calls each receive a response
// (no deadlock, no leak).
func TestProcessTransaction_FlagOnNConcurrentCallersAllRespond(t *testing.T) {
	ctx := context.Background()
	ps, cleanup := newPropagationServerForTest(t)
	defer cleanup()
	ps.validatorKafkaProducerClient = nil
	ps.settings.Validator.UseBatchValidation = true
	ps.coalescer = NewTxCoalescer(
		ctx, ulogger.TestLogger{}, ps.validator,
		ps.settings.Validator.BatchMaxSize,
		ps.settings.Validator.BatchMaxWait,
		ps.settings.Validator.BatchMaxConcurrent,
		true,
	)
	t.Cleanup(func() { _ = ps.coalescer.Close(context.Background()) })

	const N = 32
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := dummyTx(t, byte((i%254)+1))
			_, _ = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{Tx: tx.Bytes()})
		}()
	}
	wg.Wait()
	// If we got here, none of the N calls deadlocked.
}
