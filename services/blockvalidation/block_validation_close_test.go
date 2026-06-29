package blockvalidation

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

// recordingKafkaProducer is a KafkaAsyncProducerI test double that counts Stop()
// calls, so the new producer-stop path in BlockValidation.Close can be observed
// without depending on unexported kafka internals.
type recordingKafkaProducer struct {
	mu    sync.Mutex
	stops int
}

func (r *recordingKafkaProducer) Start(_ context.Context, _ chan *kafka.Message) {}

func (r *recordingKafkaProducer) Stop() error {
	r.mu.Lock()
	r.stops++
	r.mu.Unlock()

	return nil
}

func (r *recordingKafkaProducer) BrokersURL() []string             { return nil }
func (r *recordingKafkaProducer) Publish(_ *kafka.Message)         {}
func (r *recordingKafkaProducer) TryPublish(_ *kafka.Message) bool { return false }

func (r *recordingKafkaProducer) stopCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.stops
}

// Test_BlockValidation_Close_StopsInvalidBlockProducer covers the new
// producer-stop path that the existing nil-literal Stop tests never reach:
// BlockValidation.Close() must synchronously Stop the invalid-block Kafka
// producer it owns. The test FAILS if the Stop call is removed (stop count 0).
func Test_BlockValidation_Close_StopsInvalidBlockProducer(t *testing.T) {
	prod := &recordingKafkaProducer{}

	bv := &BlockValidation{
		logger:                    ulogger.TestLogger{},
		invalidBlockKafkaProducer: prod,
	}

	require.NoError(t, bv.Close())
	require.Equal(t, 1, prod.stopCount(), "Close must Stop the invalid-block kafka producer")
}

// Test_BlockValidation_Close_NilProducer verifies Close() is nil-safe when no
// invalid-block producer is configured (e.g. minimal/test fixtures).
func Test_BlockValidation_Close_NilProducer(t *testing.T) {
	bv := &BlockValidation{logger: ulogger.TestLogger{}}
	require.NoError(t, bv.Close())
}
