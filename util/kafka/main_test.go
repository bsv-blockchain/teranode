package kafka

import (
	"os"
	"testing"

	"github.com/bsv-blockchain/teranode/util/kafka/kafkatest"
)

// TestMain stops the Redpanda container the tests and benchmarks share
// (kafkatest.SharedEnv). CI turns off the testcontainers reaper, so nothing
// else would remove it when the process exits.
func TestMain(m *testing.M) {
	code := m.Run()

	kafkatest.TerminateSharedEnv()

	os.Exit(code)
}
