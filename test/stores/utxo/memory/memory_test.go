//go:build test_all || test_stores || test_utxo || test_memory || test_longlong

package memory

import (
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo/tests"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// go test -v -tags test_memory ./test/...

func TestMemorySanity(t *testing.T) {
	db := memory.New(ulogger.TestLogger{})
	tests.Sanity(t, db)
}
