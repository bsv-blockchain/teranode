package sql

import (
	"fmt"
	"os"
	"testing"

	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
)

// TestMain stops the PostgreSQL container the tests share (test/utils/postgres). CI turns
// off the testcontainers reaper, so nothing else would remove it when this process exits.
func TestMain(m *testing.M) {
	code := m.Run()

	if err := tpostgres.TerminateSharedContainer(); err != nil {
		fmt.Fprintf(os.Stderr, "terminate shared postgres container: %v\n", err)
	}

	os.Exit(code)
}
