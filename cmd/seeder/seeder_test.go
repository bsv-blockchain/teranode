package seeder

import (
	"net/url"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeederRefusesALaneStepThisBuildDoesNotRun(t *testing.T) {
	dir := t.TempDir()
	writeEmptyUTXOSetFile(t, filepath.Join(dir, "snapshot.utxo-set"))
	s := test.CreateBaseTestSettings(t)
	s.ProfilerAddr = ""
	s.UtxoStore.LaneMigration = settings.LaneMigrationRows
	s.UtxoStore.UtxoStore = &url.URL{Scheme: "sqlitememory"}
	s.BlockChain.StoreURL = &url.URL{Scheme: "sqlitememory"}
	blockPath := filepath.Join(dir, "blockstore")
	s.Block.BlockStore = &url.URL{Scheme: "file", Path: blockPath}
	err := Seeder(ulogger.TestLogger{}, s, dir, "snapshot", true, false, false, false, true)
	require.ErrorIs(t, err, errors.ErrConfiguration)
	require.NoDirExists(t, blockPath, "refuse before importing headers or changing checkpoint files")
}

// TestFormatNumber tests the formatNumber function
func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0"},
		{123, "123"},
		{1234, "1,234"},
		{1234567, "1,234,567"},
		{1234567890, "1,234,567,890"},
	}

	for _, test := range tests {
		result := formatNumber(test.input)
		assert.Equal(t, test.expected, result, "formatNumber(%d) = %s; want %s", test.input, result, test.expected)
	}
}
