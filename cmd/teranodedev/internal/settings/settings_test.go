package settings

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
	"github.com/stretchr/testify/require"
)

var rpcPassPattern = regexp.MustCompile(`rpc_pass\.dev\.alice = ([A-Za-z0-9_-]{32})\n`)

func readGeneratedRPCPass(t *testing.T, projectRoot string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(projectRoot, settingsFile))
	require.NoError(t, err)

	matches := rpcPassPattern.FindStringSubmatch(string(data))
	require.Lenf(t, matches, 2, "rpc_pass.dev.alice not found in generated settings:\n%s", string(data))

	return matches[1]
}

func TestGenerate_WritesRPCCredentials(t *testing.T) {
	projectRoot := t.TempDir()
	cfg := &config.Config{DevName: "alice", Network: "regtest", UTXOBackend: "sqlite"}

	require.NoError(t, Generate(projectRoot, cfg))

	data, err := os.ReadFile(filepath.Join(projectRoot, settingsFile))
	require.NoError(t, err)

	require.Contains(t, string(data), "rpc_user.dev.alice = alice")

	pass := readGeneratedRPCPass(t, projectRoot)
	require.Len(t, pass, 32)
}

func TestGenerate_ReinitKeepsSameRPCPassword(t *testing.T) {
	projectRoot := t.TempDir()
	cfg := &config.Config{DevName: "alice", Network: "regtest", UTXOBackend: "sqlite"}

	require.NoError(t, Generate(projectRoot, cfg))
	firstPass := readGeneratedRPCPass(t, projectRoot)

	// Re-init with a changed field to force the block to be rewritten.
	cfg.Network = "mainnet"
	require.NoError(t, Generate(projectRoot, cfg))
	secondPass := readGeneratedRPCPass(t, projectRoot)

	require.Equal(t, firstPass, secondPass)
}

func TestGenerate_DifferentDevsGetDifferentPasswords(t *testing.T) {
	projectRoot1 := t.TempDir()
	projectRoot2 := t.TempDir()
	cfg := &config.Config{DevName: "alice", Network: "regtest", UTXOBackend: "sqlite"}

	require.NoError(t, Generate(projectRoot1, cfg))
	require.NoError(t, Generate(projectRoot2, cfg))

	pass1 := readGeneratedRPCPass(t, projectRoot1)
	pass2 := readGeneratedRPCPass(t, projectRoot2)

	require.NotEqual(t, pass1, pass2)
}
