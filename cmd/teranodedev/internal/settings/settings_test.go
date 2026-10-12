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

func TestGenerate_WritesOwnerOnlyFile(t *testing.T) {
	projectRoot := t.TempDir()
	cfg := &config.Config{DevName: "alice", Network: "regtest", UTXOBackend: "sqlite"}

	require.NoError(t, Generate(projectRoot, cfg))

	info, err := os.Stat(filepath.Join(projectRoot, settingsFile))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestGenerate_TightensExistingFileMode(t *testing.T) {
	projectRoot := t.TempDir()
	path := filepath.Join(projectRoot, settingsFile)
	require.NoError(t, os.WriteFile(path, []byte("foo = bar\n"), 0o644))
	require.NoError(t, os.Chmod(path, 0o644))

	cfg := &config.Config{DevName: "alice", Network: "regtest", UTXOBackend: "sqlite"}
	require.NoError(t, Generate(projectRoot, cfg))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestGenerate_ReinitKeepsHandEditedRPCPassword(t *testing.T) {
	projectRoot := t.TempDir()
	cfg := &config.Config{DevName: "alice", Network: "regtest", UTXOBackend: "sqlite"}

	require.NoError(t, Generate(projectRoot, cfg))

	path := filepath.Join(projectRoot, settingsFile)
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	edited := rpcPassPattern.ReplaceAllString(string(data), "rpc_pass.dev.alice=handEditedPassword\n")
	require.NoError(t, os.WriteFile(path, []byte(edited), 0o600))

	require.NoError(t, Generate(projectRoot, cfg))

	data, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "rpc_pass.dev.alice = handEditedPassword\n")
}

func TestHasRPCCredentials(t *testing.T) {
	projectRoot := t.TempDir()
	cfg := &config.Config{DevName: "alice", Network: "regtest", UTXOBackend: "sqlite"}

	require.False(t, HasRPCCredentials(projectRoot, "alice"), "no settings file")

	require.NoError(t, Generate(projectRoot, cfg))
	require.True(t, HasRPCCredentials(projectRoot, "alice"))

	// A block written by init before credentials were generated.
	path := filepath.Join(projectRoot, settingsFile)
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	stripped := regexp.MustCompile(`(?m)^rpc_(user|pass)\.dev\.alice = .*\n`).ReplaceAllString(string(data), "")
	require.NoError(t, os.WriteFile(path, []byte(stripped), 0o600))

	require.False(t, HasRPCCredentials(projectRoot, "alice"))

	// A block that kept the password but lost the user.
	require.NoError(t, os.WriteFile(path, data, 0o600))

	noUser := regexp.MustCompile(`(?m)^rpc_user\.dev\.alice = .*\n`).ReplaceAllString(string(data), "")
	require.NoError(t, os.WriteFile(path, []byte(noUser), 0o600))

	require.False(t, HasRPCCredentials(projectRoot, "alice"))
}

func TestGenerate_EndMarkerBeforeStartMarkerDoesNotPanic(t *testing.T) {
	projectRoot := t.TempDir()
	path := filepath.Join(projectRoot, settingsFile)
	stray := markerEnd("alice") + "\n" + markerStart("alice") + "\n"
	require.NoError(t, os.WriteFile(path, []byte(stray), 0o600))

	cfg := &config.Config{DevName: "alice", Network: "regtest", UTXOBackend: "sqlite"}
	require.NotPanics(t, func() { require.NoError(t, Generate(projectRoot, cfg)) })
}
