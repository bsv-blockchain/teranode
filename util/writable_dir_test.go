package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateWritableDir_Empty(t *testing.T) {
	require.NoError(t, ValidateWritableDir(""))
}

func TestValidateWritableDir_ValidWritableDir(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, ValidateWritableDir(dir))
}

func TestValidateWritableDir_NonExistentDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does-not-exist")

	err := ValidateWritableDir(dir)
	require.Error(t, err)
}

func TestValidateWritableDir_PathIsFileNotDir(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "not-a-dir")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0o600))

	err := ValidateWritableDir(filePath)
	require.Error(t, err)
}

func TestValidateWritableDir_NonWritableDir(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root, permission checks do not apply")
	}

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o500)) // read+execute, no write

	defer func() {
		// Restore permissions so t.TempDir() cleanup can remove it.
		_ = os.Chmod(dir, 0o700)
	}()

	err := ValidateWritableDir(dir)
	require.Error(t, err)
}
