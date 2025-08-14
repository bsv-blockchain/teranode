package p2p

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPeerCacheFilePath(t *testing.T) {
	tests := []struct {
		name          string
		configuredDir string
		expectedFile  string
	}{
		{
			name:          "Custom directory specified",
			configuredDir: "/custom/path",
			expectedFile:  "/custom/path/teranode_peers.json",
		},
		{
			name:          "Relative directory specified",
			configuredDir: "./data",
			expectedFile:  "data/teranode_peers.json",
		},
		{
			name:          "Empty directory defaults to current directory",
			configuredDir: "",
			expectedFile:  "teranode_peers.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getPeerCacheFilePath(tt.configuredDir)
			assert.Equal(t, tt.expectedFile, result)
		})
	}
}

func TestGetPeerCacheFilePathDefaultDirectory(t *testing.T) {
	// Test the default directory behavior
	result := getPeerCacheFilePath("")

	// Should default to current directory
	assert.Equal(t, "teranode_peers.json", result)
}

func TestGetPeerCacheFilePathConsistency(t *testing.T) {
	// Test that the filename is always "teranode_peers.json" regardless of directory
	testDirs := []string{
		"",
		"/tmp",
		"./data",
		"/var/lib/teranode",
		"C:\\Program Files\\Teranode", // Windows path
	}

	for _, dir := range testDirs {
		result := getPeerCacheFilePath(dir)
		assert.Equal(t, "teranode_peers.json", filepath.Base(result),
			"Filename should always be teranode_peers.json for directory: %s", dir)
	}
}
