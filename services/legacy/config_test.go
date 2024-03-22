package legacy

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExcessiveBlockSizeUserAgentComment(t *testing.T) {
	// Wipe test args.
	os.Args = []string{"bsvd"}

	cfg, err := loadConfig()
	if err != nil {
		t.Fatal("Failed to load configuration")
	}

	if len(cfg.UserAgentComments) != 1 {
		t.Fatal("Expected EB UserAgentComment")
	}

	uac := cfg.UserAgentComments[0]
	uacExpected := "EB128.0"
	if uac != uacExpected {
		t.Fatalf("Expected UserAgentComments to contain %s but got %s", uacExpected, uac)
	}

	// Custom excessive block size.
	os.Args = []string{"bsvd", "--excessiveblocksize=64000000"}

	cfg, err = loadConfig()
	if err != nil {
		t.Fatal("Failed to load configuration")
	}

	if len(cfg.UserAgentComments) != 1 {
		t.Fatal("Expected EB UserAgentComment")
	}

	uac = cfg.UserAgentComments[0]
	uacExpected = "EB128.0"
	if uac != uacExpected {
		t.Fatalf("Expected UserAgentComments to contain %s but got %s", uacExpected, uac)
	}
}

func TestCreateDefaultConfigFile(t *testing.T) {
	// Setup a temporary directory
	tmpDir, err := os.MkdirTemp("", "bsvd")
	if err != nil {
		t.Fatalf("Failed creating a temporary directory: %v", err)
	}
	testpath := filepath.Join(tmpDir, "test.conf")

	// Clean-up
	defer func() {
		os.Remove(testpath)
		os.Remove(tmpDir)
	}()
}
