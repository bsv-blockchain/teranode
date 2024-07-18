package legacy

import (
	"github.com/bitcoin-sv/ubsv/ulogger"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestExcessiveBlockSizeUserAgentComment(t *testing.T) {
	// Wipe test args.
	os.Args = []string{"bsvd"}

	cfg, _, err := loadConfig(ulogger.TestLogger{})
	if err != nil {
		t.Fatal("Failed to load configuration")
	}

	if len(cfg.UserAgentComments) != 1 {
		t.Fatal("Expected EB UserAgentComment")
	}

	//uac := cfg.UserAgentComments[0]
	//uacExpected := "EB32.0"
	//if uac != uacExpected {
	//	t.Fatalf("Expected UserAgentComments to contain %s but got %s", uacExpected, uac)
	//}

	// Custom excessive block size.
	os.Args = []string{"bsvd", "--excessiveblocksize=64000000"}

	cfg, _, err = loadConfig(ulogger.TestLogger{})
	if err != nil {
		t.Fatal("Failed to load configuration")
	}

	if len(cfg.UserAgentComments) != 1 {
		t.Fatal("Expected EB UserAgentComment")
	}

	//uac = cfg.UserAgentComments[0]
	//uacExpected = "EB64.0"
	// we do not support the command line options an
	//if uac != uacExpected {
	//	t.Fatalf("Expected UserAgentComments to contain %s but got %s", uacExpected, uac)
	//}
}

func TestCreateDefaultConfigFile(t *testing.T) {
	// Setup a temporary directory
	tmpDir, err := ioutil.TempDir("", "bsvd")
	if err != nil {
		t.Fatalf("Failed creating a temporary directory: %v", err)
	}
	testpath := filepath.Join(tmpDir, "test.conf")

	// Clean-up
	defer func() {
		os.Remove(testpath)
		os.Remove(tmpDir)
	}()

	//err = createDefaultConfigFile(testpath)
	//
	//if err != nil {
	//	t.Fatalf("Failed to create a default config file: %v", err)
	//}

	//content, err := ioutil.ReadFile(testpath)
	//if err != nil {
	//	t.Fatalf("Failed to read generated default config file: %v", err)
	//}
}
