package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/cmd/teranode"
)

// Name used by build script for the binaries. (Please keep on single line)
const progname = "teranode"

// Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

func init() {
	// If version and commit are empty (running via go run), populate them at runtime
	if version == "" && commit == "" {
		populateVersionInfo()
	}
}

func populateVersionInfo() {
	// Get git commit (short hash)
	if cmd := exec.Command("git", "rev-parse", "--short", "HEAD"); cmd != nil {
		if output, err := cmd.Output(); err == nil {
			commit = strings.TrimSpace(string(output))
		} else {
			commit = "unknown"
		}
	}

	// Get git tag (if on a tagged commit)
	var gitTag string
	if cmd := exec.Command("git", "describe", "--tags", "--exact-match"); cmd != nil {
		if output, err := cmd.Output(); err == nil {
			gitTag = strings.TrimSpace(string(output))
		}
	}

	// Generate version using same logic as determine-git-version.sh
	if gitTag != "" && strings.HasPrefix(gitTag, "v") {
		version = gitTag
	} else {
		// Get git timestamp or use current time
		var timestamp string
		if cmd := exec.Command("git", "show", "-s", "--format=%cd", "--date=format:%Y%m%d%H%M%S", "HEAD"); cmd != nil {
			if output, err := cmd.Output(); err == nil {
				timestamp = strings.TrimSpace(string(output))
			} else {
				timestamp = time.Now().Format("20060102150405")
			}
		}

		if commit == "unknown" {
			version = fmt.Sprintf("v0.0.0-%s-unknown", timestamp)
		} else {
			version = fmt.Sprintf("v0.0.0-%s-%s", timestamp, commit)
		}
	}
}

func main() {
	// Check for --version flag before any initialization
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-v" {
			fmt.Printf("%s version %s (commit: %s)\n", progname, version, commit)
			os.Exit(0)
		}
	}

	// Verify 64-bit architecture
	if strconv.IntSize != 64 {
		fmt.Fprintf(os.Stderr, "Error: %s requires a 64-bit architecture. Current architecture: %s\n", progname, runtime.GOARCH)
		os.Exit(1)
	}

	// If not showing version, run the daemon
	teranode.RunDaemon(progname, version, commit)
}
