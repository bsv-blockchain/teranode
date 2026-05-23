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
	commit = runGitCommand("unknown", "rev-parse", "--short", "HEAD")

	gitTag := runGitCommand("", "describe", "--tags", "--exact-match")

	if gitTag != "" && strings.HasPrefix(gitTag, "v") {
		version = gitTag
		return
	}

	timestamp := runGitCommand(
		time.Now().Format("20060102150405"),
		"show", "-s", "--format=%cd", "--date=format:%Y%m%d%H%M%S", "HEAD",
	)

	if commit == "unknown" {
		version = fmt.Sprintf("v0.0.0-%s-unknown", timestamp)
	} else {
		version = fmt.Sprintf("v0.0.0-%s-%s", timestamp, commit)
	}
}

// runGitCommand runs a git command and returns the trimmed output, or fallback on failure.
func runGitCommand(fallback string, args ...string) string {
	output, err := exec.Command("git", args...).Output()
	if err != nil {
		return fallback
	}
	return strings.TrimSpace(string(output))
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
