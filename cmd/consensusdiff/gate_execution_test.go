package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestConsensusGateTool runs the real subcommands in the test binary so the
// shell driver can be exercised without building another binary or running vectors.
func TestConsensusGateTool(t *testing.T) {
	if os.Getenv("CONSENSUS_GATE_TEST_TOOL") != "1" {
		return
	}
	different, err := run(os.Args[3], os.Args[4:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if different {
		os.Exit(exitDifferent)
	}
	os.Exit(0)
}

// coverCount is the execution count the stubbed head run writes for the bumped
// module's one statement.
func coverCount(uncovered bool) string {
	if uncovered {
		return "0"
	}

	return "1"
}

func TestConsensusGateExecution(t *testing.T) {
	script, err := filepath.Abs("../../scripts/consensus_lib_gate.sh")
	require.NoError(t, err)
	binary, err := os.Executable()
	require.NoError(t, err)
	complete := goTestJSON(event("pass", "TestFirst", ""), event("pass", "TestLater", ""), event("pass", "", ""))
	partial := goTestJSON(event("pass", "TestFirst", ""), event("run", "TestLater", ""))
	failed := goTestJSON(event("pass", "TestFirst", ""), event("fail", "TestLater", ""), event("fail", "", ""))

	for _, tc := range []struct {
		name, base, head       string
		baseStatus, headStatus int
		wantStatus             int
		uncovered              bool
	}{
		{"complete equal runs", complete, complete, 0, 0, 0, false},
		{"completed test failures are compared", complete, failed, 0, 1, 3, false},
		{"interrupted baseline", partial, complete, 137, 0, 1, false},
		{"incomplete baseline with ordinary failure status", partial, complete, 1, 0, 1, false},
		{"incomplete head", complete, partial, 0, 1, 1, false},
		{"abnormal baseline status even after complete output", complete, complete, 137, 0, 1, false},
		{"abnormal head status even after complete output", complete, complete, 0, 2, 1, false},
		{"bumped module never executed", complete, complete, 0, 0, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "bin")
			require.NoError(t, os.Mkdir(bin, 0o700))
			require.NoError(t, os.MkdirAll(filepath.Join(dir, ".github"), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, ".github/consensus-modules.txt"), []byte("github.com/bsv-blockchain/go-wire\n"), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/node\nrequire github.com/bsv-blockchain/go-wire v1.1.0\n"), 0o600))
			// Only Git checkout and Go execution are substituted. The gate driver,
			// pin detection, coverage reader and result comparison all run unchanged.
			git := `#!/usr/bin/env bash
case "$1 $2" in
"rev-parse --show-toplevel") printf '%s\n' "$GATE_ROOT" ;;
"rev-parse --verify"|"rev-parse --short") echo base ;;
"show base:go.mod") printf 'module example.com/node\nrequire github.com/bsv-blockchain/go-wire v1.0.0\n' ;;
"worktree add") mkdir -p "$5" ;;
"worktree remove") ;;
*) exit 99 ;;
esac
`
			goStub := `#!/usr/bin/env bash
case "$1" in
build)
  printf '#!/usr/bin/env bash\nexec "$GATE_TEST_BINARY" -test.run=^TestConsensusGateTool$ -- "$@"\n' >"$3"
  chmod +x "$3"
  ;;
test)
  if [[ $PWD == */base-tree ]]; then
    printf '%s' "$BASE_JSON"
    exit "$BASE_STATUS"
  fi
  for arg in "$@"; do
    case $arg in
    -coverprofile=*) printf 'mode: set\ngithub.com/bsv-blockchain/go-wire/wire.go:1.1,2.1 1 %s\n' "$HEAD_COVER_COUNT" >"${arg#*=}" ;;
    esac
  done
  printf '%s' "$HEAD_JSON"
  exit "$HEAD_STATUS"
  ;;
*) exit 99 ;;
esac
`
			require.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte(git), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "go"), []byte(goStub), 0o700))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", script, "base", filepath.Join(dir, "out"))
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"GATE_ROOT="+dir, "GATE_TEST_BINARY="+binary, "CONSENSUS_GATE_TEST_TOOL=1",
				"BASE_JSON="+tc.base, "HEAD_JSON="+tc.head,
				"BASE_STATUS="+strconv.Itoa(tc.baseStatus), "HEAD_STATUS="+strconv.Itoa(tc.headStatus),
				"HEAD_COVER_COUNT="+coverCount(tc.uncovered))
			out, err := cmd.CombinedOutput()
			if tc.wantStatus == 0 {
				require.NoError(t, err, "%s", out)
			} else {
				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr, "%s", out)
				require.Equal(t, tc.wantStatus, exitErr.ExitCode(), "%s", out)
			}

			if tc.uncovered {
				require.Contains(t, string(out), "**Not checked:**", "an unexecuted bumped module is reported as not checked")
			}
		})
	}
}
