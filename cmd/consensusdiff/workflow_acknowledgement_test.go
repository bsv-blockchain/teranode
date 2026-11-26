package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConsensusGateAcknowledgement(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/consensus-lib-gate.yaml")
	require.NoError(t, err)
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Name string
				Run  string
			}
		}
	}
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	var script string
	for _, step := range workflow.Jobs["consensus-library-gate"].Steps {
		if step.Name == "Run the consensus vectors on base and pull request" {
			script = step.Run
		}
	}
	require.NotEmpty(t, script)

	for _, tc := range []struct {
		name, action, label, acknowledged string
		status                            int
		wantSuccess                       bool
	}{
		{"fresh acknowledgement", "labeled", "consensus-change-acknowledged", "true", 3, true},
		{"later push with the label still present", "synchronize", "", "true", 3, false},
		{"reopened with the label still present", "reopened", "", "true", 3, false},
		{"unrelated label", "labeled", "bug", "true", 3, false},
		{"label removed", "unlabeled", "consensus-change-acknowledged", "false", 3, false},
		{"execution failure cannot be acknowledged", "labeled", "consensus-change-acknowledged", "true", 1, false},
		{"equal outcomes need no acknowledgement", "synchronize", "", "false", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, "scripts"), 0o700))
			stub := "#!/usr/bin/env bash\nmkdir -p \"$2\"\nprintf 'comparison\\n' >\"$2/summary.md\"\nexit \"$GATE_STATUS\"\n"
			require.NoError(t, os.WriteFile(filepath.Join(dir, "scripts/consensus_lib_gate.sh"), []byte(stub), 0o700))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "-e", "-c", script)
			cmd.Dir = dir
			cmd.Env = append(os.Environ(),
				"BASE_SHA=base", "RUNNER_TEMP="+dir, "GITHUB_STEP_SUMMARY="+filepath.Join(dir, "summary.md"),
				"ACK_LABEL=consensus-change-acknowledged", "ACKNOWLEDGED="+tc.acknowledged,
				"PR_ACTION="+tc.action, "PR_LABEL="+tc.label, "GATE_STATUS="+strconv.Itoa(tc.status))
			out, err := cmd.CombinedOutput()
			if tc.wantSuccess {
				require.NoError(t, err, "%s", out)
			} else {
				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr, "%s", out)
				require.Equal(t, 1, exitErr.ExitCode(), "%s", out)
			}
		})
	}
}
