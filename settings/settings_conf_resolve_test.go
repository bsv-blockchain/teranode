package settings

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// settingsConfResolveHelperEnv is the marker the parent test sets so the
// subprocess it re-execs knows to run the resolve helper instead of the
// normal test suite.
const settingsConfResolveHelperEnv = "SETTINGS_CONF_RESOLVE_HELPER"

// TestSettingsConfSelfSufficientUnderDocker guards against settings.conf
// depending on ${VAR} definitions that only exist in an overlay file
// (compose/settings_test.conf, settings_local.conf, ...). gocore's
// replaceVariables silently substitutes the literal string "{UNKNOWN}" for
// any ${VAR} it cannot resolve - it never errors - so a missing base default
// fails silent and only surfaces as a broken connection string at runtime.
//
// The check runs gocore itself, not a reimplementation of its resolution
// rules, in a subprocess whose working directory contains nothing but a
// copy of the repo's settings.conf: no settings_local.conf and no
// settings_test.conf are reachable (gocore's processFile walks up parent
// directories looking for them), so the result reflects settings.conf alone.
func TestSettingsConfSelfSufficientUnderDocker(t *testing.T) {
	repoRoot, err := filepath.Abs("..")
	require.NoError(t, err)

	src := filepath.Join(repoRoot, "settings.conf")
	contents, err := os.ReadFile(src)
	require.NoError(t, err)

	tmpDir := t.TempDir()
	dst := filepath.Join(tmpDir, "settings.conf")
	require.NoError(t, os.WriteFile(dst, contents, 0o644))

	cmd := exec.Command(os.Args[0], "-test.run=^TestSettingsConfResolveHelper$", "-test.v")
	cmd.Dir = tmpDir
	cmd.Env = append(os.Environ(),
		settingsConfResolveHelperEnv+"=1",
		"SETTINGS_CONTEXT=docker",
	)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "resolve helper subprocess failed:\n%s", out)

	unresolved := extractHelperFindings(string(out))
	require.Empty(t, unresolved,
		"settings.conf must be self-sufficient in the docker context: every ${VAR} it references "+
			"must resolve to a value defined in settings.conf itself. Found unresolved:\n%s",
		strings.Join(unresolved, "\n"))
}

// helperFindingPrefix marks a line of helper stdout as a reportable finding,
// so log noise from gocore's own INFO/WARN lines doesn't get misread as one.
const helperFindingPrefix = "UNRESOLVED: "

func extractHelperFindings(output string) []string {
	var findings []string

	for _, line := range strings.Split(output, "\n") {
		if after, ok := strings.CutPrefix(line, helperFindingPrefix); ok {
			findings = append(findings, after)
		}
	}

	return findings
}

// TestSettingsConfResolveHelper is not a real test: it only does anything
// when invoked as the subprocess helper for TestSettingsConfSelfSufficientUnderDocker.
func TestSettingsConfResolveHelper(t *testing.T) {
	if os.Getenv(settingsConfResolveHelperEnv) != "1" {
		t.Skip("only runs as a subprocess helper of TestSettingsConfSelfSufficientUnderDocker")
	}

	c := gocore.Config()

	all := c.GetAll()
	delete(all, "_SETTINGS_CONTEXT")

	keys := make([]string, 0, len(all))
	for k := range all {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		v, _ := c.Get(k)
		if strings.Contains(v, "{UNKNOWN}") {
			fmt.Println(helperFindingPrefix + k + "=" + v)
		}
	}
}
