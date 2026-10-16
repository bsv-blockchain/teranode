package settings

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
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
// copy of the repo's settings.conf. gocore's processFile walks up parent
// directories looking for settings_local.conf / settings_test.conf, so the
// test asserts that none exists in the temp dir or any ancestor before
// spawning the helper, and the subprocess gets a minimal explicit environment
// because gocore consults environment variables before the file.
func TestSettingsConfSelfSufficientUnderDocker(t *testing.T) {
	repoRoot, err := filepath.Abs("..")
	require.NoError(t, err)

	src := filepath.Join(repoRoot, "settings.conf")
	contents, err := os.ReadFile(src)
	require.NoError(t, err)

	tmpDir := t.TempDir()
	dst := filepath.Join(tmpDir, "settings.conf")
	require.NoError(t, os.WriteFile(dst, contents, 0o644))

	for dir := tmpDir; ; dir = filepath.Dir(dir) {
		for _, name := range []string{"settings_local.conf", "settings_test.conf"} {
			_, statErr := os.Stat(filepath.Join(dir, name))
			require.True(t, os.IsNotExist(statErr), "ambient %s in %s breaks this test isolation", name, dir)
		}

		if dir == filepath.Dir(dir) {
			break
		}
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestSettingsConfResolveHelper$", "-test.v")
	cmd.Dir = tmpDir
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		settingsConfResolveHelperEnv + "=1",
		"SETTINGS_CONTEXT=docker",
	}

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "resolve helper subprocess failed:\n%s", out)
	require.Contains(t, string(out), helperOKPrefix,
		"resolve helper did not run (test pattern matched nothing?); the guard would pass vacuously:\n%s", out)

	// The sentinel alone is not enough. gocore logs a warning and carries on with
	// an empty map when it finds no settings.conf, so a helper that reached no
	// config at all still prints "HELPER-OK: 0" and exits clean. Requiring a
	// realistic key count is what makes the guard notice that it checked nothing.
	require.Greater(t, helperKeysChecked(t, string(out)), 100,
		"resolve helper checked implausibly few keys, so it did not load settings.conf:\n%s", out)

	unresolved := extractHelperFindings(string(out))
	require.Empty(t, unresolved,
		"settings.conf must be self-sufficient in the docker context: every ${VAR} it references "+
			"must resolve to a value defined in settings.conf itself. Found unresolved:\n%s",
		strings.Join(unresolved, "\n"))
}

// helperFindingPrefix marks a line of helper stdout as a reportable finding,
// so log noise from gocore's own INFO/WARN lines doesn't get misread as one.
const helperFindingPrefix = "UNRESOLVED: "

// helperOKPrefix is a sentinel the helper prints after checking every key, so
// the parent can tell the helper really ran (go test exits 0 when -test.run
// matches no test).
const helperOKPrefix = "HELPER-OK: "

// helperKeysChecked returns the key count the helper reported alongside its
// sentinel, so the caller can tell a real run from one that loaded no config.
func helperKeysChecked(t *testing.T, output string) int {
	t.Helper()

	for _, line := range strings.Split(output, "\n") {
		after, ok := strings.CutPrefix(strings.TrimSpace(line), helperOKPrefix)
		if !ok {
			continue
		}

		n, err := strconv.Atoi(strings.TrimSpace(after))
		require.NoError(t, err, "malformed helper sentinel %q", line)

		return n
	}

	require.FailNow(t, "no helper sentinel in output", output)

	return 0
}

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

	fmt.Printf("%s%d\n", helperOKPrefix, len(keys))
}
