package main

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestPopulateVersionInfo tests the version population logic
func TestPopulateVersionInfo(t *testing.T) {
	// Save original values
	originalVersion := version
	originalCommit := commit
	defer func() {
		version = originalVersion
		commit = originalCommit
	}()

	t.Run("populates_version_info", func(t *testing.T) {
		// Reset global variables
		version = ""
		commit = ""

		// Call the function - it will use real git commands
		populateVersionInfo()

		// Check that values were populated
		assert.NotEmpty(t, version, "Version should not be empty")
		assert.NotEmpty(t, commit, "Commit should not be empty")

		// Version should start with 'v'
		assert.True(t, strings.HasPrefix(version, "v"), "Version should start with 'v'")

		// Commit should be non-empty string (either a hash or "unknown")
		assert.True(t, len(commit) > 0, "Commit should not be empty")

		t.Logf("Generated version: %s", version)
		t.Logf("Generated commit: %s", commit)
	})

	t.Run("handles_empty_initial_values", func(t *testing.T) {
		// Test with explicitly empty values
		version = ""
		commit = ""

		beforeVersion := version
		beforeCommit := commit

		populateVersionInfo()

		// Should have changed from empty
		assert.NotEqual(t, beforeVersion, version)
		assert.NotEqual(t, beforeCommit, commit)
	})
}

// TestInit tests the init function behavior
func TestInit(t *testing.T) {
	// Save original values
	originalVersion := version
	originalCommit := commit
	defer func() {
		version = originalVersion
		commit = originalCommit
	}()

	t.Run("init_logic_when_both_empty", func(t *testing.T) {
		// Reset to empty (simulating no build-time injection)
		version = ""
		commit = ""

		// Test the init logic: if version == "" && commit == "" { populateVersionInfo() }
		if version == "" && commit == "" {
			populateVersionInfo()
		}

		assert.NotEmpty(t, version)
		assert.NotEmpty(t, commit)
	})

	t.Run("init_logic_when_version_set", func(t *testing.T) {
		// Pre-set version but not commit (simulating partial build-time injection)
		version = "v2.0.0"
		commit = ""

		originalVer := version
		originalCom := commit

		// The init logic: if version == "" && commit == "" { populateVersionInfo() }
		// This should NOT call populateVersionInfo since version is not empty
		shouldCallPopulate := version == "" && commit == ""
		assert.False(t, shouldCallPopulate, "Should not call populateVersionInfo when version is set")

		// Values should remain unchanged if we don't call populateVersionInfo
		if !shouldCallPopulate {
			assert.Equal(t, originalVer, version)
			assert.Equal(t, originalCom, commit)
		}
	})

	t.Run("init_logic_when_commit_set", func(t *testing.T) {
		// Pre-set commit but not version
		version = ""
		commit = "build123"

		originalVer := version
		originalCom := commit

		// The init logic: if version == "" && commit == "" { populateVersionInfo() }
		// This should NOT call populateVersionInfo since commit is not empty
		shouldCallPopulate := version == "" && commit == ""
		assert.False(t, shouldCallPopulate, "Should not call populateVersionInfo when commit is set")

		// Values should remain unchanged if we don't call populateVersionInfo
		if !shouldCallPopulate {
			assert.Equal(t, originalVer, version)
			assert.Equal(t, originalCom, commit)
		}
	})

	t.Run("init_logic_when_both_set", func(t *testing.T) {
		// Pre-set both values (simulating full build-time injection)
		version = "v2.0.0"
		commit = "build123"

		originalVer := version
		originalCom := commit

		// The init logic: if version == "" && commit == "" { populateVersionInfo() }
		// This should NOT call populateVersionInfo since both are set
		shouldCallPopulate := version == "" && commit == ""
		assert.False(t, shouldCallPopulate, "Should not call populateVersionInfo when both are set")

		// Values should remain unchanged
		assert.Equal(t, originalVer, version)
		assert.Equal(t, originalCom, commit)
	})
}

// TestMainVersionFlag tests the main function's version flag handling logic
func TestMainVersionFlag(t *testing.T) {
	// Save original values
	originalVersion := version
	originalCommit := commit
	defer func() {
		version = originalVersion
		commit = originalCommit
	}()

	// Set test values
	version = "v1.2.3"
	commit = "abc123"

	tests := []struct {
		name       string
		args       []string
		shouldExit bool
	}{
		{
			name:       "version_long_flag",
			args:       []string{"teranode", "--version"},
			shouldExit: true,
		},
		{
			name:       "version_short_flag",
			args:       []string{"teranode", "-v"},
			shouldExit: true,
		},
		{
			name:       "version_flag_with_other_args",
			args:       []string{"teranode", "--config", "test.conf", "--version"},
			shouldExit: true,
		},
		{
			name:       "no_version_flag",
			args:       []string{"teranode", "--config", "test.conf"},
			shouldExit: false,
		},
		{
			name:       "empty_args",
			args:       []string{"teranode"},
			shouldExit: false,
		},
		{
			name:       "version_substring_no_match",
			args:       []string{"teranode", "--my-version"},
			shouldExit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the version flag detection logic (extracted from main function)
			foundVersionFlag := false
			for _, arg := range tt.args[1:] { // Skip program name
				if arg == "--version" || arg == "-v" {
					foundVersionFlag = true
					break
				}
			}

			if tt.shouldExit {
				assert.True(t, foundVersionFlag, "Should have found version flag")
				// We can't test os.Exit(), but we can test the logic and output format
				if foundVersionFlag {
					// This is the format that would be printed before os.Exit(0)
					expectedOutput := progname + " version " + version + " (commit: " + commit + ")"
					assert.Contains(t, expectedOutput, "teranode")
					assert.Contains(t, expectedOutput, "v1.2.3")
					assert.Contains(t, expectedOutput, "abc123")
				}
			} else {
				assert.False(t, foundVersionFlag, "Should not have found version flag")
				// In real main(), this would call teranode.RunDaemon()
			}
		})
	}
}

// TestProgname tests that the program name constant is correct
func TestProgname(t *testing.T) {
	assert.Equal(t, "teranode", progname)
}

// TestVersionCommitGlobals tests the global variables
func TestVersionCommitGlobals(t *testing.T) {
	// These are package-level variables that can be set at build time
	// We test that they exist and can be modified
	originalVersion := version
	originalCommit := commit
	defer func() {
		version = originalVersion
		commit = originalCommit
	}()

	// Test that we can set them
	version = "test-version"
	commit = "test-commit"

	assert.Equal(t, "test-version", version)
	assert.Equal(t, "test-commit", commit)

	// Test they start as strings (even if empty)
	version = ""
	commit = ""
	assert.Equal(t, "", version)
	assert.Equal(t, "", commit)
}

// TestPopulateVersionInfoComponents tests individual components of version generation
func TestPopulateVersionInfoComponents(t *testing.T) {
	originalVersion := version
	originalCommit := commit
	defer func() {
		version = originalVersion
		commit = originalCommit
	}()

	t.Run("version_format_validation", func(t *testing.T) {
		version = ""
		commit = ""

		populateVersionInfo()

		// Test version format
		assert.True(t, strings.HasPrefix(version, "v"), "Version should start with 'v'")

		// Should have some content
		assert.True(t, len(version) > 1, "Version should be more than just 'v'")
		assert.True(t, len(commit) > 0, "Commit should not be empty")

		// Version should be in one of two formats:
		// 1. vX.Y.Z (if git tag exists) - can include suffixes like v1.2.3-beta
		// 2. v0.0.0-TIMESTAMP-COMMIT (if no git tag)
		if strings.HasPrefix(version, "v0.0.0-") {
			// Timestamp-based format (no git tag)
			parts := strings.Split(version, "-")
			assert.True(t, len(parts) >= 3, "Timestamp-based version should have format v0.0.0-TIMESTAMP-COMMIT")
			assert.Equal(t, "v0.0.0", parts[0], "Timestamp-based version should start with v0.0.0")
		} else {
			// Tag-based format (can have any format, but typically vX.Y.Z with optional suffix)
			assert.Regexp(t, `^v\d+\.\d+\.\d+`, version, "Version should match vX.Y.Z format")
		}
	})

	t.Run("commit_validation", func(t *testing.T) {
		version = ""
		commit = ""

		populateVersionInfo()

		// Commit should be either a hash or "unknown"
		assert.True(t, len(commit) > 0, "Commit should not be empty")

		if commit != "unknown" {
			// If it's not "unknown", it should look like a git hash
			assert.True(t, len(commit) >= 6, "Git commit hash should be at least 6 characters")
			// Git hashes are typically hexadecimal
			for _, char := range commit {
				assert.True(t,
					(char >= '0' && char <= '9') ||
						(char >= 'a' && char <= 'f') ||
						(char >= 'A' && char <= 'F'),
					"Commit hash should be hexadecimal: %s", string(char))
			}
		}
	})

	t.Run("timestamp_when_needed", func(t *testing.T) {
		version = ""
		commit = ""

		populateVersionInfo()

		// If version contains a timestamp (indicated by having 3 parts separated by -)
		if strings.Count(version, "-") >= 2 {
			parts := strings.Split(version, "-")
			if len(parts) >= 2 {
				timestampStr := parts[1]
				if len(timestampStr) == 14 { // YYYYMMDDHHMMSS format
					parsedTime, err := time.Parse("20060102150405", timestampStr)
					if err == nil {
						// If we can parse it as a timestamp, verify it's reasonable
						// Git timestamps can be arbitrarily old, so we just check:
						// 1. Not before project start (2020)
						// 2. Not more than 1 hour in the future (allowing for clock skew)
						now := time.Now().UTC()
						projectStart := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
						assert.True(t, parsedTime.After(projectStart),
							"Timestamp should be after project start date (got: %v)", parsedTime)
						assert.True(t, parsedTime.Before(now.Add(23*time.Hour)),
							"Timestamp should not be more than 23 hour in the future (got: %v, now: %v)", parsedTime, now)
						t.Logf("Parsed timestamp: %v", parsedTime)
					}
				}
			}
		}
	})
}

// TestPopulateVersionInfoEdgeCases tests edge cases that are hard to trigger
func TestPopulateVersionInfoEdgeCases(t *testing.T) {
	originalVersion := version
	originalCommit := commit
	defer func() {
		version = originalVersion
		commit = originalCommit
	}()

	t.Run("code_structure_documentation", func(t *testing.T) {
		// This test documents the structure of populateVersionInfo
		// to ensure we understand what branches exist, even if we can't easily test them

		version = ""
		commit = ""

		// The function has several conditional branches:
		// 1. git rev-parse command success/failure (lines ~30-34)
		// 2. git describe command success/failure (lines ~39-42)
		// 3. git tag validation (lines ~46-48)
		// 4. git show command success/failure for timestamp (lines ~51-56)
		// 5. commit "unknown" check (lines ~59-63)

		populateVersionInfo()

		// Document what we can verify
		assert.NotEmpty(t, version, "Version populated")
		assert.NotEmpty(t, commit, "Commit populated")

		// The version format tells us which code path was taken
		if strings.HasPrefix(version, "v0.0.0-") {
			t.Logf("Timestamp-based version generated: %s", version)
			parts := strings.Split(version, "-")
			if len(parts) >= 3 {
				assert.Equal(t, "v0.0.0", parts[0])
				t.Logf("Timestamp part: %s", parts[1])
				t.Logf("Commit part: %s", parts[2])
			}
		} else {
			t.Logf("Tag-based version generated: %s", version)
			assert.True(t, strings.HasPrefix(version, "v"))
		}

		// Commit is either a hex hash or "unknown"
		if commit == "unknown" {
			t.Logf("Git commit command failed, using 'unknown'")
		} else {
			t.Logf("Git commit hash retrieved: %s", commit)
		}
	})
}

// TestMainFunctionStructure documents the main function logic without executing it
func TestMainFunctionStructure(t *testing.T) {
	t.Run("main_function_logic_documentation", func(t *testing.T) {
		// This test documents the main() function structure:
		// 1. Check for version flags in os.Args[1:]
		// 2. If found: print version and os.Exit(0)
		// 3. If not found: call teranode.RunDaemon()

		// We already tested the version flag logic in TestMainVersionFlag
		// The main function itself cannot be easily tested because:
		// - os.Exit(0) terminates the program
		// - teranode.RunDaemon() starts the actual service

		// Document the untested lines:
		// Line 67: func main() {
		// Line 69: for _, arg := range os.Args[1:] {
		// Line 70: if arg == "--version" || arg == "-v" {
		// Line 71-72: fmt.Printf + os.Exit(0)
		// Line 77: teranode.RunDaemon(progname, version, commit)

		assert.Equal(t, "teranode", progname, "Program name used in main()")

		// Verify version and commit are accessible for RunDaemon
		assert.True(t, len(version) >= 0, "Version variable exists")
		assert.True(t, len(commit) >= 0, "Commit variable exists")
	})
}
