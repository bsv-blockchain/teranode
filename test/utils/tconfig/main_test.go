//go:build !test_all

package tconfig

import (
	"os"
	"testing"
)

// testConfig to be initialized at setup and used for all test suites
var testConfig TConfig

// To run all test suites in this package with custom config :
//
//	SUITE_TESTID=OverrideFromCLI go test -v ./test/utils/tconfig/... -args --config-file=../example/tconfig/testabc.json
func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		os.Exit(1)
	}

	exitCode := m.Run()

	if err := tearDown(); err != nil {
		os.Exit(1)
	}

	os.Exit(exitCode)
}

// setup once, used for all suites
func setup() error {
	os.Setenv("SUITE_NAME", "TestNameFromEnv")
	os.Setenv("SUITE_COMPOSES", "Compose1FromEnv Compose2FromEnv")
	os.Setenv("TERANODE_CONTEXTS", "Context1FromEnv Context2FromEnv")

	// Load the test config, customizable by
	// environment variable, overridden by the map[string]any
	testConfig = LoadTConfig(
		map[string]any{
			KeyTeranodeContexts: []string{"HardcodedContext1", "HardcodedContext1"},
		},
	)

	return nil
}

func tearDown() error {
	return nil
}
