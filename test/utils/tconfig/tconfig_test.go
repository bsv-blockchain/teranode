//go:build !test_all

package tconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTConfig(t *testing.T) {
	t.Run("SetConfigFromEnvVar", func(t *testing.T) {
		// These are set in environment variable in main_test.go
		assert.Equal(t, testConfig.Suite.Name, "TestNameFromEnv")

		// These are set in environment variable in main_test.go
		assert.Equal(t, len(testConfig.LocalSystem.Composes), 2)
		assert.Equal(t, testConfig.LocalSystem.Composes, []string{"Compose1FromEnv", "Compose2FromEnv"})
	})

	t.Run("SetConfigProgrammatically", func(t *testing.T) {
		// These are set in environment variable, but then 'hardcoded programmatically' in main_test.go
		assert.Equal(t, len(testConfig.Teranode.Contexts), 2)
		assert.Equal(t, testConfig.Teranode.Contexts, []string{"HardcodedContext1", "HardcodedContext1"})
	})

	t.Run("WriteConfigToYaml", func(t *testing.T) {
		// Marshal the struct to YAML
		tconfigYAML := testConfig.StringYAML()
		assert.NotEmpty(t, tconfigYAML) // fmt.Printf("\n###  Config for testing inputs  ###\n\n%v", tconfigYAML)
	})
}
