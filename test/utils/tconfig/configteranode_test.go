package tconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigTeranode(t *testing.T) {
	t.Run("SettingsMap", func(t *testing.T) {
		ct := ConfigTeranode{Contexts: []string{"contextA", "contextB", "contextC"}}
		kv := ct.SettingsMap()
		assert.Equal(t, kv["SETTINGS_CONTEXT_1"], "contextA")
		assert.Equal(t, kv["SETTINGS_CONTEXT_2"], "contextB")
		assert.Equal(t, kv["SETTINGS_CONTEXT_3"], "contextC")
	})
}
