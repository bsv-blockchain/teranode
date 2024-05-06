package options

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptionsAreCopied(t *testing.T) {
	storeOptions := &SetOptions{
		Extension: "old",
	}

	newOptions := NewSetOptions(storeOptions, WithFileExtension("new"))

	// Check the new options are changed but the old are untouched
	assert.Equal(t, "new", newOptions.Extension)
	assert.Equal(t, "old", storeOptions.Extension)

}
