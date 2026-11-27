package utxo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithCreating(t *testing.T) {
	o := &CreateOptions{}
	WithCreating(true)(o)
	require.True(t, o.Creating)

	WithCreating(false)(o)
	require.False(t, o.Creating)
}
