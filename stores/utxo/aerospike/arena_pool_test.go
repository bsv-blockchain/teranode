package aerospike

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateArenaPool_GetResetPut(t *testing.T) {
	a := getCreateArena()
	require.NotNil(t, a)
	require.Equal(t, 0, a.Used())

	buf := a.Alloc(128)
	require.Len(t, buf, 128)
	require.Equal(t, 128, a.Used())

	putCreateArena(a)

	b := getCreateArena()
	require.Equal(t, 0, b.Used()) // reset on return
	putCreateArena(b)
}
