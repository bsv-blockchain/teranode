package peer

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAssociationManagerRegisterLookupRemove(t *testing.T) {
	mgr := NewAssociationManager()

	id := []byte{0xaa, 0xbb, 0xcc}
	a := NewAssociation(id, nil)

	// Register.
	ok := mgr.Register(a)
	require.True(t, ok)
	require.Equal(t, 1, mgr.Count())

	// Duplicate register.
	ok = mgr.Register(a)
	require.False(t, ok)
	require.Equal(t, 1, mgr.Count())

	// Lookup.
	found := mgr.Lookup(id)
	require.NotNil(t, found)
	require.Equal(t, a.ID(), found.ID())

	// Lookup unknown.
	require.Nil(t, mgr.Lookup([]byte{0xff}))

	// Remove.
	mgr.Remove(id)
	require.Equal(t, 0, mgr.Count())
	require.Nil(t, mgr.Lookup(id))
}

func TestAssociationManagerConcurrentAccess(t *testing.T) {
	mgr := NewAssociationManager()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := []byte{byte(idx)}
			a := NewAssociation(id, nil)
			mgr.Register(a)
			mgr.Lookup(id)
			_ = mgr.Count()
			mgr.Remove(id)
		}(i)
	}
	wg.Wait()
}
