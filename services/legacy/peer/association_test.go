package peer

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

func TestNewAssociation(t *testing.T) {
	id := []byte{0x01, 0x02, 0x03}
	a := NewAssociation(id, nil)

	require.Equal(t, id, a.RawID())
	require.Equal(t, "010203", a.ID())
	require.Equal(t, 1, a.StreamCount(), "should have GENERAL stream by default")
	require.NotNil(t, a.Stream(wire.StreamTypeGeneral))
}

func TestAssociationAddRemoveStream(t *testing.T) {
	id := []byte{0x01, 0x02, 0x03}
	a := NewAssociation(id, nil)

	// Add DATA1 stream.
	ok := a.AddStream(wire.StreamTypeData1, nil)
	require.True(t, ok)
	require.Equal(t, 2, a.StreamCount())

	// Duplicate should fail.
	ok = a.AddStream(wire.StreamTypeData1, nil)
	require.False(t, ok)

	// Retrieve it.
	s := a.Stream(wire.StreamTypeData1)
	require.NotNil(t, s)
	require.Equal(t, wire.StreamTypeData1, s.Type)

	// Non-existent stream.
	require.Nil(t, a.Stream(wire.StreamTypeData2))

	// Remove DATA1.
	a.RemoveStream(wire.StreamTypeData1)
	require.Equal(t, 1, a.StreamCount())
	require.Nil(t, a.Stream(wire.StreamTypeData1))
}

func TestAssociationPolicy(t *testing.T) {
	a := NewAssociation([]byte{0x01}, nil)

	require.Equal(t, "", a.Policy())

	a.SetPolicy(wire.BlockPriorityStreamPolicy)
	require.Equal(t, wire.BlockPriorityStreamPolicy, a.Policy())
}

func TestAssociationConcurrentAccess(t *testing.T) {
	a := NewAssociation([]byte{0x01, 0x02, 0x03, 0x04}, nil)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = a.ID()
			_ = a.RawID()
			_ = a.StreamCount()
			_ = a.Policy()
			_ = a.Stream(wire.StreamTypeGeneral)
			a.SetPolicy("test")
		}()
	}
	wg.Wait()
}

func TestGenerateAssociationID(t *testing.T) {
	id, err := GenerateAssociationID()
	require.NoError(t, err)
	require.Len(t, id, 17)
	require.Equal(t, byte(0x00), id[0])

	// Each call should produce a different ID.
	id2, err := GenerateAssociationID()
	require.NoError(t, err)
	require.NotEqual(t, id, id2)
}
