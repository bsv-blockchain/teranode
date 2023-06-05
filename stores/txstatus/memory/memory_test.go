package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemory(t *testing.T) {

	t.Run("memory set", func(t *testing.T) {
		db := New()
		err := db.Delete(context.Background(), hash)
		require.NoError(t, err)

		testStore(t, db)
	})
}

func TestMemorySanity(t *testing.T) {
	db := New()
	testSanity(t, db)
}

func BenchmarkMemory(b *testing.B) {
	db := New()
	benchmark(b, db)
}
