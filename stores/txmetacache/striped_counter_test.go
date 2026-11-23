package txmetacache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStripedCounter_SumsConcurrentAdds(t *testing.T) {
	var c stripedCounter

	const (
		goroutines = 32
		perG       = 10_000
	)

	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < perG; i++ {
				c.Add(3)
			}
		}()
	}

	wg.Wait()

	require.Equal(t, uint64(goroutines*perG*3), c.Load())
}
