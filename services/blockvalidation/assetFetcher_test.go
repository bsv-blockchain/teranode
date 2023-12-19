package blockvalidation

import (
	"fmt"
	"testing"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestAll(t *testing.T) {
	fetcher := NewAssetFetcher[chainhash.Hash, string](simulateBuildItem, 5*time.Second)

	g := errgroup.Group{}

	for i := 0; i < 5; i++ {
		g.Go(func() error {
			var key = new(chainhash.Hash)

			res, err := fetcher.GetAsset(*key)
			require.NoError(t, err)
			assert.Equal(t, "Hash is 0000000000000000000000000000000000000000000000000000000000000000", res)

			return nil
		})
	}

	err := g.Wait()
	require.NoError(t, err)
}

// SimulateResourceBuilder simulates building a resource.
func simulateBuildItem(key chainhash.Hash) (string, error) {
	fmt.Println("Building resource...")
	time.Sleep(2 * time.Second)
	return fmt.Sprintf("Hash is %v", key), nil
}
