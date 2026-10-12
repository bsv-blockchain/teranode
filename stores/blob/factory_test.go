// Package blob provides blob storage functionality with various storage backend implementations.
package blob

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blob/batcher"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestNewStore_BatchSizeInBytes pins the sizeInBytes handling on the batch path. A
// non-positive size used to reach batcher.New and panic inside make(), which is a
// configuration mistake reported as a crash; NewStore must reject it instead.
//
// The batched store is built through NewStore rather than createBatchedStore so the
// test exercises the path a settings URL actually takes.
func TestNewStore_BatchSizeInBytes(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{name: "negative size is rejected", query: "?batch=true&sizeInBytes=-1", expectError: true},
		{name: "zero size is rejected", query: "?batch=true&sizeInBytes=0", expectError: true},
		{name: "unparseable size is rejected", query: "?batch=true&sizeInBytes=abc", expectError: true},
		{name: "positive size is accepted", query: "?batch=true&sizeInBytes=1024", expectError: false},
		{name: "absent size falls back to the default", query: "?batch=true", expectError: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storeURL, err := url.Parse("memory://" + tc.query)
			require.NoError(t, err)

			store, err := NewStore(ulogger.TestLogger{}, storeURL)

			if tc.expectError {
				require.Error(t, err)
				require.Nil(t, store)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, store)
			// A store built on the batch path must actually be batched: without this the
			// accepted cases would pass just as well if batch=true were ignored entirely.
			require.IsType(t, &batcher.Batcher{}, store)
			require.NoError(t, store.Close(context.Background()))
		})
	}
}
