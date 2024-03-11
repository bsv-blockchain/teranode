package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockExister struct{}

func (m MockExister) Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error) {
	return false, nil
}

func TestLock(t *testing.T) {
	exister := MockExister{}

	hash := chainhash.HashH([]byte("test"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotLock, _, err := tryLockIfNotExists(ctx, exister, &hash)
	require.NoError(t, err)
	assert.True(t, gotLock)

	gotLock, _, err = tryLockIfNotExists(ctx, exister, &hash)
	require.NoError(t, err)
	assert.False(t, gotLock)

}
