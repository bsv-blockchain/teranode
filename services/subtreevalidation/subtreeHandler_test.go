// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"os"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/quorum"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockExister struct{}

func (m MockExister) Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error) {
	return false, nil
}

func TestLock(t *testing.T) {
	exister := MockExister{}

	tSettings := test.CreateBaseTestSettings()

	tSettings.SubtreeValidation.QuorumPath = "./data/subtree_quorum"

	defer func() {
		_ = os.RemoveAll(tSettings.SubtreeValidation.QuorumPath)
	}()

	q, err := quorum.New(ulogger.TestLogger{}, exister, tSettings.SubtreeValidation.QuorumPath)
	require.NoError(t, err)

	hash := chainhash.HashH([]byte("test"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotLock, _, releaseFn, err := q.TryLockIfNotExists(ctx, &hash)
	require.NoError(t, err)
	assert.True(t, gotLock)

	defer releaseFn()

	gotLock, _, releaseFn, err = q.TryLockIfNotExists(ctx, &hash)
	require.NoError(t, err)
	assert.False(t, gotLock)

	defer releaseFn()
}
