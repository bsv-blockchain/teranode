package subtreevalidation

import (
	"context"
	"os"
	"testing"

	"github.com/ordishs/gocore"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
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
	defer func() {
		// remove quorum path
		quorumPath, _ := gocore.Config().Get("subtree_quorum_path", "")
		if quorumPath != "" {
			_ = os.RemoveAll(quorumPath)
		}
	}()

	hash := chainhash.HashH([]byte("test"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotLock, _, _, err := tryLockIfNotExists(ctx, ulogger.TestLogger{}, exister, &hash)
	require.NoError(t, err)
	assert.True(t, gotLock)

	gotLock, _, _, err = tryLockIfNotExists(ctx, ulogger.TestLogger{}, exister, &hash)
	require.NoError(t, err)
	assert.False(t, gotLock)

}
