package model

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// rawSubtreeStore is a minimal SubtreeStore that returns the exact serialized bytes it
// was given, keyed by subtree root hash. Unlike a blob store it adds NO file-format
// header, so GetAndValidateSubtrees deserializes the bytes cleanly on the first attempt
// — this is deliberate: a deserialize failure would enter GetAndValidateSubtrees' retry
// (Block.go), which for a missing WithRetryCount defaults to unbounded retries. Returning
// exactly Subtree.Serialize() output guarantees a clean load so the test reaches the
// post-load size check without any retry/backoff.
type rawSubtreeStore struct {
	data map[chainhash.Hash][]byte
}

func (s *rawSubtreeStore) GetIoReader(_ context.Context, key []byte, _ fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	var h chainhash.Hash
	copy(h[:], key)

	b, ok := s.data[h]
	if !ok {
		return nil, errors.ErrNotFound
	}

	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *rawSubtreeStore) put(t *testing.T, st *subtreepkg.Subtree) {
	t.Helper()

	b, err := st.Serialize()
	require.NoError(t, err)

	s.data[*st.RootHash()] = b
}

// randCorruptTestHash returns a random 32-byte hash for building test subtrees.
func randCorruptTestHash(t *testing.T) chainhash.Hash {
	t.Helper()

	b := make([]byte, 32)
	_, err := rand.Read(b)
	require.NoError(t, err)

	h, err := chainhash.NewHash(b)
	require.NoError(t, err)

	return *h
}

// TestGetAndValidateSubtrees_SizeMismatchIsCorrupt loads three subtrees where a NON-final
// subtree is shorter than the first. Only the final subtree may be incomplete, so this is
// a body-derived shape violation: GetAndValidateSubtrees must classify it corrupt
// (re-download), never invalid (poison) — bitcoin-sv/teranode#4692. The subtrees are served as raw
// serialized bytes so they deserialize cleanly and the check runs without any retry; a
// short-deadline context is a hard no-hang ceiling.
func TestGetAndValidateSubtrees_SizeMismatchIsCorrupt(t *testing.T) {
	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	// First subtree: full (coinbase placeholder + 3 nodes) → length 4.
	st0, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st0.AddCoinbaseNode())
	for i := 0; i < 3; i++ {
		require.NoError(t, st0.AddNode(randCorruptTestHash(t), 1, 0))
	}

	// Middle (non-final) subtree: only 2 nodes → length 2, an illegal size mismatch.
	st1, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		require.NoError(t, st1.AddNode(randCorruptTestHash(t), 1, 0))
	}

	// Final subtree: full length 4 (so the only violation is the middle subtree).
	st2, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		require.NoError(t, st2.AddNode(randCorruptTestHash(t), 1, 0))
	}

	store := &rawSubtreeStore{data: make(map[chainhash.Hash][]byte)}
	store.put(t, st0)
	store.put(t, st1)
	store.put(t, st2)

	b, err := NewBlock(blockHeader, coinbase,
		[]*chainhash.Hash{st0.RootHash(), st1.RootHash(), st2.RootHash()},
		10, 123, 0, 0)
	require.NoError(t, err)

	// Hard no-hang ceiling: a clean load returns in microseconds; if a regression ever
	// re-introduced a retry loop this bounds the test instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, store, 1)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "non-final subtree size mismatch must be corrupt, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "must not poison")
	require.Contains(t, err.Error(), "has length")
}
