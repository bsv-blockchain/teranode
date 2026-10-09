package utxo_test

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/stretchr/testify/require"
)

// knownStore answers SetMinedMulti for the hashes it holds and reports the rest not found, as
// the utxoset store does for a transaction it has never seen.
type knownStore struct {
	*nullstore.NullStore
	mu     sync.Mutex
	known  map[chainhash.Hash]bool
	marked []chainhash.Hash
	fail   error
}

func (s *knownStore) SetMinedMulti(_ context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fail != nil {
		return nil, s.fail
	}

	out := map[chainhash.Hash][]uint32{}

	for _, h := range hashes {
		if !s.known[*h] {
			return nil, errors.NewTxNotFoundError("%s", h.String())
		}

		s.marked = append(s.marked, *h)
		out[*h] = []uint32{info.BlockID}
	}

	return out, nil
}

// A block path that chose not to store some transactions marks those it finds were stored
// anyway, by an earlier failed attempt at the same block, and ignores the rest.
func TestSetMinedIfPresentMarksOnlyWhatTheStoreHolds(t *testing.T) {
	held, never := chainhash.Hash{1}, chainhash.Hash{2}
	store := &knownStore{NullStore: &nullstore.NullStore{}, known: map[chainhash.Hash]bool{held: true}}

	n, err := utxo.SetMinedIfPresent(context.Background(), store, []*chainhash.Hash{&held, &never}, utxo.MinedBlockInfo{BlockID: 9}, 4)
	require.NoError(t, err, "a transaction the store has never seen is the normal case")
	require.Equal(t, 1, n)
	require.Equal(t, []chainhash.Hash{held}, store.marked)
}

func TestSetMinedIfPresentReturnsAStoreFailure(t *testing.T) {
	h := chainhash.Hash{3}
	store := &knownStore{NullStore: &nullstore.NullStore{}, fail: errors.NewStorageError("down")}

	_, err := utxo.SetMinedIfPresent(context.Background(), store, []*chainhash.Hash{&h}, utxo.MinedBlockInfo{BlockID: 9}, 4)
	require.Error(t, err)
}
