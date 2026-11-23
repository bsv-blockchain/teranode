package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// positionStore answers Get with a lane position, or err, and records the fields asked for.
type positionStore struct {
	utxo.Store
	pos   uint64
	err   error
	asked [][]fields.FieldName
}

func (s *positionStore) Get(_ context.Context, _ *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	s.asked = append(s.asked, f)

	if s.err != nil {
		return nil, s.err
	}

	return &meta.Data{LanePosition: s.pos}, nil
}

// The shadow takes a new record's position from its metadata, reads it for an existing mined
// record whose metadata came without it, and reads nothing for an unmined one.
func TestLaneShadowPosition(t *testing.T) {
	store := &positionStore{pos: lane.Pack(3, 9)}
	u := &Server{utxoStore: store}
	hash := chainhash.Hash{1}

	require.Equal(t, lane.Pack(1, 1), u.laneShadowPosition(context.Background(), &hash, &meta.Data{LanePosition: lane.Pack(1, 1), BlockIDs: []uint32{5}}))
	require.Equal(t, uint64(0), u.laneShadowPosition(context.Background(), &hash, &meta.Data{}))
	require.Empty(t, store.asked, "a position in hand, or an unmined record, needs no read")

	require.Equal(t, lane.Pack(3, 9), u.laneShadowPosition(context.Background(), &hash, &meta.Data{BlockIDs: []uint32{5}}))
	require.Equal(t, [][]fields.FieldName{{fields.LanePosition}}, store.asked)

	store.err = errors.NewStorageError("down")
	require.Equal(t, uint64(0), u.laneShadowPosition(context.Background(), &hash, &meta.Data{BlockIDs: []uint32{5}}))
}
