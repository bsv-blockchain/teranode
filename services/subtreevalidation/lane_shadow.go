package subtreevalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// laneShadowPosition is the lane position the already-mined check's lanes shadow judges hash by,
// at utxostore_laneMigration shadow. A new record's metadata from the validator carries it. A
// record that already existed comes back through GetMeta, which does not read it; if that record
// names a mined block, which is the case the check can reject, the position is read with one more
// store read, asking for fields.LanePosition alone. A failed read gives 0, counted no_position.
func (u *Server) laneShadowPosition(ctx context.Context, hash *chainhash.Hash, txMeta *meta.Data) uint64 {
	if lane.Valid(txMeta.LanePosition) || len(txMeta.BlockIDs) == 0 {
		return txMeta.LanePosition
	}

	md, err := u.utxoStore.Get(ctx, hash, fields.LanePosition)
	if err != nil || md == nil {
		return 0
	}

	return md.LanePosition
}
