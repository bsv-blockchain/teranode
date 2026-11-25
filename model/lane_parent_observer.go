package model

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// LaneParentObserver is told of each parent record block validation's parent check reads, at
// utxostore_laneMigration shadow, so lanes can answer the same check from its index and count
// agreement (util/lanereader). coinbase is the parent record's coinbase flag: a Phase 1 row never
// lists a coinbase, so lanes counts such a parent apart. It never changes the check's verdict. ObserveParent may be called
// from many goroutines at once, and must not hold on to blockIDs.
type LaneParentObserver interface {
	ObserveParent(parentHash chainhash.Hash, lanePosition uint64, coinbase bool, blockIDs []uint32)
}

type laneParentObserverKey struct{}

// ContextWithLaneParentObserver returns ctx carrying observer for Block.Valid's parent check. A
// context without one, the case at off, reads exactly the fields it read before lanes.
func ContextWithLaneParentObserver(ctx context.Context, observer LaneParentObserver) context.Context {
	if observer == nil {
		return ctx
	}

	return context.WithValue(ctx, laneParentObserverKey{}, observer)
}

func laneParentObserverFrom(ctx context.Context) LaneParentObserver {
	observer, _ := ctx.Value(laneParentObserverKey{}).(LaneParentObserver)

	return observer
}
