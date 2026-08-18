package utxo

import (
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
)

// BlockStateHolder keeps the chain-tip pair — block height and median block
// time — behind a single atomic pointer, so a GetBlockState reader receives a
// genuine snapshot: both fields written together, never a new height paired
// with a stale median time (issue 1443). Before this, stores read two
// independent atomics one after the other, and a reader landing between the
// two setter calls saw a torn pair that could drive one finality decision
// with values from two different chain tips.
//
// SetPair is the consistent write path (both values from the same tip, as the
// blockchain notification listener supplies them). SetHeight and
// SetMedianTime exist for callers that legitimately update one field alone
// (tests, height-only bootstraps); they carry the other field forward with a
// compare-and-swap loop so concurrent single-field writers cannot erase each
// other's update — but a pair produced that way is only as consistent as the
// caller's sequencing, which is why production code uses SetPair.
type BlockStateHolder struct {
	pair atomic.Pointer[BlockState]
}

// Load returns the current snapshot; the zero BlockState before any write.
func (h *BlockStateHolder) Load() BlockState {
	if p := h.pair.Load(); p != nil {
		return *p
	}

	return BlockState{}
}

// SetPair atomically publishes both fields as one snapshot.
func (h *BlockStateHolder) SetPair(height, medianTime uint32) {
	h.pair.Store(&BlockState{Height: height, MedianTime: medianTime})
}

// SetHeight publishes a new height, carrying the latest median time forward.
func (h *BlockStateHolder) SetHeight(height uint32) {
	for {
		old := h.pair.Load()

		next := &BlockState{Height: height}
		if old != nil {
			next.MedianTime = old.MedianTime
		}

		if h.pair.CompareAndSwap(old, next) {
			return
		}
	}
}

// SetMedianTime publishes a new median time, carrying the latest height forward.
func (h *BlockStateHolder) SetMedianTime(medianTime uint32) {
	for {
		old := h.pair.Load()

		next := &BlockState{MedianTime: medianTime}
		if old != nil {
			next.Height = old.Height
		}

		if h.pair.CompareAndSwap(old, next) {
			return
		}
	}
}

// BlockStateFields supplies the Store interface's six chain-tip methods —
// SetBlockHeight, GetBlockHeight, SetMedianBlockTime, GetMedianBlockTime,
// SetBlockState and GetBlockState — to any store that embeds it, over a single
// BlockStateHolder.
//
// Sharing one implementation is what makes the snapshot guarantee hold across
// stores rather than per store. Each store used to carry its own copy of these
// bodies beside a second, independent height atomic, which meant the height a
// store reported through GetBlockHeight and the height inside the snapshot it
// returned from GetBlockState were different pieces of memory: two readers, or
// one reader making two calls, could see them disagree even under the
// single-writer discipline production relies on. Here both come from the same
// holder, so they cannot.
//
// A store that must mirror the height somewhere else — aerospike pushes it into
// its external blob store — declares its own SetBlockHeight and SetBlockState
// and delegates to the embedded methods for the state change itself. Stores
// also wrap the setters where they want a debug line; the embedded type holds
// no logger.
type BlockStateFields struct {
	blockState BlockStateHolder
}

// SetBlockHeight publishes a new chain-tip height, carrying the current median
// time forward. Height zero is rejected: it cannot be told apart from a store
// that has never been written, so accepting it would let a caller silently
// reset the tip.
func (f *BlockStateFields) SetBlockHeight(height uint32) error {
	if height == 0 {
		return errors.NewInvalidArgumentError("block height cannot be zero")
	}

	f.blockState.SetHeight(height)

	return nil
}

// GetBlockHeight returns the height of the current snapshot.
func (f *BlockStateFields) GetBlockHeight() uint32 {
	return f.blockState.Load().Height
}

// SetMedianBlockTime publishes a new median block time, carrying the current
// height forward. Zero is legitimate here — it is the "not yet known" value.
func (f *BlockStateFields) SetMedianBlockTime(medianTime uint32) error {
	f.blockState.SetMedianTime(medianTime)

	return nil
}

// GetMedianBlockTime returns the median block time of the current snapshot.
func (f *BlockStateFields) GetMedianBlockTime() uint32 {
	return f.blockState.Load().MedianTime
}

// SetBlockState publishes both chain-tip values as one snapshot; see
// Store.SetBlockState for why callers holding both should use this rather than
// the two single-field setters. Height zero is rejected, as in SetBlockHeight.
func (f *BlockStateFields) SetBlockState(height, medianTime uint32) error {
	if height == 0 {
		return errors.NewInvalidArgumentError("block height cannot be zero")
	}

	f.blockState.SetPair(height, medianTime)

	return nil
}

// GetBlockState returns both chain-tip values from a single atomic load.
func (f *BlockStateFields) GetBlockState() BlockState {
	return f.blockState.Load()
}
