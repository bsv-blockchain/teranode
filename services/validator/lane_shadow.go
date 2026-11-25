package validator

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
)

// parentShadowLinearMax is how many parents parentShadow compares directly before it keeps a map.
const parentShadowLinearMax = 16

// parentShadow is the lanes shadow of one transaction's parent read, at utxostore_laneMigration
// shadow: each distinct parent is answered from the process's lane index once, beside the store's
// answer, and the comparison counted (util/lanereader). It never changes the read's result. With
// no lane index, at off, it does nothing.
type parentShadow struct {
	reader *lanereader.Reader
	// view is the main chain as the index held it when the read began, for answers given as a
	// block list.
	view   *lane.ChainView
	seen   []chainhash.Hash
	seenAt map[chainhash.Hash]struct{}
}

// compare answers parent, at lane position pos, beside today's answer: mined at height, or not.
// coinbase is the parent record's coinbase flag, which a Phase 1 row never lists.
func (s *parentShadow) compare(reader string, parent chainhash.Hash, pos uint64, coinbase, mined bool, height uint32) {
	if s.reader == nil || s.repeat(parent) {
		return
	}

	s.reader.CompareHeight(reader, parent, pos, coinbase, mined, height)
}

// compareBlocks answers parent, at lane position pos, beside today's answer given as its mined
// block list: a prefetched parent's record carries the list, which a height alone cannot stand in
// for, since the lowest height may be a fork block's.
func (s *parentShadow) compareBlocks(reader string, parent chainhash.Hash, pos uint64, coinbase bool, blockIDs []uint32) {
	if s.reader == nil || s.repeat(parent) {
		return
	}

	s.reader.CompareBlocks(reader, s.view, parent, pos, coinbase, blockIDs)
}

// repeat reports whether parent was seen before, and records it.
func (s *parentShadow) repeat(parent chainhash.Hash) bool {
	if s.seenAt != nil {
		if _, ok := s.seenAt[parent]; ok {
			return true
		}

		s.seenAt[parent] = struct{}{}

		return false
	}

	for _, h := range s.seen {
		if h == parent {
			return true
		}
	}

	s.seen = append(s.seen, parent)

	if len(s.seen) > parentShadowLinearMax {
		s.seenAt = make(map[chainhash.Hash]struct{}, 2*len(s.seen))
		for _, h := range s.seen {
			s.seenAt[h] = struct{}{}
		}

		s.seen = nil
	}

	return false
}
