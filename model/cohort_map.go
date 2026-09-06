package model

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/cohort"
)

// CohortMapRow records that a block contains members of a cohort.
//
// It lives here, alongside BlockRef and ChainTip, rather than in the blockchain
// store package, because both the store interface and its SQL implementation
// need it and the store package already imports the SQL one.
type CohortMapRow struct {
	// Cohort is the cohort label whose members the block contains.
	Cohort cohort.ID

	// BlockID is the blocks-table id of the block.
	BlockID uint32

	// MemberCount is how many of that cohort's transactions are in that block.
	MemberCount uint64

	// Verified records whether that count has been confirmed against the block.
	Verified bool
}

// CohortBlock is a block a cohort maps to, with the chain state needed to
// interpret it.
//
// OnMainChain and Invalid are carried as data, not as a verdict. In particular
// blocks.on_main_chain can be transiently false for a block that is on the best
// chain, which is why the SQL store's CheckBlockIsInCurrentChain refuses to
// answer false from the flag alone and falls back to a flag-free parent_id walk
// before rejecting anything. Callers deciding whether a cohort is mined have to
// take the same care.
type CohortBlock struct {
	// BlockID is the blocks-table id of the block.
	BlockID uint32

	// Height is the block's height.
	Height uint32

	// Hash is the block's hash.
	Hash *chainhash.Hash

	// MemberCount is how many of the cohort's transactions are in that block.
	MemberCount uint64

	// Verified records whether that count has been confirmed against the block.
	Verified bool

	// OnMainChain is the blocks.on_main_chain flag, with the caveat above.
	OnMainChain bool

	// Invalid is the blocks.invalid flag.
	Invalid bool
}
