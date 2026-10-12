package model

import "github.com/bsv-blockchain/go-bt/v2/chainhash"

// BlockRef is a lightweight reference to a stored block — just the primary
// key, block hash, and height. Used by enumeration paths (e.g. rewind
// tooling) that need to order blocks by height without loading full block
// contents.
type BlockRef struct {
	ID     uint32
	Hash   *chainhash.Hash
	Height uint32
}
