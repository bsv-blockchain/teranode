package model

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

const (
	// subtreeMetaEntryCountSize is the width of the little-endian uint32 entry
	// count that go-subtree's Meta.serializeTxInpoints writes straight after the
	// root hash.
	subtreeMetaEntryCountSize = 4

	// subtreeMetaHeaderSize is the fixed header go-subtree's Meta.Serialize
	// emits: the subtree root hash followed by the entry count.
	subtreeMetaHeaderSize = chainhash.HashSize + subtreeMetaEntryCountSize
)

// ValidateSubtreeMatchesKey checks that a subtree answers to the key it was
// fetched under. Every consumer of a .subtree file needs this and none of them
// can derive it from the file alone, so it lives here once rather than being
// restated at each call site with its own wording and error class.
//
// The comparison is of the file's own claim, not a recomputation of the tree, so
// it catches the wrong file under the right key and not a rewritten header.
// RootHash() returns the value cached from the .subtree header for anything
// deserialized from storage; for a subtree built in memory it is derived from the
// nodes, which makes the check stronger there rather than weaker.
func ValidateSubtreeMatchesKey(subtree *subtreepkg.Subtree, key *chainhash.Hash) error {
	if subtree == nil {
		return errors.NewProcessingError("subtree is nil")
	}

	if key == nil {
		return errors.NewProcessingError("subtree key is nil")
	}

	rootHash := subtree.RootHash()
	if rootHash == nil {
		return errors.NewProcessingError("subtree has no root hash, so it cannot be matched against key %s", key.String())
	}

	if !rootHash.IsEqual(key) {
		return errors.NewProcessingError("subtree does not match its key %s: file was built for %s", key.String(), rootHash.String())
	}

	return nil
}

// NewSubtreeMetaFromValidatedReader deserializes a .subtreeMeta stream after
// checking its fixed 36-byte header — the root hash the file was built for and
// the entry count it claims — against the subtree and key it is being read for
// (issue 1425).
//
// Two constraints are not obvious from the call site. The count is compared
// against Length(), because that is what Meta.serializeTxInpoints writes; Size()
// is cap(Nodes) and is larger whenever a pooled allocator or a short final
// subtree leaves headroom. And the subtree is compared against the key, because
// RootHash() returns the .subtree header's cached bytes for anything read from
// storage, rather than recomputing from the nodes.
//
// Every producer writes the count as the subtree's node count keyed by its root,
// so any mismatch means a torn or foreign file. Callers with a regenerator behind
// them should rebuild rather than trust the file.
func NewSubtreeMetaFromValidatedReader(subtreeHash chainhash.Hash, subtree *subtreepkg.Subtree, reader io.Reader) (*subtreepkg.Meta, error) {
	if subtree == nil {
		return nil, errors.NewProcessingError("cannot validate subtree meta for %s: subtree is nil", subtreeHash.String())
	}

	// The subtree has to answer to the same key before the meta is checked
	// against it, or this compares one file's claim to another's. Full subtrees in
	// a block share a leaf count, so the entry count check below would not catch a
	// foreign subtree on its own.
	if err := ValidateSubtreeMatchesKey(subtree, &subtreeHash); err != nil {
		return nil, errors.NewProcessingError("cannot validate subtree meta for %s", subtreeHash.String(), err)
	}

	var metaHeader [subtreeMetaHeaderSize]byte
	if _, err := io.ReadFull(reader, metaHeader[:]); err != nil {
		return nil, errors.NewProcessingError("failed to read subtree meta header for %s", subtreeHash.String(), err)
	}

	if !bytes.Equal(metaHeader[:chainhash.HashSize], subtreeHash[:]) {
		// Print the foreign hash in display order like every other hash in the
		// logs, or the one line meant for triage shows two incomparable hex strings.
		metaRootHash := chainhash.Hash(metaHeader[:chainhash.HashSize])

		return nil, errors.NewProcessingError("subtree meta root hash mismatch for %s: meta was built for %s", subtreeHash.String(), metaRootHash.String())
	}

	subtreeLength, err := safeconversion.IntToUint32(subtree.Length())
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert subtree length for %s", subtreeHash.String(), err)
	}

	if claimedCount := binary.LittleEndian.Uint32(metaHeader[chainhash.HashSize:]); claimedCount != subtreeLength {
		return nil, errors.NewProcessingError("subtree meta entry count mismatch for %s: meta claims %d entries, subtree has %d transactions", subtreeHash.String(), claimedCount, subtreeLength)
	}

	subtreeMeta, err := subtreepkg.NewSubtreeMetaFromReader(subtree, io.MultiReader(bytes.NewReader(metaHeader[:]), reader))
	if err != nil {
		return nil, errors.NewProcessingError("failed to deserialize subtree meta for %s", subtreeHash.String(), err)
	}

	return subtreeMeta, nil
}
