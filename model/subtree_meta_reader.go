package model

import (
	"bufio"
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

// ValidateSubtreeNodesMatchKey recomputes the subtree's merkle root from its
// Nodes and compares that to the key, then compares the file's claimed root to
// the key as well.
//
// The second comparison is not redundant. RootHash() returns the .subtree
// header's cached bytes for anything deserialized from storage, and
// Block.CheckMerkleRoot composes that cached value for every subtree after the
// first. Proving claim == recomputed == key is what makes every later
// RootHash() consumer sound: the cache is then known to be the tree's own root
// rather than a peer-supplied field that happens to sit in the same file.
//
// Use this rather than ValidateSubtreeMatchesKey wherever the node list goes on
// to drive a decision. The claim-only check catches the wrong file under the
// right key; only this one catches a rewritten header over an unrelated node
// list (bitcoin-sv/teranode#4838).
func ValidateSubtreeNodesMatchKey(subtree *subtreepkg.Subtree, key *chainhash.Hash) error {
	if subtree == nil {
		return errors.NewProcessingError("subtree is nil")
	}

	if key == nil {
		return errors.NewProcessingError("subtree key is nil")
	}

	// BuildMerkleTreeStoreFromBytes returns an EMPTY slice for an empty node
	// list, so the len-1 index below would panic without this.
	if len(subtree.Nodes) == 0 {
		return errors.NewProcessingError("subtree has no nodes, so it cannot be matched against key %s", key.String())
	}

	store, err := subtreepkg.BuildMerkleTreeStoreFromBytes(subtree.Nodes)
	if err != nil {
		return errors.NewProcessingError("failed to recompute merkle root of subtree for key %s", key.String(), err)
	}

	if store == nil || len(*store) == 0 {
		return errors.NewProcessingError("recomputed merkle tree of subtree for key %s is empty", key.String())
	}

	computed := (*store)[len(*store)-1]
	if !computed.IsEqual(key) {
		return errors.NewProcessingError("subtree nodes do not hash to its key %s: they hash to %s", key.String(), computed.String())
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
//
// reader is read through a buffer, so it can be consumed past the end of the
// meta; callers must not read from it afterwards.
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

	// go-subtree decodes each entry with several small reads, and callers pass
	// raw subtree-store file handles, so read through the same pooled buffer the
	// .subtree load uses instead of paying a syscall per field.
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(reader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()

	reader = bufferedReader

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

	subtreeMeta, err := decodeSubtreeMetaBody(subtree, subtreeLength, bufferedReader)
	if err != nil {
		return nil, errors.NewProcessingError("failed to deserialize subtree meta for %s", subtreeHash.String(), err)
	}

	return subtreeMeta, nil
}

const (
	// metaHashSlab and metaWordSlab are the most parent hashes and vout words the decoded
	// txs of a subtree meta share an allocation of.
	metaHashSlab = 1 << 16
	metaWordSlab = 1 << 18
)

// decodeSubtreeMetaBody decodes the count tx entries that follow a .subtreeMeta header, as
// go-subtree's NewSubtreeMetaFromReader does, into a meta for subtree. Per tx the wire holds
// the parent count, the parent hashes, then per parent a vout count and the vouts: the
// count-prefixed vout layout TxInpoints keeps in memory. Entries are parsed straight from the
// read buffer, and their hashes and vout words are cut from slabs shared by many txs, so the
// decode costs a few allocations per subtree instead of two per tx. A tx's slices are capped
// at their length, so an append to one never writes into the next.
//
// A tx with no parents gets nil parents, as go-subtree's decoder gives. A tx too large to fit
// the read buffer is decoded by go-subtree's reader, which grows into a count it cannot trust
// instead of allocating it up front.
func decodeSubtreeMetaBody(subtree *subtreepkg.Subtree, count uint32, br *bufio.Reader) (*subtreepkg.Meta, error) {
	if uint64(count) > uint64(subtree.Size()) { // nolint: gosec
		return nil, errors.NewProcessingError("subtree meta claims %d entries, subtree has room for %d", count, subtree.Size())
	}

	meta := &subtreepkg.Meta{
		Subtree:    subtree,
		TxInpoints: make([]subtreepkg.TxInpoints, subtree.Size()),
	}

	var (
		hashes []chainhash.Hash
		words  []uint32
	)

	for i := uint32(0); i < count; {
		buf, peekErr := br.Peek(br.Size())
		pos := 0

		for ; i < count; i++ {
			size, parents, nWords, ok := metaEntryShape(buf[pos:])
			if !ok {
				break
			}

			if parents > 0 {
				// a new slab holds at most what the rest of the buffer can, so a small meta
				// allocates about what it holds: a parent takes at least 36 bytes, a word 4
				rest := len(buf) - pos

				if cap(hashes)-len(hashes) < parents {
					hashes = make([]chainhash.Hash, 0, max(parents, min(metaHashSlab, rest/(chainhash.HashSize+4))))
				}

				if cap(words)-len(words) < nWords {
					words = make([]uint32, 0, max(nWords, min(metaWordSlab, rest/4)))
				}

				txHashes := hashes[len(hashes) : len(hashes)+parents : len(hashes)+parents]
				hashes = hashes[:len(hashes)+parents]

				entry := buf[pos+4:]
				for k := range txHashes {
					copy(txHashes[k][:], entry[k*chainhash.HashSize:])
				}

				txWords := words[len(words) : len(words)+nWords : len(words)+nWords]
				words = words[:len(words)+nWords]

				entry = entry[parents*chainhash.HashSize:]
				for k := range txWords {
					txWords[k] = binary.LittleEndian.Uint32(entry[4*k:])
				}

				meta.TxInpoints[i] = subtreepkg.NewTxInpointsFromPacked(txHashes, txWords)
			}

			pos += size
		}

		if _, err := br.Discard(pos); err != nil {
			return nil, errors.NewProcessingError("failed to advance past subtree meta entries", err)
		}

		if i == count || pos > 0 {
			continue
		}

		// not one whole entry in the buffer
		if peekErr != nil {
			return nil, errors.NewProcessingError("failed to read subtree meta entry %d of %d", i, count, peekErr)
		}

		txInpoints, err := subtreepkg.NewTxInpointsFromReader(br)
		if err != nil {
			return nil, errors.NewProcessingError("failed to read subtree meta entry %d of %d", i, count, err)
		}

		meta.TxInpoints[i] = txInpoints
		i++
	}

	return meta, nil
}

// metaEntryShape measures the tx entry at the start of b: its size in bytes, its parent count
// and the vout words it holds (one count and the vouts per parent). ok is false if b does not
// hold all of it.
func metaEntryShape(b []byte) (size, parents, nWords int, ok bool) {
	if len(b) < 4 {
		return 0, 0, 0, false
	}

	p := uint64(binary.LittleEndian.Uint32(b))

	end := 4 + p*chainhash.HashSize
	if end > uint64(len(b)) {
		return 0, 0, 0, false
	}

	words := uint64(0)

	for k := uint64(0); k < p; k++ {
		if end+4 > uint64(len(b)) {
			return 0, 0, 0, false
		}

		vouts := uint64(binary.LittleEndian.Uint32(b[end:]))
		words += 1 + vouts

		if end += 4 + 4*vouts; end > uint64(len(b)) {
			return 0, 0, 0, false
		}
	}

	return int(end), int(p), int(words), true // nolint: gosec
}
