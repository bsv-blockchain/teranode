package util

import (
	"bytes"
	"encoding/binary"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2/chainhash"
	"io"
	"math"
)

type SubtreeMeta struct {
	Subtree        *Subtree
	ParentTxHashes [][]chainhash.Hash

	// RootHash is the hash of the root node of the subtree
	rootHash chainhash.Hash
}

// NewSubtreeMeta creates a new SubtreeMeta object
// the size parameter is the number of nodes in the subtree,
// the index in that array should match the index of the node in the subtree
func NewSubtreeMeta(subtree *Subtree) *SubtreeMeta {
	return &SubtreeMeta{
		Subtree:        subtree,
		ParentTxHashes: make([][]chainhash.Hash, subtree.Size()),
	}
}

func NewSubtreeMetaFromBytes(subtree *Subtree, dataBytes []byte) (*SubtreeMeta, error) {
	s := &SubtreeMeta{
		Subtree: subtree,
	}
	if err := s.serializeFromReader(bytes.NewReader(dataBytes)); err != nil {
		return nil, errors.NewProcessingError("unable to create subtree meta from bytes", err)
	}

	return s, nil
}

func NewSubtreeMetaFromReader(subtree *Subtree, dataReader io.Reader) (*SubtreeMeta, error) {
	s := &SubtreeMeta{
		Subtree: subtree,
	}
	if err := s.serializeFromReader(dataReader); err != nil {
		return nil, errors.NewProcessingError("unable to create subtree meta from reader", err)
	}

	return s, nil
}

func (s *SubtreeMeta) serializeFromReader(buf io.Reader) error {
	var err error
	var bytesUint64 [8]byte
	var hashBytes [32]byte

	// read the root hash
	if _, err = io.ReadFull(buf, hashBytes[:]); err != nil {
		return errors.NewProcessingError("unable to read root hash", err)
	}
	s.rootHash = hashBytes

	var dataBytes [8]byte
	// read the number of parent tx hashes
	if _, err = io.ReadFull(buf, dataBytes[:]); err != nil {
		return errors.NewProcessingError("unable to read number of parent tx hashes", err)
	}
	parentTxHashesLen := binary.LittleEndian.Uint64(dataBytes[:])
	if parentTxHashesLen > math.MaxUint32 {
		return errors.NewProcessingError("parent tx hashes length is too large")
	}

	// read the parent tx hashes
	s.ParentTxHashes = make([][]chainhash.Hash, parentTxHashesLen)
	for i := uint64(0); i < parentTxHashesLen; i++ {
		// read hash len from buffer
		_, err = io.ReadFull(buf, bytesUint64[:])
		if err != nil {
			return errors.NewProcessingError("unable to read parent tx hash length", err)
		}
		hashLen := binary.LittleEndian.Uint64(bytesUint64[:])
		if hashLen > math.MaxUint32 {
			return errors.NewProcessingError("parent tx hash length is too large")
		}
		if hashLen > 0 {
			s.ParentTxHashes[i] = make([]chainhash.Hash, hashLen)
			for j := uint64(0); j < hashLen; j++ {
				_, err = io.ReadFull(buf, hashBytes[:])
				if err != nil {
					return errors.NewProcessingError("unable to read parent tx hash", err)
				}
				s.ParentTxHashes[i][j] = hashBytes
			}
		}
	}

	return nil
}

// SetParentTxHash sets the parent tx hash for a given node in the subtree
func (s *SubtreeMeta) SetParentTxHash(index int, parentTxHash chainhash.Hash) error {
	if s.ParentTxHashes[index] == nil {
		s.ParentTxHashes[index] = make([]chainhash.Hash, 0)
	}

	if s.Subtree.Length() <= index || s.Subtree.Nodes[index].Hash.Equal(chainhash.Hash{}) {
		// TODO GOKHAN:
		// processing error is not recoverable. Because same input same output.
		/// check processing errors, maybe some of them are recoverable, no should not be marked as irrecoverable.
		return errors.NewProcessingError("node at index %d is not set in subtree", index)
	}

	s.ParentTxHashes[index] = append(s.ParentTxHashes[index], parentTxHash)

	return nil
}

// SetParentTxHashes sets the parent tx hashes for a given node in the subtree
func (s *SubtreeMeta) SetParentTxHashes(index int, parentTxHashes []chainhash.Hash) error {
	if s.Subtree.Length() <= index || s.Subtree.Nodes[index].Hash.Equal(chainhash.Hash{}) {
		return errors.NewProcessingError("node at index %d is not set in subtree", index)
	}

	s.ParentTxHashes[index] = parentTxHashes

	return nil
}

// Serialize returns the serialized form of the subtree meta
func (s *SubtreeMeta) Serialize() ([]byte, error) {
	var err error

	// only serialize when we have the matching subtree
	if s.Subtree == nil {
		return nil, errors.NewProcessingError("cannot serialize, subtree is not set")
	}
	// check the data in the subtree matches the data in the parent tx hashes
	subtreeLen := s.Subtree.Length()
	for i := 0; i < subtreeLen; i++ {
		if s.ParentTxHashes[i] == nil && i != 0 {
			return nil, errors.NewProcessingError("subtree length does not match parent tx hashes length")
		}
	}

	bufBytes := make([]byte, 0, 32*1024) // 16MB (arbitrary size, should be enough for most cases)
	buf := bytes.NewBuffer(bufBytes)

	if s.Subtree != nil {
		s.rootHash = *s.Subtree.RootHash()
	}

	// write root hash
	if _, err = buf.Write(s.rootHash[:]); err != nil {
		return nil, errors.NewProcessingError("unable to write root hash", err)
	}

	var bytesUint64 [8]byte

	// write number of parent tx hashes
	binary.LittleEndian.PutUint64(bytesUint64[:], uint64(len(s.ParentTxHashes)))
	if _, err = buf.Write(bytesUint64[:]); err != nil {
		return nil, errors.NewProcessingError("unable to write number of nodes", err)
	}

	// write parent tx hashes
	for _, hashes := range s.ParentTxHashes {
		binary.LittleEndian.PutUint64(bytesUint64[:], uint64(len(hashes)))
		if _, err = buf.Write(bytesUint64[:]); err != nil {
			return nil, errors.NewProcessingError("unable to write number of parent tx hashes", err)
		}

		for _, hash := range hashes {
			if _, err = buf.Write(hash[:]); err != nil {
				return nil, errors.NewProcessingError("unable to write parent tx hash", err)
			}
		}
	}

	return buf.Bytes(), nil
}
