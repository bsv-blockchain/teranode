package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2/chainhash"
	"io"
)

type SubtreeMeta struct {
	Subtree        *Subtree
	ParentTxHashes [][]chainhash.Hash
	ParentTxMeta   map[chainhash.Hash]txmeta.Data

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
		ParentTxMeta:   make(map[chainhash.Hash]txmeta.Data),
	}
}

func NewSubtreeMetaFromBytes(dataBytes []byte) (*SubtreeMeta, error) {
	s := &SubtreeMeta{}

	// read the root hash
	s.rootHash = chainhash.Hash(dataBytes[:32])

	// read the number of parent tx hashes
	parentTxHashesLen := binary.LittleEndian.Uint64(dataBytes[32:40])

	buf := bytes.NewReader(dataBytes[40:])

	var err error
	var bytesUint64 [8]byte

	// read the parent tx hashes
	var hashBytes [32]byte
	s.ParentTxHashes = make([][]chainhash.Hash, parentTxHashesLen)
	for i := uint64(0); i < parentTxHashesLen; i++ {
		// read hash len from buffer
		_, err = io.ReadFull(buf, bytesUint64[:])
		if err != nil {
			return nil, fmt.Errorf("unable to read parent tx hash length: %v", err)
		}
		hashLen := binary.LittleEndian.Uint64(bytesUint64[:])
		if hashLen > 0 {
			s.ParentTxHashes[i] = make([]chainhash.Hash, hashLen)
			for j := uint64(0); j < hashLen; j++ {
				_, err = io.ReadFull(buf, hashBytes[:])
				if err != nil {
					return nil, fmt.Errorf("unable to read parent tx hash: %v", err)
				}
				s.ParentTxHashes[i][j] = hashBytes
			}
		}
	}

	// read the number of parent tx meta
	_, err = io.ReadFull(buf, bytesUint64[:])
	if err != nil {
		return nil, fmt.Errorf("unable to read number of parent tx meta: %v", err)
	}
	parentTxMetaLen := binary.LittleEndian.Uint64(bytesUint64[:])

	s.ParentTxMeta = make(map[chainhash.Hash]txmeta.Data)

	// read the parent tx meta
	var meta *txmeta.Data
	var hash chainhash.Hash
	for i := uint64(0); i < parentTxMetaLen; i++ {
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, fmt.Errorf("unable to read parent tx hash: %v", err)
		}
		copy(hash[:], hashBytes[:])

		// read meta len from buffer
		_, err = io.ReadFull(buf, bytesUint64[:])
		if err != nil {
			return nil, fmt.Errorf("unable to read number of parent tx meta: %v", err)
		}
		metaLen := binary.LittleEndian.Uint64(bytesUint64[:])
		if metaLen > 0 {
			metaBytes := make([]byte, metaLen)
			_, err = io.ReadFull(buf, metaBytes)
			if err != nil {
				return nil, fmt.Errorf("unable to read parent tx meta: %v", err)
			}
			meta = &txmeta.Data{}
			txmeta.NewMetaDataFromBytes(&metaBytes, meta)
			if err != nil {
				return nil, fmt.Errorf("unable to create parent tx meta: %v", err)
			}

			s.ParentTxMeta[hash] = *meta
		}
	}

	return s, nil
}

// SetParentTxHash sets the parent tx hash for a given node in the subtree
func (s *SubtreeMeta) SetParentTxHash(index int, parentTxHash chainhash.Hash) error {
	if s.ParentTxHashes[index] == nil {
		s.ParentTxHashes[index] = make([]chainhash.Hash, 0)
	}

	if s.Subtree.Length() <= index || s.Subtree.Nodes[index].Hash.Equal(chainhash.Hash{}) {
		return fmt.Errorf("node at index %d is not set in subtree", index)
	}

	s.ParentTxHashes[index] = append(s.ParentTxHashes[index], parentTxHash)

	return nil
}

// SetParentTxHashes sets the parent tx hashes for a given node in the subtree
func (s *SubtreeMeta) SetParentTxHashes(index int, parentTxHashes []chainhash.Hash) error {
	if s.Subtree.Length() <= index || s.Subtree.Nodes[index].Hash.Equal(chainhash.Hash{}) {
		return fmt.Errorf("node at index %d is not set in subtree", index)
	}

	s.ParentTxHashes[index] = parentTxHashes

	return nil
}

// SetParentTxMeta sets the parent tx meta for a given node in the subtree
func (s *SubtreeMeta) SetParentTxMeta(txHash chainhash.Hash, meta txmeta.Data) {
	s.ParentTxMeta[txHash] = meta
}

// IsSetParentTxMeta sets the parent tx meta for a given node in the subtree
func (s *SubtreeMeta) IsSetParentTxMeta(txHash chainhash.Hash) bool {
	_, exists := s.ParentTxMeta[txHash]
	return exists
}

// Serialize returns the serialized form of the subtree meta
func (s *SubtreeMeta) Serialize() ([]byte, error) {
	var err error

	// only serialize when we have the matching subtree
	if s.Subtree == nil {
		return nil, fmt.Errorf("cannot serialize, subtree is not set")
	}
	// check the data in the subtree matches the data in the parent tx hashes
	subtreeLen := s.Subtree.Length()
	for i := 0; i < subtreeLen; i++ {
		if s.ParentTxHashes[i] == nil && i != 0 {
			return nil, fmt.Errorf("subtree length does not match parent tx hashes length")
		}
	}

	bufBytes := make([]byte, 0, 32*1024) // 16MB (arbitrary size, should be enough for most cases)
	buf := bytes.NewBuffer(bufBytes)

	if s.Subtree != nil {
		s.rootHash = *s.Subtree.RootHash()
	}

	// write root hash
	if _, err = buf.Write(s.rootHash[:]); err != nil {
		return nil, fmt.Errorf("unable to write root hash: %v", err)
	}

	var bytesUint64 [8]byte

	// write number of parent tx hashes
	binary.LittleEndian.PutUint64(bytesUint64[:], uint64(len(s.ParentTxHashes)))
	if _, err = buf.Write(bytesUint64[:]); err != nil {
		return nil, fmt.Errorf("unable to write number of nodes: %v", err)
	}

	// write parent tx hashes
	for _, hashes := range s.ParentTxHashes {
		binary.LittleEndian.PutUint64(bytesUint64[:], uint64(len(hashes)))
		if _, err = buf.Write(bytesUint64[:]); err != nil {
			return nil, fmt.Errorf("unable to write number of parent tx hashes: %v", err)
		}

		for _, hash := range hashes {
			if _, err = buf.Write(hash[:]); err != nil {
				return nil, fmt.Errorf("unable to write parent tx hash: %v", err)
			}
		}
	}

	// write number of parent tx meta
	binary.LittleEndian.PutUint64(bytesUint64[:], uint64(len(s.ParentTxMeta)))
	if _, err = buf.Write(bytesUint64[:]); err != nil {
		return nil, fmt.Errorf("unable to write number of parent tx meta: %v", err)
	}

	// write parent tx meta
	for hash, meta := range s.ParentTxMeta {
		if _, err = buf.Write(hash[:]); err != nil {
			return nil, fmt.Errorf("unable to write parent tx hash: %v", err)
		}
		metaBytes := meta.MetaBytes()

		binary.LittleEndian.PutUint64(bytesUint64[:], uint64(len(metaBytes)))
		if _, err = buf.Write(bytesUint64[:]); err != nil {
			return nil, fmt.Errorf("unable to write parent tx meta length: %v", err)
		}

		if _, err = buf.Write(metaBytes); err != nil {
			return nil, fmt.Errorf("unable to write parent tx meta: %v", err)
		}
	}

	return buf.Bytes(), nil
}
