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
	RootHash       chainhash.Hash
	ParentTxHashes [][]chainhash.Hash
	ParentTxMeta   map[chainhash.Hash]txmeta.Data
}

// NewSubtreeMeta creates a new SubtreeMeta object
// the size parameter is the number of nodes in the subtree,
// the index in that array should match the index of the node in the subtree
func NewSubtreeMeta(rootHash chainhash.Hash, size int) *SubtreeMeta {
	return &SubtreeMeta{
		RootHash:       rootHash,
		ParentTxHashes: make([][]chainhash.Hash, size),
		ParentTxMeta:   make(map[chainhash.Hash]txmeta.Data),
	}
}

func NewSubtreeMetaFromBytes(dataBytes []byte) (*SubtreeMeta, error) {
	s := &SubtreeMeta{}

	// read the root hash
	copy(s.RootHash[:], dataBytes[:32])

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
		s.ParentTxHashes[i] = make([]chainhash.Hash, hashLen)
		for j := uint64(0); j < hashLen; j++ {
			_, err = io.ReadFull(buf, hashBytes[:])
			if err != nil {
				return nil, fmt.Errorf("unable to read parent tx hash: %v", err)
			}
			s.ParentTxHashes[i][j] = hashBytes
		}
	}

	// read the number of parent tx meta
	_, err = io.ReadFull(buf, bytesUint64[:])
	if err != nil {
		return nil, fmt.Errorf("unable to read number of parent tx meta: %v", err)
	}
	parentTxMetaLen := binary.LittleEndian.Uint64(bytesUint64[:])

	// read the parent tx meta
	var meta *txmeta.Data
	for i := uint64(0); i < parentTxMetaLen; i++ {
		var hash chainhash.Hash
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, fmt.Errorf("unable to read parent tx hash: %v", err)
		}
		copy(hash[:], hashBytes[:])

		metaLen := binary.LittleEndian.Uint64(dataBytes[40+32*parentTxHashesLen+8+32*i : 40+32*parentTxHashesLen+8+32*i+8])

		metaBytes := make([]byte, metaLen)
		_, err = io.ReadFull(buf, metaBytes)
		if err != nil {
			return nil, fmt.Errorf("unable to read parent tx meta: %v", err)
		}

		meta, err = txmeta.NewDataFromBytes(metaBytes)
		if err != nil {
			return nil, fmt.Errorf("unable to create parent tx meta: %v", err)
		}

		s.ParentTxMeta[hash] = *meta
	}

	return s, nil
}

// SetParentTxHash sets the parent tx hash for a given node in the subtree
func (s *SubtreeMeta) SetParentTxHash(index int, parentTxHash chainhash.Hash) {
	if s.ParentTxHashes[index] == nil {
		s.ParentTxHashes[index] = make([]chainhash.Hash, 0)
	}
	s.ParentTxHashes[index] = append(s.ParentTxHashes[index], parentTxHash)
}

// SetParentTxMeta sets the parent tx meta for a given node in the subtree
func (s *SubtreeMeta) SetParentTxMeta(txHash chainhash.Hash, meta txmeta.Data) {
	s.ParentTxMeta[txHash] = meta
}

// Serialize returns the serialized form of the subtree meta
func (s *SubtreeMeta) Serialize() ([]byte, error) {
	var err error

	bufBytes := make([]byte, 0, 16*1024) // 16MB (arbitrary size, should be enough for most cases)
	buf := bytes.NewBuffer(bufBytes)

	// write root hash
	if _, err = buf.Write(s.RootHash[:]); err != nil {
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
		metaBytes := meta.Bytes()

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
