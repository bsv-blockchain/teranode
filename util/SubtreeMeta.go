package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SubtreeMeta struct {
	Subtree      *Subtree
	RootHash     chainhash.Hash
	ParentTxMeta []txmeta.Data
}

// NewSubtreeMeta creates a new SubtreeMeta object
// the size parameter is the number of nodes in the subtree,
// the index in that array should match the index of the node in the subtree
func NewSubtreeMeta(subtree *Subtree) *SubtreeMeta {
	return &SubtreeMeta{
		Subtree:      subtree,
		RootHash:     *subtree.RootHash(),
		ParentTxMeta: make([]txmeta.Data, subtree.Size()),
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
	var txMeta *txmeta.Data
	s.ParentTxMeta = make([]txmeta.Data, parentTxHashesLen)
	for i := uint64(0); i < parentTxHashesLen; i++ {
		_, err = buf.Read(bytesUint64[:])
		if err != nil {
			return nil, fmt.Errorf("unable to read parent tx meta length: %v", err)
		}

		metaLen := binary.LittleEndian.Uint64(bytesUint64[:])

		metaBytes := make([]byte, metaLen)
		_, err = buf.Read(metaBytes)
		if err != nil {
			return nil, fmt.Errorf("unable to read parent tx meta: %v", err)
		}

		txMeta, err = txmeta.NewDataFromBytes(metaBytes)
		if err != nil {
			return nil, fmt.Errorf("unable to create parent tx meta: %v", err)
		}

		s.ParentTxMeta[i] = *txMeta
	}

	return s, nil
}

// AddNode is not concurrency safe
func (s *SubtreeMeta) AddNode(hash chainhash.Hash, txMeta *txmeta.Data) error {
	err := s.Subtree.AddNode(hash, txMeta.Fee, txMeta.SizeInBytes)
	if err != nil {
		return err
	}

	// set the metadata for the node to the index we just set
	s.ParentTxMeta[s.Subtree.Length()-1] = *txMeta

	return nil
}

// SetParentTxMeta sets the parent tx meta for a given node in the subtree
func (s *SubtreeMeta) SetParentTxMeta(index int, meta txmeta.Data) {
	s.ParentTxMeta[index] = meta
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

	// write number of parent tx metadata
	binary.LittleEndian.PutUint64(bytesUint64[:], uint64(len(s.ParentTxMeta)))
	if _, err = buf.Write(bytesUint64[:]); err != nil {
		return nil, fmt.Errorf("unable to write number of nodes: %v", err)
	}

	// write parent tx hashes
	for _, meta := range s.ParentTxMeta {
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
