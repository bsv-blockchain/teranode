package util

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SubtreeMeta struct {
	Subtree  *Subtree
	RootHash chainhash.Hash
	TxMeta   []txmeta.Data
}

// NewSubtreeMeta creates a new SubtreeMeta object
// the size parameter is the number of nodes in the subtree,
// the index in that array should match the index of the node in the subtree
func NewSubtreeMeta(subtree *Subtree) *SubtreeMeta {
	return &SubtreeMeta{
		Subtree: subtree,
		TxMeta:  make([]txmeta.Data, subtree.Size()),
	}
}

func NewSubtreeMetaFromBytes(dataBytes []byte) (*SubtreeMeta, error) {
	s := &SubtreeMeta{}

	// read the root hash
	copy(s.RootHash[:], dataBytes[:32])

	// read the number of tx txmeta
	txMetaLen := binary.LittleEndian.Uint64(dataBytes[32:40])

	buf := bytes.NewReader(dataBytes[40:])

	var err error
	var bytesUint64 [8]byte

	// read the tx meta
	s.TxMeta = make([]txmeta.Data, txMetaLen)
	for i := uint64(0); i < txMetaLen; i++ {
		_, err = buf.Read(bytesUint64[:])
		if err != nil {
			return nil, fmt.Errorf("unable to read tx meta len: %v", err)
		}
		metaLen := binary.LittleEndian.Uint64(bytesUint64[:])

		metaBytes := make([]byte, metaLen)
		_, err = buf.Read(metaBytes)
		if err != nil {
			return nil, fmt.Errorf("unable to read tx meta: %v", err)
		}

		txMeta := &txmeta.Data{}
		txmeta.NewMetaDataFromBytes(&metaBytes, txMeta)

		s.TxMeta[i] = *txMeta
	}

	return s, nil
}

// AddNode is not concurrency safe
func (s *SubtreeMeta) AddNode(hash chainhash.Hash, txMeta *txmeta.Data) error {
	if txMeta == nil {
		return fmt.Errorf("txMeta is nil")
	}

	err := s.Subtree.AddNode(hash, txMeta.Fee, txMeta.SizeInBytes)
	if err != nil {
		return err
	}

	// set the metadata for the node to the index we just set
	s.TxMeta[s.Subtree.Length()-1] = *txMeta

	s.RootHash = *s.Subtree.RootHash()

	return nil
}

// Serialize returns the serialized form of the subtree meta
func (s *SubtreeMeta) Serialize() ([]byte, error) {
	if s.Subtree == nil {
		return nil, fmt.Errorf("subtree is nil")
	}
	if s.RootHash == (chainhash.Hash{}) {
		return nil, fmt.Errorf("root hash is nil")
	}
	// if s.Subtree.Length() != len(s.TxMeta) {
	// 	return nil, fmt.Errorf("subtree length does not match tx meta length")
	// }

	var err error

	bufBytes := make([]byte, 0, 16*1024) // 16MB (arbitrary size, should be enough for most cases)
	buf := bytes.NewBuffer(bufBytes)

	// write root hash
	if _, err = buf.Write(s.RootHash[:]); err != nil {
		return nil, fmt.Errorf("unable to write root hash: %v", err)
	}

	var bytesUint64 [8]byte

	// write number of tx metadata
	txMetaLen := len(s.TxMeta)
	binary.LittleEndian.PutUint64(bytesUint64[:], uint64(txMetaLen))
	if _, err = buf.Write(bytesUint64[:]); err != nil {
		return nil, fmt.Errorf("unable to write number of nodes: %v", err)
	}

	// write tx meta
	for _, meta := range s.TxMeta {
		metaBytes := meta.MetaBytes()

		metaBytesLen := len(metaBytes)
		binary.LittleEndian.PutUint64(bytesUint64[:], uint64(metaBytesLen))
		if _, err = buf.Write(bytesUint64[:]); err != nil {
			return nil, fmt.Errorf("unable to write number of nodes: %v", err)
		}

		if _, err = buf.Write(metaBytes); err != nil {
			return nil, fmt.Errorf("unable to write parent tx meta: %v", err)
		}
	}

	return buf.Bytes(), nil
}
