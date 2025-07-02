package subtree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SubtreeMeta struct {
	// Subtree is the subtree this meta is for
	Subtree *Subtree
	// TxInpoints is a lookup of the parent tx inpoints for each node in the subtree
	TxInpoints []TxInpoints

	// RootHash is the hash of the root node of the subtree
	rootHash chainhash.Hash
}

// NewSubtreeMeta creates a new SubtreeMeta object
// the size parameter is the number of nodes in the subtree,
// the index in that array should match the index of the node in the subtree
//
// Parameters:
//   - subtree: The subtree for which to create the meta
//
// Returns:
//   - *SubtreeMeta: A new SubtreeMeta object with the specified subtree and an empty TxInpoints slice
func NewSubtreeMeta(subtree *Subtree) *SubtreeMeta {
	return &SubtreeMeta{
		Subtree:    subtree,
		TxInpoints: make([]TxInpoints, subtree.Size()),
	}
}

// NewSubtreeMetaFromBytes creates a new SubtreeMeta object from the provided byte slice.
// It reads the subtree meta data from the byte slice and populates the SubtreeMeta struct.
//
// Parameters:
//   - subtree: The subtree for which to create the meta
//   - dataBytes: The byte slice containing the serialized subtree meta data
//
// Returns:
//   - *SubtreeMeta: A new SubtreeMeta object populated with data from the byte slice
//   - error: An error if the deserialization fails
func NewSubtreeMetaFromBytes(subtree *Subtree, dataBytes []byte) (*SubtreeMeta, error) {
	s := &SubtreeMeta{
		Subtree: subtree,
	}
	if err := s.deserializeFromReader(bytes.NewReader(dataBytes)); err != nil {
		return nil, fmt.Errorf("unable to create subtree meta from bytes: %s", err)
	}

	return s, nil
}

// NewSubtreeMetaFromReader creates a new SubtreeMeta object from the provided reader.
//
// Parameters:
//   - subtree: The subtree for which to create the meta
//   - dataReader: The reader from which to read the subtree meta data
//
// Returns:
//   - *SubtreeMeta: A new SubtreeMeta object populated with data from the reader
//   - error: An error if the deserialization fails
func NewSubtreeMetaFromReader(subtree *Subtree, dataReader io.Reader) (*SubtreeMeta, error) {
	s := &SubtreeMeta{
		Subtree:    subtree,
		TxInpoints: make([]TxInpoints, subtree.Size()),
	}

	if err := s.deserializeFromReader(dataReader); err != nil {
		return nil, fmt.Errorf("unable to create subtree meta from reader: %s", err)
	}

	return s, nil
}

// GetParentTxHashes returns the unique parent transaction hashes for the specified index in the subtree meta.
// It returns an error if the index is out of range.
//
// Parameters:
//   - index: The index of the subtree node for which to get the parent transaction hashes
//
// Returns:
//   - []chainhash.Hash: The unique parent transaction hashes for the specified index
//   - error: An error if the index is out of range or if there is an issue retrieving the parent transaction hashes
func (s *SubtreeMeta) GetParentTxHashes(index int) ([]chainhash.Hash, error) {
	if index >= len(s.TxInpoints) {
		return nil, fmt.Errorf("index out of range")
	}

	return s.TxInpoints[index].GetParentTxHashes(), nil
}

// GetTxInpoints returns the TxInpoints for the specified index in the subtree meta.
// It returns an error if the index is out of range.
//
// Parameters:
//   - index: The index of the subtree node for which to get the TxInpoints
//
// Returns:
//   - []meta.Inpoint: The TxInpoints for the specified index
//   - error: An error if the index is out of range or if there is an issue retrieving the TxInpoints
func (s *SubtreeMeta) GetTxInpoints(index int) ([]Inpoint, error) {
	if index >= len(s.TxInpoints) {
		return nil, fmt.Errorf("index out of range getting tx inpoints")
	}

	return s.TxInpoints[index].GetTxInpoints(), nil
}

// deserializeFromReader reads the subtree meta from the provided reader
// and populates the SubtreeMeta struct with the data.
//
// Parameters:
//   - buf: The reader from which to read the subtree meta data
//
// Returns:
//   - error: An error if the deserialization fails
func (s *SubtreeMeta) deserializeFromReader(buf io.Reader) error {
	var (
		err       error
		dataBytes [4]byte
		hashBytes [32]byte
	)

	// read the root hash
	if _, err = io.ReadFull(buf, hashBytes[:]); err != nil {
		return fmt.Errorf("unable to read root hash: %s", err)
	}

	s.rootHash = hashBytes

	// read the number of parent tx hashes
	if _, err = io.ReadFull(buf, dataBytes[:]); err != nil {
		return fmt.Errorf("unable to read number of parent tx hashes: %s", err)
	}

	txInpointsLen := binary.LittleEndian.Uint32(dataBytes[:])

	// read the parent tx hashes
	s.TxInpoints = make([]TxInpoints, s.Subtree.Size())

	return s.deserializeTxInpointsFromReader(buf, txInpointsLen)
}

// deserializeTxInpointsFromReader reads the TxInpoints from the provided reader
// and populates the TxInpoints slice in the SubtreeMeta.
//
// Parameters:
//   - buf: The reader from which to read the TxInpoints
//   - txInpointsLen: The number of TxInpoints to read
//
// Returns:
//   - error: An error if the deserialization fails
func (s *SubtreeMeta) deserializeTxInpointsFromReader(buf io.Reader, txInpointsLen uint32) error {
	var (
		err        error
		txInpoints TxInpoints
	)

	for i := uint32(0); i < txInpointsLen; i++ {
		txInpoints, err = NewTxInpointsFromReader(buf)
		if err != nil {
			return fmt.Errorf("unable to deserialize parent outpoints: %s", err)
		}

		s.TxInpoints[i] = txInpoints
	}

	return nil
}

// SetTxInpointsFromTx sets the TxInpoints for the subtree meta from a transaction.
// It finds the index of the transaction in the subtree and sets the TxInpoints at that index.
// If the transaction is not found in the subtree, it returns an error.
//
// Parameters:
//   - tx: The transaction to set the TxInpoints from
//
// Returns:
//   - error: An error if the transaction is not found in the subtree or if there is an issue creating the TxInpoints
func (s *SubtreeMeta) SetTxInpointsFromTx(tx *bt.Tx) error {
	index := s.Subtree.NodeIndex(*tx.TxIDChainHash())
	if index == -1 {
		return fmt.Errorf("[SetParentTxHashesFromTx][%s] node not found in subtree", tx.TxID())
	}

	p, err := NewTxInpointsFromTx(tx)
	if err != nil {
		return err
	}

	s.TxInpoints[index] = p

	return nil
}

// SetTxInpoints sets the TxInpoints at the specified index in the subtree meta.
// It returns an error if the index is out of range.
//
// Parameters:
//   - idx: The index at which to set the TxInpoints
//   - txInpoints: The TxInpoints to set at the specified index
//
// Returns:
//   - error: An error if the index is out of range
func (s *SubtreeMeta) SetTxInpoints(idx int, txInpoints TxInpoints) error {
	if idx >= len(s.TxInpoints) {
		return fmt.Errorf("index out of range")
	}

	s.TxInpoints[idx] = txInpoints

	return nil
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
		if i != 0 && s.TxInpoints[i].ParentTxHashes == nil {
			return nil, fmt.Errorf("cannot serialize, parent tx hashes are not set for node %d: %s", i, s.Subtree.Nodes[i].Hash.String())
		}
	}

	bufBytes := make([]byte, 0, 32*1024) // 32MB (arbitrary size, should be enough for most cases)
	buf := bytes.NewBuffer(bufBytes)

	s.rootHash = *s.Subtree.RootHash()

	// write root hash
	if _, err = buf.Write(s.rootHash[:]); err != nil {
		return nil, fmt.Errorf("cannot serialize, unable to write root hash: %s", err)
	}

	if err = s.serializeTxInpoints(buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (s *SubtreeMeta) serializeTxInpoints(buf *bytes.Buffer) error {
	var (
		err         error
		bytesUint32 [4]byte
	)

	parentTxHashesLen32, err := safeconversion.IntToUint32(s.Subtree.Length())
	if err != nil {
		return fmt.Errorf("cannot serialize, unable to get safe uint32: %s", err)
	}

	// write number of parent tx hashes
	binary.LittleEndian.PutUint32(bytesUint32[:], parentTxHashesLen32)

	if _, err = buf.Write(bytesUint32[:]); err != nil {
		return fmt.Errorf("cannot serialize, unable to write total number of nodes: %s", err)
	}

	var txInPointBytes []byte

	// write parent txInpoints
	// for _, txInpoint := range s.TxInpoints {
	for i := uint32(0); i < parentTxHashesLen32; i++ {
		txInpoint := s.TxInpoints[i]

		txInPointBytes, err = txInpoint.Serialize()
		if err != nil {
			return fmt.Errorf("cannot serialize, unable to write parent tx hash: %s", err)
		}

		if _, err = buf.Write(txInPointBytes); err != nil {
			return fmt.Errorf("cannot serialize, unable to write parent tx hash: %s", err)
		}
	}

	return nil
}
