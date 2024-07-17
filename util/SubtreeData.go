package util

import (
	"bytes"
	"fmt"
	"io"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

var CoinbasePlaceholder [32]byte
var CoinbasePlaceholderHash *chainhash.Hash

func init() {
	for i := 0; i < len(CoinbasePlaceholder); i++ {
		CoinbasePlaceholder[i] = 0xFF
	}
	CoinbasePlaceholderHash, _ = chainhash.NewHash(CoinbasePlaceholder[:])

}

type SubtreeData struct {
	Subtree *Subtree
	Txs     []*bt.Tx
}

// NewSubtreeData creates a new SubtreeData object
// the size parameter is the number of nodes in the subtree,
// the index in that array should match the index of the node in the subtree
func NewSubtreeData(subtree *Subtree) *SubtreeData {
	return &SubtreeData{
		Subtree: subtree,
		Txs:     make([]*bt.Tx, subtree.Size()),
	}
}

func NewSubtreeDataFromBytes(subtree *Subtree, dataBytes []byte) (*SubtreeData, error) {
	s := &SubtreeData{
		Subtree: subtree,
	}
	if err := s.serializeFromReader(bytes.NewReader(dataBytes)); err != nil {
		return nil, fmt.Errorf("unable to create subtree meta from bytes: %v", err)
	}

	return s, nil
}

func NewSubtreeDataFromReader(subtree *Subtree, dataReader io.Reader) (*SubtreeData, error) {
	s := &SubtreeData{
		Subtree: subtree,
	}
	if err := s.serializeFromReader(dataReader); err != nil {
		return nil, fmt.Errorf("unable to create subtree meta from reader: %v", err)
	}

	return s, nil
}

func (s *SubtreeData) RootHash() *chainhash.Hash {
	return s.Subtree.RootHash()
}

func (s *SubtreeData) AddTx(tx *bt.Tx, index int) error {
	// check whether this is set in the main subtree
	if !s.Subtree.Nodes[index].Hash.Equal(*tx.TxIDChainHash()) {
		return errors.New(errors.ERR_PROCESSING, "transaction hash does not match subtree node hash")
	}

	s.Txs[index] = tx
	return nil
}

func (s *SubtreeData) serializeFromReader(buf io.Reader) error {
	var err error

	var txIndex int
	if s.Subtree.Nodes[0].Hash.Equal(*CoinbasePlaceholderHash) {
		txIndex = 1
	}

	// initialize the txs array
	s.Txs = make([]*bt.Tx, s.Subtree.Length())

	for {
		tx := &bt.Tx{}
		_, err = tx.ReadFrom(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.New(errors.ERR_PROCESSING, "error reading transaction", err)
		}

		if !s.Subtree.Nodes[txIndex].Hash.Equal(*tx.TxIDChainHash()) {
			return errors.New(errors.ERR_PROCESSING, "transaction hash does not match subtree node hash")
		}

		s.Txs[txIndex] = tx
		txIndex++
	}

	return nil
}

// Serialize returns the serialized form of the subtree meta
func (s *SubtreeData) Serialize() ([]byte, error) {
	var err error

	// only serialize when we have the matching subtree
	if s.Subtree == nil {
		return nil, fmt.Errorf("cannot serialize, subtree is not set")
	}

	var txStartIndex int
	if s.Subtree.Nodes[0].Hash.Equal(*CoinbasePlaceholderHash) {
		txStartIndex = 1
	}

	// check the data in the subtree matches the data in the tx data
	subtreeLen := s.Subtree.Length()
	for i := txStartIndex; i < subtreeLen; i++ {
		if s.Txs[i] == nil && i != 0 {
			return nil, fmt.Errorf("subtree length does not match tx data length")
		}
	}

	bufBytes := make([]byte, 0, 32*1024) // 16MB (arbitrary size, should be enough for most cases)
	buf := bytes.NewBuffer(bufBytes)

	for i := txStartIndex; i < subtreeLen; i++ {
		b := s.Txs[i].ExtendedBytes()
		_, err = buf.Write(b)
		if err != nil {
			return nil, fmt.Errorf("error writing tx data: %w", err)
		}
	}

	return buf.Bytes(), nil
}
