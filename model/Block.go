package model

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/blob"
	txmetastore "github.com/TAAL-GmbH/ubsv/stores/txmeta"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/bitcoinsv/bsvd/txscript"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
)

var (
	serializedHeightVersion = 2
)

type Block struct {
	Header           *BlockHeader
	CoinbaseTx       *bt.Tx
	TransactionCount uint64
	Subtrees         []*chainhash.Hash

	// local
	hash          *chainhash.Hash
	subtreeLength uint64
	subtreeSlices []*util.Subtree
	txMap         *util.SplitSwissMapUint64
}

func NewBlock(header *BlockHeader, coinbase *bt.Tx, subtrees []*chainhash.Hash) (*Block, error) {
	return &Block{
		Header:        header,
		CoinbaseTx:    coinbase,
		Subtrees:      subtrees,
		subtreeLength: uint64(len(subtrees)),
	}, nil
}

func NewBlockFromBytes(blockBytes []byte) (*Block, error) {
	block := &Block{}

	var err error

	// read the first 80 bytes as the block header
	blockHeaderBytes := blockBytes[:80]
	block.Header, err = NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, err
	}

	// create new buffer reader for the block bytes
	buf := bytes.NewReader(blockBytes[80:])

	// read the length of the subtree list
	block.subtreeLength, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, err
	}

	// read the subtree list
	var hashBytes [32]byte
	var subtreeHash *chainhash.Hash
	for i := uint64(0); i < block.subtreeLength; i++ {
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, err
		}

		subtreeHash, err = chainhash.NewHash(hashBytes[:])
		if err != nil {
			return nil, err
		}

		block.Subtrees = append(block.Subtrees, subtreeHash)
	}

	coinbaseTxBytes, _ := io.ReadAll(buf) // read the rest of the bytes as the coinbase tx
	block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTxBytes)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (b *Block) Hash() *chainhash.Hash {
	if b.hash != nil {
		return b.hash
	}

	b.hash = b.Header.Hash()

	return b.hash
}

func (b *Block) String() string {
	return b.Hash().String()
}

func (b *Block) Valid(ctx context.Context, subtreeStore blob.Store, txMetaStore txmetastore.Store, currentChain []*BlockHeader) (bool, error) {
	// 1. Check that the block header hash is less than the target difficulty.
	headerValid, err := b.Header.HasMetTargetDifficulty()
	if !headerValid {
		return false, fmt.Errorf("invalid block header: %s - %v", b.Header.Hash().String(), err)
	}

	if len(b.Subtrees) == 0 {
		return false, fmt.Errorf("block has no subtrees")
	}

	// 2. Check that the block timestamp is not more than two hours in the future.
	if b.Header.Timestamp > uint32(time.Now().Add(2*time.Hour).Unix()) {
		return false, fmt.Errorf("block timestamp is more than two hours in the future")
	}

	// 3. Check that the median time past of the block is after the median time past of the last 11 blocks.
	// TODO

	// 4. Check that the coinbase transaction is valid (reward checked later).
	if b.CoinbaseTx == nil {
		return false, fmt.Errorf("block has no coinbase tx")
	}
	if !b.CoinbaseTx.IsCoinbase() {
		return false, fmt.Errorf("block coinbase tx is not a valid coinbase tx")
	}

	var height uint32
	// 5. Check that the coinbase transaction includes the correct block height.
	height, err = b.ExtractCoinbaseHeight()
	if err != nil {
		return false, err
	}

	// only do the subtree checks if we have a subtree store
	// missing the subtreeStore should only happen when we are validating an internal block
	if subtreeStore != nil {
		// 6. Get and validate any missing subtrees.
		if err = b.GetAndValidateSubtrees(ctx, subtreeStore); err != nil {
			return false, err
		}

		// 7. Check that the first transaction in the first subtree is a coinbase placeholder (zeros)
		if *b.subtreeSlices[0].Nodes[0] != CoinbasePlaceholder {
			return false, fmt.Errorf("first transaction in first subtree is not a coinbase placeholder")
		}

		//8. Calculate the merkle root of the list of subtrees and check it matches the MR in the block header.
		//    making sure to replace the coinbase placeholder with the coinbase tx hash in the first subtree
		if err = b.CheckMerkleRoot(); err != nil {
			return false, err
		}
	}

	// 9. Check that the total fees of the block are less than or equal to the block reward.
	// 10. Check that the coinbase transaction includes the correct block reward.
	err = b.checkBlockRewardAndFees(height)
	if err != nil {
		return false, err
	}

	// 11. Check that there are no duplicate transactions in the block.
	err = b.checkDuplicateTransactions()
	if err != nil {
		return false, err
	}

	// 12. Check that all transactions are in the valid order and blessed
	//     Can only be done with a valid texMetaStore passed in
	if txMetaStore != nil {
		err = b.validOrderAndBlessed(ctx, txMetaStore, currentChain)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (b *Block) checkBlockRewardAndFees(height uint32) error {
	coinbaseOutputSatoshis := uint64(0)
	for _, tx := range b.CoinbaseTx.Outputs {
		coinbaseOutputSatoshis += tx.Satoshis
	}

	subtreeFees := uint64(0)
	for _, subtree := range b.subtreeSlices {
		subtreeFees += subtree.Fees
	}

	coinbaseReward := util.GetBlockSubsidyForHeight(height)
	// TODO should this be != instead of > ?
	if coinbaseOutputSatoshis != subtreeFees+coinbaseReward {
		return fmt.Errorf("fees paid (%d) are noteequal to fees + block reward (%d)", coinbaseOutputSatoshis, subtreeFees+coinbaseReward)
	}

	return nil
}

func (b *Block) checkDuplicateTransactions() error {
	b.txMap = util.NewSplitSwissMapUint64(int(b.TransactionCount))
	for subIdx, subtree := range b.subtreeSlices {
		size := len(subtree.Nodes)
		for txIdx, txHash := range subtree.Nodes {
			if b.txMap.Exists(*txHash) {
				return fmt.Errorf("duplicate transaction %s", txHash.String())
			}
			err := b.txMap.Put(*txHash, uint64((subIdx*size)+txIdx))
			if err != nil {
				return fmt.Errorf("error adding transaction %s to txMap: %v", txHash.String(), err)
			}
		}
	}

	return nil
}

func (b *Block) validOrderAndBlessed(ctx context.Context, txMetaStore txmetastore.Store, currentChain []*BlockHeader) error {
	if b.txMap == nil {
		return fmt.Errorf("txMap is nil, cannot check transaction order")
	}

	for _, subtree := range b.subtreeSlices {
		for _, txHash := range subtree.Nodes {
			txMeta, err := txMetaStore.Get(ctx, txHash)
			if err != nil {
				return err
			}

			if txMeta == nil {
				return fmt.Errorf("transaction %s is not blessed", txHash.String())
			}

			if len(txMeta.BlockHashes) > 0 {
				// TODO check whether this tx is on our chain, it should NOT be
				return fmt.Errorf("transaction %s is blessed by block(s) %s, not by block %s", txHash.String(), txMeta.BlockHashes, b.Hash().String())
			}

			txIdx, ok := b.txMap.Get(*txHash)
			if !ok {
				return fmt.Errorf("transaction %s is not in the txMap", txHash.String())
			}

			for _, parentTxHash := range txMeta.ParentTxHashes {
				parentTxIdx, ok := b.txMap.Get(*parentTxHash)
				if !ok {
					// check whether the parent is in a block on our chain
					parentTxMeta, err := txMetaStore.Get(ctx, parentTxHash)
					if err != nil {
						return err
					}
					if len(parentTxMeta.BlockHashes) > 0 {
						// TODO check whether this block is on our chain, it should be
						return nil
					}
				}

				if parentTxIdx > txIdx {
					return fmt.Errorf("transaction %s is before parent transaction %s", txHash.String(), parentTxHash.String())
				}
			}
		}
	}

	return nil
}

func (b *Block) GetAndValidateSubtrees(ctx context.Context, subtreeStore blob.Store) error {
	b.subtreeSlices = make([]*util.Subtree, len(b.Subtrees))
	var subtreeBytes []byte
	var err error
	for i, subtreeHash := range b.Subtrees {
		subtreeBytes, err = subtreeStore.Get(ctx, subtreeHash[:])
		if err != nil {
			return err
		}

		subtree := &util.Subtree{}
		err = subtree.Deserialize(subtreeBytes)
		if err != nil {
			return err
		}

		b.subtreeSlices[i] = subtree
	}

	// TODO something with conflicts

	return nil
}

func (b *Block) CheckMerkleRoot() error {
	if len(b.Subtrees) != len(b.subtreeSlices) {
		return fmt.Errorf("number of subtrees does not match number of subtree slices")
	}

	hashes := make([]*chainhash.Hash, len(b.Subtrees))
	for i, subtree := range b.subtreeSlices {
		if i == 0 {
			// We need to inject the coinbase txid into the first position of the first subtree
			coinbaseHash, err := chainhash.NewHash(b.CoinbaseTx.TxIDBytes())
			if err != nil {
				return err
			}
			subtree.ReplaceRootNode(coinbaseHash)
		}

		hashes[i] = subtree.RootHash()
	}

	// Create a new subtree with the hashes of the subtrees
	st := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(b.Subtrees)))
	for _, hash := range b.Subtrees {
		err := st.AddNode(hash, 1)
		if err != nil {
			return err
		}
	}

	calculatedMerkleRoot := st.RootHash()
	calculatedMerkleRootHash, err := chainhash.NewHash(calculatedMerkleRoot[:])
	if err != nil {
		return err
	}

	if !b.Header.HashMerkleRoot.IsEqual(calculatedMerkleRootHash) {
		return errors.New("merkle root does not match")
	}

	return nil
}

// ExtractCoinbaseHeight attempts to extract the height of the block from the
// scriptSig of a coinbase transaction.  Coinbase's heights are only present in
// blocks of version 2 or later.  This was added as part of BIP0034.
func (b *Block) ExtractCoinbaseHeight() (uint32, error) {
	if b.CoinbaseTx == nil {
		return 0, fmt.Errorf("ErrMissingCoinbase")
	}

	if len(b.CoinbaseTx.Inputs) != 1 {
		return 0, fmt.Errorf("ErrMultipleCoinbase")
	}

	sigScript := *b.CoinbaseTx.Inputs[0].UnlockingScript
	if len(sigScript) < 1 {
		str := "the coinbase signature script for blocks of " +
			"version %d or greater must start with the " +
			"length of the serialized block height"
		str = fmt.Sprintf(str, serializedHeightVersion)
		//return 0, ruleError(ErrMissingCoinbaseHeight, str)
		return 0, fmt.Errorf("ErrMissingCoinbaseHeight: %s", str)
	}

	// Detect the case when the block height is a small integer encoded with
	// as single byte.
	opcode := int(sigScript[0])
	if opcode == txscript.OP_0 {
		return 0, nil
	}
	if opcode >= txscript.OP_1 && opcode <= txscript.OP_16 {
		return uint32(opcode - (txscript.OP_1 - 1)), nil
	}

	// Otherwise, the opcode is the length of the following bytes which
	// encode in the block height.
	serializedLen := int(sigScript[0])
	if len(sigScript[1:]) < serializedLen {
		str := "the coinbase signature script for blocks of " +
			"version %d or greater must start with the " +
			"serialized block height"
		str = fmt.Sprintf(str, serializedLen)
		return 0, fmt.Errorf("ErrMissingCoinbaseHeight: %s", str)
	}

	serializedHeightBytes := make([]byte, 8)
	copy(serializedHeightBytes, sigScript[1:serializedLen+1])
	serializedHeight := binary.LittleEndian.Uint64(serializedHeightBytes)

	return uint32(serializedHeight), nil
}

func (b *Block) SubTreeBytes() ([]byte, error) {
	// write the subtree list
	buf := bytes.NewBuffer(nil)
	err := wire.WriteVarInt(buf, 0, uint64(len(b.Subtrees)))
	if err != nil {
		return nil, err
	}
	for _, subTree := range b.Subtrees {
		_, err = buf.Write(subTree[:])
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (b *Block) SubTreesFromBytes(subtreesBytes []byte) error {
	buf := bytes.NewBuffer(subtreesBytes)
	subTreeCount, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return err
	}

	var subtreeBytes [32]byte
	var subtreeHash *chainhash.Hash
	for i := uint64(0); i < subTreeCount; i++ {
		_, err = buf.Read(subtreeBytes[:])
		if err != nil {
			return err
		}
		subtreeHash, err = chainhash.NewHash(subtreeBytes[:])
		if err != nil {
			return err
		}
		b.Subtrees = append(b.Subtrees, subtreeHash)
	}

	b.subtreeLength = subTreeCount

	return err
}

func (b *Block) Bytes() ([]byte, error) {
	// TODO not tested, due to discussion around storing subtrees in the block

	// write the header
	hash := b.Header.Hash()
	buf := bytes.NewBuffer(hash[:])

	// write the subtree list
	subtreeBytes, err := b.SubTreeBytes()
	if err != nil {
		return nil, err
	}
	buf.Write(subtreeBytes)

	// write the coinbase tx
	_, err = buf.Write(b.CoinbaseTx.Bytes())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
