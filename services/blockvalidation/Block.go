package blockvalidation

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-bk/crypto"
	"github.com/libsv/go-bt/v2"
	p2p_bc "github.com/libsv/go-p2p/blockchain"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
)

var (
	ErrInvalidPOW = errors.New("invalid proof of work")
)

type Block struct {
	Header           *bc.BlockHeader
	CoinbaseTx       *bt.Tx
	SubTrees         []*util.SubTree
	BlockChainClient *blockchain.Client
	// Height uint32

	// local
	hash          *chainhash.Hash
	subTreeLength uint64
}

func NewBlock(header *bc.BlockHeader, coinbase *bt.Tx, subTrees []*util.SubTree) (*Block, error) {
	blockchainClient, err := blockchain.NewClient()
	if err != nil {
		return nil, err
	}

	return &Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		SubTrees:         subTrees,
		BlockChainClient: blockchainClient,
	}, nil
}

func NewBlockFromBytes(blockBytes []byte) (*Block, error) {
	block := &Block{}

	var err error

	// read the first 80 bytes as the block header
	blockHeaderBytes := blockBytes[:80]
	block.Header, err = bc.NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, err
	}

	// create new buffer reader for the block bytes
	buf := bytes.NewReader(blockBytes[80:])

	// read the length of the subtree list
	block.subTreeLength, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, err
	}

	// read the subtree list
	var hashBytes [32]byte
	var subTreeHash *chainhash.Hash
	for i := uint64(0); i < block.subTreeLength; i++ {
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, err
		}

		subTreeHash, err = chainhash.NewHash(hashBytes[:])
		if err != nil {
			return nil, err
		}

		_ = subTreeHash

		//
		// TODO load the full subtree from the store ???
		//
		block.SubTrees = append(block.SubTrees, util.NewTree(20))
	}

	coinbaseTxBytes, _ := io.ReadAll(buf) // read the rest of the bytes as the coinbase tx
	block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTxBytes)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (b *Block) ValidateBlock(ctx context.Context) error {
	// 1. Check that the block header hash is less than the target difficulty.
	if err := b.checkPOW(ctx); err != nil {
		return err
	}

	// 2. Check that the block timestamp is not more than two hours in the future.

	// 3. Check that the median time past of the block is after the median time past of the last 11 blocks.

	// 4. Check that the coinbase transaction is valid (reward checked later).
	// if err := b.checkValidCoinbase(); err != nil {
	// 	return err
	// }

	// 5. Check that the coinbase transaction includes the correct block height.
	// if err := b.checkCoinbaseHeight(); err != nil {
	// 	return err
	// }

	// 6. Get and validate any missing subtrees.
	// if err := b.getAndValidateSubTrees(ctx); err != nil {
	// 	return err
	// }

	// 7. Check that the first transaction in the first subtree is a coinbase placeholder (zeros)
	// if err := b.checkCoinbasePlaceholder(); err != nil {
	// 	return err
	// }

	// 8. Calculate the merkle root of the list of subtrees and check it matches the MR in the block header.
	if err := b.checkMerkleRoot(); err != nil {
		return err
	}

	// 4. Check that the coinbase transaction includes the correct block height.

	// 3. Check that each subtree is know and if not, get and process it.
	// 4. Add up the fees of each subtree.

	// 5. Check that the total fees of the block are less than or equal to the block reward.
	// 4. Check that the coinbase transaction includes the correct block reward.

	// 5. Check the there are no duplicate transactions in the block.
	// 6. Check that all transactions are valid (or blessed)

	return nil
}

func (b *Block) checkPOW(ctx context.Context) error {
	// TODO Check the nBits value is correct for this block
	previousBlockHash, err := chainhash.NewHash(b.Header.HashPrevBlock)
	if err == nil {
		return err
	}

	// TODO - replace the following with a call to the blockchain service that gets the correct nBits value for the block
	b.BlockChainClient.GetBlock(ctx, previousBlockHash)

	// Check that the block header hash is less than the target difficulty.
	ok := b.Header.Valid()

	if !ok {
		return ErrInvalidPOW
	}

	return nil
}

func (b *Block) checkMerkleRoot() error {
	hashes := make([][]byte, 0, len(b.SubTrees))

	for i, subtree := range b.SubTrees {
		if i == 0 {
			// We need to inject the coinbase txid into the first position of the first subtree
			var coinbaseHash [32]byte
			copy(coinbaseHash[:], b.CoinbaseTx.TxIDBytes())
			subtree.ReplaceRootNode(coinbaseHash)
		}

		hash := subtree.RootHash()
		hashes[i] = hash[:]
	}

	merkleTree := p2p_bc.BuildMerkleTreeStore(hashes)

	calculatedMerkleRoot := merkleTree[len(merkleTree)-1]

	if !bytes.Equal(calculatedMerkleRoot, b.Header.HashMerkleRoot) {
		return errors.New("merkle root does not match")
	}

	return nil
}

func (b *Block) Hash() *chainhash.Hash {
	if b.hash != nil {
		return b.hash
	}

	hash, err := chainhash.NewHash(bt.ReverseBytes(crypto.Sha256d(b.Header.Bytes())))
	if err == nil {
		b.hash = hash
	}

	return hash
}

func (b *Block) Bytes() ([]byte, error) {
	// TODO not tested, due to discussion around storing subtrees in the block

	// write the header
	blockBytes := b.Header.Bytes()

	// write the subtree list
	buf := bytes.NewBuffer(blockBytes)
	err := wire.WriteVarInt(buf, 0, uint64(len(b.SubTrees)))
	if err != nil {
		return nil, err
	}

	for _, subTree := range b.SubTrees {
		hash := subTree.RootHash()
		_, err = buf.Write(hash[:])
		if err != nil {
			return nil, err
		}
	}

	// write the coinbase tx
	_, err = buf.Write(b.CoinbaseTx.Bytes())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
