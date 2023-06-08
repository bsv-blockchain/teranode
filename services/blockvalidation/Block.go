package blockvalidation

import (
	"bytes"
	"context"
	"errors"

	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-bt/v2"
	p2p_bc "github.com/libsv/go-p2p/blockchain"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
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

/*



 */
