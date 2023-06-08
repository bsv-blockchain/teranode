package blockvalidation

import (
	"errors"

	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-bt"
)

var (
	ErrInvalidPOW = errors.New("invalid proof of work")
)

type Block struct {
	Header     *bc.BlockHeader
	CoinbaseTx *bt.Tx
	SubTrees   []*util.SubTree
	// Height uint32
}

func NewBlock(header *bc.BlockHeader, coinbase *bt.Tx, subTrees []*util.SubTree) *Block {
	return &Block{
		Header:     header,
		CoinbaseTx: coinbase,
		SubTrees:   subTrees,
	}
}

func (b *Block) CheckPOW() error {
	// TODO Check the nBits value is correct for this block

	// Check that the block header hash is less than the target difficulty.
	ok := b.Header.Valid()

	if !ok {
		return ErrInvalidPOW
	}

	return nil
}

/*

1. Check that the block header hash is less than the target difficulty.
2. Calculate the merkle root of the list of subtrees and check it matches the MR in the block header.
5. Check that the coinbase transaction is valid (reward checked later).
3. Check that the coinbase transaction includes the correct block height.
6. Check that the block timestamp is not more than two hours in the future.
5. Check that the first transaction in the block is a coinbase transaction.
7. Check that the median time past of the block is after the median time past of the last 11 blocks.

3. Check that each subtree is know and if not, get and process it.
4. Add up the fees of each subtree.

5. Check that the total fees of the block are less than or equal to the block reward.
4. Check that the coinbase transaction includes the correct block reward.

5. Check the there are no duplicate transactions in the block.
6. Check that all transactions are valid (or blessed)

*/
