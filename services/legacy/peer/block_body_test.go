package peer

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// A BlockBody describes a block whose transactions are not in memory. It
// carries what every caller before processing actually needs: the header, so
// the block can be identified and its parent found, the transaction count, so
// an empty block can be refused, and the size, so the park can charge its
// budget without walking anything.
func TestBlockBodyDescribesABlockWithoutHoldingIt(t *testing.T) {
	hdr := wire.NewBlockHeader(1, &chainhash.Hash{0x01}, &chainhash.Hash{0x02}, 0x1d00ffff, 7)

	b := &BlockBody{Header: *hdr, TxCount: 4096, Size: 3_996_413_002, Hash: hdr.BlockHash()}

	require.Equal(t, hdr.BlockHash(), b.Hash)
	require.Equal(t, hdr.PrevBlock, b.Header.PrevBlock, "the parent is readable without the body")
	require.Equal(t, int64(3_996_413_002), b.Size)
}
