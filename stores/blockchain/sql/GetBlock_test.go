package sql

import (
	"testing"
)

func TestSQL_GetBlock(t *testing.T) {
	//t.Run("block 0 - genesis block", func(t *testing.T) {
	//	storeUrl, err := url.Parse("sqlitememory:///")
	//	require.NoError(t, err)
	//
	//	s, err := New(storeUrl)
	//	require.NoError(t, err)
	//
	//	headerHash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	//	require.NoError(t, err)
	//
	//	block, err := s.GetBlock(context.Background(), headerHash)
	//	require.NoError(t, err)
	//
	//	// header
	//	assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", block.Header.Hash().String())
	//	assert.Equal(t, uint32(1), block.Header.Version)
	//	assert.Equal(t, &chainhash.Hash{}, block.Header.HashPrevBlock)
	//	assert.Equal(t, "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", block.Header.HashMerkleRoot.String())
	//	assert.Equal(t, uint32(1231006505), block.Header.Timestamp)
	//	assert.Equal(t, []byte{0x1d, 0x0, 0xff, 0xff}, block.Header.Bits)
	//	assert.Equal(t, uint32(2083236893), block.Header.Nonce)
	//
	//	// block
	//	assert.Equal(t, uint32(1), block.TransactionCount)
	//	assert.Len(t, block.Subtrees, 1)
	//	txHash, err := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
	//	assert.Equal(t, txHash, block.Subtrees[0])
	//})
}
