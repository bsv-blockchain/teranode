package svp2ptest

import (
	"bytes"
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestFixtureChain_KeepsTheKeyAndCoinbasesItMined(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	require.NotNil(t, chain.PrivKey, "the fixture key must be kept, or no coinbase output can ever be spent")
	require.NotEmpty(t, chain.Address)
	require.Len(t, chain.Coinbases, 3, "one coinbase per mined block")

	for i, coinbase := range chain.Coinbases {
		block := chain.Blocks[chain.Headers[i].BlockHash()]
		require.Equal(t, block.Transactions[0].TxHash().String(), coinbase.TxIDChainHash().String(),
			"Coinbases[%d] must be the coinbase of the block at height %d", i, i+1)
	}
}

func TestFixtureChain_SpendCoinbaseBuildsASignedSpend(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	const fee = uint64(1000)

	spend := chain.SpendCoinbase(t, 1, fee)

	require.Len(t, spend.Inputs, 1)
	require.Len(t, spend.Outputs, 1)

	coinbase := chain.Coinbases[0]
	require.Equal(t, coinbase.TxIDChainHash().String(), spend.Inputs[0].PreviousTxIDChainHash().String())
	require.Equal(t, uint32(0), spend.Inputs[0].PreviousTxOutIndex)
	require.Equal(t, coinbase.Outputs[0].Satoshis-fee, spend.Outputs[0].Satoshis)
	require.NotEmpty(t, spend.Inputs[0].UnlockingScript.Bytes(), "the input must be signed")
}

func TestFixtureChain_SpendCoinbaseGivesADistinctTxPerHeight(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	first := chain.SpendCoinbase(t, 1, 1000)
	second := chain.SpendCoinbase(t, 2, 1000)

	require.NotEqual(t, first.TxIDChainHash().String(), second.TxIDChainHash().String())
}

func TestFixtureChain_BuildNextBlockCarriesEveryTransaction(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	first := chain.SpendCoinbase(t, 1, 1000)
	second := chain.SpendCoinbase(t, 2, 2000)

	tip := chain.Tip()
	block := chain.BuildNextBlock(t, tSettings, []*bt.Tx{first, second})

	require.Len(t, block.Transactions, 3, "coinbase plus the two spends")
	require.Equal(t, tip.String(), block.Header.PrevBlock.String())
	require.Equal(t, first.TxIDChainHash().String(), block.Transactions[1].TxHash().String())
	require.Equal(t, second.TxIDChainHash().String(), block.Transactions[2].TxHash().String())
}

func TestFixtureChain_BuildNextBlockHasTheCanonicalMerkleRoot(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	block := chain.BuildNextBlock(t, tSettings, []*bt.Tx{chain.SpendCoinbase(t, 1, 1000), chain.SpendCoinbase(t, 2, 1000)})

	// The oracle is the node's OWN merkle check, the one that refuses a block
	// whose header does not commit to its transactions, rather than a second
	// copy of this package's tree walk.
	modelBlock, err := model.NewBlockFromMsgBlock(block, tSettings)
	require.NoError(t, err)
	require.NoError(t, modelBlock.CheckMerkleRoot(context.Background()),
		"a block whose merkle root is not the tree over its txids is rejected before it is ever reconstructed")
}

func TestFixtureChain_BuildNextBlockMeetsTargetDifficulty(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	block := chain.BuildNextBlock(t, tSettings, []*bt.Tx{chain.SpendCoinbase(t, 1, 1000)})

	header, err := model.NewBlockHeaderFromBytes(headerBytesOf(t, &block.Header))
	require.NoError(t, err)

	ok, _, err := header.HasMetTargetDifficulty()
	require.NoError(t, err)
	require.True(t, ok, "the mined header must meet the regtest target")
}

func TestFixtureChain_BuildNextBlockIsServeableButUnannounced(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	before := len(chain.Headers)
	block := chain.BuildNextBlock(t, tSettings, []*bt.Tx{chain.SpendCoinbase(t, 1, 1000)})
	hash := block.Header.BlockHash()

	require.Len(t, chain.Headers, before, "the block must not be announced by getheaders until it is published")
	require.Same(t, block, chain.Blocks[hash], "getdata and getblocktxn must be able to answer for it")
	require.Equal(t, int32(before+1), chain.Heights[hash])
	require.NotEqual(t, hash.String(), chain.Tip().String())
}

func TestFixtureChain_PublishHeaderExtendsTheAnnouncedChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	block := chain.BuildNextBlock(t, tSettings, []*bt.Tx{chain.SpendCoinbase(t, 1, 1000)})
	hash := block.Header.BlockHash()

	chain.PublishHeader(t, block)

	require.Len(t, chain.Headers, 4)
	require.Equal(t, hash.String(), chain.Tip().String())
}

func TestFixtureChain_BuildNextBlockCoinbasePaysTheSubsidyAtItsHeight(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	block := chain.BuildNextBlock(t, tSettings, nil)

	var paid uint64
	for _, out := range block.Transactions[0].TxOut {
		paid += uint64(out.Value) //nolint:gosec // fixture values are small
	}

	require.Equal(t, uint64(50e8), paid, "the coinbase must claim the subsidy and no fee, so it can never exceed subsidy+fees")
}

// headerBytesOf serializes a wire header into the 80 bytes model.BlockHeader
// parses.
func headerBytesOf(t *testing.T, header *wire.BlockHeader) []byte {
	t.Helper()

	var buf bytes.Buffer

	require.NoError(t, header.Serialize(&buf))

	return buf.Bytes()
}

func TestFixtureChain_BuildBlockOnMinesARunWithoutAnnouncingIt(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	sixth := chain.BuildNextBlock(t, tSettings, nil)
	seventh := chain.BuildBlockOn(t, tSettings, sixth.Header.BlockHash(), nil)
	eighth := chain.BuildBlockOn(t, tSettings, seventh.Header.BlockHash(), nil)

	require.Equal(t, sixth.Header.BlockHash().String(), seventh.Header.PrevBlock.String())
	require.Equal(t, seventh.Header.BlockHash().String(), eighth.Header.PrevBlock.String())

	for want, block := range map[int32]*wire.MsgBlock{4: sixth, 5: seventh, 6: eighth} {
		height, known := chain.Height(block.Header.BlockHash())
		require.True(t, known)
		require.Equal(t, want, height)
	}

	require.Equal(t, 3, chain.Len(), "a run built this way must stay unannounced")
	require.NotEqual(t, eighth.Header.BlockHash().String(), chain.Tip().String())
}
