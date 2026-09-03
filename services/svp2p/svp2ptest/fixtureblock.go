package svp2ptest

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Multi-transaction fixture blocks. BuildFixtureChainPadded mines coinbase-only
// blocks, whose merkle root is the coinbase txid alone; compact block
// reconstruction needs a block with transactions the node can be missing, so
// this file mines one the real pipeline accepts: a BIP34 coinbase claiming the
// subsidy and nothing more, real signed spends of earlier fixture coinbases,
// and the merkle root of the whole set.
// ---------------------------------------------------------------------------

// blockIntervalSeconds is the spacing every fixture header uses. Keeping the
// appended block on the same spacing keeps it above the median time past of
// the eleven headers below it and, since the fixture chain is mined into the
// past, still behind the two-hour future limit.
const blockIntervalSeconds = 600

// SpendCoinbase builds a signed P2PKH spend of output 0 of the coinbase at the
// given height, paying value-minus-fee back to the fixture address. The fee is
// what makes the transaction acceptable to the node's own fee policy, and what
// leaves the appended block's coinbase claiming strictly less than subsidy plus
// fees.
//
// height is a chain height (1 for the first mined block), not a slice index.
func (c *FixtureChain) SpendCoinbase(t *testing.T, height int, fee uint64) *bt.Tx {
	t.Helper()

	require.Greater(t, height, 0, "height 0 is the genesis block, whose coinbase this fixture never holds")
	require.LessOrEqual(t, height, len(c.Coinbases), "the fixture chain does not reach height %d", height)

	coinbase := c.Coinbases[height-1]
	out := coinbase.Outputs[0]

	require.Greater(t, out.Satoshis, fee, "the coinbase output must cover the fee")

	spend := bt.NewTx()

	require.NoError(t, spend.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbase.TxIDChainHash(),
		Vout:          0,
		LockingScript: out.LockingScript,
		Satoshis:      out.Satoshis,
	}))

	require.NoError(t, spend.AddP2PKHOutputFromAddress(c.Address, out.Satoshis-fee))
	require.NoError(t, spend.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: c.PrivKey}))

	return spend
}

// BuildNextBlock mines the block that follows the ANNOUNCED tip. It is
// BuildBlockOn with the tip as the parent, which is what almost every scenario
// wants.
func (c *FixtureChain) BuildNextBlock(t *testing.T, tSettings *settings.Settings, txs []*bt.Tx) *wire.MsgBlock {
	t.Helper()

	return c.BuildBlockOn(t, tSettings, c.Tip(), txs)
}

// BuildBlockOn mines the block that follows parent: a coinbase claiming the
// subsidy at the new height, then txs in the order given. The header carries the
// merkle root over every one of those transactions, so the block passes the
// merkle check whichever way the node obtained its transactions — whole from a
// getdata, or reassembled from a compact block.
//
// The block is registered for serving (Blocks, Heights) but its header is NOT
// appended to Headers, so getheaders and getblocks keep answering with the chain
// as it stood. That separation is what lets a scenario announce the block by
// cmpctblock, which carries its own header, without the node having already
// learnt of it from a headers round. PublishHeader is the other half.
//
// The explicit parent is what lets a scenario mine a RUN of blocks without
// announcing any of them: BuildNextBlock alone always builds on the announced
// tip, so two calls without an intervening PublishHeader produce two siblings.
func (c *FixtureChain) BuildBlockOn(t *testing.T, tSettings *settings.Settings, parent chainhash.Hash, txs []*bt.Tx) *wire.MsgBlock {
	t.Helper()

	parentHeader, parentHeight := c.parentOf(t, parent)

	height := uint32(parentHeight + 1) //nolint:gosec // test heights are small

	// The subsidy halves every SubsidyReductionInterval blocks, the same rule
	// BuildFixtureChainPadded's own coinbase follows; a coinbase that claims
	// more than subsidy plus fees is refused by checkBlockRewardAndFees.
	subsidy := uint64(50e8) >> (height / tSettings.ChainCfgParams.SubsidyReductionInterval)

	coinbase, err := model.CreateCoinbase(height, subsidy, "svp2p compact test", []string{c.Address})
	require.NoError(t, err)

	bits, err := model.NewNBitFromString(fmt.Sprintf("%08x", tSettings.ChainCfgParams.GenesisBlock.Header.Bits))
	require.NoError(t, err)

	leaves := make([]chainhash.Hash, 0, len(txs)+1)
	leaves = append(leaves, *coinbase.TxIDChainHash())

	for _, tx := range txs {
		leaves = append(leaves, *tx.TxIDChainHash())
	}

	timestamp := uint32(parentHeader.Timestamp.Unix()) + blockIntervalSeconds //nolint:gosec // fixture timestamps are in range

	header := mineHeader(t, parent, merkleRootOf(leaves), timestamp, *bits)

	block := wire.NewMsgBlock(header)
	require.NoError(t, block.AddTransaction(WireTx(t, coinbase)))

	for _, tx := range txs {
		require.NoError(t, block.AddTransaction(WireTx(t, tx)))
	}

	hash := header.BlockHash()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Blocks[hash] = block
	c.Heights[hash] = int32(height) //nolint:gosec // test heights are small

	return block
}

// parentOf reads the header and height of the block a new one will extend. It
// takes the read lock rather than the write lock the caller ends with, because
// mining sits between the two and is the long part.
func (c *FixtureChain) parentOf(t *testing.T, parent chainhash.Hash) (*wire.BlockHeader, int32) {
	t.Helper()

	c.mu.RLock()
	defer c.mu.RUnlock()

	block, known := c.Blocks[parent]
	require.True(t, known, "the fixture chain does not hold the parent %s", parent)

	height, known := c.Heights[parent]
	require.True(t, known, "the fixture chain has no height for the parent %s", parent)

	header := block.Header

	return &header, height
}

// PublishHeader appends a block built by BuildNextBlock to the announced
// chain, so getheaders, getblocks and Tip see it. It is what a scenario calls
// on the leg that announces by inv, where the node learns the header from a
// headers round rather than from the announcement itself.
func (c *FixtureChain) PublishHeader(t *testing.T, block *wire.MsgBlock) {
	t.Helper()

	hash := block.Header.BlockHash()

	c.mu.Lock()
	defer c.mu.Unlock()

	require.Contains(t, c.Blocks, hash, "only a block this chain already holds can be published")
	require.Equal(t, c.Headers[len(c.Headers)-1].BlockHash().String(), block.Header.PrevBlock.String(),
		"a published header must extend the current tip")

	header := block.Header
	c.Headers = append(c.Headers, &header)
}

// merkleRootOf is the plain Bitcoin merkle tree over a block's txids, coinbase
// first, duplicating the last leaf of every odd level.
func merkleRootOf(leaves []chainhash.Hash) chainhash.Hash {
	level := make([]chainhash.Hash, len(leaves))
	copy(level, leaves)

	for len(level) > 1 {
		if len(level)%2 != 0 {
			level = append(level, level[len(level)-1])
		}

		next := make([]chainhash.Hash, 0, len(level)/2)

		for i := 0; i < len(level); i += 2 {
			buf := make([]byte, 0, 2*chainhash.HashSize)
			buf = append(buf, level[i][:]...)
			buf = append(buf, level[i+1][:]...)
			next = append(next, chainhash.DoubleHashH(buf))
		}

		level = next
	}

	return level[0]
}

// WireTx re-parses a bt transaction as a wire one, the form a MsgBlock holds
// and the form a peer relays it in.
func WireTx(t *testing.T, tx *bt.Tx) *wire.MsgTx {
	t.Helper()

	out := wire.NewMsgTx(1)
	require.NoError(t, out.Deserialize(bytes.NewReader(tx.Bytes())))

	return out
}
