// Package svp2ptest holds test-support code shared by the svp2p integration
// tests and the parity harness: a fixture chain the real pipeline accepts, a
// scripted wire-level peer, in-process starters for the Teranode services a
// node needs, and a recording logger. It is imported by tests only.
package svp2ptest

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Chain fixture: a run of regtest coinbase-only blocks the real pipeline
// accepts (valid proof of work under the regtest limit, BIP34 coinbase, merkle
// root = the coinbase txid, which is what a single-transaction block's root is).
// ---------------------------------------------------------------------------

// FixtureChain is the chain the scripted peers serve from.
//
// CONCURRENCY. The three exported collections below are written by
// BuildNextBlock and PublishHeader, which a scenario calls from the test
// goroutine WHILE peer serve goroutines are answering getheaders, getblocks and
// getdata out of the same collections. Every such write and every serving read
// goes through mu; the accessors below are the read side, and are what
// ScriptedPeer uses.
//
// The exported fields stay exported because a scenario builds its peers and
// reads its fixture blocks before any goroutine is serving, which is safe. A
// read taken WHILE the node is running must go through the accessors.
type FixtureChain struct {
	// mu guards Headers, Blocks and Heights. Never held across a call that
	// can block: every accessor below copies or returns a map value and
	// releases it.
	mu sync.RWMutex

	Headers []*wire.BlockHeader // headers[i] is at height i+1
	Blocks  map[chainhash.Hash]*wire.MsgBlock
	Heights map[chainhash.Hash]int32 // includes genesis at height 0

	// PrivKey is the key every fixture coinbase pays to, kept so a later
	// block can spend one of those outputs (fixtureblock.go SpendCoinbase).
	// Without it the fixture chain can only ever hold coinbase-only blocks,
	// which is what it held before Task 10.
	PrivKey *bec.PrivateKey
	// Address is PrivKey's P2PKH address, the one CreateCoinbase paid.
	Address string
	// Coinbases[i] is the coinbase of the block at height i+1, kept in bt
	// form because that is the form a spend's input is built from.
	Coinbases []*bt.Tx
}

// Tip is the hash of the highest ANNOUNCED header — the chain as getheaders
// reports it, which a block built by BuildNextBlock does not join until
// PublishHeader.
func (c *FixtureChain) Tip() chainhash.Hash {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.Headers[len(c.Headers)-1].BlockHash()
}

// Len is how many headers the announced chain holds.
func (c *FixtureChain) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.Headers)
}

// HeaderAt is the header at index i, or nil when i is outside the chain.
func (c *FixtureChain) HeaderAt(i int) *wire.BlockHeader {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if i < 0 || i >= len(c.Headers) {
		return nil
	}

	return c.Headers[i]
}

// Block is the block for hash, whether or not its header is announced.
func (c *FixtureChain) Block(hash chainhash.Hash) (*wire.MsgBlock, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	block, known := c.Blocks[hash]

	return block, known
}

// Height is the height recorded for hash, whether or not its header is
// announced.
func (c *FixtureChain) Height(hash chainhash.Hash) (int32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	height, known := c.Heights[hash]

	return height, known
}

func BuildFixtureChain(t *testing.T, tSettings *settings.Settings, count int) *FixtureChain {
	t.Helper()

	return BuildFixtureChainPadded(t, tSettings, count, 0)
}

// BuildFixtureChainPadded is BuildFixtureChain with padBytes of OP_RETURN data
// on every coinbase, so a block carries real bytes. Legacy netsync rotates a
// sync peer delivering under 51200 bytes/s for three 30-second ticks
// (syncPeerTickerInterval, maxNetworkViolations), and no rate of 190-byte
// blocks can satisfy that; 200 KB blocks at any live pace do.
func BuildFixtureChainPadded(t *testing.T, tSettings *settings.Settings, count, padBytes int) *FixtureChain {
	t.Helper()

	privKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privKey.PubKey(), true)
	require.NoError(t, err)

	genesis := tSettings.ChainCfgParams.GenesisBlock.Header
	genesisHash := genesis.BlockHash()

	chain := &FixtureChain{
		Blocks:  make(map[chainhash.Hash]*wire.MsgBlock, count),
		Heights: map[chainhash.Hash]int32{genesisHash: 0},
		PrivKey: privKey,
		Address: address.AddressString,
	}

	bits, err := model.NewNBitFromString(fmt.Sprintf("%08x", genesis.Bits))
	require.NoError(t, err)

	prevHash := genesisHash
	// Headers are spaced ten minutes apart, and a block more than two hours in
	// the future is rejected. Starting the run far enough back that its LAST
	// header still lands in the past keeps that rule out of the way however
	// long the fixture chain is.
	baseTime := time.Now().Add(-time.Duration(count+2) * 10 * time.Minute).Unix()

	for i := 0; i < count; i++ {
		height := uint32(i + 1) //nolint:gosec // test heights are small

		// The subsidy halves every SubsidyReductionInterval blocks (150 on
		// regtest); a coinbase that ignores that is rejected by
		// checkBlockRewardAndFees at the first halving and the chain stalls there.
		subsidy := uint64(50e8) >> (height / tSettings.ChainCfgParams.SubsidyReductionInterval)

		coinbase, cbErr := model.CreateCoinbase(height, subsidy, "svp2p sync test", []string{address.AddressString})
		require.NoError(t, cbErr)

		if padBytes > 0 {
			pad := &bscript.Script{}
			require.NoError(t, pad.AppendOpcodes(bscript.OpFALSE, bscript.OpRETURN))
			require.NoError(t, pad.AppendPushData(make([]byte, padBytes)))
			coinbase.AddOutput(&bt.Output{Satoshis: 0, LockingScript: pad})
		}

		wireHeader := mineHeader(t, prevHash, *coinbase.TxIDChainHash(), uint32(baseTime+int64(i)*600), *bits) //nolint:gosec // test timestamps are in range

		coinbaseWire := wire.NewMsgTx(1)
		require.NoError(t, coinbaseWire.Deserialize(bytes.NewReader(coinbase.Bytes())))

		block := wire.NewMsgBlock(wireHeader)
		require.NoError(t, block.AddTransaction(coinbaseWire))

		hash := wireHeader.BlockHash()

		chain.Headers = append(chain.Headers, wireHeader)
		chain.Coinbases = append(chain.Coinbases, coinbase)
		chain.Blocks[hash] = block
		chain.Heights[hash] = int32(height) //nolint:gosec // test heights are small

		prevHash = hash
	}

	return chain
}

// mineHeader mines one regtest header over the given parent, merkle root and
// timestamp, and returns it in wire form. The nonce loop is the whole of
// "mining" at the regtest limit; the round-trip check is what proves the wire
// header the peers serve is the header that was mined.
func mineHeader(t *testing.T, prev, merkleRoot chainhash.Hash, timestamp uint32, bits model.NBit) *wire.BlockHeader {
	t.Helper()

	prevHash := prev
	root := merkleRoot

	modelHeader := &model.BlockHeader{
		Version:        0x20000000,
		HashPrevBlock:  &prevHash,
		HashMerkleRoot: &root,
		Timestamp:      timestamp,
		Bits:           bits,
	}

	for {
		ok, _, _ := modelHeader.HasMetTargetDifficulty()
		if ok {
			break
		}

		modelHeader.Nonce++
	}

	wireHeader := &wire.BlockHeader{}
	require.NoError(t, wireHeader.Deserialize(bytes.NewReader(modelHeader.Bytes())))
	require.Equal(t, *modelHeader.Hash(), wireHeader.BlockHash(), "the wire header must round-trip the mined model header")

	return wireHeader
}

// ---------------------------------------------------------------------------
// Scripted serving peer: raw go-wire over TCP. It listens, so the svp2p server
// reaches it through Legacy.ConnectPeers exactly as it reaches a real node.
// ---------------------------------------------------------------------------
