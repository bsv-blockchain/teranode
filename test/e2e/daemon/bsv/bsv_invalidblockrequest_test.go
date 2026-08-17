package bsv

import (
	"bytes"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// TestBSVInvalidBlockRequest is the Teranode port of bitcoin-sv's
// invalidblockrequest.py.
//
// Upstream drives a node through ComparisonTestFramework, feeding it blocks over
// p2p and asserting the reject each one draws: a block whose transaction list
// contains a duplicate (RejectResult(16, b'bad-txns-duplicate')), a block whose
// transaction spends the same outpoint twice
// (RejectResult(16, b'bad-txns-inputs-duplicate')), and a block whose coinbase
// pays more than the subsidy (RejectResult(16, b'bad-cb-amount')). Around them it
// asserts a valid block is requested and becomes the tip.
//
// This replaces an earlier port that predates the porting exercise and never
// passed: it built a three-node P2P ring that could not form under
// SETTINGS_CONTEXT=test, and none of its subtests actually constructed an invalid
// block or asserted a rejection reason. See the invalidblockrequest-port-red gap,
// whose plan this discharges.
//
// The three defects are submitted through BlockValidationClient.ProcessBlock
// rather than over the wire, and that choice is the result of measuring what the
// wire can carry. Teranode does answer an invalid block with a reject of code
// RejectInvalid (16, matching upstream), but the reason is the fixed string
// "block rejected" for every cause (netsync/manager.go PushRejectMsg) - so a wire
// peer cannot tell bad-txns-duplicate from bad-cb-amount, and asserting over the
// wire would assert strictly less than asserting the error ProcessBlock returns.
// See the opaque-block-reject-reason gap. The wire leg below therefore covers the
// assertion the wire genuinely can carry - that a valid block is requested via
// getdata and becomes the tip - and the three rejection reasons are asserted
// where they are distinguishable.
//
// Reproduced from upstream:
//   - a valid block is requested via getdata and becomes the chain tip
//   - a block containing a duplicated transaction is rejected
//   - a block whose transaction spends the same outpoint twice is rejected
//   - a block whose coinbase pays more than subsidy + fees is rejected
//   - each rejected block leaves the chain tip where it was
func TestBSVInvalidBlockRequest(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	// Upstream mines block1 for its spendable coinbase and then 100 more blocks to
	// mature it. The test chain params set CoinbaseMaturity to 1, so two blocks buy
	// the same thing: block 1's coinbase is spendable once block 2 is on top.
	require.EqualValues(t, 1, td.Settings.ChainCfgParams.CoinbaseMaturity,
		"this port mines 2 blocks because maturity is 1; if that changes, mine maturity+1")

	td.MineBlocks(t, 2)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err, "read block 1")

	// Upstream's tx1 (spends block1's coinbase) and tx2 (spends tx1). Both are
	// valid; the blocks built from them below are what is invalid.
	tx1 := td.CreateTransaction(t, block1.CoinbaseTx)
	tx2 := td.CreateTransaction(t, tx1)

	t.Run("valid_block_requested_via_getdata_and_becomes_tip", func(t *testing.T) {
		requireValidBlockRequestedAndAccepted(t, td)
	})

	t.Run("duplicate_transaction_rejected", func(t *testing.T) {
		// Upstream mutates a valid 3-transaction block by appending its last
		// transaction again, which leaves the merkle root - and so the block hash -
		// unchanged, because the classic merkle tree duplicates the final node to
		// pad an odd row. That specific malleability is not expressible against a
		// Teranode block: the merkle root comes from the subtree, so the duplicate
		// changes the root and the hash. The defect upstream is testing for is the
		// duplicate itself, and that is what is submitted here.
		// Caught by subtree validation ("duplicate transaction in subtree at index
		// N") rather than by model.Block.checkDuplicateTransactions, which enforces
		// the same rule a layer further in. Asserting the message that actually
		// comes back keeps the test honest about which check is load-bearing.
		requireBlockRejected(t, td, "duplicate transaction in subtree", tx1, tx2, tx2)
	})

	t.Run("duplicate_inputs_rejected", func(t *testing.T) {
		dupInputTx := spendSameOutpointTwice(t, td, tx1)

		// The one place this port reproduces an upstream reason string exactly.
		// Teranode rejects at the transaction rather than the block, and the reason
		// comes from GoBDK - the same script engine bitcoin-sv runs - so the string
		// upstream asserts arrives verbatim.
		requireBlockRejected(t, td, "bad-txns-inputs-duplicate", dupInputTx)
	})

	t.Run("bad_coinbase_amount_rejected", func(t *testing.T) {
		requireBadCoinbaseAmountRejected(t, td)
	})
}

// requireValidBlockRequestedAndAccepted is upstream's first TestInstance: a block
// offered to the node is asked for and becomes the tip.
//
// It goes through the wire rather than ProcessBlock because the request half of
// the assertion only exists there. Teranode does not accept an unrequested block
// from a peer - peer_server.go disconnects for it - so the exchange is the whole
// point: announce by inv, wait to be asked, then answer.
func requireValidBlockRequestedAndAccepted(t *testing.T, td *daemon.TestDaemon) {
	t.Helper()

	before := tipHeader(t, td)

	_, block := td.CreateTestBlock(t, blockOf(t, td, before), nextNonce(t))

	p := wirepeer.Connect(t, td)
	defer p.Close()

	inv := wire.NewMsgInv()
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, block.Hash())), "build inv")
	p.Send(t, inv)

	p.WaitForGetDataOf(t, 30*time.Second, block.Hash())

	p.Send(t, asMsgBlock(t, block))

	require.Eventually(t, func() bool {
		return tryBestBlockHash(td) == block.Hash().String()
	}, 30*time.Second, 200*time.Millisecond, "the block the node asked for should have become the tip")
}

// asMsgBlock renders a coinbase-only Teranode block as a wire block.
//
// It goes through the serialized form rather than copying fields across, so the
// header encoding stays the node's own. Only coinbase-only blocks are supported:
// anything else lives in subtrees that would have to be walked to recover the
// transaction list, which no caller here needs.
func asMsgBlock(t *testing.T, block *model.Block) *wire.MsgBlock {
	t.Helper()

	require.Empty(t, block.Subtrees, "asMsgBlock only serializes coinbase-only blocks")

	raw := block.Header.Bytes()
	raw = append(raw, 0x01) // transaction count, as a single-byte varint
	raw = append(raw, block.CoinbaseTx.Bytes()...)

	msg := &wire.MsgBlock{}
	require.NoError(t, msg.Bsvdecode(bytes.NewReader(raw), wire.ProtocolVersion, wire.BaseEncoding),
		"decode the block back as a wire message")

	return msg
}

// requireBlockRejected builds a block containing txs on top of the current tip,
// submits it, and asserts it is refused for wantReason and leaves the tip alone.
//
// The tip check is upstream's implicit assertion throughout: every rejected
// TestInstance is followed by the next one building on the same parent, which
// only holds if the rejected block never became the tip.
func requireBlockRejected(t *testing.T, td *daemon.TestDaemon, wantReason string, txs ...*bt.Tx) {
	t.Helper()

	before := tipHeader(t, td)

	_, block := td.CreateTestBlock(t, blockOf(t, td, before), nextNonce(t), txs...)

	err := td.BlockValidationClient.ProcessBlock(td.Ctx, block, block.Height, "", "legacy", 0)
	require.Error(t, err, "block should have been rejected")
	require.Contains(t, err.Error(), wantReason,
		"block was rejected, but not for the reason this port is asserting")

	requireTipUnchanged(t, td, before)
}

// requireBadCoinbaseAmountRejected is upstream's block3: a coinbase-only block
// whose coinbase claims 100 coins where the subsidy allows 50.
//
// It is built by hand rather than through requireBlockRejected because the
// coinbase is what has to be wrong, and CreateTestBlock always builds a correct
// one. With no other transactions the merkle root is just the coinbase hash, so
// rewriting the output means recomputing the root and re-mining the header.
func requireBadCoinbaseAmountRejected(t *testing.T, td *daemon.TestDaemon) {
	t.Helper()

	before := tipHeader(t, td)

	_, block := td.CreateTestBlock(t, blockOf(t, td, before), nextNonce(t))

	require.Len(t, block.CoinbaseTx.Outputs, 1, "expected a single-output coinbase to inflate")
	block.CoinbaseTx.Outputs[0].Satoshis = 100e8 // Too high, as upstream puts it.

	block.Header.HashMerkleRoot = block.CoinbaseTx.TxIDChainHash()
	remine(block)

	err := td.BlockValidationClient.ProcessBlock(td.Ctx, block, block.Height, "", "legacy", 0)
	require.Error(t, err, "an over-paying coinbase should have been rejected")
	require.Contains(t, err.Error(), "is greater than the fees + block subsidy",
		"block was rejected, but not for the coinbase amount")

	requireTipUnchanged(t, td, before)
}

// spendSameOutpointTwice builds upstream's duplicate-input transaction: the same
// outpoint appears in two inputs.
//
// Both inputs are signed properly, so the only rule the transaction breaks is the
// one under test. Appending a copy of an already-signed input would work too, but
// would leave the second signature invalid and risk the block being refused for
// that instead.
func spendSameOutpointTwice(t *testing.T, td *daemon.TestDaemon, parent *bt.Tx) *bt.Tx {
	t.Helper()

	outpoint := &bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(outpoint), "add first input")
	require.NoError(t, tx.FromUTXOs(outpoint), "add the same outpoint a second time")

	require.NoError(t, tx.AddP2PKHOutputFromPubKeyBytes(
		td.GetPrivateKey(t).PubKey().Compressed(), parent.Outputs[0].Satoshis/2), "add output")

	require.NoError(t, tx.FillAllInputs(td.Ctx,
		&unlocker.Getter{PrivateKey: td.GetPrivateKey(t)}), "sign both inputs")

	return tx
}

// tipHeader returns the current best block header.
func tipHeader(t *testing.T, td *daemon.TestDaemon) *model.BlockHeader {
	t.Helper()

	header, _, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err, "read best block header")

	return header
}

// blockOf resolves a header to the full block, which CreateTestBlock needs as the
// parent.
func blockOf(t *testing.T, td *daemon.TestDaemon, header *model.BlockHeader) *model.Block {
	t.Helper()

	block, err := td.BlockchainClient.GetBlock(td.Ctx, header.Hash())
	require.NoError(t, err, "read block %s", header.Hash())

	return block
}

// requireTipUnchanged asserts the tip is still want, and stays there. A block that
// is rejected synchronously could still be adopted a moment later by an
// asynchronous path, which a single read would miss.
func requireTipUnchanged(t *testing.T, td *daemon.TestDaemon, want *model.BlockHeader) {
	t.Helper()

	require.Never(t, func() bool {
		return tryBestBlockHash(td) != want.Hash().String()
	}, 2*time.Second, 200*time.Millisecond, "a rejected block must not become the tip")
}

// nextNonce hands out a distinct starting nonce per block so two blocks built on
// the same parent in one run cannot collide.
func nextNonce(t *testing.T) uint32 {
	t.Helper()

	nonceSeq++

	return nonceSeq
}

var nonceSeq uint32 = 10000

// remine advances the nonce until the header meets the target, which is what
// CreateTestBlock does after assembling a block and what any edit to the header
// invalidates.
func remine(block *model.Block) {
	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			return
		}

		block.Header.Nonce++
	}
}
