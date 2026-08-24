package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// Port of bitcoin-sv's bsv-getdata.py.
//
// Upstream checks the four outcomes of a GETDATA message, and its own header
// lists them: an unknown block draws no action, a known block returns the block,
// an unknown transaction returns NOTFOUND, and a known transaction returns the
// transaction.
//
// THIS ENTRY WAS RECORDED AS NEEDING `funding-shim`, AND IT DOES NOT.
//
// Upstream's fourth case needs "a transaction the node knows about", and gets one
// from the wallet: sendtoaddress(getnewaddress(), 1.0). The registry read that as
// requiring wallet-shaped funding that Teranode has no counterpart for. It turns
// out the pieces already exist in test/utils/transactions:
//
//   - bec.NewPrivateKey plus WithP2PKHOutputs(n, amount, pubKey) is getnewaddress
//     followed by a payment to it;
//   - WithInput(tx, vout, priv...) takes a PER-INPUT private key, so an output
//     paid to a freshly generated address can be spent with that address's own
//     key - which is signrawtransaction.
//
// So this port deliberately takes the long way round on case 4: it generates a
// key, pays a coinbase output to that key's address, and then spends THAT output
// using the generated key. Upstream needs only one transaction the node has seen,
// and a single coinbase spend would have satisfied it. The extra hop is here
// because it exercises the whole wallet-shaped path end to end and pins it with
// assertions, which is what retires the funding-shim prerequisite rather than
// merely working around it once.
func TestBSVGetData(t *testing.T) {
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	// Upstream: self.nodes[0].generate(5). MineToMaturityAndGetSpendableCoinbaseTx
	// mines CoinbaseMaturity+1 and hands back block 1's coinbase, which gives both
	// the chain upstream wants and the funding case 4 needs.
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	peer := wirepeer.Connect(t, td)
	defer peer.Close()

	// Upstream uses 0xdecaf for both unknown-hash cases.
	unknown := unknownHash(t)

	// 1. A GETDATA for an unknown block draws no action.
	//
	// Upstream asserts this negatively, at the end: unknown_hash not in
	// receivedBlocks. Asserting it here, before any block is requested, is
	// stronger - at this point NO block has been asked for, so a block arriving at
	// all is a failure, and the assertion cannot be satisfied by the node simply
	// having answered a different request.
	peer.Send(t, getDataFor(wire.InvTypeBlock, unknown))
	peer.AssertNotReceived(t, 3*time.Second, "a block for an unknown hash",
		func(m wire.Message) bool {
			b, ok := m.(*wire.MsgBlock)
			if !ok {
				return false
			}

			h := b.BlockHash()

			return h.IsEqual(unknown)
		})

	// 2. A GETDATA for a known block returns that block.
	best, err := chainhash.NewHashFromStr(bestBlockHash(t, td))
	require.NoError(t, err, "parse the best block hash")

	peer.Send(t, getDataFor(wire.InvTypeBlock, best))

	got := peer.Wait(t, 30*time.Second, "the requested block", func(m wire.Message) bool {
		b, ok := m.(*wire.MsgBlock)
		if !ok {
			return false
		}

		h := b.BlockHash()

		return h.IsEqual(best)
	})
	require.Equal(t, best.String(), blockHashOf(got),
		"the block returned should be the one requested")

	// 3. A GETDATA for an unknown transaction returns NOTFOUND naming it.
	peer.Send(t, getDataFor(wire.InvTypeTx, unknown))

	peer.Wait(t, 30*time.Second, "notfound for the unknown transaction",
		func(m wire.Message) bool {
			nf, ok := m.(*wire.MsgNotFound)
			if !ok {
				return false
			}

			for _, inv := range nf.InvList {
				if inv.Hash.IsEqual(unknown) {
					return true
				}
			}

			return false
		})

	// 4. A GETDATA for a known transaction returns that transaction.
	//
	// The wallet-shaped path, done properly. Upstream: sendtoaddress(getnewaddress()).
	recipientKey, err := bec.NewPrivateKey()
	require.NoError(t, err, "generate a recipient key - upstream's getnewaddress")

	// Pay a coinbase output to the generated key's address.
	fundTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 10e8, recipientKey.PubKey()),
	)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, fundTx),
		"the funding payment to the generated address should be accepted")
	td.WaitForBlockAssemblyToProcessTx(t, fundTx.TxID())
	td.MineAndWait(t, 1)

	// Spend it back out using the GENERATED key. This is the step the
	// funding-shim prerequisite existed for: signing an input whose locking
	// script belongs to a key the daemon does not own.
	spendTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(fundTx, 0, recipientKey),
		transactions.WithP2PKHOutputs(1, 9e8),
	)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, spendTx),
		"a transaction signed with the generated key should be accepted - if this "+
			"fails, per-input keyed signing does not work and funding-shim is a real "+
			"prerequisite after all")
	td.WaitForBlockAssemblyToProcessTx(t, spendTx.TxID())

	spendHash, err := chainhash.NewHashFromStr(spendTx.TxID())
	require.NoError(t, err, "parse the spending transaction's hash")

	peer.Send(t, getDataFor(wire.InvTypeTx, spendHash))

	gotTx := peer.Wait(t, 30*time.Second, "the requested transaction",
		func(m wire.Message) bool {
			tx, ok := m.(*wire.MsgTx)
			if !ok {
				return false
			}

			h := tx.TxHash()

			return h.IsEqual(spendHash)
		})
	require.Equal(t, spendTx.TxID(), txHashOf(gotTx),
		"the transaction returned should be the one requested")

	// Upstream: assert_equal(len(receivedTxs), 1). Only one transaction was ever
	// requested, so only one should have arrived.
	require.Equal(t, 1, peer.Count(wire.CmdTx),
		"exactly one transaction should have been received")
}

// unknownHash is upstream's 0xdecaf - a hash no block or transaction has.
func unknownHash(t *testing.T) *chainhash.Hash {
	t.Helper()

	var h chainhash.Hash
	h[0] = 0xaf
	h[1] = 0xec
	h[2] = 0x0d

	return &h
}

// getDataFor builds a single-vector getdata, upstream's
// msg_getdata([CInv(type, hash)]).
func getDataFor(invType wire.InvType, hash *chainhash.Hash) *wire.MsgGetData {
	msg := wire.NewMsgGetData()
	_ = msg.AddInvVect(wire.NewInvVect(invType, hash))

	return msg
}

// blockHashOf and txHashOf exist because go-wire returns hashes by value, so the
// String method cannot be chained off the call directly.
func blockHashOf(m wire.Message) string {
	h := m.(*wire.MsgBlock).BlockHash()

	return h.String()
}

func txHashOf(m wire.Message) string {
	h := m.(*wire.MsgTx).TxHash()

	return h.String()
}
