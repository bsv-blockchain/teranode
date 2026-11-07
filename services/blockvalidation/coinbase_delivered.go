package blockvalidation

import "github.com/bsv-blockchain/teranode/model"

// coinbaseNotDelivered reports whether the serving peer failed to hand over a coinbase the
// block's miner can be held to. Such a block is ERR_BLOCK_INCOMPLETE: fetch it again, from
// another peer, and never condemn the hash for it.
//
// A missing coinbase does not arrive as nil. The seeder stores a block with a nil coinbase when
// its utxo-headers file carries none, model.Block.Bytes writes an empty transaction in place of a
// nil coinbase, and the decoder always builds a non-nil one. So a peer that lacks the coinbase
// serves a transaction with no inputs, the same shape as a miner who committed a coinbase with no
// inputs. Shape cannot tell the two apart; binding can.
//
// The seeder's block rows carry no subtrees, so a stand-in only ever arrives on a coinbase-only
// body. That is the one shape this precheck binds. A coinbase-only body whose header merkle root
// is this coinbase's txid is the miner's own committed body, so the coinbase was delivered and a
// missing input is consensus invalidity (SV Node: bad-txns-vin-empty). It falls through to the
// coinbase shape checks behind the binding, which return ERR_BLOCK_INVALID. A stand-in has the
// wrong txid and cannot bind, so it stays incomplete. So does a 64-byte one, which
// model.Block.CheckCoinbaseOnlyBodyBound refuses to bind because those bytes may be the two child
// hashes under an honest block's merkle root.
//
// A body that carries subtrees always falls through. CheckCoinbaseOnlyBodyBound is a no-op for it;
// the merkle check over its subtrees binds it later, on every route, and the coinbase shape check
// behind that binding returns ERR_BLOCK_INVALID for a committed coinbase with no inputs and
// ERR_BLOCK_CORRUPT when the header does not commit to it. Nothing between this precheck and that
// binding reads the coinbase's inputs.
func coinbaseNotDelivered(block *model.Block) bool {
	if block.CoinbaseTx == nil {
		return true
	}

	if len(block.CoinbaseTx.Inputs) > 0 {
		return false
	}

	return block.CheckCoinbaseOnlyBodyBound() != nil
}
