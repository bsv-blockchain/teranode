package blockvalidation

import "github.com/bsv-blockchain/teranode/model"

// coinbaseNotDelivered reports whether the serving peer failed to hand over a coinbase the
// block's miner can be held to. Such a block is ERR_BLOCK_INCOMPLETE: fetch it again, from
// another peer, and never condemn the hash for it.
//
// A missing coinbase does not arrive as nil. model.Block.Bytes writes an empty transaction in
// place of a nil coinbase, the decoder always builds a non-nil one, and the seeder stores an
// output-only coinbase (no input) when it cannot recover the real one. So a peer that lacks the
// coinbase serves a transaction with no inputs, the same shape as a miner who committed a
// coinbase with no inputs. Shape cannot tell the two apart; binding can.
//
// A coinbase-only body whose header merkle root is this coinbase's txid is the miner's own
// committed body, so the coinbase was delivered and a missing input is consensus invalidity
// (SV Node: bad-txns-vin-empty). That body falls through to the coinbase shape checks
// behind the binding, which return ERR_BLOCK_INVALID. A stand-in coinbase from a peer that
// lacks the data has the wrong txid and cannot bind, so it stays incomplete.
//
// A body carrying subtrees cannot be bound this early: that needs the subtrees loaded. A
// zero-input coinbase on such a body stays incomplete, as before.
func coinbaseNotDelivered(block *model.Block) bool {
	if block.CoinbaseTx == nil {
		return true
	}

	if len(block.CoinbaseTx.Inputs) > 0 {
		return false
	}

	return len(block.Subtrees) != 0 || block.CheckCoinbaseOnlyBodyBound() != nil
}
