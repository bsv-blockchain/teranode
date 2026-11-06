package blockvalidation

import "github.com/bsv-blockchain/teranode/model"

// mutatedBodyTxSize is the serialised size of a transaction whose txid could be an inner merkle
// node rather than a leaf: an inner node is the double SHA-256 of two 32-byte child hashes, so 64
// bytes that parse as a transaction hash to the merkle root of the block they were cut from.
const mutatedBodyTxSize = 64

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
// A coinbase-only body whose header merkle root is this coinbase's txid is the miner's own
// committed body, so the coinbase was delivered and a missing input is consensus invalidity
// (SV Node: bad-txns-vin-empty). That body falls through to the coinbase shape checks
// behind the binding, which return ERR_BLOCK_INVALID. A stand-in coinbase from a peer that
// lacks the data has the wrong txid and cannot bind, so it stays incomplete.
//
// One exception to binding: a 64-byte transaction. The two child hashes under an honest block's
// merkle root, concatenated, are 64 bytes whose double SHA-256 is that root. Those bytes parse as
// a zero-input transaction once a few of their bytes take the right values (three, for the
// one-output form). In a two-transaction block those bytes sit in the coinbase txid, which a
// miner can grind. Served as a coinbase-only body they bind to an honest header, so
// condemning them would condemn a valid block's hash and split this node from the chain. A
// 64-byte zero-input coinbase is therefore treated as not delivered. Bitcoin Core's
// IsBlockMutated likewise treats any 64-byte transaction as a sign of a mutated body, never as
// grounds to condemn the hash.
//
// A body carrying subtrees is not bound here: binding it needs its subtrees fetched and loaded,
// which happens after this precheck. A zero-input coinbase on such a body stays incomplete, as it
// was before this check took binding into account, so a stand-in on it is fetched again rather
// than sent on to subtree binding, where a merkle mismatch is charged to the peer as corrupt. The
// cost is that a miner's committed zero-input coinbase on a body with subtrees is fetched again,
// never condemned.
func coinbaseNotDelivered(block *model.Block) bool {
	if block.CoinbaseTx == nil {
		return true
	}

	if len(block.CoinbaseTx.Inputs) > 0 {
		return false
	}

	if block.CoinbaseTx.Size() == mutatedBodyTxSize {
		return true
	}

	return len(block.Subtrees) != 0 || block.CheckCoinbaseOnlyBodyBound() != nil
}
