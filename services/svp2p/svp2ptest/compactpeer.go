package svp2ptest

import (
	"sort"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// CompactBlockFor builds the compact block a peer would announce for block:
// the block's header, the nonce that keys the short IDs, a prefilled
// transaction for every index in prefilled, and a short ID for every other
// slot in block order.
//
// prefilled is normalised, not policed: it is sorted and de-duplicated,
// because the wire form stores each prefilled index as the difference from
// the previous one and go-wire refuses a non-increasing sequence
// (MsgCmpctBlock.AddPrefilledTransaction). An index outside the block is an
// error.
//
// Nothing here forces slot 0 into prefilled, though BIP152 requires a sender
// to prefill the coinbase. A harness must be able to script the
// non-compliant announcement as well as the compliant one, so the caller
// decides.
func (p *ScriptedPeer) CompactBlockFor(block *wire.MsgBlock, nonce uint64, prefilled []int) (*wire.MsgCmpctBlock, error) {
	if block == nil || len(block.Transactions) == 0 {
		return nil, errors.NewProcessingError("svp2ptest: cannot announce a block with no transactions")
	}

	header := block.Header

	msg := wire.NewMsgCmpctBlock(&header, nonce)

	isPrefilled := make(map[int]struct{}, len(prefilled))

	for _, index := range prefilled {
		if index < 0 || index >= len(block.Transactions) {
			return nil, errors.NewProcessingError("svp2ptest: prefilled index %d is outside the block's %d transactions",
				index, len(block.Transactions))
		}

		isPrefilled[index] = struct{}{}
	}

	ordered := make([]int, 0, len(isPrefilled))
	for index := range isPrefilled {
		ordered = append(ordered, index)
	}

	sort.Ints(ordered)

	for _, index := range ordered {
		if err := msg.AddPrefilledTransaction(uint32(index), block.Transactions[index]); err != nil { //nolint:gosec // bounded by the block's transaction count
			return nil, errors.NewProcessingError("svp2ptest: cannot prefill index %d", index, err)
		}
	}

	k0, k1 := shortIDKeys(&header, nonce)

	for index, tx := range block.Transactions {
		if _, skip := isPrefilled[index]; skip {
			continue
		}

		msg.ShortIDs = append(msg.ShortIDs, shortID(k0, k1, tx.TxHash()))
	}

	return msg, nil
}

// AnnounceCompact builds the compact block for block and writes it on every
// GENERAL connection, which is Send's own rule and for Send's own reason: a
// DATA1 stream is one more socket of the SAME association, and announcing on
// both would deliver the announcement to the node twice.
//
// It returns the build error when the announcement cannot be made, and the
// first write error otherwise, so a scenario knows whether the node was told.
//
// Having NO general connection is an error, not a quiet success. A scenario
// that announces before the node has connected would otherwise pass here and
// then block waiting for a getblocktxn that no peer was ever told to expect.
// This is where AnnounceCompact parts company with Send, which is free to
// write to nobody: Send's callers do not ask whether anyone heard.
func (p *ScriptedPeer) AnnounceCompact(block *wire.MsgBlock, nonce uint64, prefilled []int) error {
	msg, err := p.CompactBlockFor(block, nonce, prefilled)
	if err != nil {
		return err
	}

	conns := p.generalConns()
	if len(conns) == 0 {
		return errors.NewProcessingError("svp2ptest: cannot announce compact block %s: no general connection",
			msg.Header.BlockHash())
	}

	var firstErr error

	for _, conn := range conns {
		if writeErr := p.write(conn, msg); writeErr != nil && firstErr == nil {
			firstErr = writeErr
		}
	}

	return firstErr
}

// BlockTxnFor is the honest getblocktxn answer: the transactions at exactly
// the requested indexes, in exactly the requested order, taken from the
// fixture chain.
//
// It answers nil for a block this peer does not hold, and nil when any index
// is outside the block. Both are the all-or-nothing reading of BIP152, where
// blocktxn is positional: a reply that silently drops a slot would be
// indistinguishable on the wire from a reply to a shorter request. A scenario
// that wants a short or malformed reply scripts one with Script.OnGetBlockTxn.
func (p *ScriptedPeer) BlockTxnFor(msg *wire.MsgGetBlockTxn) *wire.MsgBlockTxn {
	block, known := p.Chain.Block(msg.BlockHash)
	if !known {
		return nil
	}

	out := wire.NewMsgBlockTxn(&msg.BlockHash)

	for _, index := range msg.Indexes {
		if index >= uint32(len(block.Transactions)) { //nolint:gosec // a fixture block holds far fewer than MaxUint32 transactions
			return nil
		}

		_ = out.AddTransaction(block.Transactions[index])
	}

	return out
}
