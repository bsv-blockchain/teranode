package bridge

import (
	"context"
	"fmt"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// legacyBlockWirePath is the asset service route that streams a block in
// legacy wire format, ported from legacy pushBlockMsg's URL construction
// (services/legacy/peer_server.go:2082, the "%s/block_legacy/%s?wire=1" URL).
const legacyBlockWirePath = "block_legacy"

// minLegacyBlockWireBytes is a generous floor on FetchBlock's declared
// length — it exists only to catch an implausible value (0 included) before
// it is written into a wire "block" message header (Task 10), where it would
// silently corrupt the frame and disconnect every peer reading it. It is not
// a consensus rule: 80 (header) + 1 (the smallest possible tx-count varint) +
// 60 (the smallest a syntactically valid coinbase transaction can serialize
// to — version(4) + input count(1) + prevout hash(32) + prevout index(4) +
// empty unlocking script(1) + sequence(4) + output count(1) + satoshis(8) +
// empty locking script(1) + locktime(4) = 60).
const minLegacyBlockWireBytes = 80 + 1 + 60

// FetchBlock streams a block's legacy-wire bytes (header(80) +
// varint(txCount) + transactions, coinbase first) from the asset service,
// and reports the declared length the caller (Task 10) writes into the wire
// "block" message header before streaming the body.
//
// Ported from legacy pushBlockMsg (services/legacy/peer_server.go:2075)
// without the full-materialization step: legacy reads the whole response
// into memory via NewRawBlockMessage (services/legacy/raw_block_message.go:27,
// io.ReadAll) before it ever sends a byte; this returns the live HTTP body
// reader instead, so the caller streams it.
//
// The declared length does NOT come from the HTTP response's Content-Length.
// block_legacy is one of the asset service's streaming routes
// (services/asset/httpimpl/stream.go:83), and only the routes that answer
// with a materialized slice via c.Blob carry a Content-Length
// (services/asset/httpimpl/stream.go:88) — this one does not, so
// resp.ContentLength is -1 on this path. The length instead comes from the
// blockchain service's own record of the block:
// blockchainClient.GetBlockHeader's BlockHeaderMeta.SizeInBytes
// (model/BlockHeaderMeta.go:15). That field is written from
// model.Block.SizeInBytes (model/Block.go:1708:
// sum(subtree.SizeInBytes) + 80 + varint(txCount) + coinbase.Size()), which
// is exactly the byte count the block_legacy?wire=1 route emits
// (services/asset/repository/GetLegacyBlock.go writeLegacyBlockHeader) —
// proven against a real, recomputed block through a real store and the real
// GetLegacyBlockReader streaming code in
// TestFetchBlock_A1_RealMainnetBlock_RecomputedSizeMatchesWireBytes and
// TestFetchBlock_A1_CoinbaseOnlyBlock_SizeMatchesWireBytes (fetch_test.go).
//
// A block the blockchain service does not know about fails here, before any
// HTTP request is made, with whatever GetBlockHeader returns (a
// BlockNotFoundError, matched by errors.ErrBlockNotFound, from the SQL
// blockchain store). A block it knows about but the asset service answers 404
// for is folded into that SAME error, deliberately: the two causes are
// different — the blockchain miss means "we do not know this block at all",
// the asset 404 means "we know it but do not retain its body" — and the answer
// to the peer is identical, so one code covers both and the caller needs no
// second branch. Every other asset-service status keeps its own type, because
// the caller distinguishes absence from failure on ErrBlockNotFound alone (see
// the classification note at the HTTP call below).
//
// The returned stream is single-use, but this method is NOT: Task 10 calls it
// TWICE for every block it serves — once to hash the payload for the wire
// message checksum, once to stream it to the socket — because SVNode
// ban-scores a wrong checksum (net_processing.cpp:5005-5015) and the payload
// must never be materialized to compute one. Anything added here must stay
// safe and cheap to repeat, and must keep reporting the same declared length
// for the same block.
func (b *svp2pBridge) FetchBlock(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, uint64, error) {
	_, meta, err := b.blockchainClient.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, 0, err
	}

	// A declared length below what any real block_legacy?wire=1 body can be
	// (0 included) means the stored record is corrupt or the wrong field —
	// fail here, before the HTTP call, rather than hand Task 10 a number
	// that corrupts the wire frame it writes.
	if meta.SizeInBytes < minLegacyBlockWireBytes {
		return nil, 0, errors.NewBlockInvalidError("block %s has an implausible declared size of %d bytes (minimum possible is %d)",
			hash.String(), meta.SizeInBytes, minLegacyBlockWireBytes)
	}

	url := fmt.Sprintf("%s/%s/%s?wire=1", b.settings.Asset.HTTPAddress, legacyBlockWirePath, hash.String())

	body, err := util.DoTrustedHTTPRequestBodyReader(ctx, url)
	if err != nil {
		// The asset service answering 404 for a block the blockchain service
		// knows about means the body is not retained here. util's HTTP helper
		// already classifies by status (util/http.go:466-473 buildHTTPError:
		// 404 -> ErrNotFound, 503 -> ErrServiceUnavailable, everything else ->
		// ServiceError), so only the code needs narrowing: the caller answers
		// a peer notfound on ErrBlockNotFound alone, and folding a 500 into
		// that would tell the peer to stop asking for a block we do hold.
		if errors.Is(err, errors.ErrNotFound) {
			return nil, 0, errors.NewBlockNotFoundError("block %s is not served by the asset service", hash.String(), err)
		}

		return nil, 0, err
	}

	return body, meta.SizeInBytes, nil
}

// FetchTx returns a transaction's serialized bytes from the UTXO store,
// fetching only fields.Tx — the projection legacy's getTxFromStore/pushTxMsg
// ask for (services/legacy/peer_server.go:1993,
// s.utxoStore.Get(ctx, hash, fields.Tx)) — since that is all a caller
// forwarding raw tx bytes to a peer needs.
//
// A missing hash returns whatever the store's Get returns for it (a
// TxNotFoundError, matched by errors.ErrTxNotFound, from the SQL UTXO
// store).
//
// A row that exists but is not retained in full is folded into the same
// typed not-found error. Legacy's pushTxMsg has two branches for this
// (services/legacy/peer_server.go:1976):
//
//   - txMeta == nil || txMeta.Tx == nil (:1998-2003) returns a nil error,
//     which sends the connected peer nothing at all — silence,
//     indistinguishable from a dropped message. That is a latent bug, not a
//     behaviour worth porting: Task 8 already ruled the same shape the other
//     way (Phase 3 execution ledger, Decision 5), keeping SVNode's answer
//     over legacy's silence. Here it becomes errors.ErrTxNotFound instead,
//     so Task 10's getdata handler can answer notfound — matching SVNode's
//     ProcessGetData / vNotFound push.
//   - !txMeta.TxIsSerializable() || a txid mismatch (:2015-2020, the
//     snapshot-bootstrapped-node guard) already returns a real error in
//     legacy; the not-found return is carried forward here for the same
//     reason as the branch above (the peer still gets notfound either way),
//     but unlike the branch above this one is not mere absence — a txid
//     mismatch means the store itself handed back bytes for the wrong
//     transaction, a store-integrity fault. That is logged (not folded away)
//     so an operator sees it even though the peer answer doesn't change.
func (b *svp2pBridge) FetchTx(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	txMeta, err := b.utxoStore.Get(ctx, hash, fields.Tx)
	if err != nil {
		return nil, err
	}

	if txMeta == nil || txMeta.Tx == nil || !txMeta.TxIsSerializable() {
		return nil, errors.NewTxNotFoundError("tx %s is not retained in full by this node", hash.String())
	}

	if gotHash := txMeta.Tx.TxIDChainHash(); !gotHash.IsEqual(hash) {
		b.logger.Warnf("[FetchTx] store returned a transaction whose txid (%s) does not match the requested hash (%s) — store-integrity fault, answering notfound",
			gotHash.String(), hash.String())

		return nil, errors.NewTxNotFoundError("tx %s is not retained in full by this node", hash.String())
	}

	return txMeta.Tx.Bytes(), nil
}
