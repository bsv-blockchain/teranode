package rpc

import (
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
)

// This file is the boundary between Teranode's internal error chains and the
// JSON-RPC surface. Two things cross that boundary and neither should be the
// raw internal error:
//
//   - The CODE is a contract. Callers branch on it, so "no such block" and
//     "that block is bad" must not share one. Handlers used to return
//     ErrRPCVerify (-25, "rejected on its merits") for every failure including
//     a missing block, which bitcoind reports as ErrRPCInvalidAddressOrKey
//     (-5). See bitcoin-sv/teranode issue 4778.
//
//   - The MESSAGE is a disclosure. err.Error() renders the whole wrapped
//     chain — internal service and method names, error codes, and the storage
//     layer's own text ("sql: no rows in result set") — across a documented
//     API boundary. Every handler already logs the full chain before
//     returning, so nothing diagnostic is lost by trimming what goes on the
//     wire.
//
// The message is trimmed rather than replaced. (*errors.Error).Message()
// returns only the topmost error's own text, which is where the caller-facing
// reason lives ("script execution failed for input 0"), while the wrapped
// remainder — which is where the internals live — is dropped.

// notFoundMessages are the bitcoind-compatible texts for the not-found classes
// we translate. Matching bitcoind matters because callers port their error
// handling across implementations.
const (
	blockNotFoundMessage = "Block not found"
	txNotFoundMessage    = "No such mempool or blockchain transaction"
)

// publicErrorMessage returns the part of err that is safe and useful to put on
// the wire: the topmost error's own message, without the wrapped chain behind
// it. A non-Teranode error has no chain to strip, so its text is used as-is.
//
// Returns "" only when err is nil, which callers must not pass.
func publicErrorMessage(err error) string {
	if err == nil {
		return ""
	}

	var tErr *errors.Error
	if errors.As(err, &tErr) {
		if msg := tErr.Message(); msg != "" {
			return msg
		}
	}

	return err.Error()
}

// rpcError converts an internal error into the RPC error a caller should see.
//
// Not-found classes are mapped to bitcoind's code and text regardless of how
// they were wrapped: errors.Is matches by code through the whole chain, so a
// BLOCK_NOT_FOUND buried under a SERVICE_ERROR is still recognised as one.
// Everything else keeps the calling site's own code — only the message is
// trimmed — because this helper is a boundary, not a re-classification of
// every failure mode in the node.
//
// prefix is the site's existing caller-facing context (e.g. "TX rejected: ").
// It is applied only to the fallback branch; a mapped not-found message is
// returned verbatim so it matches bitcoind exactly.
func rpcError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	switch {
	case errors.Is(err, errors.ErrBlockNotFound):
		return &bsvjson.RPCError{Code: bsvjson.ErrRPCInvalidAddressOrKey, Message: blockNotFoundMessage}
	case errors.Is(err, errors.ErrTxNotFound):
		return &bsvjson.RPCError{Code: bsvjson.ErrRPCInvalidAddressOrKey, Message: txNotFoundMessage}
	}

	return &bsvjson.RPCError{Code: fallbackCode, Message: prefix + publicErrorMessage(err)}
}
