package rpc

import (
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
)

// This file is the boundary between Teranode's internal error chains and the
// JSON-RPC surface. Two things cross that boundary and neither should be the
// raw internal error.
//
// # The code is a contract
//
// Callers branch on it, so "no such block" and "that block is bad" must not
// share one. Handlers used to return ErrRPCVerify (-25, "rejected on its
// merits") for every failure including a missing block, which bitcoind
// reports as ErrRPCInvalidAddressOrKey (-5). See bitcoin-sv/teranode issue
// 4778.
//
// Reclassification is a property of the CALL SITE, never of this helper.
// (*errors.Error).Is matches by code anywhere in the chain, so a not-found
// buried under an unrelated failure is indistinguishable from one the request
// actually asked for. A blanket mapping therefore answers "no such
// transaction" to a sendrawtransaction whose inputs are merely missing, and
// "no such block" to a getbestblockhash that names no block at all. Each site
// declares what kind of request it is:
//
//	rpcError       - no reclassification, the site's code stands. The default.
//	rpcLookupError - the request NAMES an object; a not-found is that object.
//	rpcSubmitError - the request SUBMITS a transaction; a not-found is one of
//	                 its inputs, which is a rejection and not a lookup miss.
//
// # The message is a disclosure
//
// err.Error() renders the whole wrapped chain: internal service and method
// names, Teranode error codes, gRPC dial targets, and the storage layer's own
// text ("sql: no rows in result set").
//
// Which part of that chain is safe to show is decided by the error's CLASS,
// not by its position. Teranode wraps breadcrumb-outward: the service and
// method context is at the TOP and the reason is at the BOTTOM. Taking the
// topmost message therefore keeps precisely the internals and drops precisely
// the reason. On the validator path it puts "[Validate][<txid>] error
// validating transaction" on the wire and discards "bad-txns-inputs-duplicate"
// - the string the caller needs, and the one upstream's functional tests
// match on.

// maxMessageChainDepth bounds the walk in deepestMessage. A mass spend failure
// on a high-input-count transaction can produce a chain tens of thousands of
// links deep (see the note on (*errors.Error).Is), and an unbounded walk over
// one on the RPC path would be a denial-of-service vector.
const maxMessageChainDepth = 32

// The bitcoind-compatible texts for the classes we translate. Matching
// bitcoind matters because callers port their error handling across
// implementations.
const (
	blockNotFoundMessage = "Block not found"
	txNotFoundMessage    = "No such mempool or blockchain transaction"
	missingInputsMessage = "Missing inputs"

	// genericErrorMessage is what a caller gets when the failure is the
	// node's own and nothing in the chain is safe to show. The code carries
	// the classification; the chain goes to the log.
	genericErrorMessage = "internal error"
)

// rejectionClasses are the classes where the failure is the caller's own
// submission being refused, and the reason is a closed-vocabulary string the
// caller is expected to read and match on: GoBDK's "bad-txns-*" and
// "mandatory-script-verify-flag-failed", the block-level "bad-blk-txns-*".
// For these, and only these, the wire message is taken from the BOTTOM of the
// chain, because that is where such a string lives.
var rejectionClasses = []*errors.Error{
	errors.ErrTxInvalid,
	errors.ErrTxPolicy,
	errors.ErrTxConflicting,
	errors.ErrTxInvalidDoubleSpend,
	errors.ErrTxLocked,
	errors.ErrTxCoinbaseImmature,
	errors.ErrTxLockTime,
	errors.ErrFrozen,
	errors.ErrSpent,
	errors.ErrNonFinal,
	errors.ErrBlockInvalid,
}

// isRejection reports whether err is a refusal of what the caller submitted,
// as opposed to a fault in the node. Chain-wide: a verdict wrapped in a service
// breadcrumb is still a verdict.
func isRejection(err error) bool {
	for _, class := range rejectionClasses {
		if errors.Is(err, class) {
			return true
		}
	}

	return false
}

// linkIsRejection reports whether THIS link is itself a verdict, testing its own
// code rather than the chain beneath it.
//
// The distinction matters and cost a round to find: (*errors.Error).Is matches by
// code anywhere in the chain, so isRejection answers true for the service
// breadcrumb wrapping a verdict as readily as for the verdict itself. Using it to
// pick where the reason starts therefore started at the outermost link every time
// and put "error validating transaction" on the wire.
func linkIsRejection(e *errors.Error) bool {
	if e == nil {
		return false
	}

	for _, class := range rejectionClasses {
		if e.Code() == class.Code() {
			return true
		}
	}

	return false
}

// maxRejectionReasonParts bounds how many links of a rejection chain are joined
// into the wire message, so a deep chain cannot turn into an unbounded string.
const maxRejectionReasonParts = 3

// rejectionReason builds the wire message for a rejection class by joining the
// meaningful messages from the outermost rejection-class link downward.
//
// Neither end of the chain is reliably "the reason", which an earlier version of
// this file got wrong in one direction by always taking the deepest.
//
//   - On the GoBDK path the outer links are fixed strings and the reason is at
//     the bottom: mapBDKValidationError puts the constant "GoBDK fail to
//     ValidateTransaction" on top of the real BDK error at every one of its
//     returns, and validateInternal wraps that again with its own breadcrumb.
//   - On the finality path it is the other way round. Validator.go builds
//     NewUtxoNonFinalError("[Validate][%s] transaction is not final", txID, err),
//     so the CATEGORY that callers match on is the outer message and the DETAIL
//     ("lock time (133) ... is not less than block height (12)") is beneath it.
//     Taking the deepest here dropped "transaction is not final" and broke a
//     smoke test that asserts on it.
//
// So take both and let the caller read them. The walk starts at the outermost
// link that is itself a rejection class, which skips the service breadcrumbs
// above it without needing to recognise them by shape.
func rejectionReason(e *errors.Error) string {
	parts := make([]string, 0, maxRejectionReasonParts)
	seen := make(map[string]struct{}, maxRejectionReasonParts)

	add := func(msg string) bool {
		msg = stripBreadcrumb(msg)
		if msg == "" {
			return true
		}

		if _, dup := seen[msg]; dup {
			return true
		}

		seen[msg] = struct{}{}
		parts = append(parts, msg)

		return len(parts) < maxRejectionReasonParts
	}

	started := false

	for cur, depth := e, 0; cur != nil && depth < maxMessageChainDepth; depth++ {
		// Skip the breadcrumb links above the verdict. Once inside, every
		// remaining link is part of the reason.
		if !started && linkIsRejection(cur) {
			started = true
		}

		if started && !add(cur.Message()) {
			break
		}

		wrapped := cur.WrappedErr()
		if wrapped == nil {
			break
		}

		next, ok := wrapped.(*errors.Error)
		if !ok {
			// The tail of a rejection chain is a foreign error the validator was
			// handed, whose text is the closed-vocabulary reason itself.
			if started {
				add(wrapped.Error())
			}

			break
		}

		cur = next
	}

	return strings.Join(parts, ": ")
}

// stripBreadcrumb removes the leading "[Service][arg]" markers Teranode
// prefixes to its error text. They name internal methods, they can carry a
// whole rendered block (NewBlockInvalidError formats block.String() into one),
// and they are never part of the reason.
func stripBreadcrumb(msg string) string {
	for strings.HasPrefix(msg, "[") {
		end := strings.IndexByte(msg, ']')
		if end < 0 {
			break
		}

		msg = strings.TrimLeft(msg[end+1:], " ")
	}

	return msg
}

// sanitised trims the breadcrumbs off a message that has been judged safe to
// show. A message that is nothing but breadcrumbs carries no reason, so it is
// replaced rather than emitted empty.
func sanitised(msg string) string {
	if stripped := stripBreadcrumb(msg); stripped != "" {
		return stripped
	}

	return genericErrorMessage
}

// publicErrorMessage returns the part of err that is both safe and useful to
// put on the wire. Selection is by error class; see the note at the top of
// this file for why position in the chain is not a usable proxy.
//
// Returns "" only when err is nil, which callers must not pass.
func publicErrorMessage(err error) string {
	if err == nil {
		return ""
	}

	var tErr *errors.Error
	if !errors.As(err, &tErr) {
		// Not a Teranode error: no chain to strip and no breadcrumb
		// convention to apply. bsvutil.DecodeAddress and bt.NewTxFromBytes
		// both land here, and their text is the reason.
		return err.Error()
	}

	switch {
	case isRejection(tErr):
		return sanitised(rejectionReason(tErr))

	case errors.Is(tErr, errors.ErrInvalidArgument):
		// The caller's own input was wrong. The topmost message says how, and
		// there is no deeper cause to look for.
		return sanitised(tErr.Message())
	}

	// A fault in the node. None of it is the caller's business, and this is
	// also where a detail-less gRPC status lands: UnwrapGRPC synthesises an
	// *Error whose Message() is the rendered "rpc error: code = Unavailable
	// desc = ... dial tcp <host:port>" text, which would otherwise put
	// internal service addressing on the wire.
	return genericErrorMessage
}

// rpcRequestScope is what the call site knows and this file cannot: what the
// request was asking for, and therefore what a not-found in the chain means.
type rpcRequestScope int

const (
	// scopeOpaque makes no claim about the request, so no reclassification is
	// safe.
	scopeOpaque rpcRequestScope = iota

	// scopeLookup is a request that names an object to fetch or act on.
	scopeLookup

	// scopeSubmit is a request that submits a transaction for acceptance.
	scopeSubmit
)

// buildRPCError is the pure half of the boundary: no logging, no server.
func buildRPCError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string, scope rpcRequestScope) *bsvjson.RPCError {
	switch scope {
	case scopeLookup:
		// The request named an object, so a not-found is about that object.
		// bitcoind's code and text are returned verbatim and WITHOUT the
		// site's prefix, so a caller porting from bitcoind matches the
		// string exactly.
		//
		// Dropping the prefix is safe here and only here: on a correctly
		// scoped lookup site the mapped branch means "the object you named
		// is missing", so the prefix can only be restating the operation.
		// It is not safe in general - see the warning on rpcLookupError.
		if errors.Is(err, errors.ErrBlockNotFound) {
			return &bsvjson.RPCError{Code: bsvjson.ErrRPCInvalidAddressOrKey, Message: blockNotFoundMessage}
		}

		if errors.Is(err, errors.ErrTxNotFound) {
			return &bsvjson.RPCError{Code: bsvjson.ErrRPCInvalidAddressOrKey, Message: txNotFoundMessage}
		}

	case scopeSubmit:
		// The request submitted a transaction, so a not-found is a missing
		// input. bitcoind and svnode both report that as a rejection
		// (-25 "Missing inputs"), never as -5: the transaction the caller
		// asked about is right there, it is a parent that is absent.
		// Answering -5 tells a wallet "does not exist, do not retry" about a
		// transaction that is merely early.
		if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrTxMissingParent) {
			return &bsvjson.RPCError{Code: fallbackCode, Message: prefix + missingInputsMessage}
		}

	case scopeOpaque:
	}

	return &bsvjson.RPCError{Code: fallbackCode, Message: prefix + publicErrorMessage(err)}
}

// isCallerFault reports whether err describes something wrong with the request
// rather than with the node. It selects the log level: a caller fault keeps its
// reason on the wire, so nothing is lost by logging it at debug, and
// sendrawtransaction rejections are routine enough that logging them louder
// would be a noise problem of its own.
func isCallerFault(err error, scope rpcRequestScope) bool {
	if isRejection(err) || errors.Is(err, errors.ErrInvalidArgument) {
		return true
	}

	switch scope {
	case scopeLookup:
		return errors.Is(err, errors.ErrBlockNotFound) || errors.Is(err, errors.ErrTxNotFound)
	case scopeSubmit:
		return errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrTxMissingParent)
	case scopeOpaque:
	}

	return false
}

// logAndBuild records the full chain and returns the trimmed, classified form.
//
// The logging lives here rather than at the call sites because trimming the
// wire message is only defensible if the chain is recorded somewhere, and four
// of the ten original sites returned without logging - a blob store failing
// with AccessDenied reached neither the caller nor the log.
func (s *RPCServer) logAndBuild(err error, fallbackCode bsvjson.RPCErrorCode, prefix string, scope rpcRequestScope) *bsvjson.RPCError {
	rpcErr := buildRPCError(err, fallbackCode, prefix, scope)

	if s != nil && s.logger != nil {
		if isCallerFault(err, scope) {
			s.logger.Debugf("[rpc] returning %d to caller: %v", rpcErr.Code, err)
		} else {
			s.logger.Errorf("[rpc] returning %d to caller: %v", rpcErr.Code, err)
		}
	}

	return rpcErr
}

// rpcError converts an internal error into the RPC error a caller should see.
// The message is trimmed to what is safe and useful; the code is the site's
// own and is not reclassified.
//
// prefix is the site's existing caller-facing context (e.g. "TX rejected: ")
// and is always preserved.
func (s *RPCServer) rpcError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeOpaque)
}

// rpcLookupError is rpcError for a request that names an object to look up. A
// not-found anywhere in the chain is reported with bitcoind's code and text.
//
// Use it only where the request genuinely names the object. "The block you
// asked for does not exist" must not be the answer to a request that named no
// block (getbestblockhash), nor to one whose named block was found and whose
// DESCENDANT was not (the reconsiderblock children branch). Both of those keep
// rpcError.
//
// prefix applies to the fallback branch only; the mapped branch returns
// bitcoind's text alone. Do not pass a prefix carrying a fact the caller needs
// in order to tell the two branches apart - if the site has one, it is not a
// lookup site.
func (s *RPCServer) rpcLookupError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeLookup)
}

// rpcSubmitError is rpcError for a request that submits a transaction. A
// not-found in the chain is one of the transaction's inputs, and is reported
// as bitcoind reports it: the site's rejection code with "Missing inputs".
func (s *RPCServer) rpcSubmitError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeSubmit)
}
