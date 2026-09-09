package rpc

import (
	"regexp"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/services/validator"
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

// maxMessageChainDepth bounds both chain walks in this file, isRejection and
// rejectionReason. They must share it: see TestRejectionDepthCapsAgree. A mass spend failure
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

// localRejectionClasses are the codes this surface treats as verdicts that the
// shared public-cause allowlist does not carry.
//
// Exactly one, and it needs recording rather than merely allowing: the shared
// list is scoped to transaction verdicts, and reconsiderblock needs the
// block-level reject reason. It is admin-only - absent from rpcLimited, enforced
// at Server.go's authorisation check - so widening the surface here does not
// widen it for an unprivileged caller.
var localRejectionClasses = []*errors.Error{
	errors.ErrBlockInvalid,
}

// localRejectionCodes is localRejectionClasses indexed by code.
var localRejectionCodes = func() map[errors.ERR]struct{} {
	m := make(map[errors.ERR]struct{}, len(localRejectionClasses))
	for _, class := range localRejectionClasses {
		m[class.Code()] = struct{}{}
	}

	return m
}()

// isRejection reports whether err is a refusal of what the caller submitted, as
// opposed to a fault in the node. Chain-wide: a verdict wrapped in a service
// breadcrumb is still a verdict.
//
// One walk, not one per class. The obvious spelling is errors.Is once per entry
// in rejectionClasses, but each of those walks the whole chain, so an eleven-entry
// list costs eleven traversals - and this is called twice per error, from
// publicErrorMessage and from isCallerFault. Walking once and testing each link
// against a code set is the same answer for a fraction of the work.
func isRejection(err error) bool {
	var tErr *errors.Error
	if !errors.As(err, &tErr) {
		return false
	}

	for cur, depth := tErr, 0; cur != nil && depth < maxMessageChainDepth; depth++ {
		if linkIsRejection(cur) {
			return true
		}

		wrapped := cur.WrappedErr()
		if wrapped == nil {
			return false
		}

		next, ok := wrapped.(*errors.Error)
		if !ok {
			if !errors.As(wrapped, &next) {
				return false
			}
		}

		cur = next
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

	// The shared allowlist is the authority; see localRejectionClasses for the one
	// code this surface adds and why.
	if errors.IsPublicCause(e.Code()) {
		return true
	}

	_, ok := localRejectionCodes[e.Code()]

	return ok
}

// maxRejectionReasonParts bounds how many links of a rejection chain are joined
// into the wire message, so a deep chain cannot turn into an unbounded string.
const maxRejectionReasonParts = 4

// uninformativeVerdictText are wrapper strings that carry no verdict of their own.
//
// mapBDKValidationError returns errMsgInvalidTx over errMsgPolicy over the real
// reason on every policy rejection, so joining blindly produced
// "GoBDK fail to ValidateTransaction: GoBDK fail to ValidateTransaction by policy
// settings: <reason>" - two of three parts naming an internal library and neither
// saying anything about the transaction. The code already carries what they say.
//
// Dropping them also restores headroom: with the part cap at the exact depth of
// the BDK policy chain, one more wrap anywhere above the verdict would have
// pushed the reason off the end silently, which is the failure ChiR6 described
// relocated from the depth cap to the part cap.
// Referencing the constants rather than copying their text means a RENAME is a
// compile error. A REWORD is not - this map keys on the value - so the reword
// case is covered by TestRPCError_TrimsTheRealValidatorChain, which builds its
// input from the same constants and fails if the suppression stops matching.
var uninformativeVerdictText = map[string]struct{}{
	validator.ErrMsgInvalidTx: {},
	validator.ErrMsgPolicy:    {},
}

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
		msg = stripInternalPrefixes(msg)
		if msg == "" {
			return true
		}

		if _, noise := uninformativeVerdictText[msg]; noise {
			return true
		}

		if _, dup := seen[msg]; dup {
			return true
		}

		seen[msg] = struct{}{}
		parts = append(parts, msg)

		return len(parts) < maxRejectionReasonParts
	}

	for cur, depth := e, 0; cur != nil && depth < maxMessageChainDepth; depth++ {
		// EVERY link is class-checked, not just the first one found.
		//
		// An earlier version latched a "started" flag at the first allowlisted link
		// and then took every message below it, assuming that once inside a verdict
		// the rest of the chain is the reason. That is false, and it made
		// errors.IsPublicCause govern only the ENTRY to the join: aerospike joins
		// per-input verdicts linearly, so a frozen input ahead of an
		// immature-coinbase one surfaced the coinbase message and with it the
		// store's internal batch id, and a verdict wrapping a StorageError or a
		// NetworkError put an S3 bucket name or an internal host:port on the wire.
		if linkIsRejection(cur) && !add(cur.Message()) {
			break
		}

		wrapped := cur.WrappedErr()
		if wrapped == nil {
			break
		}

		next, ok := wrapped.(*errors.Error)
		if !ok {
			// A foreign link carries no code, so it cannot be class-checked and is
			// therefore not surfaced. Rendering it put the whole remaining subtree
			// on the wire, internal code names included, which is the widest thing
			// this branch could have done. Not currently reachable - New() re-wraps
			// a trailing plain error, and every producer traced builds a Teranode
			// error - but stopping is the safe answer if one ever appears.
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
	// Bounded by the message shrinking, not by a count. An earlier version stopped
	// after two groups on the grounds that Teranode emits at most a service and an
	// argument marker; BlockValidation emits three
	// ("[Block Validation][checkOldBlockIDs][<rendered block>]"), so the third
	// survived and carried the store's internal block id onto the wire.
	//
	// Never strips to empty, and never strips a lone bracketed token: reject
	// reasons cross a CGO boundary from BDK, so a reason that legitimately opens
	// with a bracket must survive rather than be peeled away.
	for strings.HasPrefix(msg, "[") {
		end := strings.IndexByte(msg, ']')
		if end < 0 {
			break
		}

		stripped := strings.TrimLeft(msg[end+1:], " ")
		if stripped == "" {
			break
		}

		msg = stripped
	}

	return msg
}

// codeRenderPrefix matches a message that opens with the errors package's own
// "NAME (code): " rendering.
//
// The join reads Message() precisely because that never contains a code - which
// is what lets the boundary promise no internal enum reaches the wire. One
// producer falsified that by formatting errCodeMsgFmt into its own message, so
// an ordinary double-spend answered "TX rejected: UTXO_SPENT (70): ...". That is
// fixed at the producer, in errors/error_data_utxo_spent.go; this is the guard
// that stops the next one, because a promise the boundary cannot enforce is not
// a promise.
var codeRenderPrefix = regexp.MustCompile(`^[A-Z][A-Z0-9_]* \(\d+\): `)

// stripCodeRender removes a leading code rendering from a producer's message.
func stripCodeRender(msg string) string {
	return codeRenderPrefix.ReplaceAllString(msg, "")
}

// stripInternalPrefixes removes leading breadcrumbs and code renderings until
// neither is left.
//
// A loop, not one pass of each, because either can hide the other and the order
// that fixes one exposes the other. Stripping breadcrumbs first leaves
// "TX_POLICY (39): [Spend][deadbeef:0] utxo is frozen" with its breadcrumb no
// longer leading, so it survives and only the code is removed - putting the
// internal marker on the wire. Stripping codes first has the mirror problem.
// Iterating to a fixed point has neither, and terminates because every
// iteration that changes anything makes the string shorter.
func stripInternalPrefixes(msg string) string {
	for {
		stripped := stripCodeRender(stripBreadcrumb(msg))
		if stripped == msg {
			return msg
		}

		msg = stripped
	}
}

// sanitised trims the breadcrumbs off a message that has been judged safe to
// show. A message that is nothing but breadcrumbs carries no reason, so it is
// replaced rather than emitted empty.
func sanitised(msg string) string {
	stripped := stripInternalPrefixes(msg)
	if stripped == "" || !carriesReason(stripped) {
		return genericErrorMessage
	}

	return stripped
}

// carriesReason reports whether msg has any text outside bracketed groups.
//
// A message that is nothing but markers carries no reason, whatever
// stripBreadcrumb managed to remove from the front of it. Without this,
// sanitised's own doc was false: stripBreadcrumb never returns empty, so a
// wholly bracketed message came back verbatim and the generic substitution the
// comment describes never happened.
func carriesReason(msg string) bool {
	depth := 0

	for _, r := range msg {
		switch {
		case r == '[':
			depth++
		case r == ']':
			if depth > 0 {
				depth--
			}
		case depth == 0 && r != ' ':
			return true
		}
	}

	return false
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
		// The caller's own input was wrong, and the topmost message says how.
		//
		// This pairs a chain-wide match with a topmost-message read, which is the
		// pairing linkIsRejection exists to avoid, and it is only sound while this
		// class is never nested. Every NewInvalidArgumentError in blockchain,
		// blockassembly and blockvalidation is the outermost link today. If a
		// producer ever nests one, this must move to a per-link test or it will
		// read a wrapper's text instead.
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

	// scopeBlockLookup is a request that names a BLOCK to fetch or act on.
	scopeBlockLookup

	// scopeTxLookup is a request that names a TRANSACTION.
	//
	// Split from the block case rather than sharing one lookup scope. A single
	// scope mapped both not-found classes, so reconsiderblock - which names a
	// block and nothing else - answered "No such mempool or blockchain
	// transaction" whenever a revalidation failure happened to carry an
	// ErrTxNotFound underneath it, and discarded its own prefix doing so.
	scopeTxLookup

	// scopeSubmit is a request that submits a transaction for acceptance.
	scopeSubmit
)

// buildRPCError is the pure half of the boundary: no logging, no server.
func buildRPCError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string, scope rpcRequestScope) *bsvjson.RPCError {
	switch scope {
	case scopeBlockLookup:
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

	case scopeTxLookup:
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
	// A block failing revalidation is rare, admin-invoked and operationally
	// significant, so it is not a caller fault for logging purposes even though it
	// is a rejection for message purposes. reconsiderblock is absent from
	// rpcLimited, so it cannot be a noise source - and without this the only
	// record of the failure is a Debugf, which is off at default levels, leaving
	// the trimmed response as the whole story.
	if errors.Is(err, errors.ErrBlockInvalid) {
		return false
	}

	if isRejection(err) || errors.Is(err, errors.ErrInvalidArgument) {
		return true
	}

	switch scope {
	case scopeBlockLookup:
		// Only a genuine missing block. Anything else failing an admin-invoked
		// reconsiderblock or invalidateblock is worth an operator's attention, and
		// treating the whole scope as a caller fault left two of the four realistic
		// revalidation failures with no error-level trace at all.
		return errors.Is(err, errors.ErrBlockNotFound)

	case scopeTxLookup:
		return errors.Is(err, errors.ErrTxNotFound)
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

// rpcLookupError is rpcError for a request that names a BLOCK to look up. A
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
	return s.logAndBuild(err, fallbackCode, prefix, scopeBlockLookup)
}

// rpcTxLookupError is rpcLookupError for a request that names a TRANSACTION.
func (s *RPCServer) rpcTxLookupError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeTxLookup)
}

// rpcSubmitError is rpcError for a request that submits a transaction. A
// not-found in the chain is one of the transaction's inputs, and is reported
// as bitcoind reports it: the site's rejection code with "Missing inputs".
func (s *RPCServer) rpcSubmitError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeSubmit)
}
