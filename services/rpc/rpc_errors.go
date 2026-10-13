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
// reports as ErrRPCInvalidAddressOrKey (-5).
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
//	                 rpcTxLookupError and rpcHeightLookupError name a
//	                 transaction and a height.
//	rpcSubmitError - the request SUBMITS a transaction; a not-found is one of
//	                 its inputs, which is a rejection and not a lookup miss.
//	rpcRevalidationError - the request REVALIDATES a block already resolved;
//	                 no mapping, and a corrupt body is a verdict on that block.
//	rpcInputError  - the site parsed the caller's own argument; a plain
//	                 error's text is the reason.
//	rpcMiningSubmissionError - the request SUBMITS a mining solution;
//	                 blockassembly's own words for refusing it are shown.
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
	heightOutOfRange     = "Block height out of range"
	txNotFoundMessage    = "No such mempool or blockchain transaction"
	missingInputsMessage = "Missing inputs"

	// inWarmupMessage answers a service that is up but still loading. svnode
	// answers its own warm-up with RPC_IN_WARMUP and a progress text; this one is
	// fixed, because the class says only that the state clears, not what is loading.
	inWarmupMessage = "Still loading, try again shortly"

	// genericErrorMessage is what a caller gets when the failure is the
	// node's own and nothing in the chain is safe to show. The code carries
	// the classification; the chain goes to the log.
	genericErrorMessage = "internal error"
)

// localRejectionClasses are the codes every site on this surface treats as
// verdicts that the shared public-cause allowlist does not carry. The shared list
// is scoped to transaction verdicts; these are block-level.
//
// The list applies wherever this boundary runs, limited-tier methods included,
// so each entry is justified for any caller that can reach it:
//
//   - ErrBlockInvalid: a consensus verdict. reconsiderblock (admin) needs it, and
//     submitminingsolution (rpcLimited) reaches it through blockassembly's
//     block.Valid, where its text is the miner's own consensus failure.
//   - ErrBlockPolicyDeclined: a refusal under this node's local policy. In
//     blockvalidation it is the excessiveblocksize limit, which only
//     reconsiderblock consumes, and the text names a limit the operator
//     configured. In blockassembly it is the mining guard against a coinbase
//     that pays to P2SH, reached by submitminingsolution and generatetoaddress,
//     and the text names the submitter's own payout.
//
// ErrBlockCorrupt is deliberately absent; see revalidationRejectionClasses.
var localRejectionClasses = []*errors.Error{
	errors.ErrBlockInvalid,
	errors.ErrBlockPolicyDeclined,
}

// revalidationRejectionClasses are verdicts only the block-revalidation scope
// surfaces.
//
// ErrBlockCorrupt says a body does not bind to its header. On reconsiderblock that
// is a verdict on the named block (or a damaged local copy of it) and the admin
// needs to know which check failed. On submitminingsolution it is not: the
// block is one this node assembled, so a corrupt body is the node's own fault,
// and blockassembly says as much. A global entry would present that fault to a
// limited caller as though it were a verdict on what they sent.
var revalidationRejectionClasses = []*errors.Error{
	errors.ErrBlockCorrupt,
}

// submitRejectionClasses are verdicts only the transaction-submission scope
// surfaces.
//
//   - ErrTxCoinbaseImmature: the submitted transaction spends a coinbase that has
//     not matured, and waiting is what fixes it. The shared allowlist leaves it
//     off because aerospike formats the store's spend-batch counter into the
//     message, so it crosses only as the fixed text in fixedVerdictText, never as
//     its own words. That text is svnode's, which neither store's matches.
var submitRejectionClasses = []*errors.Error{
	errors.ErrTxCoinbaseImmature,
}

// fixedVerdictText is the public text for a verdict class whose message cannot
// cross as written. rejectionReason uses it in place of the message.
// The text is svnode's reject reason for the same check (validation.cpp,
// CheckTxInputs), which callers already match on.
var fixedVerdictText = map[errors.ERR]string{
	errors.ERR_TX_COINBASE_IMMATURE: "bad-txns-premature-spend-of-coinbase",
}

// verdictCodes is a set of codes a site treats as verdicts on top of the shared
// allowlist. Which set applies is a property of the call site; see
// rpcRequestScope.verdicts.
type verdictCodes map[errors.ERR]struct{}

func newVerdictCodes(classes ...*errors.Error) verdictCodes {
	m := make(verdictCodes, len(classes))
	for _, class := range classes {
		m[class.Code()] = struct{}{}
	}

	return m
}

var (
	// localRejectionCodes is localRejectionClasses indexed by code.
	localRejectionCodes = newVerdictCodes(localRejectionClasses...)

	// revalidationRejectionCodes is what the block-revalidation scope surfaces.
	revalidationRejectionCodes = newVerdictCodes(append(append([]*errors.Error{},
		localRejectionClasses...), revalidationRejectionClasses...)...)

	// submitRejectionCodes is what the transaction-submission scope surfaces.
	submitRejectionCodes = newVerdictCodes(append(append([]*errors.Error{},
		localRejectionClasses...), submitRejectionClasses...)...)
)

// isRejection reports whether err is a refusal of what the caller submitted, as
// opposed to a fault in the node. Chain-wide: a verdict wrapped in a service
// breadcrumb is still a verdict.
//
// One walk, not one per class. The obvious spelling is errors.Is once per code on
// the shared allowlist and the site's verdict set, but each of those walks the
// whole chain, so a list of n codes costs n traversals - and this is called twice
// per error, from
// publicErrorMessage and from isCallerFault. Walking once and testing each link
// against a code set is the same answer for a fraction of the work.
func isRejection(err error, local verdictCodes) bool {
	var tErr *errors.Error
	if !errors.As(err, &tErr) {
		return false
	}

	for cur, depth := tErr, 0; cur != nil && depth < maxMessageChainDepth; depth++ {
		if linkIsRejection(cur, local) {
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
func linkIsRejection(e *errors.Error, local verdictCodes) bool {
	if e == nil {
		return false
	}

	// The shared allowlist is the authority; local is what the site adds, see
	// localRejectionClasses and revalidationRejectionClasses for which and why.
	if errors.IsPublicCause(e.Code()) {
		return true
	}

	_, ok := local[e.Code()]

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
//
// The map reads the validator's own constants, the ones mapBDKValidationError
// builds the chain from, so a rename is a compile error here and a reword changes
// both sides together. What the tests pin is that the suppression works on the
// real chains: TestRPCError_TrimsTheRealValidatorChain and
// TestRPCError_TrimsTheRealPolicyChain fail if either entry is dropped.
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
func rejectionReason(e *errors.Error, local verdictCodes) string {
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
		// Where a scope does surface the immature class, it crosses as its fixed
		// text, never as that message.
		if linkIsRejection(cur, local) {
			msg := cur.Message()
			if fixed, ok := fixedVerdictText[cur.Code()]; ok {
				msg = fixed
			}

			if !add(msg) {
				break
			}
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
	return publicErrorMessageFor(err, scopeOpaque)
}

// publicErrorMessageFor is publicErrorMessage for a site whose scope widens what
// counts as a verdict.
func publicErrorMessageFor(err error, scope rpcRequestScope) string {
	if err == nil {
		return ""
	}

	var tErr *errors.Error
	if !errors.As(err, &tErr) {
		// Not a Teranode error, so nothing here can say whose words these are.
		// A parser reporting what is wrong with the caller's argument and a SQL
		// driver reporting a missing row or a refused connection look the same
		// from this side. Only the site knows which it called, so the text
		// crosses only where the site says it parsed the caller's input.
		if scope == scopeCallerInput {
			return err.Error()
		}

		return genericErrorMessage
	}

	verdicts := scope.verdicts()

	switch {
	case isRejection(tErr, verdicts):
		return sanitised(rejectionReason(tErr, verdicts))

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

	case scope == scopeMiningSubmission && isAssemblyRefusal(tErr):
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

	// scopeHeightLookup is a request that names a block by HEIGHT. A height with
	// no block is a parameter out of range in svnode, not an unknown block.
	scopeHeightLookup

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

	// scopeCallerInput is a site that parses the caller's own argument and hands
	// back the parser's error. A plain error's text is the reason here, and only
	// here; see rpcInputError.
	scopeCallerInput

	// scopeBlockRevalidation is a revalidation of a block already known to exist:
	// reconsiderblock's named block and its descendants.
	//
	// No not-found mapping, because the block was resolved before this site ran,
	// so a not-found in the chain is an ancestor or a descendant. It surfaces
	// revalidationRejectionClasses as verdicts, which no other scope does.
	scopeBlockRevalidation

	// scopeMiningSubmission is a mining solution handed to blockassembly. Its
	// refusals of what the miner sent - a stale candidate, an nTime below the
	// floor, a malformed coinbase, a proof of work above target - are PROCESSING
	// errors in blockassembly's own words, which every other scope treats as the
	// node's fault. Here, and only while nothing beneath says otherwise, the top
	// message is shown; see isAssemblyRefusal.
	scopeMiningSubmission
)

// verdicts is the set of codes this scope surfaces as verdicts on top of the
// shared allowlist.
func (s rpcRequestScope) verdicts() verdictCodes {
	switch s {
	case scopeBlockRevalidation:
		return revalidationRejectionCodes
	case scopeSubmit:
		return submitRejectionCodes
	}

	return localRejectionCodes
}

// isAssemblyRefusal reports whether err is blockassembly refusing a submission in
// its own words: a PROCESSING error with nothing beneath it but more of its own
// breadcrumbs and unclassified library errors.
//
// The class cannot tell a refusal of the miner's input from a node condition such
// as "no current best block yet" - both are PROCESSING, and main showed both - so
// this decides what is safe to show, not whose fault it was. The top message is
// blockassembly's, built from the job id, a block hash and numbers. What it wraps
// is not shown, and any other class beneath it means something else failed: a
// corrupt body on a block this node assembled, a storage or network fault, or a
// gRPC status with no details, whose text carries the dial target.
func isAssemblyRefusal(e *errors.Error) bool {
	if e.Code() != errors.ERR_PROCESSING {
		return false
	}

	for cur, depth := e, 0; depth < maxMessageChainDepth; depth++ {
		if code := cur.Code(); code != errors.ERR_PROCESSING && code != errors.ERR_UNKNOWN {
			return false
		}

		wrapped := cur.WrappedErr()
		if wrapped == nil {
			return true
		}

		next, ok := wrapped.(*errors.Error)
		if !ok {
			return false
		}

		cur = next
	}

	return false
}

// asTeranodeError converts the protobuf form of a Teranode error into the chain
// this file reads.
//
// *errors.TError is what a response message carries when errors travel as a field
// rather than as a gRPC status, as the validator's batch response does. It is not
// an *errors.Error, so without this every class check below would see a foreign
// error and publicErrorMessage would put its rendered chain on the wire. The
// validator client converts at source; this is the boundary refusing to fail open
// on a type the system produces, wherever it next comes from.
func asTeranodeError(err error) error {
	if t, ok := err.(*errors.TError); ok {
		if converted := errors.UnwrapTError(t); converted != nil {
			return converted
		}
	}

	return err
}

// buildRPCError is the pure half of the boundary: no logging, no server.
func buildRPCError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string, scope rpcRequestScope) *bsvjson.RPCError {
	err = asTeranodeError(err)

	// A node fault anywhere in the chain decides first, as it does for the log in
	// isCallerFault. A not-found beneath one is the store failing while it looked,
	// not an answer about the object, so none of the mappings below may claim a
	// miss. Verdicts still cross through publicErrorMessageFor; the fault's own
	// text never does.
	if hasNodeFault(err) {
		return &bsvjson.RPCError{Code: fallbackCode, Message: prefix + publicErrorMessageFor(err, scope)}
	}

	// A service that is up but still loading refuses work it will accept shortly.
	// svnode answers its own warm-up with RPC_IN_WARMUP, and callers retry on it.
	if errors.Is(err, errors.ErrServiceNotReady) {
		return &bsvjson.RPCError{Code: bsvjson.ErrRPCInWarmup, Message: inWarmupMessage}
	}

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

	case scopeHeightLookup:
		if errors.Is(err, errors.ErrBlockNotFound) {
			return &bsvjson.RPCError{Code: bsvjson.ErrRPCInvalidParameter, Message: heightOutOfRange}
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

	case scopeOpaque, scopeCallerInput, scopeBlockRevalidation, scopeMiningSubmission:
	}

	return &bsvjson.RPCError{Code: fallbackCode, Message: prefix + publicErrorMessageFor(err, scope)}
}

// isCallerFault reports whether err describes something wrong with the request
// rather than with the node. It selects the log level: a caller fault keeps its
// reason on the wire, so nothing is lost by logging it at debug, and
// sendrawtransaction rejections are routine enough that logging them louder
// would be a noise problem of its own.
func isCallerFault(err error, scope rpcRequestScope) bool {
	// A block-level failure is rare and operationally significant, so it is not a
	// caller fault for logging purposes even when it is a rejection for message
	// purposes: without this the only record of a failed reconsiderblock is a
	// Debugf, off at default levels, leaving the trimmed response as the whole
	// story. All three classes, in every scope. BLOCK_CORRUPT over a transaction
	// verdict is the case that needs the explicit entry: its TX_INVALID link makes
	// it a rejection below. Log-only, so applying it where submitminingsolution
	// also sees these classes changes nothing on the wire.
	if errors.Is(err, errors.ErrBlockInvalid) ||
		errors.Is(err, errors.ErrBlockCorrupt) ||
		errors.Is(err, errors.ErrBlockPolicyDeclined) {
		return false
	}

	// A node fault anywhere in the chain is a node fault, even beneath a verdict.
	// The verdict's own text still crosses, but the fault's never does, so if this
	// logged at debug the storage or network detail would reach neither the
	// caller nor a default-level log. buildRPCError reads the chain in the same
	// order.
	if hasNodeFault(err) {
		return false
	}

	// Not the caller's fault, and no fault at all: a service still loading. The
	// service that refused has already logged it at Warn.
	if errors.Is(err, errors.ErrServiceNotReady) {
		return true
	}

	if isRejection(err, scope.verdicts()) || errors.Is(err, errors.ErrInvalidArgument) {
		return true
	}

	switch scope {
	case scopeBlockLookup:
		// Only a genuine missing block. Anything else failing to resolve the block
		// a caller named - a store or a service that could not answer - is worth an
		// operator's attention.
		return errors.Is(err, errors.ErrBlockNotFound)

	case scopeHeightLookup:
		return errors.Is(err, errors.ErrBlockNotFound)
	case scopeTxLookup:
		return errors.Is(err, errors.ErrTxNotFound)
	case scopeSubmit:
		return errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrTxMissingParent)
	case scopeCallerInput:
		// The site parsed the caller's argument and nothing else.
		return true
	case scopeOpaque, scopeBlockRevalidation:
	case scopeMiningSubmission:
		// Error, as main logged it: the class that lets a refusal's text cross is
		// shared with node conditions, and a proof of work above target resets
		// block assembly.
	}

	return false
}

// nodeFaultClasses name an infrastructure failure wherever they appear in a chain.
// The breadcrumb wrappers (SERVICE_ERROR, PROCESSING) wrap verdicts routinely and
// are not on it.
var nodeFaultClasses = []*errors.Error{
	errors.ErrStorageError,
	errors.ErrNetworkError,
	errors.ErrNetworkTimeout,
	errors.ErrServiceUnavailable,
}

// hasNodeFault reports whether any link of err is a node fault. buildRPCError and
// isCallerFault both decide on it first, so the wire and the log agree.
func hasNodeFault(err error) bool {
	for _, fault := range nodeFaultClasses {
		if errors.Is(err, fault) {
			return true
		}
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
	// Converted once here as well as in buildRPCError, because isCallerFault reads
	// the chain too and would otherwise log every batched rejection as a node fault.
	err = asTeranodeError(err)
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
// ANCESTOR or DESCENDANT was not (reconsiderblock's revalidation and children
// branches, which resolve the named block first and use this only for that).
//
// prefix applies to the fallback branch only; the mapped branch returns
// bitcoind's text alone. Do not pass a prefix carrying a fact the caller needs
// in order to tell the two branches apart - if the site has one, it is not a
// lookup site.
func (s *RPCServer) rpcLookupError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeBlockLookup)
}

// rpcRevalidationError is rpcError for a revalidation of a block already resolved:
// reconsiderblock's named block and its descendants. No not-found mapping, and
// BLOCK_CORRUPT is surfaced as the verdict it is on this path; see
// revalidationRejectionClasses for why nowhere else.
func (s *RPCServer) rpcRevalidationError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeBlockRevalidation)
}

// rpcHeightLookupError is rpcLookupError for a request that names a block by
// HEIGHT: a height with no block is answered as svnode answers it, -8 "Block
// height out of range".
func (s *RPCServer) rpcHeightLookupError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeHeightLookup)
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

// rpcMiningSubmissionError is rpcError for submitminingsolution: blockassembly's
// own reason for refusing the solution is shown, and nothing it wraps; see
// scopeMiningSubmission.
func (s *RPCServer) rpcMiningSubmissionError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeMiningSubmission)
}

// rpcInputError is rpcError for a site that parses the caller's own argument with
// a library that reports failure as a plain error - an address decoder, a
// transaction parser, a range check on a numeric parameter. That text is the
// reason and describes only what the caller sent, so it crosses verbatim, and the
// failure is the request's, so it is logged at debug.
//
// Use it only around such a parse. Everywhere else a plain error is a library or
// driver speaking for the node, and fails closed.
func (s *RPCServer) rpcInputError(err error, fallbackCode bsvjson.RPCErrorCode, prefix string) *bsvjson.RPCError {
	return s.logAndBuild(err, fallbackCode, prefix, scopeCallerInput)
}
