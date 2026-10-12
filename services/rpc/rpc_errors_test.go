package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The fixtures below are the shapes this tree actually produces. Each names
// the code that builds it, because a hand-rolled two-level chain with the
// reason on top would let every assertion here pass while production put the
// breadcrumb on the wire and dropped the reason.

// blockNotFoundChain is the shape reported in bitcoin-sv/teranode issue 4778:
// a BLOCK_NOT_FOUND raised by the store, wrapped by the blockchain client,
// wrapped again by the block validation service. The storage layer's own text
// sits at the bottom.
func blockNotFoundChain() error {
	return errors.NewServiceError("[RevalidateBlock][1000] failed to get block",
		errors.NewBlockNotFoundError("error in GetBlock",
			fmt.Errorf("sql: no rows in result set")))
}

// validatorRejectionChain is what reaches handleSendRawTransaction when GoBDK
// refuses a transaction. mapBDKValidationError puts the fixed string
// errMsgInvalidTx on top of the real BDK error at every one of its returns
// (services/validator/ScriptVerifierGoBDK.go), and validateInternal wraps that
// again (services/validator/Validator.go). The reason is therefore at the
// bottom and every link above it is a constant.
func validatorRejectionChain() error {
	// The tail is an *errors.Error, not a plain Go error: bdkCause re-wraps the
	// CGO string as errors.New(ERR_TX_INVALID, "%s", cause). An earlier fixture
	// used fmt.Errorf here and so exercised a shape production no longer builds.
	return errors.NewProcessingError("[Validate][%s] error validating transaction", "abcd1234",
		errors.NewTxInvalidError(validator.ErrMsgInvalidTx,
			errors.New(errors.ERR_TX_INVALID, "bad-txns-inputs-duplicate")))
}

// missingParentChain is what reaches handleSendRawTransaction when a submitted
// transaction spends an outpoint the node does not hold. PreviousOutputsDecorate
// raises a bare TX_NOT_FOUND (stores/utxo/aerospike/get.go) and the validator
// wraps it without reclassifying (services/validator/Validator.go).
func missingParentChain() error {
	return errors.NewProcessingError("[Validate][%s] error spending utxos", "abcd1234",
		errors.NewTxNotFoundError("previous tx not found: %v", "deadbeef:0"))
}

// blockInvalidChain is a genuine revalidation failure behind reconsiderblock:
// BlockValidation raises BLOCK_INVALID with the reject reason fused into a
// breadcrumb-prefixed message, and the gRPC server wraps it in a SERVICE_ERROR
// (services/blockvalidation/Server.go).
func blockInvalidChain() error {
	return errors.NewServiceError("[RevalidateBlock][00000000000000000abc] failed block re-validation",
		errors.NewBlockInvalidError("[ValidateBlock][00000000000000000abc] bad-blk-txns-inputs-missingorspent"))
}

// childNotFoundChain is the reconsiderblock children branch: the block the
// caller named was reconsidered successfully and a DESCENDANT is missing, so
// the not-found in this chain is not about the object the request named.
func childNotFoundChain() error {
	return errors.NewServiceError("failed to revalidate child block %s", "00000000000000000def",
		errors.NewBlockNotFoundError("error in GetBlock",
			fmt.Errorf("sql: no rows in result set")))
}

// detaillessGRPCChain is what UnwrapGRPC synthesises for a status carrying no
// details - any framework-generated failure, such as a dial error. Its
// Message() is the whole rendered status string, including the internal
// target address (errors/errors.go, len(st.Details()) == 0).
func detaillessGRPCChain() error {
	return errors.UnwrapGRPC(status.Error(codes.Unavailable,
		`connection error: desc = "transport: Error while dialing dial tcp 10.4.2.17:8081: connect: connection refused"`))
}

// capturingLogger records what logAndBuild writes, so the tests can assert
// that a chain trimmed off the wire still lands somewhere.
type capturingLogger struct {
	ulogger.Logger

	mu     sync.Mutex
	errorf []string
	warnf  []string
	debugf []string
}

func newCapturingLogger() *capturingLogger {
	return &capturingLogger{Logger: mocklogger.NewTestLogger()}
}

func (l *capturingLogger) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errorf = append(l.errorf, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warnf = append(l.warnf, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) Debugf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.debugf = append(l.debugf, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return strings.Join(append(append(append([]string{}, l.errorf...), l.warnf...), l.debugf...), "\n")
}

// ---------------------------------------------------------------------------
// Message selection
// ---------------------------------------------------------------------------

func TestPublicErrorMessage_KeepsTheReasonNotTheBreadcrumb(t *testing.T) {
	// The reason a submitter needs is at the BOTTOM of a validator chain.
	// Selecting the topmost message would emit the breadcrumb and discard it,
	// which is the failure this whole file exists to prevent.
	err := validatorRejectionChain()

	// Preconditions: the chain really is shaped that way. Without these the
	// assertions below could pass against a fixture that never had the
	// problem.
	require.Contains(t, err.Error(), "bad-txns-inputs-duplicate", "the reason is in the chain")
	require.Contains(t, err.Error(), "[Validate]", "the breadcrumb is in the chain")

	msg := publicErrorMessage(err)

	require.Contains(t, msg, "bad-txns-inputs-duplicate",
		"the closed-vocabulary reason is what callers and upstream's functional tests match on")
	require.NotContains(t, msg, "[Validate]", "the service breadcrumb must not cross the API boundary")
	require.NotContains(t, msg, "error validating transaction",
		"nor the breadcrumb's own text, which is what naming the outermost link would have emitted")
}

// nonFinalChain is what Validator.go:753 builds when a transaction fails the
// finality check: the CATEGORY is the outer message and the DETAIL is beneath
// it, which is the opposite way round from the GoBDK path.
func nonFinalChain() error {
	return errors.NewUtxoNonFinalError("[Validate][%s] transaction is not final", "abcd1234",
		errors.NewTxLockTimeError("lock time (133) as block height is not less than block height (12)"))
}

// TestPublicErrorMessage_KeepsBothEndsOfARejectionChain is the regression test
// for the smoke-test break. Taking only the deepest message emitted
// "lock time (133) ... is not less than block height (12)" and dropped
// "transaction is not final", which test/e2e/daemon/ready/smoke_test.go asserts
// on. Neither end of a rejection chain is reliably the reason, so both are kept.
func TestPublicErrorMessage_KeepsBothEndsOfARejectionChain(t *testing.T) {
	err := nonFinalChain()

	// Preconditions: the chain really is shaped with the category on top.
	var tErr *errors.Error
	require.ErrorAs(t, err, &tErr)
	require.Contains(t, tErr.Message(), "transaction is not final", "the category is the outer message")
	require.Contains(t, err.Error(), "lock time (133)", "the detail is below it")

	msg := publicErrorMessage(err)

	require.Contains(t, msg, "transaction is not final",
		"the category is what callers and the e2e smoke test match on")
	require.Contains(t, msg, "lock time (133)",
		"the detail is what tells the submitter why")
	require.NotContains(t, msg, "[Validate]", "the breadcrumb still must not cross")
}

// TestRejectionReason_StartsAtTheVerdictNotTheBreadcrumb pins why the walk uses
// each link's OWN code. (*errors.Error).Is matches by code anywhere in the
// chain, so testing the chain would start at the outermost link every time and
// put the service breadcrumb's text on the wire.
func TestRejectionReason_StartsAtTheVerdictNotTheBreadcrumb(t *testing.T) {
	breadcrumb := errors.NewProcessingError("[Validate][%s] error validating transaction", "abcd1234",
		errors.NewTxInvalidError(validator.ErrMsgInvalidTx, fmt.Errorf("bad-txns-inputs-duplicate")))

	require.True(t, isRejection(breadcrumb, localRejectionCodes), "the chain-wide test matches the breadcrumb link too")

	var tErr *errors.Error
	require.ErrorAs(t, breadcrumb, &tErr)
	require.False(t, linkIsRejection(tErr, localRejectionCodes), "the per-link test must not")

	require.NotContains(t, publicErrorMessage(breadcrumb), "error validating transaction")
}

func TestPublicErrorMessage_StripsBreadcrumbsFusedIntoTheReason(t *testing.T) {
	// BlockValidation formats the reject reason into the same string as the
	// breadcrumb, so there is no deeper link to fall back to.
	msg := publicErrorMessage(blockInvalidChain())

	require.Equal(t, "bad-blk-txns-inputs-missingorspent", msg)
	require.NotContains(t, msg, "ValidateBlock")
}

func TestPublicErrorMessage_SuppressesInternalFailures(t *testing.T) {
	// A node fault is not the caller's business. The code carries the
	// classification and the chain goes to the log.
	err := errors.NewServiceError("[BlobStore][S3] failed to set data",
		fmt.Errorf("AccessDenied: bucket policy denies PutObject"))

	msg := publicErrorMessage(err)

	require.Equal(t, genericErrorMessage, msg)
	require.NotContains(t, msg, "S3")
	require.NotContains(t, msg, "AccessDenied")
}

func TestPublicErrorMessage_DoesNotLeakTheGRPCDialTarget(t *testing.T) {
	err := detaillessGRPCChain()

	// Precondition: UnwrapGRPC really does put the rendered status, host and
	// port included, in the message rather than in a wrapped error.
	var tErr *errors.Error
	require.ErrorAs(t, err, &tErr)
	require.Contains(t, tErr.Message(), "10.4.2.17:8081",
		"the synthesised shell carries the target address as its own message")

	msg := publicErrorMessage(err)

	require.NotContains(t, msg, "10.4.2.17:8081", "internal service addressing must not cross the boundary")
	require.NotContains(t, msg, "rpc error: code =", "a rendered gRPC status is not a caller-facing message")
}

// TestPublicErrorMessage_NonTeranodeErrorFailsClosed pins the default for an
// error this boundary cannot classify. It used to pass a plain error's text
// through on the grounds that the address and transaction parsers return the
// reason that way - but so does a SQL driver. freeze on an unknown txid put the
// store's "sql: no rows in result set" on the wire, and a database outage would
// have put its dial target there. A site that parses the caller's own input opts
// in to the text instead (scopeCallerInput).
func TestPublicErrorMessage_NonTeranodeErrorFailsClosed(t *testing.T) {
	require.Equal(t, genericErrorMessage, publicErrorMessage(fmt.Errorf("sql: no rows in result set")))
	require.Equal(t, genericErrorMessage,
		publicErrorMessage(fmt.Errorf("dial tcp 10.0.3.4:5432: connect: connection refused")))
}

func TestCallerInputError_KeepsTheParserReasonAndLogsQuietly(t *testing.T) {
	// The parser's text is the reason, and the request is what was wrong, so the
	// text crosses and the log stays at debug - which is what these sites logged
	// before the boundary existed (nothing).
	logger := newCapturingLogger()
	s := &RPCServer{logger: logger}

	rpcErr := s.rpcInputError(fmt.Errorf("decoded address is of unknown format"),
		bsvjson.ErrRPCInvalidAddressOrKey, "Invalid address or key: ")

	require.Equal(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code)
	require.Equal(t, "Invalid address or key: decoded address is of unknown format", rpcErr.Message)
	require.Empty(t, logger.errorf, "a malformed argument is not a node fault")
	require.NotEmpty(t, logger.debugf)
}

// plainErrorUtxoStore fails freeze with a SQL driver's own error, which is how the
// SQL utxo store answers an unknown txid (stores/utxo/sql/alert_system.go).
type plainErrorUtxoStore struct {
	utxo.Store
	err error
}

func (p *plainErrorUtxoStore) FreezeUTXOs(context.Context, []*utxo.Spend, *settings.Settings) error {
	return p.err
}

// TestHandleFreeze_StoreErrorTextStaysOffTheWire is the review's reproduction of
// the fail-open default: a limited user freezing an unknown txid on a SQL-backed
// node was told "sql: no rows in result set".
func TestHandleFreeze_StoreErrorTextStaysOffTheWire(t *testing.T) {
	logger := newCapturingLogger()
	s := &RPCServer{
		logger:    logger,
		settings:  &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams},
		utxoStore: &plainErrorUtxoStore{err: fmt.Errorf("sql: no rows in result set")},
	}

	_, handlerErr := handleFreeze(context.Background(), s,
		&bsvjson.FreezeCmd{TxID: strings.Repeat("ab", 32), Vout: 0}, nil)
	require.Error(t, handlerErr)

	reply, err := s.createMarshalledReply(1, nil, handlerErr)
	require.NoError(t, err)

	var parsed bsvjson.Response
	require.NoError(t, json.Unmarshal(reply, &parsed))
	require.NotNil(t, parsed.Error)
	require.Equal(t, genericErrorMessage, parsed.Error.Message)
	require.NotEmpty(t, logger.errorf, "the driver's text goes to the log instead")
	require.Contains(t, logger.all(), "sql: no rows in result set")
}

// TestCallerInputSites_KeepTheirReasonWhenTheDefaultFailsClosed covers every
// handler that hands back a parser's error about the caller's own argument.
// Each used to rely on the fail-open default to show its reason; failing closed
// would have answered "internal error" to a malformed request.
func TestCallerInputSites_KeepTheirReasonWhenTheDefaultFailsClosed(t *testing.T) {
	badHash := strings.Repeat("zz", 32)

	for name, tc := range map[string]struct {
		call func(*RPCServer) (interface{}, error)
		code bsvjson.RPCErrorCode
		text string
	}{
		"sendrawtransaction, valid hex that is not a transaction": {
			call: func(s *RPCServer) (interface{}, error) {
				return handleSendRawTransaction(context.Background(), s, &bsvjson.SendRawTransactionCmd{HexTx: "00"}, nil)
			},
			code: bsvjson.ErrRPCDeserialization,
			text: txRejectedPrefix,
		},
		"getblockhash, negative height": {
			call: func(s *RPCServer) (interface{}, error) {
				return handleGetBlockHash(context.Background(), s, &bsvjson.GetBlockHashCmd{Index: -1}, nil)
			},
			code: bsvjson.ErrRPCInvalidParameter,
			text: "negative",
		},
		"freeze, malformed txid": {
			call: func(s *RPCServer) (interface{}, error) {
				return handleFreeze(context.Background(), s, &bsvjson.FreezeCmd{TxID: badHash}, nil)
			},
			code: bsvjson.ErrRPCDecodeHexString,
			text: badHash,
		},
		"unfreeze, malformed txid": {
			call: func(s *RPCServer) (interface{}, error) {
				return handleUnfreeze(context.Background(), s, &bsvjson.UnfreezeCmd{TxID: badHash}, nil)
			},
			code: bsvjson.ErrRPCDecodeHexString,
			text: badHash,
		},
		"reassign, malformed txid": {
			call: func(s *RPCServer) (interface{}, error) {
				return handleReassign(context.Background(), s, &bsvjson.ReassignCmd{OldTxID: badHash}, nil)
			},
			code: bsvjson.ErrRPCDecodeHexString,
			text: badHash,
		},
	} {
		t.Run(name, func(t *testing.T) {
			logger := newCapturingLogger()
			s := &RPCServer{logger: logger, settings: &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams}}

			_, err := tc.call(s)

			rpcErr := requireRPCError(t, err)
			require.Equal(t, tc.code, rpcErr.Code)
			require.Contains(t, rpcErr.Message, tc.text)
			require.NotEqual(t, genericErrorMessage, rpcErr.Message)
			require.Empty(t, logger.errorf, "a malformed argument is not a node fault")
		})
	}
}

// TestLogAndBuild_AVerdictOverANodeFaultIsLoggedAsAFault pins the log half of
// "a verdict does not launder the node fault beneath it". The wire half was
// already right - the storage text stays off it - but the chain was classed as a
// caller fault and logged at debug, so the storage detail reached neither the
// caller nor a default-level log.
func TestLogAndBuild_AVerdictOverANodeFaultIsLoggedAsAFault(t *testing.T) {
	for name, err := range map[string]error{
		"storage": errors.NewTxInvalidError("bad-txns",
			errors.NewStorageError("s3 403 AccessDenied bucket=teranode-prod-subtrees")),
		"network": errors.NewTxInvalidError("could not read tx from stream",
			errors.NewNetworkError("read tcp 10.0.1.5:34512->10.0.2.9:9000: connection reset by peer")),
		"network timeout": errors.NewTxInvalidError("bad-txns",
			errors.NewNetworkTimeoutError("utxo batch read timed out")),
		"service unavailable": errors.NewTxInvalidError("bad-txns",
			errors.NewServiceUnavailableError("aerospike outpoint batch did not complete within 5s")),
	} {
		t.Run(name, func(t *testing.T) {
			logger := newCapturingLogger()
			s := &RPCServer{logger: logger}

			_ = s.rpcSubmitError(err, bsvjson.ErrRPCVerify, txRejectedPrefix)

			require.NotEmpty(t, logger.errorf, "a node fault under a verdict is still a node fault")
		})
	}
}

func TestPublicErrorMessage_InvalidArgumentKeepsItsOwnMessage(t *testing.T) {
	err := errors.NewInvalidArgumentError("[handleGetBlock] height 99 is above the tip")

	require.Equal(t, "height 99 is above the tip", publicErrorMessage(err))
}

func TestPublicErrorMessage_NeverReturnsEmptyForANonNilError(t *testing.T) {
	require.NotEmpty(t, publicErrorMessage(errors.New(errors.ERR_ERROR, "")))
	require.NotEmpty(t, publicErrorMessage(errors.NewTxInvalidError("")))
	require.NotEmpty(t, publicErrorMessage(errors.NewTxInvalidError("[Validate][abcd]")),
		"a message that is nothing but breadcrumbs leaves no reason to show")
}

func TestRejectionReason_TerminatesOnAPathologicalChain(t *testing.T) {
	// A mass spend failure can build a chain tens of thousands of links deep.
	// An unbounded walk over one on the RPC path would be a DoS vector.
	// Distinct wrapper text per link. With all 128 identical, add's dedup collapsed
	// them to one part and the assertion could not see whether anything was
	// discarded - the test documented termination and nothing else.
	err := errors.NewTxInvalidError("innermost reason")
	for i := 0; i < maxMessageChainDepth*4; i++ {
		err = errors.NewTxInvalidError(fmt.Sprintf("wrapper %d", i), err)
	}

	done := make(chan string, 1)
	go func() { done <- publicErrorMessage(err) }()

	select {
	case msg := <-done:
		require.NotEmpty(t, msg)
		// The outermost wrappers are what a bounded walk can reach; the point is
		// that it says so rather than presenting a mid-chain wrapper as the reason
		// with no signal, and that it never returns empty.
		require.Contains(t, msg, "wrapper",
			"a bounded walk reports what it reached")
	case <-time.After(5 * time.Second):
		// A real deadline. The previous version selected on
		// context.Background().Done(), which is a nil channel and never fires, so
		// the select was a plain blocking receive and the test could only ever
		// hang rather than fail.
		t.Fatal("publicErrorMessage did not terminate on a pathological chain")
	}
}

// TestRejectionDepthCapsAgree pins that the walk that decides IF an error is a
// rejection and the walk that builds its message stop at the same depth.
//
// They did not. isRejection delegated to errors.Is, capped at 1<<20, while the
// message walk capped at 32, so a chain with its verdict beyond 32 was routed
// down the rejection branch and then handed back a shallow wrapper's text as
// though it were the reason. Sharing one constant makes that disagreement
// unrepresentable; this fails if the two are ever given separate bounds again.
func TestRejectionDepthCapsAgree(t *testing.T) {
	// Bury the verdict beyond the cap under links that are not verdicts.
	var err error = errors.NewTxInvalidError("bad-txns-inputs-duplicate")
	for i := 0; i < maxMessageChainDepth*2; i++ {
		err = errors.NewProcessingError("wrapper", err)
	}

	require.False(t, isRejection(err, localRejectionCodes),
		"a verdict past the depth cap is not reachable, and must not be claimed as one")

	msg := publicErrorMessage(err)

	require.Equal(t, genericErrorMessage, msg,
		"an unreachable verdict yields the generic text, never a wrapper's own words")
	require.NotContains(t, msg, "wrapper")
}

// ---------------------------------------------------------------------------
// Disclosure: these must fail if the trimming is removed
// ---------------------------------------------------------------------------

func TestRPCError_DoesNotDiscloseTheStorageLayer(t *testing.T) {
	// This exercises the FALLBACK branch deliberately. The earlier version of
	// this test fed a not-found, which takes the mapped branch and returns a
	// constant, so it passed with the trimming deleted outright.
	err := blockNotFoundChain()

	require.Contains(t, err.Error(), "sql: no rows in result set")
	require.Contains(t, err.Error(), "RevalidateBlock")

	rpcErr := buildRPCError(err, bsvjson.ErrRPCInternal.Code, "", scopeOpaque)

	require.NotContains(t, rpcErr.Message, "sql: no rows in result set", "storage layer must not cross the API boundary")
	require.NotContains(t, rpcErr.Message, "SERVICE_ERROR", "internal error codes must not cross the API boundary")
	require.NotContains(t, rpcErr.Message, "RevalidateBlock", "internal method names must not cross the API boundary")
	require.NotContains(t, rpcErr.Message, "BLOCK_NOT_FOUND")
}

func TestRPCError_TrimsTheRealValidatorChain(t *testing.T) {
	rpcErr := buildRPCError(validatorRejectionChain(), bsvjson.ErrRPCVerify, txRejectedPrefix, scopeSubmit)

	require.Equal(t, "TX rejected: bad-txns-inputs-duplicate", rpcErr.Message,
		"the reason, without the service breadcrumb or the BDK adapter's own constant")
	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
}

// ---------------------------------------------------------------------------
// Reclassification is per call site
// ---------------------------------------------------------------------------

func TestLookupError_NotFoundMatchesBitcoind(t *testing.T) {
	rpcErr := buildRPCError(blockNotFoundChain(), bsvjson.ErrRPCVerify, "", scopeBlockLookup)

	require.Equal(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code,
		"an unknown block is -5 (RPC_INVALID_ADDRESS_OR_KEY), matching bitcoind, not -25 (RPC_VERIFY_ERROR)")
	require.Equal(t, blockNotFoundMessage, rpcErr.Message,
		"the mapped message is verbatim so callers porting from bitcoind match on it")
}

// TestHandleGetBlockByHeight_AnswersAsSvnode pins getblockbyheight's two
// outcomes. A height with no block is a parameter out of range in svnode
// (blockchain.cpp, -8 "Block height out of range"), not an unknown block: the
// caller named a height, not a block. A failing store is the node's fault and
// says so.
func TestHandleGetBlockByHeight_AnswersAsSvnode(t *testing.T) {
	verbosity := uint32(1)

	for name, tc := range map[string]struct {
		err      error
		code     bsvjson.RPCErrorCode
		message  string
		errorLog bool
	}{
		"no block at that height": {
			err: errors.NewBlockNotFoundError("[Blockchain] block not found at height",
				errors.NewBlockNotFoundError("failed to get block by height for height %d", 1_000_000)),
			code:    bsvjson.ErrRPCInvalidParameter,
			message: "Block height out of range",
		},
		"store failing": {
			err:      errors.NewStorageError("failed to get block by height", fmt.Errorf("dial tcp 10.0.3.4:5432: connect: connection refused")),
			code:     bsvjson.ErrRPCInternal.Code,
			message:  genericErrorMessage,
			errorLog: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			mockClient := &blockchain.Mock{}
			mockClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(nil, overGRPC(t, tc.err))

			logger := newCapturingLogger()
			s := &RPCServer{logger: logger, blockchainClient: mockClient}

			_, err := handleGetBlockByHeight(context.Background(), s,
				&bsvjson.GetBlockByHeightCmd{Height: 1_000_000, Verbosity: &verbosity}, nil)

			rpcErr := requireRPCError(t, err)
			require.Equal(t, tc.code, rpcErr.Code)
			require.Equal(t, tc.message, rpcErr.Message)

			if tc.errorLog {
				require.NotEmpty(t, logger.errorf)
			} else {
				require.Empty(t, logger.errorf, "a height above the tip is the caller's mistake")
			}
		})
	}
}

// TestHandleGetRawTransaction_MalformedTxidIsRefusedBeforeAnyRequest pins that the
// txid is parsed before it goes anywhere, as svnode's ParseHashV does (-8). It is
// interpolated into the asset service URL, where an unchecked value addressed
// other routes ("../block/<hash>" resolves to /api/v1/block/<hash>/hex), and a
// malformed one came back as the asset service's 400: -32603 "internal error",
// logged at Error, for the caller's own typo.
func TestHandleGetRawTransaction_MalformedTxidIsRefusedBeforeAnyRequest(t *testing.T) {
	var requests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	assetURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	for name, tc := range map[string]struct {
		txid    string
		message string
	}{
		"not hex":        {strings.Repeat("zz", 32), "parameter 1 must be hexadecimal string (not '" + strings.Repeat("zz", 32) + "')"},
		"too short":      {"abcd", "parameter 1 must be of length 64 (not 4)"},
		"another route":  {"../block/" + strings.Repeat("00", 32), "parameter 1 must be hexadecimal string (not '../block/" + strings.Repeat("00", 32) + "')"},
		"a forged line":  {"deadbeef\nFORGED ERROR LINE", "parameter 1 must be hexadecimal string (not 'deadbeef\nFORGED ERROR LINE')"},
		"odd length hex": {strings.Repeat("a", 63), "parameter 1 must be hexadecimal string (not '" + strings.Repeat("a", 63) + "')"},
	} {
		t.Run(name, func(t *testing.T) {
			logger := newCapturingLogger()
			s := &RPCServer{logger: logger, assetHTTPURL: assetURL}
			before := requests.Load()
			verbose := 0

			_, err := handleGetRawTransaction(context.Background(), s,
				&bsvjson.GetRawTransactionCmd{Txid: tc.txid, Verbose: &verbose}, nil)

			rpcErr := requireRPCError(t, err)
			require.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
			require.Equal(t, tc.message, rpcErr.Message)
			require.Equal(t, before, requests.Load(), "nothing is sent to the asset service")
			require.Empty(t, logger.all(), "and nothing caller-supplied reaches a log line")
		})
	}
}

// TestHandleGetRawTransaction_OnlyAMarkedNotFoundIsAnUnknownTx pins how a 404 from
// the asset service is read. -5 is the strongest answer this API has, so it needs
// the asset handler's own word that the transaction is absent
// (util.AssetNotFoundHeader). An unmarked 404 is a route that did not match - a
// misconfigured asset URL answers every request that way - and answering -5 for
// it denied every transaction while the node looked healthy. With the mark there
// to tell them apart, a routine unknown-txid poll no longer needs a WARN per
// request.
func TestHandleGetRawTransaction_OnlyAMarkedNotFoundIsAnUnknownTx(t *testing.T) {
	for name, tc := range map[string]struct {
		marked  bool
		code    bsvjson.RPCErrorCode
		message string
	}{
		"marked by the asset handler": {true, bsvjson.ErrRPCInvalidAddressOrKey, txNotFoundMessage},
		"unmarked":                    {false, bsvjson.ErrRPCInternal.Code, genericErrorMessage},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.marked {
					w.Header().Set(util.AssetNotFoundHeader, util.AssetNotFoundTx)
				}

				w.WriteHeader(http.StatusNotFound)
			}))
			defer server.Close()

			assetURL, err := url.Parse(server.URL)
			require.NoError(t, err)

			logger := newCapturingLogger()
			s := &RPCServer{logger: logger, assetHTTPURL: assetURL}
			verbose := 0

			_, err = handleGetRawTransaction(context.Background(), s,
				&bsvjson.GetRawTransactionCmd{Txid: strings.Repeat("ab", 32), Verbose: &verbose}, nil)

			rpcErr := requireRPCError(t, err)
			require.Equal(t, tc.code, rpcErr.Code)
			require.Equal(t, tc.message, rpcErr.Message)

			if tc.marked {
				require.Empty(t, logger.warnf, "an unknown txid is routine, not worth a WARN per request")
				require.Empty(t, logger.errorf)
			} else {
				require.NotEmpty(t, logger.errorf, "an unmarked 404 is a node fault")
				require.Contains(t, logger.all(), "404")
			}
		})
	}
}

// TestHandleSubmitMiningSolution_StaleJobKeepsInternalsOffTheWire covers the stale
// job branch, a limited-tier path miners hit routinely. It put the whole rendered
// chain on the wire: "NOT_FOUND (3): [BlockAssembly][<jobid>] job not found".
func TestHandleSubmitMiningSolution_StaleJobKeepsInternalsOffTheWire(t *testing.T) {
	baMock := &blockassembly.Mock{}
	baMock.On("SubmitMiningSolution", mock.Anything, mock.Anything).Return(overGRPC(t,
		errors.NewNotFoundError("[BlockAssembly][%s] job not found", "cafe")))

	s := &RPCServer{logger: newCapturingLogger(), blockAssemblyClient: baMock}

	_, err := handleSubmitMiningSolution(context.Background(), s, &bsvjson.SubmitMiningSolutionCmd{
		MiningSolution: bsvjson.MiningSolution{ID: "cafe", Coinbase: "00"},
	}, nil)

	rpcErr := requireRPCError(t, err)
	require.Equal(t, bsvjson.ErrRPCMisc, rpcErr.Code)
	require.Equal(t, "job not found", rpcErr.Message)
}

// TestGenerateBlocks_FailuresKeepInternalsOffTheWire covers the three failure
// sites in generate and generatetoaddress. Each rendered its own wrapper's whole
// chain into the message, so a status without details put the service's dial
// target on the wire.
func TestGenerateBlocks_FailuresKeepInternalsOffTheWire(t *testing.T) {
	dialFailure := func() error {
		return errors.UnwrapGRPC(status.Error(codes.Unavailable,
			`connection error: desc = "transport: Error while dialing dial tcp 10.4.2.17:8085: connect: connection refused"`))
	}

	for name, configure := range map[string]func(*blockchain.Mock, *blockassembly.Mock){
		"best header": func(bc *blockchain.Mock, _ *blockassembly.Mock) {
			bc.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, dialFailure())
		},
		"generate": func(bc *blockchain.Mock, ba *blockassembly.Mock) {
			bc.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 1}, nil)
			ba.On("GenerateBlocks", mock.Anything, mock.Anything).Return(dialFailure())
		},
		"generated block": func(bc *blockchain.Mock, ba *blockassembly.Mock) {
			bc.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 1}, nil)
			ba.On("GenerateBlocks", mock.Anything, mock.Anything).Return(nil)
			bc.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(nil, dialFailure())
		},
	} {
		t.Run(name, func(t *testing.T) {
			bc, ba := &blockchain.Mock{}, &blockassembly.Mock{}
			configure(bc, ba)

			logger := newCapturingLogger()
			s := &RPCServer{logger: logger, blockchainClient: bc, blockAssemblyClient: ba}

			_, err := s.generateBlocksAndReturnHashes(context.Background(), 1, nil, nil)

			rpcErr := requireRPCError(t, err)
			require.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
			require.Equal(t, genericErrorMessage, rpcErr.Message)
			require.Contains(t, logger.all(), "10.4.2.17:8085", "the dial target goes to the log instead")
		})
	}
}

func TestLookupError_TxNotFoundMatchesBitcoind(t *testing.T) {
	rpcErr := buildRPCError(errors.NewTxNotFoundError("no tx"), bsvjson.ErrRPCVerify, "", scopeTxLookup)

	require.Equal(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code)
	require.Equal(t, txNotFoundMessage, rpcErr.Message)
}

func TestSubmitError_MissingInputsIsARejectionNotALookupMiss(t *testing.T) {
	// The transaction the caller submitted exists - they just sent it. It is a
	// PARENT that is absent. bitcoind and svnode both answer -25 here.
	// Answering -5 "No such mempool or blockchain transaction" tells a wallet
	// the transaction does not exist and must not be retried, which is the
	// opposite of the truth for a transaction that is merely early.
	for name, err := range map[string]error{
		"bare from PreviousOutputsDecorate": errors.NewTxNotFoundError("previous tx not found: %v", "deadbeef:0"),
		"wrapped by the validator":          missingParentChain(),
		"classified as a missing parent":    errors.NewTxMissingParentError("parent missing"),
	} {
		t.Run(name, func(t *testing.T) {
			rpcErr := buildRPCError(err, bsvjson.ErrRPCVerify, txRejectedPrefix, scopeSubmit)

			require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code, "a missing input is a rejection, not a lookup miss")
			require.NotEqual(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code)
			require.Equal(t, "TX rejected: Missing inputs", rpcErr.Message)
		})
	}
}

func TestRPCError_DoesNotReclassifyWithoutSiteContext(t *testing.T) {
	// The reconsiderblock children branch: the named block was reconsidered
	// successfully, so a not-found under a descendant's failure must not be
	// reported as "the block you asked for does not exist".
	prefix := "Block 00000000000000000abc was reconsidered but failed to reconsider children: "

	rpcErr := buildRPCError(childNotFoundChain(), bsvjson.ErrRPCInternal.Code, prefix, scopeOpaque)

	require.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code, "the site's own code stands")
	require.NotEqual(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code)
	require.True(t, strings.HasPrefix(rpcErr.Message, prefix),
		"the prefix is the only fact distinguishing this from an unknown block, so it must survive")
}

func TestRPCError_KeepsThePrefixOnEveryFallback(t *testing.T) {
	for name, err := range map[string]error{
		"internal":      blockNotFoundChain(),
		"rejection":     validatorRejectionChain(),
		"plain":         fmt.Errorf("plain failure"),
		"gRPC shell":    detaillessGRPCChain(),
		"missing input": missingParentChain(),
	} {
		t.Run(name, func(t *testing.T) {
			rpcErr := buildRPCError(err, bsvjson.ErrRPCVerify, "prefix: ", scopeSubmit)
			require.True(t, strings.HasPrefix(rpcErr.Message, "prefix: "), rpcErr.Message)
		})
	}
}

// ---------------------------------------------------------------------------
// The chain has to land in the log, because it no longer lands on the wire
// ---------------------------------------------------------------------------

func TestLogAndBuild_RecordsTheChainItRefusesToSend(t *testing.T) {
	logger := newCapturingLogger()
	s := &RPCServer{logger: logger}

	err := errors.NewServiceError("[BlobStore][S3] failed to set data",
		fmt.Errorf("AccessDenied: bucket policy denies PutObject"))

	rpcErr := s.rpcError(err, bsvjson.ErrRPCInternal.Code, "Failed to store transaction: ")

	require.Equal(t, "Failed to store transaction: "+genericErrorMessage, rpcErr.Message)
	require.Contains(t, logger.all(), "AccessDenied",
		"the cause must reach the log, since trimming removed it from the response")
	require.NotEmpty(t, logger.errorf, "a node fault is logged at error level")
}

func TestLogAndBuild_DoesNotLogRoutineRejectionsAtErrorLevel(t *testing.T) {
	// sendrawtransaction rejections are routine and high volume, and they keep
	// their reason on the wire, so nothing is lost by logging them quietly.
	logger := newCapturingLogger()
	s := &RPCServer{logger: logger}

	_ = s.rpcSubmitError(validatorRejectionChain(), bsvjson.ErrRPCVerify, txRejectedPrefix)

	require.Empty(t, logger.errorf, "a caller's invalid submission is not a node fault")
	require.NotEmpty(t, logger.debugf)
}

// ---------------------------------------------------------------------------
// End to end through the handlers
// ---------------------------------------------------------------------------

// TestHandleReconsiderBlock_UnknownBlockMatchesBitcoind is the end-to-end form
// of the contract bitcoin-sv's bsv-command-line-invalid-block.py asserts:
//
//	assert_raises_rpc_error(-5, 'Block not found', reconsiderblock, <unknown hash>)
//
// Before this mapping the handler answered -25 with four levels of internal
// error chain, so a caller could not distinguish "no such block" from "that
// block is bad", and was told which storage engine had missed.
//
// The handler resolves the named block before revalidating anything, as svnode's
// reconsiderblock does (mapBlockIndex lookup first). The store's not-found is the
// one fixture here: GetBlockHeader distinguishes a missing row from a storage
// failure (stores/blockchain/sql/GetBlockHeader.go), and the blockchain server
// passes that through.
func TestHandleReconsiderBlock_UnknownBlockMatchesBitcoind(t *testing.T) {
	revalidated := false

	_, err := reconsiderFixture{
		headerErr: errors.NewBlockNotFoundError("error in GetBlockHeader", errors.ErrNotFound),
		revalidate: func(chainhash.Hash) error {
			revalidated = true
			return nil
		},
	}.run(t)

	rpcErr := requireRPCError(t, err)
	require.Equal(t, bsvjson.RPCErrorCode(-5), rpcErr.Code)
	require.Equal(t, "Block not found", rpcErr.Message)
	require.False(t, revalidated, "an unknown block is answered before anything tries to revalidate it")
}

// TestHandleReconsiderBlock_MissingAncestorIsNotAnUnknownBlock pins that a
// not-found inside a revalidation failure is not about the block the caller
// named. Revalidation asks for the parent's work (BlockValidation.go,
// GetNextWorkRequired on HashPrevBlock), and that miss used to answer -5 "Block
// not found" about a block that had just been fetched.
func TestHandleReconsiderBlock_MissingAncestorIsNotAnUnknownBlock(t *testing.T) {
	rpcErr := reconsiderBlockRPCError(t,
		errors.NewServiceError("[RevalidateBlock][%s] failed block re-validation", "0abc",
			errors.NewServiceError("[ValidateBlock][%s] failed to get expected work required", "0abc",
				errors.NewBlockNotFoundError("[Blockchain][GetNextWorkRequired] request's block hash is not valid",
					errors.ErrNotFound))))

	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code, "the named block exists; its revalidation failed")
	require.NotEqual(t, blockNotFoundMessage, rpcErr.Message)
	require.True(t, strings.HasPrefix(rpcErr.Message, "Block failed revalidation: "), rpcErr.Message)
}

// TestHandleReconsiderBlock_ValidBlockIsANoOpButItsChildrenAreReconsidered pins
// bitcoind's answer for a block that is not marked invalid: success. svnode's
// reconsiderblock calls ResetBlockFailureFlags, which clears the flag on the block
// and on every descendant, so a valid block's invalid children are still
// reconsidered. Revalidating the named block itself is what used to fail here:
// BlockValidation refuses a block that is not marked invalid, and the boundary
// rendered that refusal as "internal error".
func TestHandleReconsiderBlock_ValidBlockIsANoOpButItsChildrenAreReconsidered(t *testing.T) {
	named := chainhash.Hash{}
	child := &model.BlockHeader{Version: 1, HashPrevBlock: &named, HashMerkleRoot: &chainhash.Hash{}, Timestamp: 1}

	var revalidated []chainhash.Hash

	result, err := reconsiderFixture{
		valid: true,
		revalidate: func(h chainhash.Hash) error {
			revalidated = append(revalidated, h)

			if h == named {
				return errors.NewServiceError("[RevalidateBlock][%s] failed block re-validation", h.String(),
					errors.NewProcessingError("[ValidateBlock][%s] cannot revalidate block that is not marked as invalid", h.String()))
			}

			return nil
		},
		invalidBlocks: []*model.BlockInfo{{BlockHeader: child.Bytes()}},
	}.run(t)

	require.NoError(t, err, "reconsiderblock on a valid block succeeds, as it does in bitcoind")
	require.Nil(t, result)
	require.Equal(t, []chainhash.Hash{*child.Hash()}, revalidated,
		"the named block is not revalidated, its invalid child is")
}

func TestHandleReconsiderBlock_InvalidBlockKeepsItsReason(t *testing.T) {
	// The block exists and is bad. That must not be reported as -5, and the
	// caller needs the reject reason rather than the service breadcrumb.
	rpcErr := reconsiderBlockRPCError(t, blockInvalidChain())

	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code, "a block that exists and fails is not a lookup miss")
	require.Equal(t, "Block failed revalidation: bad-blk-txns-inputs-missingorspent", rpcErr.Message)
	require.NotContains(t, rpcErr.Message, "RevalidateBlock")
}

// overGRPC sends err across the encoding a real service boundary uses: WrapGRPC on
// the server, the status's wire bytes, UnwrapGRPC on the client. Not a live
// connection, but the same trailer the blockvalidation client decodes.
func overGRPC(t *testing.T, err error) error {
	t.Helper()

	st, ok := status.FromError(errors.WrapGRPC(err))
	require.True(t, ok, "WrapGRPC produces a status")

	wire := st.Proto()

	bytes, mErr := proto.Marshal(wire)
	require.NoError(t, mErr)

	proto.Reset(wire)
	require.NoError(t, proto.Unmarshal(bytes, wire))

	return errors.UnwrapGRPC(status.FromProto(wire).Err())
}

// revalidationFailure is how the blockvalidation server wraps every revalidation
// failure (Server.go, RevalidateBlock): a SERVICE_ERROR whose breadcrumb carries
// the rendered block, internal store id included.
func revalidationFailure(inner error) error {
	return errors.NewServiceError("[RevalidateBlock][%s] failed block re-validation",
		"Block 0000000000000000045a7e1b (height: 884212, id: 77123, txCount: 120000, size: 2400000000)", inner)
}

// blockVerdicts are the block-level failures a revalidation can return, each in
// the shape its producer builds, with the reason a caller should be shown.
func blockVerdicts() map[string]struct {
	err    error
	reason string
} {
	return map[string]struct {
		err    error
		reason string
	}{
		"invalid": {
			err:    errors.NewBlockInvalidError("[ValidateBlock][0abc] bad-blk-txns-inputs-missingorspent"),
			reason: "bad-blk-txns-inputs-missingorspent",
		},
		"policy declined": { // BlockValidation.go, excessiveblocksize
			err: errors.NewBlockPolicyDeclinedError("[ValidateBlock][%s] block size %d exceeds excessiveblocksize %d (local policy)",
				"0abc", 2400000000, 1000000000),
			reason: "block size 2400000000 exceeds excessiveblocksize 1000000000 (local policy)",
		},
		"corrupt, merkle root": { // model/Block.go
			err:    errors.NewBlockCorruptError("[BLOCK][%s] merkle root does not match", "0abc"),
			reason: "merkle root does not match",
		},
		"corrupt, subtree shape with the store id in a breadcrumb": { // model/Block.go
			err: errors.NewBlockCorruptError("[BLOCK][%s][ID %d] subtree %d has length %d, expected %d",
				"0abc", 77123, 3, 2, 4),
			reason: "subtree 3 has length 2, expected 4",
		},
		"corrupt over a transaction verdict": { // BlockValidation.go, unbound subtree list
			err: errors.NewBlockCorruptError("[ValidateBlock][%s] block contains invalid transactions", "0abc",
				errors.NewProcessingError("[processTransactionsInLevels] failed to process level",
					errors.NewTxInvalidError("[Validate][%s] error validating transaction", "9f2c",
						errors.New(errors.ERR_TX_INVALID, "bad-txns-inputs-duplicate")))),
			reason: "block contains invalid transactions: error validating transaction: bad-txns-inputs-duplicate",
		},
	}
}

// TestHandleReconsiderBlock_BlockVerdictsKeepTheirReasonAcrossGRPC covers every
// block-level class a revalidation returns, at both reconsiderblock sites, after
// the gRPC hop the blockvalidation client really makes. Main added
// BLOCK_POLICY_DECLINED and BLOCK_CORRUPT after this boundary was written, and
// both answered "internal error" - with the corrupt-over-transaction case logged
// only at debug, since its transaction verdict made it look like a caller fault.
func TestHandleReconsiderBlock_BlockVerdictsKeepTheirReasonAcrossGRPC(t *testing.T) {
	for name, tc := range blockVerdicts() {
		t.Run(name+", named block", func(t *testing.T) {
			logger := newCapturingLogger()

			_, err := reconsiderFixture{
				logger:     logger,
				revalidate: func(chainhash.Hash) error { return overGRPC(t, revalidationFailure(tc.err)) },
			}.run(t)

			rpcErr := requireRPCError(t, err)
			require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
			require.Equal(t, "Block failed revalidation: "+tc.reason, rpcErr.Message)
			require.NotContains(t, rpcErr.Message, "77123", "the store's internal block id must not reach the wire")
			require.NotEmpty(t, logger.errorf, "a failed revalidation leaves an error-level trace")
			require.Empty(t, logger.debugf, "and is not mistaken for a routine caller fault")
		})

		t.Run(name+", child", func(t *testing.T) {
			named := chainhash.Hash{}
			child := &model.BlockHeader{Version: 1, HashPrevBlock: &named, HashMerkleRoot: &chainhash.Hash{}, Timestamp: 1}

			_, err := reconsiderFixture{
				revalidate: func(h chainhash.Hash) error {
					if h == named {
						return nil
					}

					return overGRPC(t, revalidationFailure(tc.err))
				},
				invalidBlocks: []*model.BlockInfo{{BlockHeader: child.Bytes()}},
			}.run(t)

			rpcErr := requireRPCError(t, err)
			require.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code, "the site's own code: the named block succeeded")
			require.Equal(t, "Block "+named.String()+" was reconsidered but failed to reconsider children: "+tc.reason,
				rpcErr.Message)
		})
	}
}

// TestSubmitMiningSolution_CorruptBlockStaysInternal pins why BLOCK_CORRUPT is
// surfaced at the reconsiderblock sites only. submitminingsolution is open to
// limited callers and returns the blockassembly error bare; blockassembly wraps
// every non-invalid block.Valid failure in PROCESSING (blockassembly/Server.go),
// and for a block this node assembled a corrupt body is the node's own fault, not
// a verdict on what the miner sent.
func TestSubmitMiningSolution_CorruptBlockStaysInternal(t *testing.T) {
	s := &RPCServer{logger: newCapturingLogger()}

	reply, err := s.createMarshalledReply(1, nil, overGRPC(t,
		errors.NewProcessingError("[BlockAssembly][%s] block is not valid", "0abc",
			errors.NewBlockCorruptError("[BLOCK][%s] merkle root does not match", "0abc"))))
	require.NoError(t, err)

	var parsed bsvjson.Response
	require.NoError(t, json.Unmarshal(reply, &parsed))
	require.NotNil(t, parsed.Error)
	require.Equal(t, bsvjson.ErrRPCInternal.Code, parsed.Error.Code)
	require.Equal(t, genericErrorMessage, parsed.Error.Message)
}

// reconsiderFixture is the state reconsiderblock reads: whether the block the
// caller names exists and is marked invalid, what revalidating a block answers,
// and which invalid blocks the store holds for the children walk.
type reconsiderFixture struct {
	headerErr     error                      // GetBlockHeader's answer for the named block; nil means it exists
	valid         bool                       // the named block is NOT marked invalid
	revalidate    func(chainhash.Hash) error // nil revalidates everything successfully
	invalidBlocks []*model.BlockInfo
	logger        ulogger.Logger
}

// reconsideredHash is the block every reconsiderFixture names.
const reconsideredHash = "0000000000000000000000000000000000000000000000000000000000000000"

func (f reconsiderFixture) run(t *testing.T) (interface{}, error) {
	t.Helper()

	mockClient := &blockchain.Mock{}

	if f.headerErr != nil {
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, f.headerErr)
	} else {
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Invalid: !f.valid}, nil)
	}

	invalidBlocks := f.invalidBlocks
	if invalidBlocks == nil {
		invalidBlocks = []*model.BlockInfo{}
	}

	mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return(invalidBlocks, nil)

	revalidate := f.revalidate
	if revalidate == nil {
		revalidate = func(chainhash.Hash) error { return nil }
	}

	logger := f.logger
	if logger == nil {
		logger = ulogger.TestLogger{}
	}

	s := &RPCServer{
		logger:           logger,
		blockchainClient: mockClient,
		blockValidationClient: &mockBlockValidationClient{
			revalidateBlockFunc: func(_ context.Context, h chainhash.Hash) error {
				return revalidate(h)
			},
		},
		settings: &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams},
	}

	return handleReconsiderBlock(context.Background(), s, &bsvjson.ReconsiderBlockCmd{BlockHash: reconsideredHash}, nil)
}

// requireRPCError asserts a handler failed with a typed RPC error.
func requireRPCError(t *testing.T, err error) *bsvjson.RPCError {
	t.Helper()

	require.Error(t, err)

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok, "handlers must return a typed RPC error, not a bare Go error")

	return rpcErr
}

// reconsiderBlockRPCError runs reconsiderblock on a block that exists and is
// marked invalid, with its revalidation answering revalidateErr.
func reconsiderBlockRPCError(t *testing.T, revalidateErr error) *bsvjson.RPCError {
	t.Helper()

	result, err := reconsiderFixture{
		revalidate: func(chainhash.Hash) error { return revalidateErr },
	}.run(t)
	require.Nil(t, result)

	return requireRPCError(t, err)
}

// failingDecorateStore fails PreviousOutputsDecorate, which is how a submitted
// transaction spending an outpoint the node does not hold surfaces.
type failingDecorateStore struct {
	utxo.Store
	err error
}

func (f *failingDecorateStore) PreviousOutputsDecorate(_ context.Context, _ *bt.Tx) error {
	return f.err
}

// failingValidator fails Validate with a caller-supplied chain.
type failingValidator struct {
	validator.Interface
	err error
}

func (f failingValidator) Validate(_ context.Context, _ *bt.Tx, _ uint32, _ ...validator.Option) (*meta.Data, error) {
	return nil, f.err
}

// TestHandleSendRawTransaction_MissingInputsIsNotALookupMiss covers the site at
// the PreviousOutputsDecorate call. A blanket not-found mapping answered -5
// "No such mempool or blockchain transaction" here, about a transaction the
// caller had just supplied.
func TestHandleSendRawTransaction_MissingInputsIsNotALookupMiss(t *testing.T) {
	s := newRPCServerForAbsurdFeeTest(t, 10_000_000, 100_000_000, acceptingValidator{})
	s.utxoStore = &failingDecorateStore{
		err: errors.NewTxNotFoundError("previous tx not found: %v", "deadbeef:0"),
	}

	_, err := handleSendRawTransaction(context.Background(), s, buildSendRawTxCmd(t, 99_999_000, nil), nil)
	require.Error(t, err)

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok)
	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code,
		"bitcoind and svnode both answer -25 for a submission with missing inputs")
	require.NotEqual(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code,
		"-5 tells a wallet the transaction does not exist and must not be retried")
	require.Equal(t, "TX rejected: Missing inputs", rpcErr.Message)
}

// TestHandleSendRawTransaction_MissingInputsOnTheSQLStore is the same contract on
// a real SQL-backed store rather than a stub. Aerospike reported the missing
// parent as TX_NOT_FOUND and the SQL store as PROCESSING, so the same submission
// answered "Missing inputs" on one backend and "internal error" on the other.
func TestHandleSendRawTransaction_MissingInputsOnTheSQLStore(t *testing.T) {
	ctx := context.Background()

	storeURL, err := url.Parse("sqlitememory:///rpc_missing_inputs")
	require.NoError(t, err)

	store, err := utxosql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })

	// buildSendRawTxCmd spends an outpoint this store has never seen.
	s := newRPCServerForAbsurdFeeTest(t, 10_000_000, 100_000_000, acceptingValidator{})
	s.utxoStore = store

	_, err = handleSendRawTransaction(ctx, s, buildSendRawTxCmd(t, 99_999_000, nil), nil)

	rpcErr := requireRPCError(t, err)
	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
	require.Equal(t, "TX rejected: Missing inputs", rpcErr.Message)
}

// TestHandleSendRawTransaction_RejectionKeepsItsReason covers the site at the
// Validate call, with the chain the validator really builds.
func TestHandleSendRawTransaction_RejectionKeepsItsReason(t *testing.T) {
	s := newRPCServerForAbsurdFeeTest(t, 10_000_000, 100_000_000,
		failingValidator{err: validatorRejectionChain()})

	_, err := handleSendRawTransaction(context.Background(), s, buildSendRawTxCmd(t, 99_999_000, nil), nil)
	require.Error(t, err)

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok)
	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
	require.Contains(t, rpcErr.Message, "bad-txns-inputs-duplicate",
		"the submitter needs the reject reason")
	require.NotContains(t, rpcErr.Message, "[Validate]")
}

// TestHandleSendRawTransaction_MissingParentThroughValidateIsNotALookupMiss
// covers the same misclassification arriving via the validator's spend path
// rather than via decoration.
func TestHandleSendRawTransaction_MissingParentThroughValidateIsNotALookupMiss(t *testing.T) {
	s := newRPCServerForAbsurdFeeTest(t, 10_000_000, 100_000_000,
		failingValidator{err: missingParentChain()})

	_, err := handleSendRawTransaction(context.Background(), s, buildSendRawTxCmd(t, 99_999_000, nil), nil)
	require.Error(t, err)

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok)
	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
	require.Equal(t, "TX rejected: Missing inputs", rpcErr.Message)
}

// TestHandleSendRawTransaction_BatchedValidatorErrorIsTrimmed covers the wire form
// of a remote validator that batches. Its response carries each item's chain as
// *errors.TError (validator Server.go encodes with errors.Wrap), which is not an
// *errors.Error, so the boundary used to treat it as a foreign error and put the
// whole rendered chain on the wire - internal codes and breadcrumbs for a
// rejection, and the store's own text for a node fault.
func TestHandleSendRawTransaction_BatchedValidatorErrorIsTrimmed(t *testing.T) {
	for name, tc := range map[string]struct {
		err       error
		message   string
		errorLog  bool
		leaksWhat string
	}{
		"rejection": {
			err:       errors.Wrap(validatorRejectionChain()),
			message:   "TX rejected: bad-txns-inputs-duplicate",
			errorLog:  false,
			leaksWhat: "PROCESSING (4)",
		},
		"node fault": {
			err: errors.Wrap(errors.NewProcessingError("[Validate][%s] error spending utxos", "abcd1234",
				errors.NewStorageError("aerospike get failed",
					fmt.Errorf("dial tcp 10.0.0.5:3000: i/o timeout")))),
			message:   "TX rejected: " + genericErrorMessage,
			errorLog:  true,
			leaksWhat: "10.0.0.5:3000",
		},
	} {
		t.Run(name, func(t *testing.T) {
			logger := newCapturingLogger()
			s := newRPCServerForAbsurdFeeTest(t, 10_000_000, 100_000_000, failingValidator{err: tc.err})
			s.logger = logger

			_, err := handleSendRawTransaction(context.Background(), s, buildSendRawTxCmd(t, 99_999_000, nil), nil)
			require.Error(t, err)

			rpcErr, ok := err.(*bsvjson.RPCError)
			require.True(t, ok)
			require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
			require.Equal(t, tc.message, rpcErr.Message)
			require.NotContains(t, rpcErr.Message, tc.leaksWhat)

			if tc.errorLog {
				require.NotEmpty(t, logger.errorf, "a node fault is logged at error level")
			} else {
				require.Empty(t, logger.errorf, "a routine rejection is not a node fault")
				require.NotEmpty(t, logger.debugf)
			}
		})
	}
}

// TestCreateMarshalledReply_DoesNotReclassify is the getbestblockhash case.
// That handler returns its error bare, and GetBestBlockHeader reports an empty
// header table as BLOCK_NOT_FOUND. Mapping here answered -5 "Block not found"
// to a request that named no block at all.
func TestCreateMarshalledReply_DoesNotReclassify(t *testing.T) {
	s := &RPCServer{logger: newCapturingLogger()}

	reply, err := s.createMarshalledReply(1, nil, blockNotFoundChain())
	require.NoError(t, err)

	var parsed bsvjson.Response
	require.NoError(t, json.Unmarshal(reply, &parsed))
	require.NotNil(t, parsed.Error)

	require.Equal(t, bsvjson.ErrRPCInternal.Code, parsed.Error.Code,
		"this site has no idea what the request asked for, so it cannot claim the object was not found")
	require.NotEqual(t, bsvjson.ErrRPCInvalidAddressOrKey, parsed.Error.Code)
	require.NotContains(t, parsed.Error.Message, "sql: no rows in result set")
	require.NotContains(t, parsed.Error.Message, "RevalidateBlock")
}

// TestLogAndBuild_BlockRevalidationFailureIsLoggedAtErrorLevel pins ChiR12: a
// block that exists and fails validation is rare, admin-invoked and worth an
// operator's attention. Classifying it as a routine caller fault left the only
// record of the failure at debug, which is off at default levels - so the
// trimmed response became the whole story on exactly the path this file trims.
func TestLogAndBuild_BlockRevalidationFailureIsLoggedAtErrorLevel(t *testing.T) {
	logger := newCapturingLogger()
	s := &RPCServer{logger: logger}

	rpcErr := s.rpcLookupError(blockInvalidChain(), bsvjson.ErrRPCVerify, "Block failed revalidation: ")

	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
	require.NotEmpty(t, logger.errorf, "a failed revalidation must leave an error-level trace")
	require.Contains(t, logger.all(), "bad-blk-txns-inputs-missingorspent")
}

// TestStripBreadcrumb_DoesNotEatABracketedReason pins ChiR13. The reject reasons
// this file exists to preserve cross a CGO boundary from BDK, so their shape is
// not Teranode's to assume. An unbounded loop would peel a reason that
// legitimately opens with a bracket one token at a time, and one that is wholly
// bracketed down to nothing.
func TestStripBreadcrumb_DoesNotEatABracketedReason(t *testing.T) {
	require.Equal(t, "reason", stripBreadcrumb("[Service][arg] reason"),
		"the breadcrumbs Teranode does emit are still removed")

	require.Equal(t, "[mandatory-script-verify-flag-failed]",
		stripBreadcrumb("[mandatory-script-verify-flag-failed]"),
		"stripBreadcrumb never returns empty, so it hands the message back untouched")

	// But end to end it is suppressed, because a message that is nothing but
	// bracketed groups carries no reason to show. That is what sanitised's doc
	// always claimed and what it did not previously do. No producer of an
	// allowlisted class is known to emit one; suppressing is the safe direction
	// for a boundary whose job is disclosure control.
	require.Equal(t, genericErrorMessage, sanitised("[mandatory-script-verify-flag-failed]"))
	require.Equal(t, genericErrorMessage, sanitised("[Validate][abcd]"))
	require.Equal(t, "bad-txns", sanitised("[Validate][abcd] bad-txns"))

	// ChiR18: BlockValidation emits three groups, the third a rendered block
	// carrying the store's internal id. A fixed bound of two left it on the wire.
	require.Equal(t, "block is not valid",
		stripBreadcrumb("[Block Validation][checkOldBlockIDs][Block 0abc (height: 812345, id: 40912)] block is not valid"),
		"every leading marker is removed, however many a producer emits")
}

// TestRejectionMembershipFollowsTheSharedAllowlist pins ChiR14. This file used to
// keep its own eleven-entry list of what may be surfaced, and it had drifted from
// errors.publicCauseCodes in both directions at once.
func TestRejectionMembershipFollowsTheSharedAllowlist(t *testing.T) {
	t.Run("an immature coinbase does not surface the aerospike batch id", func(t *testing.T) {
		// errors.go excludes ERR_TX_COINBASE_IMMATURE because this producer embeds
		// the node's height and the store's process-lifetime spend-batch counter,
		// which two submissions diffed turn into a throughput oracle.
		//
		// The shape matters and an earlier version of this subtest got it wrong: it
		// put the immature link under a bare PROCESSING wrapper, where there is no
		// allowlisted link at all, so it passed without exercising the join. The
		// reachable shape has an allowlisted verdict AHEAD of the excluded one -
		// aerospike collects per-input errors in input order and errors.Join chains
		// them linearly (spend.go, JoinCapped), so a frozen input followed by an
		// immature-coinbase input produces exactly this.
		err := errors.NewUtxoError("error in aerospike spend (batched mode) - errors",
			errors.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] transaction is frozen, blockHeight %d - %s", "cafe", 12, "FROZEN",
				errors.NewTxCoinbaseImmatureError("[SPEND_BATCH_LUA][%s] coinbase is locked, blockHeight %d: %d - %s",
					"cafe", 12, 4711, "COINBASE_IMMATURE")))

		msg := publicErrorMessage(err)

		require.Contains(t, msg, "transaction is frozen", "the allowlisted verdict is still surfaced")
		require.NotContains(t, msg, "4711", "the internal batch id must not cross the boundary")
		require.NotContains(t, msg, "COINBASE_IMMATURE",
			"a code the shared allowlist excludes must not ride along below one it allows")
	})

	t.Run("a verdict does not launder the node fault beneath it", func(t *testing.T) {
		// The general form of the above. Being inside a verdict is not a licence to
		// surface whatever the verdict wraps: a class check on the first link only
		// let a StorageError's bucket name and a NetworkError's host:port through.
		for name, tc := range map[string]struct {
			err  error
			leak string
		}{
			"storage": {errors.NewTxInvalidError("bad-txns",
				errors.NewStorageError("s3 403 AccessDenied bucket=teranode-prod-subtrees")), "teranode-prod-subtrees"},
			"network": {errors.NewTxInvalidError("could not read tx from stream",
				errors.NewNetworkError("read tcp 10.0.1.5:34512->10.0.2.9:9000: connection reset by peer")), "10.0.2.9:9000"},
		} {
			t.Run(name, func(t *testing.T) {
				msg := publicErrorMessage(tc.err)

				require.NotContains(t, msg, tc.leak, "internal state must not cross the boundary under a verdict")
				require.NotEmpty(t, msg, "the verdict's own reason still survives")
			})
		}
	})

	t.Run("a still-creating parent keeps the signal to retry", func(t *testing.T) {
		// errors.go includes ERR_TX_CREATING on the grounds that collapsing this
		// family made an ordering artifact look like a permanent rejection, so
		// clients broadcasting a chain terminalized their own children.
		err := errors.NewProcessingError("[Validate][%s] error spending utxos", "abcd",
			errors.NewTxCreatingError("[SPEND_BATCH_LUA][%s] transaction is creating, blockHeight %d - %s",
				"cafe", 812345, "TX is being created and cannot be spent yet"))

		require.Contains(t, publicErrorMessage(err), "creating",
			"a submitter told 'internal error' here has no reason to retry")
	})

	t.Run("membership tracks errors.IsPublicCause", func(t *testing.T) {
		// The point of deriving rather than copying: a code removed from the
		// shared allowlist stops being surfaced here without anyone editing this
		// package. Asserting the agreement directly is what stops a copy creeping
		// back in.
		for _, code := range []errors.ERR{
			errors.ERR_TX_INVALID, errors.ERR_TX_POLICY, errors.ERR_TX_CREATING,
			errors.ERR_TX_LOCKED, errors.ERR_UTXO_NON_FINAL, errors.ERR_TX_COINBASE_IMMATURE,
			errors.ERR_PROCESSING, errors.ERR_SERVICE_ERROR,
		} {
			require.Equal(t, errors.IsPublicCause(code), linkIsRejection(errors.New(code, "x"), localRejectionCodes),
				"code %v must be surfaced here exactly when the shared allowlist says so", code)
		}
	})

	t.Run("a double spend does not surface the internal error code", func(t *testing.T) {
		// The most ordinary rejection there is, and the one shape the earlier
		// fixtures skipped. NewUtxoSpentError formatted errCodeMsgFmt into its own
		// message, so the code was INSIDE Message() rather than around it - which
		// defeats the join's whole safety argument, since it reads Message()
		// precisely because that never renders a code.
		txID := chainhash.Hash{}
		utxoHash := chainhash.Hash{}

		err := errors.NewProcessingError("[Validate][%s] error spending utxos", "abcd",
			errors.NewUtxoError("error in aerospike spend (batched mode) - errors",
				errors.NewUtxoSpentError(txID, 0, utxoHash, nil)))

		msg := publicErrorMessage(err)

		require.Contains(t, msg, "utxo already spent", "the caller still learns why")
		require.NotContains(t, msg, "UTXO_SPENT", "internal error codes must not cross the boundary")
		require.NotContains(t, msg, "(70)")
	})

	t.Run("a code render at a nested join position is stripped too", func(t *testing.T) {
		// Pins the strip inside add(), which the leading-position case cannot: the
		// sanitised() call is anchored at the front of the JOINED string, so it
		// never sees a code render that arrives at the second part. That is the
		// position NewUtxoSpentError actually occupies - it is a leaf, always
		// joined under a validator wrapper - so this is the half that matters.
		err := errors.NewTxInvalidError("bad-txns-in-belowout",
			errors.NewTxPolicyError("UTXO_SPENT (70): abcd:0 utxo already spent by tx ef"))

		msg := publicErrorMessage(err)

		require.Equal(t, "bad-txns-in-belowout: abcd:0 utxo already spent by tx ef", msg)
		require.NotContains(t, msg, "UTXO_SPENT")
	})

	t.Run("a code render in front of a breadcrumb does not smuggle the breadcrumb through", func(t *testing.T) {
		// The two strips are order-dependent and each can hide the other. Stripping
		// breadcrumbs first leaves this message's marker no longer leading, so it
		// survives and only the code is removed - putting [Spend][...] on the wire.
		// stripInternalPrefixes iterates to a fixed point, which has neither
		// ordering's blind spot.
		err := errors.NewTxInvalidError("bad-txns-in-belowout",
			errors.NewTxPolicyError("TX_POLICY (39): [Spend][deadbeef:0] utxo is frozen"))

		msg := publicErrorMessage(err)

		require.Equal(t, "bad-txns-in-belowout: utxo is frozen", msg)
		require.NotContains(t, msg, "[Spend]", "internal markers must not survive a code render in front of them")
		require.NotContains(t, msg, "TX_POLICY")
	})

	t.Run("a producer that renders its own code is defanged at the boundary", func(t *testing.T) {
		// The producer above is fixed, so this pins the guard rather than the bug:
		// the boundary must not depend on every producer getting it right.
		err := errors.NewTxInvalidError("TX_INVALID (31): bad-txns-inputs-duplicate")

		msg := publicErrorMessage(err)

		require.Equal(t, "bad-txns-inputs-duplicate", msg)
		require.NotContains(t, msg, "TX_INVALID")
	})

	t.Run("the local additions are block-level, and corrupt is revalidation-only", func(t *testing.T) {
		// The shared list is scoped to transaction verdicts. These are what this
		// surface adds, and each one is justified in localRejectionClasses for every
		// caller that can reach it - limited-tier submitminingsolution included.
		for _, code := range []errors.ERR{errors.ERR_BLOCK_INVALID, errors.ERR_BLOCK_POLICY_DECLINED, errors.ERR_BLOCK_CORRUPT} {
			require.False(t, errors.IsPublicCause(code), "code %v is local to this surface", code)
		}

		require.True(t, linkIsRejection(errors.NewBlockInvalidError("x"), localRejectionCodes))
		require.True(t, linkIsRejection(errors.NewBlockPolicyDeclinedError("x"), localRejectionCodes))
		require.Len(t, localRejectionClasses, 2, "a further global addition needs its own justification")

		// BLOCK_CORRUPT is a verdict on reconsiderblock only. On
		// submitminingsolution it describes a block this node built.
		require.False(t, linkIsRejection(errors.NewBlockCorruptError("x"), localRejectionCodes))
		require.True(t, linkIsRejection(errors.NewBlockCorruptError("x"), scopeBlockRevalidation.verdicts()))
		require.Len(t, revalidationRejectionClasses, 1, "a further revalidation-only addition needs its own justification")
	})
}

// TestBoundaryConstantsArePinned exists because the tests that use these
// constants build their fixtures FROM them, so they scale with any change and
// can never notice one. An adversarial pass found three mutations that left the
// whole suite green: halving the part cap (silently truncating real reasons),
// quadrupling it (widening what a chain can surface), and cutting the depth cap
// to 3 (collapsing ordinary production chains to "internal error").
//
// Pinning the values makes a change deliberate rather than silent.
func TestBoundaryConstantsArePinned(t *testing.T) {
	require.Equal(t, 32, maxMessageChainDepth,
		"bounds both walks; raising it raises the cost of the string-building one")
	require.Equal(t, 4, maxRejectionReasonParts,
		"one more than the deepest real verdict chain, so the reason cannot be crowded out")
}

// TestRejectionReasonBoundIsIndependentlyPinned covers the direction
// TestRejectionDepthCapsAgree cannot: that test builds its chain FROM the
// constant, so it scales with any change and only fails when isRejection's bound
// moves. Widening rejectionReason's own walk survived it, and that is the
// expensive, string-building walk the DoS rationale is actually about.
//
// The shape matters. Filling the chain with verdicts does not work, because the
// part cap fills after four links and the walk stops for that reason instead -
// so the depth bound is masked and the mutation survives anyway, which is what a
// first attempt at this test did. Non-verdict links add nothing, so the walk has
// to traverse all of them to reach the reason, and only the depth bound can stop
// it.
func TestRejectionReasonBoundIsIndependentlyPinned(t *testing.T) {
	// Literal lengths, deliberately not derived from the constant.
	const (
		breadcrumbs = 100
		reason      = "innermost reason"
	)

	var err error = errors.NewTxInvalidError(reason)
	for i := 0; i < breadcrumbs; i++ {
		err = errors.NewProcessingError("wrapper %d", i, err)
	}

	var tErr *errors.Error
	require.ErrorAs(t, err, &tErr)

	require.NotContains(t, rejectionReason(tErr, localRejectionCodes), reason,
		"a verdict 100 links down must not be reached by a walk bounded at 32")
}

// TestBlockLookupDoesNotAnswerAboutTransactions pins the scope split. One shared
// lookup scope mapped both not-found classes, so a request naming a block and
// nothing else answered "No such mempool or blockchain transaction" whenever its
// failure carried an ErrTxNotFound underneath it, and discarded its own prefix
// doing so. The fixture is the shape that first exposed it: BlockValidation
// raises a transient incomplete-block error wrapping a missing transaction.
// reconsiderblock no longer reaches it through a lookup site (it resolves the
// named block first), but the split is a property of the scope, not of one
// handler.
func TestBlockLookupDoesNotAnswerAboutTransactions(t *testing.T) {
	logger := newCapturingLogger()
	s := &RPCServer{logger: logger}

	err := errors.NewServiceError("[RevalidateBlock][%s] failed block re-validation", "0abc",
		errors.NewBlockIncompleteTransientError("missing subtree data",
			errors.NewTxNotFoundError("tx cafe not found")))

	rpcErr := s.rpcLookupError(err, bsvjson.ErrRPCVerify, "Block failed revalidation: ")

	require.NotEqual(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code,
		"a request that named a block must not be answered as a missing transaction")
	require.NotContains(t, rpcErr.Message, txNotFoundMessage)
	require.True(t, strings.HasPrefix(rpcErr.Message, "Block failed revalidation: "),
		"and the site's prefix must survive, since the mapped branch is not taken")

	require.NotEmpty(t, logger.errorf,
		"an admin-invoked reconsiderblock failure that is not a missing block leaves an error-level trace")
}

// policyRejectionChain is what reaches handleSendRawTransaction for every BDK
// policy rejection - NOT_STANDARD, SIGOPS, NOT_FREE_CONSOLIDATION and the
// policy-class script errors. mapBDKValidationError puts errMsgPolicy under
// errMsgInvalidTx above the reason (services/validator/ScriptVerifierGoBDK.go),
// and validateInternal adds its breadcrumb on top: four links, two of them
// constants naming the adapter.
func policyRejectionChain() error {
	return errors.NewProcessingError("[Validate][%s] error validating transaction", "abcd1234",
		errors.NewTxInvalidError(validator.ErrMsgInvalidTx,
			errors.NewTxPolicyError(validator.ErrMsgPolicy,
				errors.New(errors.ERR_TX_POLICY, "%s",
					"non-mandatory-script-verify-flag (Operation not valid with the current stack size)"))))
}

// TestRPCError_TrimsTheRealPolicyChain covers the deepest real verdict chain. Its
// two adapter constants are what uninformativeVerdictText suppresses, and the
// chain is exactly as deep as the part cap allows, so dropping either constant
// from the map or lowering the cap changes what a policy rejection says.
func TestRPCError_TrimsTheRealPolicyChain(t *testing.T) {
	rpcErr := buildRPCError(policyRejectionChain(), bsvjson.ErrRPCVerify, txRejectedPrefix, scopeSubmit)

	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
	require.Equal(t,
		"TX rejected: non-mandatory-script-verify-flag (Operation not valid with the current stack size)",
		rpcErr.Message,
		"the reason alone: no breadcrumb and neither adapter constant")
}

// TestRejectionDepthCaps_BindAtTheBoundary pins both chain walks at the exact
// depth they stop, from both sides. TestRejectionDepthCapsAgree buries the verdict
// far past the cap, so it only notices a walk that goes too far; a walk cut short,
// or one stretched to just past the cap, survived it.
func TestRejectionDepthCaps_BindAtTheBoundary(t *testing.T) {
	buried := func(wrappers int) error {
		var err error = errors.NewTxInvalidError("bad-txns-inputs-duplicate")
		for i := 0; i < wrappers; i++ {
			err = errors.NewProcessingError("wrapper %d", i, err)
		}

		return err
	}

	t.Run("the last reachable link", func(t *testing.T) {
		err := buried(maxMessageChainDepth - 1)

		require.True(t, isRejection(err, localRejectionCodes), "a verdict at the last walked link is reachable")
		require.Equal(t, "bad-txns-inputs-duplicate", publicErrorMessage(err),
			"and the message walk reaches it too")
	})

	t.Run("the first unreachable link", func(t *testing.T) {
		err := buried(maxMessageChainDepth)

		require.False(t, isRejection(err, localRejectionCodes), "one link further is past the cap")
		require.Equal(t, genericErrorMessage, publicErrorMessage(err))
	})
}

// failingTxStore fails every Set the way the file blob store does, with the path
// in the message.
type failingTxStore struct {
	blob.Store
}

func (failingTxStore) Set(context.Context, []byte, fileformat.FileType, []byte, ...options.FileOption) error {
	return errors.NewStorageError("[File][Set] failed to write /var/teranode/txstore/ab/cd/abcd.tx",
		fmt.Errorf("open /var/teranode/txstore/ab/cd/abcd.tx: no space left on device"))
}

// TestHandleSendRawTransaction_TxStoreFailureIsInternal covers the blob store
// site. Every other handler test leaves txStore nil, so reverting this site to
// splicing the error left the suite green - and the file store's errors carry
// paths.
func TestHandleSendRawTransaction_TxStoreFailureIsInternal(t *testing.T) {
	logger := newCapturingLogger()
	s := newRPCServerForAbsurdFeeTest(t, 10_000_000, 100_000_000, acceptingValidator{})
	s.logger = logger
	s.txStore = failingTxStore{Store: blobmemory.New()}

	_, err := handleSendRawTransaction(context.Background(), s, buildSendRawTxCmd(t, 99_999_000, nil), nil)

	rpcErr := requireRPCError(t, err)
	require.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
	require.Equal(t, "Failed to store transaction: "+genericErrorMessage, rpcErr.Message)
	require.NotEmpty(t, logger.errorf)
	require.Contains(t, logger.all(), "no space left on device", "the cause goes to the log")
}

// TestBlockLookupHandlers_AnswerAnUnknownBlockAsBitcoind pins the -5 mapping at
// each handler that names a block by hash. The handler tests fed a generic error,
// so swapping any of these sites back to rpcError left the suite green.
func TestBlockLookupHandlers_AnswerAnUnknownBlockAsBitcoind(t *testing.T) {
	hash := strings.Repeat("00", 31) + "ab"
	verbosity := uint32(1)
	verbose := true

	notFound := func(t *testing.T) error {
		return overGRPC(t, errors.NewBlockNotFoundError("error in GetBlockHeader", errors.ErrNotFound))
	}

	for name, tc := range map[string]struct {
		mock func(*testing.T, *blockchain.Mock)
		call func(*RPCServer) (interface{}, error)
	}{
		"getblock": {
			mock: func(t *testing.T, m *blockchain.Mock) {
				m.On("GetBlock", mock.Anything, mock.Anything).Return(nil, notFound(t))
			},
			call: func(s *RPCServer) (interface{}, error) {
				return handleGetBlock(context.Background(), s, &bsvjson.GetBlockCmd{Hash: hash, Verbosity: &verbosity}, nil)
			},
		},
		"getblockheader": {
			mock: func(t *testing.T, m *blockchain.Mock) {
				m.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, notFound(t))
			},
			call: func(s *RPCServer) (interface{}, error) {
				return handleGetBlockHeader(context.Background(), s, &bsvjson.GetBlockHeaderCmd{Hash: hash, Verbose: &verbose}, nil)
			},
		},
		"invalidateblock": {
			mock: func(t *testing.T, m *blockchain.Mock) {
				m.On("InvalidateBlock", mock.Anything, mock.Anything).Return(([]chainhash.Hash)(nil), notFound(t))
			},
			call: func(s *RPCServer) (interface{}, error) {
				return handleInvalidateBlock(context.Background(), s, &bsvjson.InvalidateBlockCmd{BlockHash: hash}, nil)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := &blockchain.Mock{}
			tc.mock(t, m)

			s := &RPCServer{
				logger:           newCapturingLogger(),
				blockchainClient: m,
				settings:         &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams},
			}

			_, err := tc.call(s)

			rpcErr := requireRPCError(t, err)
			require.Equal(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code)
			require.Equal(t, blockNotFoundMessage, rpcErr.Message)
		})
	}
}

// TestHandleReconsiderBlock_MissingDescendantIsNotAnUnknownBlock pins the children
// branch's scope at the handler. The named block was reconsidered; a descendant
// whose revalidation finds something missing must not answer "Block not found"
// about the block the caller named, and the prefix saying which block succeeded
// has to survive.
func TestHandleReconsiderBlock_MissingDescendantIsNotAnUnknownBlock(t *testing.T) {
	named := chainhash.Hash{}
	child := &model.BlockHeader{Version: 1, HashPrevBlock: &named, HashMerkleRoot: &chainhash.Hash{}, Timestamp: 1}

	_, err := reconsiderFixture{
		revalidate: func(h chainhash.Hash) error {
			if h == named {
				return nil
			}

			return overGRPC(t, revalidationFailure(childNotFoundChain()))
		},
		invalidBlocks: []*model.BlockInfo{{BlockHeader: child.Bytes()}},
	}.run(t)

	rpcErr := requireRPCError(t, err)
	require.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
	require.NotEqual(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code)
	require.True(t, strings.HasPrefix(rpcErr.Message,
		"Block "+named.String()+" was reconsidered but failed to reconsider children: "), rpcErr.Message)
}
