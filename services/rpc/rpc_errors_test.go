package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		errors.NewTxInvalidError("GoBDK fail to ValidateTransaction",
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

func (l *capturingLogger) Debugf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.debugf = append(l.debugf, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return strings.Join(append(append([]string{}, l.errorf...), l.debugf...), "\n")
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
		errors.NewTxInvalidError("GoBDK fail to ValidateTransaction", fmt.Errorf("bad-txns-inputs-duplicate")))

	require.True(t, isRejection(breadcrumb), "the chain-wide test matches the breadcrumb link too")

	var tErr *errors.Error
	require.ErrorAs(t, breadcrumb, &tErr)
	require.False(t, linkIsRejection(tErr), "the per-link test must not")

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

func TestPublicErrorMessage_NonTeranodeErrorKeepsItsText(t *testing.T) {
	// bsvutil.DecodeAddress and bt.NewTxFromBytes return plain errors whose
	// text is the reason and which have no chain to strip.
	require.Equal(t, "plain failure", publicErrorMessage(fmt.Errorf("plain failure")))
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
	err := errors.NewTxInvalidError("innermost reason")
	for i := 0; i < maxMessageChainDepth*4; i++ {
		err = errors.NewTxInvalidError("wrapper", err)
	}

	done := make(chan string, 1)
	go func() { done <- publicErrorMessage(err) }()

	select {
	case msg := <-done:
		require.NotEmpty(t, msg)
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

	require.False(t, isRejection(err),
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

	require.Equal(t, "TX rejected: GoBDK fail to ValidateTransaction: bad-txns-inputs-duplicate", rpcErr.Message,
		"the caller keeps the prefix and every link of the verdict, and loses the service breadcrumb")
	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
}

// ---------------------------------------------------------------------------
// Reclassification is per call site
// ---------------------------------------------------------------------------

func TestLookupError_NotFoundMatchesBitcoind(t *testing.T) {
	rpcErr := buildRPCError(blockNotFoundChain(), bsvjson.ErrRPCVerify, "", scopeLookup)

	require.Equal(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code,
		"an unknown block is -5 (RPC_INVALID_ADDRESS_OR_KEY), matching bitcoind, not -25 (RPC_VERIFY_ERROR)")
	require.Equal(t, blockNotFoundMessage, rpcErr.Message,
		"the mapped message is verbatim so callers porting from bitcoind match on it")
}

func TestLookupError_TxNotFoundMatchesBitcoind(t *testing.T) {
	rpcErr := buildRPCError(errors.NewTxNotFoundError("no tx"), bsvjson.ErrRPCVerify, "", scopeLookup)

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
func TestHandleReconsiderBlock_UnknownBlockMatchesBitcoind(t *testing.T) {
	rpcErr := reconsiderBlockRPCError(t, blockNotFoundChain())

	require.Equal(t, bsvjson.RPCErrorCode(-5), rpcErr.Code)
	require.Equal(t, "Block not found", rpcErr.Message)
}

func TestHandleReconsiderBlock_InvalidBlockKeepsItsReason(t *testing.T) {
	// The block exists and is bad. That must not be reported as -5, and the
	// caller needs the reject reason rather than the service breadcrumb.
	rpcErr := reconsiderBlockRPCError(t, blockInvalidChain())

	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code, "a block that exists and fails is not a lookup miss")
	require.Equal(t, "Block failed revalidation: bad-blk-txns-inputs-missingorspent", rpcErr.Message)
	require.NotContains(t, rpcErr.Message, "RevalidateBlock")
}

func reconsiderBlockRPCError(t *testing.T, revalidateErr error) *bsvjson.RPCError {
	t.Helper()

	mockClient := &blockchain.Mock{}
	mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

	s := &RPCServer{
		logger:           ulogger.TestLogger{},
		blockchainClient: mockClient,
		blockValidationClient: &mockBlockValidationClient{
			revalidateBlockFunc: func(_ context.Context, _ chainhash.Hash) error {
				return revalidateErr
			},
		},
		settings: &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams},
	}

	cmd := &bsvjson.ReconsiderBlockCmd{
		BlockHash: "0000000000000000000000000000000000000000000000000000000000000000",
	}

	result, err := handleReconsiderBlock(context.Background(), s, cmd, nil)
	require.Error(t, err)
	require.Nil(t, result)

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok, "handlers must return a typed RPC error, not a bare Go error")

	return rpcErr
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

	require.Equal(t, "[16] is not a valid stack size",
		stripBreadcrumb("[Validate][abcd] [16] is not a valid stack size"),
		"a reason opening with a bracket survives once the two real breadcrumbs are off")

	require.Equal(t, "[mandatory-script-verify-flag-failed]",
		stripBreadcrumb("[mandatory-script-verify-flag-failed]"),
		"a wholly bracketed reason is kept rather than stripped to nothing")
}
