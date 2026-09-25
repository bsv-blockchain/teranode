/*
Package validator implements BSV Blockchain transaction validation functionality.

This package provides comprehensive transaction validation for BSV Blockchain nodes,
including BDK transaction validation, UTXO management, and policy enforcement.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - BDK transaction validation
  - Policy enforcement
  - Block assembly integration
  - Kafka integration for transaction metadata

Usage:

	validator := NewTxValidator(logger, policy, params)
	err := validator.ValidateTransaction(tx, blockHeight, nil)
*/
package validator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/settings"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// batchHandoffTimeout backstops the batch hand-off wait so a dispatcher that never signals
// (panic, missed code path, wedged transport) releases the submitter instead of parking it for
// the life of the process. It must OUTLAST the deepest downstream wait this client fronts, or
// it aborts work the lower layers would still have completed — see the settings.conf note on
// aerospike_batchPolicy.docker.m for that exact bug happening one layer down.
//
// Sized against the committed default configuration:
//
//   - the utxo store's own submitter guard, which the downstream service waits on:
//     batch TotalTimeout (settings.conf: 5m) plus aerospike_overload_retry_max_elapsed (2m)
//     plus 30s grace, i.e. 7m30s
//   - the store's spend wait, a separate sequential stage: utxostore_spendWaitTimeout (30s)
//
// 8m floor, rounded to 10m. Client-side queueing is deliberately NOT modelled: the flush
// interval (validator_sendBatchTimeout) is a batching trigger, not an end-to-end bound, and
// worker saturation or the batcher's max-concurrency limiter can hold an item longer than it.
// A var, not a const, purely so tests can shorten it; production never reassigns it.
var batchHandoffTimeout = 10 * time.Minute

// batchItem represents a single item in a validation batch request
type batchItem struct {
	// req contains the validation request for a single transaction
	req *validator_api.ValidateTransactionRequest

	// group is the shared completion group the submitter waits on; the
	// dispatcher calls Done exactly once per item via complete.
	group *completion.Group

	// completed guards exactly-once completion (CAS).
	completed atomic.Bool

	// result holds the validation outcome (metadata + error) for this item.
	// Written by the CAS winner, after the CAS and before group.Done(); safe
	// to read only once group.Wait returns nil.
	result validateBatchResponse
}

// complete writes resp into the item's result slot and marks the shared
// group's completion counter. Idempotent: only the first call has any effect,
// so a panic-recovery sweep over an already-completed item never
// double-signals or races a second write into result.
func (it *batchItem) complete(resp validateBatchResponse) {
	if it.completed.CompareAndSwap(false, true) {
		it.result = resp
		if it.group != nil {
			it.group.Done()
		}
	}
}

// Client implements a gRPC client for the validator service, providing transaction
// validation capabilities through remote procedure calls
type Client struct {
	// client is the gRPC client implementation for validator API calls
	client validator_api.ValidatorAPIClient

	// running indicates whether the client is currently operational
	running *atomic.Bool

	// conn holds the gRPC connection to the validator service
	conn *grpc.ClientConn

	// logger provides logging functionality for the client
	logger ulogger.Logger

	// batchSize defines the maximum number of transactions to batch together
	// for validation requests
	batchSize int

	// batchTimeout defines the maximum time to wait for batching transactions
	// before sending the batch, in milliseconds
	batchTimeout int

	// batcher handles the batching of transaction validation requests
	batcher *batcher.Batcher[batchItem]

	// validatorHTTPAddr holds the HTTP endpoint address for validator fallback
	validatorHTTPAddr *url.URL
}

// NewClient creates and initializes a new validator client
//
// Parameters:
//   - ctx: Context for client initialization
//   - logger: Logger instance for client operations
//
// Returns:
//   - *Client: Initialized client instance
//   - error: Any error encountered during initialization
func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*Client, error) {
	validatorGrpcAddress := tSettings.Validator.GRPCAddress
	if validatorGrpcAddress == "" {
		return nil, errors.NewConfigurationError("missing validator_grpcAddress")
	}

	conn, err := util.GetGRPCClient(ctx, validatorGrpcAddress, &util.ConnectionOptions{
		MaxRetries: 3,
		CallerName: "validator",
	}, tSettings)

	if err != nil {
		return nil, err
	}

	grpcClient := validator_api.NewValidatorAPIClient(conn)

	sendBatchSize := tSettings.Validator.SendBatchSize
	sendBatchTimeout := tSettings.Validator.SendBatchTimeout

	running := atomic.Bool{}
	running.Store(true)

	client := &Client{
		client:            grpcClient,
		logger:            logger,
		running:           &running,
		conn:              conn,
		batchSize:         sendBatchSize,
		batchTimeout:      sendBatchTimeout,
		validatorHTTPAddr: tSettings.Validator.HTTPAddress,
	}

	if sendBatchSize > 0 {
		sendBatch := func(batch []*batchItem) {
			client.sendBatchToValidator(ctx, batch)
		}
		duration := time.Duration(sendBatchTimeout) * time.Millisecond
		// Hold the batcher by pointer: SetTickInterval mutates per-instance state
		// (the ticker) and the v2.0.3 Batcher contains a sync.Pool, so a value
		// copy would both drop the tick config and trip go vet copylocks.
		// Default 0 = disabled.
		bp := batcher.NewWithPool(sendBatchSize, duration, sendBatch, true,
			batcher.WithName("validator_client"),
			batcher.WithLogger(logger),
			batcher.WithMetrics(batchermetrics.Provider()),
			batcher.WithTracer(tracing.Tracer("validator").OTelTracer()),
		)
		if ms := tSettings.Validator.SendBatchTickerIntervalMillis; ms > 0 {
			bp.SetTickInterval(time.Duration(ms) * time.Millisecond)
		}
		client.batcher = bp
	}

	return client, nil
}

// Close gracefully shuts down the validator client: it drains the request
// batcher (flushing queued validations while the connection is still live) and
// then releases the gRPC connection it dialed in NewClient. The bounded drain
// runs BEFORE the conn close.
func (c *Client) Close() error {
	if c.batcher != nil {
		util.DrainBatcher(c.logger, "validator_client", util.DefaultBatcherDrainTimeout, c.batcher.Close)
	}

	if c.conn != nil {
		return c.conn.Close()
	}

	return nil
}

// Health checks the health of the remote validator service. When checkLiveness is true,
// only a local liveness check is performed. Otherwise, a full readiness check is made
// via gRPC to verify the validator and its dependencies are operational.
func (c *Client) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	res, err := c.client.HealthGRPC(ctx, &validator_api.EmptyMessage{})
	if !res.GetOk() || err != nil {
		return http.StatusFailedDependency, res.GetDetails(), errors.UnwrapGRPC(err)
	}

	return http.StatusOK, res.GetDetails(), nil
}

// GetBlockHeight returns the current block height from the remote validator service.
// Returns zero if the gRPC call fails.
func (c *Client) GetBlockHeight() uint32 {
	resp, err := c.client.GetBlockHeight(context.Background(), &validator_api.EmptyMessage{})
	if err != nil {
		return 0
	}

	return resp.Height
}

// GetMedianBlockTime returns the median timestamp of the last 11 blocks from the
// remote validator service. This value is used for transaction locktime validation.
// Returns zero if the gRPC call fails.
func (c *Client) GetMedianBlockTime() uint32 {
	resp, err := c.client.GetMedianBlockTime(context.Background(), &validator_api.EmptyMessage{})
	if err != nil {
		return 0
	}

	return resp.MedianTime
}

// TriggerBatcher forces the transaction batch processor to immediately send any
// queued validation requests. This is a no-op when batching is disabled (batchSize == 0).
func (c *Client) TriggerBatcher() {
	if c.batchSize > 0 {
		c.batcher.Trigger()
	}
}

// EnsureMTPLoaded is a no-op on the gRPC client. The remote validator service manages
// its own in-memory MTP store; EnsureMTPLoaded is called server-side before concurrent
// per-transaction goroutines start.
func (c *Client) EnsureMTPLoaded(_ context.Context, _ uint32) error {
	return nil
}

// candidateBlockTimePtr returns a pointer to opts.CandidateBlockTime when the
// value is non-zero, and nil otherwise. Keeping the proto field absent on the
// wire for the common policy-mode case (where CandidateBlockTime is always 0)
// avoids unnecessary per-request bytes on the validator hot path; the field is
// only meaningful for block-validation callers passing a candidate block
// header timestamp. Returns &opts.CandidateBlockTime directly so the pointer
// targets the caller's existing struct field (no per-request allocation),
// matching the pattern of the other request fields built from the same opts.
// unconfirmedParentsAtCandidateHeightPtr returns a pointer to true when the
// option is set and nil otherwise, so the optional proto field is only put on
// the wire for the rare legacy-sync requests that actually use it. nil and
// explicit-false reconstruct identically server-side (optionsFromValidateRequest
// leaves the default false).
func unconfirmedParentsAtCandidateHeightPtr(opts *Options) *bool {
	if !opts.UnconfirmedParentsAtCandidateHeight {
		return nil
	}

	return &opts.UnconfirmedParentsAtCandidateHeight
}

func candidateBlockTimePtr(opts *Options) *uint32 {
	if opts.CandidateBlockTime == 0 {
		return nil
	}

	return &opts.CandidateBlockTime
}

// candidateParentMedianTimePtr mirrors candidateBlockTimePtr for the
// post-CSV consensus path. Returns &opts.CandidateParentMedianTime directly
// when non-zero so the wire write is no-copy. Returns nil only when the
// field is genuinely unset — the server-side selectFinalityComparisonTime
// hard-errors on absent values for post-CSV consensus requests, so the
// nil-encoded branch exists only for policy-mode and pre-CSV consensus
// requests (where the field is not consumed).
func candidateParentMedianTimePtr(opts *Options) *uint32 {
	if opts.CandidateParentMedianTime == 0 {
		return nil
	}

	return &opts.CandidateParentMedianTime
}

// buildValidateTxRequest constructs the gRPC ValidateTransactionRequest from
// raw transaction bytes, block height, and validation options. Shared by the
// client's non-batch / batch send paths AND by the server's HTTP /tx, /txs
// handlers (which receive raw bytes from the request body) so the wire
// representation cannot diverge between any caller.
func buildValidateTxRequest(transactionData []byte, blockHeight uint32, opts *Options) *validator_api.ValidateTransactionRequest {
	return &validator_api.ValidateTransactionRequest{
		TransactionData:                     transactionData,
		BlockHeight:                         blockHeight,
		SkipUtxoCreation:                    &opts.SkipUtxoCreation,
		AddTxToBlockAssembly:                &opts.AddTXToBlockAssembly,
		SkipPolicyChecks:                    &opts.SkipPolicyChecks,
		CreateConflicting:                   &opts.CreateConflicting,
		SkipTxmetaPublishing:                &opts.SkipTxMetaPublishing,
		InBlock:                             &opts.InBlock,
		CandidateBlockTime:                  candidateBlockTimePtr(opts),
		CandidateParentMedianTime:           candidateParentMedianTimePtr(opts),
		UnconfirmedParentsAtCandidateHeight: unconfirmedParentsAtCandidateHeightPtr(opts),
		SkipScriptValidation:                &opts.SkipScriptValidation,
		OutpointOnlySpend:                   &opts.OutpointOnlySpend,
		SpenderCreatedByCaller:              &opts.SpenderCreatedByCaller,
		IgnoreLocked:                        &opts.IgnoreLocked,
	}
}

// Validate performs transaction validation by applying the given options and delegating
// to ValidateWithOptions. See ValidateWithOptions for details on the validation flow.
func (c *Client) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*utxometa.Data, error) {
	validationOptions := NewDefaultOptions()
	for _, opt := range opts {
		opt(validationOptions)
	}

	return c.ValidateWithOptions(ctx, tx, blockHeight, validationOptions)
}

type validateBatchResponse struct {
	metaData []byte
	err      error
}

// ValidateWithOptions validates a transaction against the remote validator service.
// In non-batch mode, the transaction is sent directly via gRPC. In batch mode, it is
// queued and sent as part of a batch. If the gRPC message size limit is exceeded, the
// client falls back to HTTP validation automatically.
func (c *Client) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *utxometa.Data, err error) {
	if c.batchSize == 0 {
		// Non-batch mode: direct validation
		response, err := c.client.ValidateTransaction(ctx, buildValidateTxRequest(tx.SerializeBytes(), blockHeight, validationOptions))
		if err != nil {
			c.logger.Errorf("[ValidateWithOptions] failed to validate non-batched transaction: %v", err)
			return nil, c.handleValidationError(ctx, tx, blockHeight, validationOptions, err)
		}

		result := &utxometa.Data{}

		if err = utxometa.NewMetaDataFromBytes(response.Metadata, result); err != nil {
			c.logger.Errorf("[ValidateWithOptions] failed to parse metadata: %v", err)
			return nil, err
		}

		return result, nil
	}

	// Batch mode
	group := completion.NewGroup(1)
	item := &batchItem{
		req:   buildValidateTxRequest(tx.SerializeBytes(), blockHeight, validationOptions),
		group: group,
	}
	c.batcher.PutCtx(ctx, item)

	// Bounded by the CALLER's context AND a finite backstop. Nothing in the call
	// chain guarantees a deadline, so nothing bounds this wait during normal
	// operation and the ctx arm alone would let a wedged dispatcher park this
	// goroutine; batchHandoffTimeout is what makes the wait finite.
	//
	// An early return is ABANDONMENT, not cancellation: the item is already on the
	// batcher and the dispatcher may still send it, so the transaction may yet
	// reach the validator. Two consequences, both load-bearing:
	//
	//   - item.result MUST NOT be read on this path. It is a struct value the
	//     dispatcher writes later from its own goroutine; not reading it is what
	//     keeps that write race-free (no concurrent reader), and it is why the read
	//     below is reachable only after Wait returned nil.
	//   - the error MUST NOT be mistakable for a queue-full shed. A shed is unwound
	//     by the caller (record deleted, inputs unspent); doing that to a
	//     transaction still in flight could delete a record already absorbed
	//     downstream. A ServiceError keeps it out of the ErrThresholdExceeded
	//     branch, so no caller unwinds a transaction that may still be in flight.
	if waitErr := group.Wait(ctx, batchHandoffTimeout); waitErr != nil {
		return nil, errors.NewServiceError("validator batch handoff abandoned before dispatch completed", waitErr)
	}

	r := item.result

	if r.err != nil {
		c.logger.Errorf("[ValidateWithOptions] failed to validate batched transaction: %v", r.err)
		return nil, r.err
	}

	result := &utxometa.Data{}

	if err = utxometa.NewMetaDataFromBytes(r.metaData, result); err != nil {
		c.logger.Errorf("[ValidateWithOptions] failed to parse metadata: %v", err)
		return nil, err
	}

	return result, nil
}

// handleValidationError processes validation errors and attempts HTTP fallback
// when the gRPC call failed because the message was too large. A successful
// fallback returns nil; a failed fallback returns the HTTP verdict, not the
// original ResourceExhausted error.
func (c *Client) handleValidationError(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options, err error) error {
	// Only an oversized gRPC message is fixable by re-sending over HTTP. A
	// block-assembly queue-full shed arrives with the same ResourceExhausted code
	// (via ERR_THRESHOLD_EXCEEDED) but must be surfaced to the caller instead: the
	// node has just reported itself saturated, and re-sending would drive a second
	// full validation against it. A non-status error also lands here, preserving the
	// original "not a status error → unwrap and return" behaviour.
	if !errors.IsGRPCMessageTooLarge(err) || c.validatorHTTPAddr == nil {
		if errors.Is(errors.UnwrapGRPC(err), errors.ErrThresholdExceeded) {
			c.logger.Warnf("[ValidateWithOptions][%s] block assembly shed the transaction (queue full); not retrying over HTTP", tx.TxID())
		}

		return errors.UnwrapGRPC(err)
	}

	// Try HTTP fallback. The gate above guarantees this really is a size problem,
	// so the message wording is now accurate rather than assumed.
	c.logger.Warnf("[ValidateWithOptions][%s] Transaction exceeds gRPC message limit, falling back to validator /tx endpoint: %s",
		tx.TxID(), status.Convert(err).Message())

	httpErr := c.validateTransactionViaHTTP(ctx, tx, blockHeight, validationOptions)
	if httpErr == nil {
		// HTTP validation succeeded, but we don't have metadata
		c.logger.Debugf("[ValidateWithOptions][%s] Successfully validated via HTTP fallback", tx.TxID())
		return nil
	}

	c.logger.Errorf("[ValidateWithOptions][%s] HTTP fallback also failed: %v", tx.TxID(), httpErr)

	// The HTTP response is the actual verdict (or the old wrapping, when the
	// header is absent). Returning the original ResourceExhausted would tell
	// the caller the transaction was too large for gRPC, which is no longer
	// the failure — it was submitted, and rejected.
	return httpErr
}

// sendBatchToValidator sends a batch of transactions to the validator via gRPC.
// If the batch exceeds the gRPC message size limit, it retries each transaction
// individually — over gRPC first, since the batch is usually only oversized in
// aggregate — see retryBatchItemsIndividually.
func (c *Client) sendBatchToValidator(ctx context.Context, batch []*batchItem) {
	// go-batcher recovers panics raised in this dispatch fn; without a sweep a
	// panic part-way through would leave every submitter blocked on group.Wait
	// until its caller context or batchHandoffTimeout released it. complete is
	// CAS-guarded, so re-completing an item an earlier stage already completed is
	// a no-op.
	defer func() {
		util.SignalBatchPanic(recover(), batch, "sendBatchToValidator", c.logger, func(it *batchItem, err error) {
			it.complete(validateBatchResponse{err: err})
		})
	}()

	// Prepare batch request
	requests := make([]*validator_api.ValidateTransactionRequest, 0, len(batch))
	for _, item := range batch {
		requests = append(requests, item.req)
	}

	txBatch := &validator_api.ValidateTransactionBatchRequest{
		Transactions: requests,
	}

	// Try gRPC validation first
	resp, err := c.client.ValidateTransactionBatch(ctx, txBatch)
	if err != nil {
		c.logger.Errorf("Failed to validate transaction batch: %v", err)

		// Check if the error is related to message size (ResourceExhausted).
		//
		// The retry below starts on unary gRPC and needs no HTTP address, so the
		// predicate does not ask for one: an aggregate-oversized batch is retried
		// item by item whether or not validator_httpAddress is configured. Only an
		// item that is itself too large for gRPC reaches the HTTP send, and
		// handleValidationError gates that on the address separately.
		if c.shouldAttemptHTTPFallback(err) {
			c.retryBatchItemsIndividually(ctx, batch)
			return
		}

		// For any other error, notify all transactions with the unwrapped error
		c.notifyAllBatchItems(batch, nil, errors.UnwrapGRPC(err))

		return
	}

	// Process successful responses
	c.processBatchResponse(batch, resp)
}

// shouldAttemptHTTPFallback determines whether an oversized-batch retry should be
// attempted based on the error. Kept as the named seam its call site reads through;
// it is a one-line wrapper over the shared predicate so a batch-level queue-full
// shed cannot be mistaken for an oversized message and amplified into one full
// re-validation per transaction in the batch against a saturated node.
//
// It says nothing about HTTP reachability. The retry it opens runs over unary
// gRPC, and whether an individual item may then be sent over HTTP is
// handleValidationError's decision, gated there on a configured address.
func (c *Client) shouldAttemptHTTPFallback(err error) bool {
	return errors.IsGRPCMessageTooLarge(err)
}

// retryBatchItemsIndividually re-sends every item of a batch the validator
// rejected as too large, one request at a time.
//
// The retry goes over UNARY gRPC first, not straight to HTTP. A batch exceeds the
// gRPC message limit in AGGREGATE far more often than any single transaction in it
// does, and gRPC is the transport that carries every validation option. Going
// straight to HTTP would not downgrade such an item, it would refuse it outright —
// the HTTP surface carries transaction bytes only (issue 4840) — so a
// block-validation or legacy-sync item, which travels with SkipPolicyChecks,
// InBlock and the candidate times, would hard-fail even though it fits comfortably
// in a request of its own.
//
// HTTP stays the fallback for an item that is itself too large for gRPC.
// handleValidationError makes that decision per item on exactly the same terms as
// the unary path: only a message-size error opens the fallback, a block-assembly
// queue-full shed is surfaced to the caller instead of being re-sent against a node
// that just reported itself saturated, and any other failure is returned unwrapped.
// A shed also stops the loop, not just the item that saw it: the remaining items
// complete with the same error without being sent, as does a cancelled context.
// A per-item verdict is different — it says nothing about the node's health, so the
// loop carries on to the next item.
//
// Sequential by design, but NOT for ordering. The batch path this replaces does not
// order a parent before its child either: ValidateTransactionBatch runs every item
// in its own errgroup goroutine, so a parent and child in one batch were already
// validated concurrently. Nothing downstream may rely on that path, or on this loop,
// to sequence them.
//
// The loop stays sequential for the reasons that do hold: it bounds the load placed
// on a validator that has just rejected an oversized batch, and issuing one request
// at a time is what makes the shed and cancellation abort above meaningful — a
// concurrent fan-out would already have sent the remaining items before the first
// failure came back.
func (c *Client) retryBatchItemsIndividually(ctx context.Context, batch []*batchItem) {
	c.logger.Warnf("Batch exceeds the gRPC message limit, retrying its %d transactions individually", len(batch))

	for i, item := range batch {
		txReq := item.req

		// The typed transport first. Each item carries its full option set here,
		// so a successful retry is byte-for-byte the request the batch would have
		// delivered — and it returns real metadata, which the HTTP route cannot.
		response, err := c.client.ValidateTransaction(ctx, txReq)
		if err == nil {
			item.complete(validateBatchResponse{metaData: response.Metadata})
			continue
		}

		// Only now are the transaction and its options needed: to decide whether
		// this single item is itself oversized, and to re-send it if so.
		tx, parseErr := bt.NewTxFromBytes(txReq.TransactionData)
		if parseErr != nil {
			item.complete(validateBatchResponse{
				metaData: nil,
				err:      errors.NewServiceError("Failed to parse transaction for individual retry: %v", parseErr),
			})

			continue
		}

		// Reuses the same projection the server uses. The request was built by
		// this client via buildValidateTxRequest, so optionsFromValidateRequest
		// cannot fail here in practice (every field is well-formed by
		// construction). The error is still propagated to surface any future bug
		// in the client-side builder.
		options, optErr := optionsFromValidateRequest(txReq)
		if optErr != nil {
			c.logger.Errorf("[%s] individual retry rejected: client-built request failed projection: %v", tx.TxID(), optErr)
			item.complete(validateBatchResponse{metaData: nil, err: optErr})

			continue
		}

		if retryErr := c.handleValidationError(ctx, tx, txReq.BlockHeight, options, err); retryErr != nil {
			c.logger.Errorf("[%s] individual retry failed: %v", tx.TxID(), retryErr)
			item.complete(validateBatchResponse{metaData: nil, err: retryErr})

			// Stop the loop, do not just skip this item. A shed means the node has
			// reported itself saturated. A cancelled context here means the CLIENT is
			// shutting down, not that this item's submitter left: the ctx on this path
			// is the client-lifetime one NewClient closes over when it builds the
			// sendBatch dispatch function, while a submitter's own context only ever
			// reaches the batcher through PutCtx. Either way every remaining item would
			// be a full validation that cannot succeed, which is what routing through
			// handleValidationError exists to avoid — enforced here for the loop, not
			// only per item.
			if errors.Is(retryErr, errors.ErrThresholdExceeded) || ctx.Err() != nil {
				c.notifyAllBatchItems(batch[i+1:], nil, retryErr)
				return
			}

			continue
		}

		// handleValidationError returning nil means the HTTP fallback carried it.
		// That route returns no metadata.
		c.logger.Debugf("[%s] validated via the HTTP fallback after an individual retry", tx.TxID())
		item.complete(validateBatchResponse{metaData: nil, err: nil})
	}
}

// processBatchResponse handles successful batch responses
func (c *Client) processBatchResponse(batch []*batchItem, resp *validator_api.ValidateTransactionBatchResponse) {
	// The server must return one Errors + Metadata entry per batch item. If it
	// returns fewer (a contract violation), indexing resp.Errors[i]/Metadata[i]
	// would panic; the sweep would still release callers, but with an opaque
	// "panic in sendBatchToValidator" error. Guard the lengths and fail every
	// item with a clear error instead, mirroring the propagation client.
	if len(resp.Errors) != len(batch) || len(resp.Metadata) != len(batch) {
		err := errors.NewProcessingError("[processBatchResponse] validator returned %d errors / %d metadata for a batch of %d", len(resp.Errors), len(resp.Metadata), len(batch))
		for _, item := range batch {
			item.complete(validateBatchResponse{err: err})
		}

		return
	}

	for i, item := range batch {
		if !resp.Errors[i].IsNil() {
			item.complete(validateBatchResponse{metaData: nil, err: resp.Errors[i]})
		} else {
			item.complete(validateBatchResponse{metaData: resp.Metadata[i], err: nil})
		}
	}
}

// notifyAllBatchItems notifies all items in a batch with the same response
func (c *Client) notifyAllBatchItems(batch []*batchItem, metadata []byte, err error) {
	for _, item := range batch {
		item.complete(validateBatchResponse{metaData: metadata, err: err})
	}
}

// maxHTTPFallbackErrorBodyBytes bounds the raw prefix retained for diagnostics.
const maxHTTPFallbackErrorBodyBytes = 2 * 1024

// readHTTPFallbackErrorBody leaves body ownership with the caller. One extra
// byte distinguishes a complete prefix from a truncated response.
func readHTTPFallbackErrorBody(body io.Reader) ([]byte, bool) {
	bodyBytes, _ := io.ReadAll(io.LimitReader(body, maxHTTPFallbackErrorBodyBytes+1))
	if len(bodyBytes) > maxHTTPFallbackErrorBodyBytes {
		return bodyBytes[:maxHTTPFallbackErrorBodyBytes], true
	}

	return bodyBytes, false
}

// validateTransactionViaHTTP sends a transaction to the validator's HTTP endpoint
// This is used as a fallback when gRPC message size limits are exceeded.
//
// The request body is a serialised ValidateTransactionRequest with
// Content-Type: application/x-protobuf — the same proto definition as gRPC.
// Using the proto-body shape avoids URL/query-string length limits in proxies
// and load balancers and guarantees field parity with gRPC by construction:
// anything we add to the proto reaches the server here too, with no
// scalar-query-string drift.
//
// The legacy application/octet-stream path remains supported by the server's
// /tx handler for backward compatibility with non-protobuf callers; this
// client no longer uses it.
//
// The endpoint carries transaction bytes only: it is unauthenticated, so it
// accepts no validation options and no caller-asserted block height. A request
// that needs either is refused here, before transmission, rather than sent and
// rejected — see nonDefaultValidationOptions.
func (c *Client) validateTransactionViaHTTP(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) error {
	if c.validatorHTTPAddr == nil {
		return errors.NewServiceError("[ValidateWithOptions][%s] Transaction exceeds gRPC message limit, but no HTTP endpoint configured for validator", tx.TxID())
	}

	// Create an HTTP client with a timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Prepare request to validator /tx endpoint
	endpoint, err := url.Parse("/tx")
	if err != nil {
		return errors.NewServiceError("[ValidateWithOptions][%s] error parsing endpoint /tx: %v", tx.TxID(), err)
	}

	fullURL := c.validatorHTTPAddr.ResolveReference(endpoint)

	// Bind the built request so the guard below and the marshal share one value.
	// Named vreq, not req: `req` is already taken further down by the *http.Request
	// from http.NewRequestWithContext, and the two must not be confused — the guard
	// must run on the protobuf request, and it must run BEFORE the HTTP request is
	// constructed so nothing is sent.
	vreq := buildValidateTxRequest(tx.SerializeBytes(), blockHeight, validationOptions)

	// The validator's HTTP endpoint carries transaction bytes only, so refuse here
	// rather than send options that the far end will reject. Doing it client-side
	// stops THIS client being a route for those flags whatever version the peer
	// runs; it does not make the property hold generally. The guarantee against an
	// arbitrary HTTP caller lands when the SERVER is upgraded, because an
	// un-upgraded validator still parses the old query string for anyone who asks.
	// Issue 4840, finding B-022.
	if reason := nonDefaultValidationOptions(vreq); reason != "" {
		return errors.NewServiceError(
			"[ValidateWithOptions][%s] transaction exceeds the gRPC message limit and %s cannot be sent "+
				"over the HTTP fallback; the validator gRPC message size is fixed at 1 GiB", tx.TxID(), reason)
	}

	// Marshal the full request via the shared builder — same proto, same field
	// projection as gRPC.
	body, err := proto.Marshal(vreq)
	if err != nil {
		return errors.NewServiceError("[ValidateWithOptions][%s] error marshalling protobuf body for /tx endpoint: %v", tx.TxID(), err)
	}

	// Create the HTTP request with the protobuf body
	req, err := http.NewRequestWithContext(ctx, "POST", fullURL.String(), bytes.NewReader(body))
	if err != nil {
		return errors.NewServiceError("[ValidateWithOptions][%s] error creating request to validator /tx endpoint: %v", tx.TxID(), err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		return errors.NewServiceError("[ValidateWithOptions][%s] error sending transaction to validator /tx endpoint: %v", tx.TxID(), err)
	}
	defer resp.Body.Close()

	// Check response status.
	//
	// Same contract as propagation's fallback: prefer the verdict the server
	// attached as a header over wrapping the whole response as a SERVICE_ERROR.
	// Without it a rejection reaches an RPC client through
	// services/rpc/handlers.go as an opaque service failure carrying the
	// validator's internal error chain verbatim, and callers that classify on the
	// error code read a permanent rejection as something worth retrying.
	if resp.StatusCode != http.StatusOK {
		verdict := errors.HTTPErrorFrom(resp.Header)

		// The body is an opaque diagnostic: keep a bounded prefix so a large error
		// response is never buffered or formatted in full, and quote it so control
		// bytes cannot forge extra log lines.
		body, truncated := readHTTPFallbackErrorBody(resp.Body)

		diagnostic := fmt.Sprintf("%q", string(body))
		if truncated {
			diagnostic += " (truncated)"
		}

		if verdict == nil {
			return errors.NewServiceError("[ValidateWithOptions][%s] validator /tx endpoint returned non-OK status: %d, body: %s",
				tx.TxID(), resp.StatusCode, diagnostic)
		}

		c.logger.Warnf("[ValidateWithOptions][%s] validator /tx endpoint rejected transaction: status=%d body=%s",
			tx.TxID(), resp.StatusCode, diagnostic)

		return errors.New(verdict.Code(), "[ValidateWithOptions][%s] %s", tx.TxID(), verdict.Message())
	}

	c.logger.Debugf("[ValidateWithOptions][%s] successfully validated using validator /tx endpoint", tx.TxID())

	return nil
}
